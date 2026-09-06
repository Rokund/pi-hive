"""WS command correlation ids: reqId echo on response frames (issue #6).

Every WS command MAY carry ``reqId`` (string or number); the response frame
echoes it verbatim. Absent ``reqId`` leaves the key OFF the frame entirely
(byte-compatible with pre-#6 clients). HTTP routes are unaffected: their
pydantic input models have no ``reqId`` field.
"""

from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional

from fastapi.testclient import TestClient

from hive.agent_graph import AgentGraph
from hive.config import HiveConfig
from hive.models import AgentNode, AgentProfile
from hive.server import ApiContext, EventBroadcaster, _handle_command, create_api_app

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------
PRIMARY = "prim-0000"


def build_config() -> HiveConfig:
    """Minimal valid config (no daemon, nothing read from disk)."""
    return HiveConfig.model_validate({
        "server": {"bind": "127.0.0.1", "guiPort": 3100, "apiPort": 3101},
        "agents": [
            {"name": "primary", "model": "m", "allow_as_primary": True},
            {"name": "tester", "model": "m"},
        ],
        "default_primary": "primary",
    }).validate()


def _node(nid: str, kind: str, parent: Optional[str] = None) -> AgentNode:
    return AgentNode(
        id=nid,
        kind=kind,  # type: ignore[arg-type]
        name=nid,
        parentId=parent,
        status="idle",
        profile=AgentProfile(name=nid, model="m"),
        cwd="/tmp",
        sessionFile=f"/tmp/{nid}.jsonl",
        createdAt=1700000000000 + sum(ord(c) for c in nid),
    )


def _tree() -> AgentGraph:
    return AgentGraph(nodes=[_node(PRIMARY, "primary")])


class SlowWhenTaggedPM:
    """Duck-typed ProcessManager; records commands and delays 'slow' ones.

    The delay makes completion order differ from send order, which is what
    the interleaving test needs.
    """

    def __init__(self) -> None:
        self.send_calls: List[Dict[str, Any]] = []

    def get(self, node_id: str) -> Optional[object]:
        return None

    def is_streaming(self, agent_id: str) -> bool:
        return False

    async def send_command(self, agent_id: str, cmd: Dict[str, Any]) -> None:
        if "slow" in str(cmd.get("message", "")):
            await asyncio.sleep(0.3)  # force completion-order inversion
        self.send_calls.append({"agent": agent_id, "cmd": cmd})


def _ctx(pm: SlowWhenTaggedPM) -> ApiContext:
    return ApiContext(
        graph=_tree(),
        processes=pm,  # type: ignore[arg-type]
        config=build_config(),
        broadcaster=EventBroadcaster(),
    )


def _cmd_payloads() -> List[Dict[str, Any]]:
    """One payload per supported WS command (reqId stripped)."""
    return [
        {"type": "prompt", "agentId": PRIMARY, "text": "do a thing"},
        {"type": "steer", "agentId": PRIMARY, "text": "focus on X"},
        {"type": "follow_up", "agentId": PRIMARY, "text": "summarize"},
        {"type": "abort", "agentId": PRIMARY},
        {"type": "get_tree"},
        {"type": "get_agent", "agent": PRIMARY},
        {"type": "subscribe"},
    ]


# ---------------------------------------------------------------------------
# echo behaviour
# ---------------------------------------------------------------------------
async def _test_string_reqid_echoed_for_every_command() -> None:
    pm = SlowWhenTaggedPM()
    ctx = _ctx(pm)
    for payload in _cmd_payloads():
        p = {**payload, "reqId": "abc-123"}
        resp = await _handle_command(ctx, payload["type"], p)
        assert resp["type"] == "response"
        assert resp["command"] == payload["type"]
        assert resp["reqId"] == "abc-123", f"{payload['type']}: reqId not echoed"
        assert resp["success"] is True


async def _test_numeric_reqid_passthrough_untouched() -> None:
    pm = SlowWhenTaggedPM()
    ctx = _ctx(pm)

    resp = await _handle_command(ctx, "get_tree", {"type": "get_tree", "reqId": 42})
    assert resp["reqId"] == 42
    assert isinstance(resp["reqId"], int)  # not coerced to a string

    resp = await _handle_command(
        ctx, "get_tree", {"type": "get_tree", "reqId": "42"}
    )
    assert resp["reqId"] == "42"
    assert isinstance(resp["reqId"], str)  # string stays a string


async def _test_absent_reqid_key_omitted_byte_compatible() -> None:
    pm = SlowWhenTaggedPM()
    ctx = _ctx(pm)
    for payload in _cmd_payloads():
        resp = await _handle_command(ctx, payload["type"], payload)
        assert "reqId" not in resp, f"{payload['type']}: reqId key leaked"
    # Exact-shape check for a bare success frame (pre-#6 byte layout).
    resp = await _handle_command(ctx, "abort", {"type": "abort", "agentId": PRIMARY})
    assert resp == {"type": "response", "command": "abort", "success": True}


async def _test_interleaved_prompts_match_distinct_reqids() -> None:
    """Two concurrent prompts, distinct reqIds, inverted completion order:
    each response must carry ITS OWN reqId, not the other's."""
    pm = SlowWhenTaggedPM()
    ctx = _ctx(pm)

    slow = {"type": "prompt", "agentId": PRIMARY, "text": "slow task", "reqId": "A"}
    fast = {"type": "prompt", "agentId": PRIMARY, "text": "fast task", "reqId": "B"}
    resp_slow, resp_fast = await asyncio.gather(
        _handle_command(ctx, "prompt", slow),
        _handle_command(ctx, "prompt", fast),
    )

    assert resp_slow["reqId"] == "A" and resp_slow["success"] is True
    assert resp_fast["reqId"] == "B" and resp_fast["success"] is True
    # Both prompts were delivered despite the inverted completion order.
    texts = [c["cmd"]["message"] for c in pm.send_calls]
    assert sorted(texts) == ["fast task", "slow task"]


async def _test_error_paths_echo_reqid() -> None:
    pm = SlowWhenTaggedPM()
    ctx = _ctx(pm)

    # Unknown agent id -> error frame still carries the reqId.
    resp = await _handle_command(
        ctx, "prompt",
        {"type": "prompt", "agentId": "ghost", "text": "hi", "reqId": "e1"},
    )
    assert resp["success"] is False
    assert "ghost" in resp["error"]
    assert resp["reqId"] == "e1"

    # Unknown command -> generic error frame still carries the reqId.
    resp = await _handle_command(ctx, "bogus", {"type": "bogus", "reqId": "e2"})
    assert resp["success"] is False
    assert resp["reqId"] == "e2"

    # Exception handler path: get_agent without an agent raises ValueError.
    resp = await _handle_command(ctx, "get_agent", {"type": "get_agent", "reqId": "e3"})
    assert resp["success"] is False
    assert resp["reqId"] == "e3"


# ---------------------------------------------------------------------------
# HTTP routes unaffected
# ---------------------------------------------------------------------------
def test_http_prompt_response_has_no_reqid() -> None:
    pm = SlowWhenTaggedPM()
    client = TestClient(create_api_app(_ctx(pm)))

    # Normal HTTP prompt: no reqId in the body -> no reqId in the frame.
    resp = client.post("/api/prompt", json={"agent": PRIMARY, "message": "hi"})
    assert resp.status_code == 200
    assert "reqId" not in resp.json()

    # Even if a caller smuggles a reqId into the HTTP body, the pydantic
    # PromptIn model has no such field, so it is dropped, not echoed.
    resp = client.post("/api/prompt", json={
        "agent": PRIMARY, "message": "hi", "reqId": "sneaky",
    })
    assert resp.status_code == 200
    assert "reqId" not in resp.json()

    # Other /api/* routes built via _handle_command behave the same.
    assert "reqId" not in client.get("/api/tree").json()
    assert "reqId" not in client.post("/api/abort", json={"agent": PRIMARY}).json()
    assert "reqId" not in client.post("/api/subscribe").json()


# ---------------------------------------------------------------------------
# pytest entry points
# ---------------------------------------------------------------------------
def test_string_reqid_echoed_for_every_command() -> None:
    asyncio.run(_test_string_reqid_echoed_for_every_command())


def test_numeric_reqid_passthrough_untouched() -> None:
    asyncio.run(_test_numeric_reqid_passthrough_untouched())


def test_absent_reqid_key_omitted_byte_compatible() -> None:
    asyncio.run(_test_absent_reqid_key_omitted_byte_compatible())


def test_interleaved_prompts_match_distinct_reqids() -> None:
    asyncio.run(_test_interleaved_prompts_match_distinct_reqids())


def test_error_paths_echo_reqid() -> None:
    asyncio.run(_test_error_paths_echo_reqid())
