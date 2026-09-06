"""WS spawn-by-profile: a bare `prompt` may carry an `agent` profile name
(issue #7, design option 1).

When a `prompt` has NO `agentId` and its `agent` value does NOT name an
existing node, the hive spawns a fresh primary running that profile instead of
`default_primary`. Validation mirrors `start_primary` / `/api/primary/spawn`:
the profile must exist and be primary-eligible, otherwise the command fails
with a clear error and NO node is created. Default behaviour (no `agent`) is
unchanged, `cwd` is honoured at spawn, and `agentId`-addressed prompts are
untouched.
"""

from __future__ import annotations

import uuid
from typing import Any, Dict, List, Optional

from hive.agent_graph import AgentGraph
from hive.config import HiveConfig
from hive.models import AgentNode, AgentProfile
from hive.server import ApiContext, EventBroadcaster, _handle_command

PRIMARY = "prim-0000"
EXISTING = "node-existing"


def build_config() -> HiveConfig:
    """primary (default, eligible), coder (eligible), tester (NOT eligible)."""
    return HiveConfig.model_validate({
        "server": {"bind": "127.0.0.1", "guiPort": 3100, "apiPort": 3101},
        "agents": [
            {"name": "primary", "model": "m-primary", "allow_as_primary": True},
            {"name": "coder", "model": "m-coder", "allow_as_primary": True},
            {"name": "tester", "model": "m-tester"},  # not flagged -> ineligible
        ],
        "default_primary": "primary",
    }).validate()


def _node(nid: str, kind: str, profile_name: str, parent: Optional[str] = None) -> AgentNode:
    return AgentNode(
        id=nid,
        kind=kind,  # type: ignore[arg-type]
        name=nid,
        parentId=parent,
        status="idle",
        profile=AgentProfile(name=profile_name, model="m-" + profile_name),
        cwd="/tmp",
        sessionFile=f"/tmp/{nid}.jsonl",
        createdAt=1700000000000 + sum(ord(c) for c in nid),
    )


class RecordingPM:
    """Duck-typed ProcessManager recording outbound RPC commands."""

    def __init__(self) -> None:
        self.send_calls: List[Dict[str, Any]] = []

    def get(self, node_id: str) -> Optional[object]:
        return None

    def is_streaming(self, agent_id: str) -> bool:
        return False

    async def send_command(self, agent_id: str, cmd: Dict[str, Any]) -> None:
        self.send_calls.append({"agent": agent_id, "cmd": cmd})


def _make_spawn_primary(graph: AgentGraph, cfg: HiveConfig,
                        captured: Dict[str, Any]):
    """Fake `spawn_primary` mirroring Hive.start_primary's profile validation.

    Raises BEFORE adding a node when the profile is unknown or not
    primary-eligible, so a bad name leaves the tree untouched — the same
    ordering the real `start_primary` uses.
    """

    async def spawn_primary(label=None, model=None, cwd=None, agent=None):
        captured.update(label=label, model=model, cwd=cwd, agent=agent)
        if agent is not None:
            profile = cfg.profile_by_name(agent)
            if profile is None:
                raise ValueError(
                    f"cannot spawn primary: unknown agent {agent!r}; "
                    f"known names: {sorted(cfg.known_names())}")
            if not cfg.is_primary_eligible(agent):
                raise ValueError(
                    f"cannot spawn primary: agent {agent!r} is not "
                    f"primary-eligible (allow_as_primary is not True, while "
                    f"other agents are flagged)")
        else:
            profile = cfg.default_primary_profile()
        node = AgentNode(
            id=uuid.uuid4().hex,
            kind="primary",
            name=agent or profile.name,
            parentId=None,
            status="idle",
            profile=profile,
            cwd=cwd or "/tmp",
            sessionFile="",
            createdAt=1700000000000,
        )
        graph.add_node(node)
        return node

    return spawn_primary


def _ctx(graph: AgentGraph, pm: RecordingPM, spawn) -> ApiContext:
    ctx = ApiContext(graph=graph, processes=pm, config=build_config(),
                     broadcaster=EventBroadcaster())
    ctx.spawn_primary = spawn
    return ctx


def _tree_with_existing() -> AgentGraph:
    """A graph with one existing primary and one subagent node."""
    return AgentGraph(nodes=[
        _node(PRIMARY, "primary", "primary"),
        _node(EXISTING, "subagent", "primary", parent=PRIMARY),
    ])


# ---------------------------------------------------------------------------
# spawn-by-profile over the WS/prompt path
# ---------------------------------------------------------------------------
async def _test_spawn_by_eligible_profile() -> None:
    graph = _tree_with_existing()
    pm = RecordingPM()
    cfg = build_config()
    captured: Dict[str, Any] = {}
    ctx = _ctx(graph, pm, _make_spawn_primary(graph, cfg, captured))
    before = len(graph.get_tree())

    resp = await _handle_command(
        ctx, "prompt", {"type": "prompt", "text": "code it up", "agent": "coder"}
    )

    assert resp["success"] is True, resp
    assert captured["agent"] == "coder"
    # A new node was created and the prompt went to IT, not an existing node.
    assert len(graph.get_tree()) == before + 1
    new_ids = [n.id for n in graph.get_tree()]
    new_id = [i for i in new_ids if i not in (PRIMARY, EXISTING)][0]
    assert pm.send_calls and pm.send_calls[0]["agent"] == new_id
    assert pm.send_calls[0]["cmd"]["message"] == "code it up"
    # The new primary carries the coder profile.
    node = next(n for n in graph.get_tree() if n.id == new_id)
    assert node.profile.name == "coder"


async def _test_unknown_profile_rejected_no_node() -> None:
    graph = _tree_with_existing()
    pm = RecordingPM()
    cfg = build_config()
    captured: Dict[str, Any] = {}
    ctx = _ctx(graph, pm, _make_spawn_primary(graph, cfg, captured))
    before = len(graph.get_tree())

    resp = await _handle_command(
        ctx, "prompt", {"type": "prompt", "text": "hi", "agent": "no-such"}
    )

    assert resp["success"] is False, resp
    assert "unknown agent" in resp["error"]
    assert len(graph.get_tree()) == before  # no node created
    assert pm.send_calls == []  # no prompt delivered


async def _test_ineligible_profile_rejected_no_node() -> None:
    graph = _tree_with_existing()
    pm = RecordingPM()
    cfg = build_config()
    captured: Dict[str, Any] = {}
    ctx = _ctx(graph, pm, _make_spawn_primary(graph, cfg, captured))
    before = len(graph.get_tree())

    # `tester` exists but is not primary-eligible (other agents are flagged).
    resp = await _handle_command(
        ctx, "prompt", {"type": "prompt", "text": "hi", "agent": "tester"}
    )

    assert resp["success"] is False, resp
    assert "not primary-eligible" in resp["error"]
    assert len(graph.get_tree()) == before
    assert pm.send_calls == []


async def _test_default_fallback_when_no_agent() -> None:
    graph = _tree_with_existing()
    pm = RecordingPM()
    cfg = build_config()
    captured: Dict[str, Any] = {}
    ctx = _ctx(graph, pm, _make_spawn_primary(graph, cfg, captured))

    resp = await _handle_command(
        ctx, "prompt", {"type": "prompt", "text": "bare"}
    )

    assert resp["success"] is True, resp
    assert captured.get("agent") is None  # fell back to default_primary
    # A new default primary was created and used.
    new_ids = [n.id for n in graph.get_tree() if n.id not in (PRIMARY, EXISTING)]
    assert len(new_ids) == 1
    assert new_ids[0] not in (PRIMARY, EXISTING)
    default_node = next(n for n in graph.get_tree() if n.id == new_ids[0])
    assert default_node.profile.name == "primary"


async def _test_cwd_honoured_on_profile_spawn() -> None:
    graph = _tree_with_existing()
    pm = RecordingPM()
    cfg = build_config()
    captured: Dict[str, Any] = {}
    ctx = _ctx(graph, pm, _make_spawn_primary(graph, cfg, captured))

    resp = await _handle_command(
        ctx, "prompt",
        {"type": "prompt", "text": "x", "agent": "coder", "cwd": "/work/dir"}
    )

    assert resp["success"] is True, resp
    assert captured.get("cwd") == "/work/dir"
    new_id = [n.id for n in graph.get_tree()
              if n.id not in (PRIMARY, EXISTING)][0]
    assert next(n for n in graph.get_tree() if n.id == new_id).cwd == "/work/dir"


async def _test_agentid_addressed_prompt_unchanged() -> None:
    """A prompt carrying an explicit agentId targets that node; no spawn."""
    graph = _tree_with_existing()
    pm = RecordingPM()
    cfg = build_config()
    captured: Dict[str, Any] = {}
    ctx = _ctx(graph, pm, _make_spawn_primary(graph, cfg, captured))
    before = len(graph.get_tree())

    resp = await _handle_command(
        ctx, "prompt",
        {"type": "prompt", "agentId": EXISTING, "text": "continue", "agent": "coder"}
    )

    assert resp["success"] is True, resp
    # agentId wins: no new node, no profile spawn, prompt went to EXISTING.
    assert len(graph.get_tree()) == before
    assert captured == {}  # spawn_primary never called
    assert pm.send_calls and pm.send_calls[0]["agent"] == EXISTING


async def _test_agent_matching_existing_node_targets_it() -> None:
    """When `agent` names an EXISTING node id (the M2 alias), target it; do
    NOT treat it as a profile to spawn."""
    graph = _tree_with_existing()
    pm = RecordingPM()
    cfg = build_config()
    captured: Dict[str, Any] = {}
    ctx = _ctx(graph, pm, _make_spawn_primary(graph, cfg, captured))
    before = len(graph.get_tree())

    resp = await _handle_command(
        ctx, "prompt",
        {"type": "prompt", "text": "keep going", "agent": EXISTING}
    )

    assert resp["success"] is True, resp
    assert len(graph.get_tree()) == before  # no new node
    assert captured == {}
    assert pm.send_calls and pm.send_calls[0]["agent"] == EXISTING


# ---------------------------------------------------------------------------
# pytest entry points
# ---------------------------------------------------------------------------
def test_spawn_by_eligible_profile() -> None:
    import asyncio
    asyncio.run(_test_spawn_by_eligible_profile())


def test_unknown_profile_rejected_no_node() -> None:
    import asyncio
    asyncio.run(_test_unknown_profile_rejected_no_node())


def test_ineligible_profile_rejected_no_node() -> None:
    import asyncio
    asyncio.run(_test_ineligible_profile_rejected_no_node())


def test_default_fallback_when_no_agent() -> None:
    import asyncio
    asyncio.run(_test_default_fallback_when_no_agent())


def test_cwd_honoured_on_profile_spawn() -> None:
    import asyncio
    asyncio.run(_test_cwd_honoured_on_profile_spawn())


def test_agentid_addressed_prompt_unchanged() -> None:
    import asyncio
    asyncio.run(_test_agentid_addressed_prompt_unchanged())


def test_agent_matching_existing_node_targets_it() -> None:
    import asyncio
    asyncio.run(_test_agent_matching_existing_node_targets_it())
