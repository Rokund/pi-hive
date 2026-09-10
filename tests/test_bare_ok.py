"""Bare `{ok,...}` dialect for HTTP-only drivers (#12 / #8) + read-only Q&A
visibility (#9).

The external-driving contract is a SINGLE bare `{ok, ...}` dialect, opted in
via `Accept: application/vnd.hive.bare+json`. No header = legacy envelope
unchanged (backwards compatible; GUI / existing tests unaffected).

Also covers GET /api/agent/{id}/questions (issue #9): read-only visibility
into the questions an agent ASKED (pending first, then recently answered).
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi.testclient import TestClient

from hive.agent_graph import AgentGraph
from hive.config import HiveConfig
from hive.models import AgentNode, AgentProfile
from hive.server import ApiContext, EventBroadcaster, create_api_app

PRIMARY = "prim-0000"
BARE = {"accept": "application/vnd.hive.bare+json"}


def build_config() -> HiveConfig:
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


class FakePM:
    """Duck-typed ProcessManager for the command endpoints (record + no-op)."""

    def __init__(self) -> None:
        self.send_calls: List[Dict[str, Any]] = []

    def get(self, node_id: str) -> Optional[object]:
        return None

    def is_streaming(self, agent_id: str) -> bool:
        return False

    async def send_command(self, agent_id: str, cmd: Dict[str, Any]) -> None:
        self.send_calls.append({"agent": agent_id, "cmd": cmd})


def _ctx(pm: FakePM) -> ApiContext:
    return ApiContext(
        graph=_tree(),
        processes=pm,  # type: ignore[arg-type]
        config=build_config(),
        broadcaster=EventBroadcaster(),
    )


def _client() -> TestClient:
    return TestClient(create_api_app(_ctx(FakePM())))


# ---------------------------------------------------------------------------
# bare-ok dialect: opt-in via Accept header
# ---------------------------------------------------------------------------
def test_bare_prompt_returns_bare_ok():
    c = _client()
    r = c.post("/api/prompt", headers=BARE,
               json={"agent": PRIMARY, "message": "hi"})
    assert r.status_code == 200
    body = r.json()
    assert body.get("ok") is True
    # envelope correlation fields are dropped for HTTP
    assert "type" not in body
    assert "command" not in body
    assert "reqId" not in body


def test_bare_tree_returns_tree_at_top_level():
    c = _client()
    r = c.get("/api/tree", headers=BARE)
    assert r.status_code == 200
    body = r.json()
    assert body.get("ok") is True
    assert "tree" in body
    assert isinstance(body["tree"], list)
    assert body["tree"][0]["id"] == PRIMARY


def test_bare_get_agent_returns_node_flattened():
    c = _client()
    r = c.get(f"/api/agent/{PRIMARY}", headers=BARE)
    assert r.status_code == 200
    body = r.json()
    assert body.get("ok") is True
    assert body.get("id") == PRIMARY


def test_bare_error_preserves_ok_false_and_error():
    c = _client()
    # Hive-wiring error path -> bare error shape preserves ok:false + error
    r = c.post("/api/prompt", headers=BARE,
               json={"agent": "ghost", "message": "hi"})
    assert r.status_code == 200
    body = r.json()
    assert body.get("ok") is False
    assert body.get("error")


def test_no_header_returns_legacy_envelope_unchanged():
    c = _client()
    r = c.get("/api/tree")
    assert r.status_code == 200
    body = r.json()
    assert body.get("type") == "response"
    assert body.get("command") == "get_tree"
    assert body.get("success") is True
    assert "tree" in body.get("data", {})


# ---------------------------------------------------------------------------
# read-only Q&A visibility (#9)
# ---------------------------------------------------------------------------
def test_questions_unknown_agent():
    c = _client()
    r = c.get("/api/agent/ghost/questions")
    assert r.status_code == 200
    body = r.json()
    assert body.get("ok") is False
    assert body.get("questions") == []


def test_questions_pending_then_answered():
    # Build a context with a store we can seed directly, then exercise the
    # endpoint through TestClient against that same context.
    pm = FakePM()
    ctx = _ctx(pm)
    qa = ctx.qa
    # Two pending + one answered, from PRIMARY; one from another agent.
    qa.create(frm=PRIMARY, to="other", question="q-oldest")
    qa.create(frm="other-agent", to=PRIMARY, question="not-mine")
    qa.create(frm=PRIMARY, to="other", question="q-second")
    qid = qa.create(frm=PRIMARY, to="other", question="q-answered")["id"]
    qa.answer(qid, "the answer")

    c = TestClient(create_api_app(ctx))
    r = c.get(f"/api/agent/{PRIMARY}/questions")
    assert r.status_code == 200
    body = r.json()
    assert body.get("ok") is True
    questions = body["questions"]
    # pending first (oldest), then answered; foreign agent's question excluded
    assert [q["question"] for q in questions] == [
        "q-oldest", "q-second", "q-answered",
    ]
    assert questions[-1]["status"] == "answered"
    assert questions[-1]["answer"] == "the answer"


def test_questions_read_does_not_append_pending_questions():
    # A read must never push an agent's questions back onto its pending list
    # (Scenario 3 in #4: reading must not add duplicates).
    pm = FakePM()
    ctx = _ctx(pm)
    ctx.qa.create(frm=PRIMARY, to="other", question="only-one")
    c = TestClient(create_api_app(ctx))
    before = ctx.qa.pending_asked_by(PRIMARY)
    r = c.get(f"/api/agent/{PRIMARY}/questions")
    after = ctx.qa.pending_asked_by(PRIMARY)
    assert before == after  # unchanged by the read
    assert len(after) == 1
    assert r.json()["ok"] is True
