"""Inter-agent Q&A (GitHub issue #4 / ADR-0001) — store, routes, delivery.

The Q&A channel is restricted to agents sharing a DIRECT parent/child edge in
the SAME conversation (no sibling, no cross-level); upward questions are
addressed by relation (the hive resolves the asker's parentId), downward
questions by explicit child id. ``agent_allowlist`` never participates in Q&A
addressing.

Covered here, per the spec:

  (a) scope — both directions accepted for direct parent/child; siblings,
      grandparent<->grandchild (cross-level), unknown ids, and a primary
      asking upward (no parent) all rejected;
  (b) exactly-once — first answer accepted, second rejected with the existing
      answer returned, non-addressee answer rejected;
  (c) retrieval — question_status by asker and by addressee, unrelated agent
      rejected, pending_questions lists only the asker's pending questions
      and drops answered ones;
  (d) delivery — a RUNNING asker gets a steer command containing the
      questionId; an idle asker gets NOTHING (no wake); an idle addressee is
      woken (prompt delivered); a running addressee gets a steer containing
      the questionId;
  (e) store pruning — answered records pruned beyond the configurable cap,
      open (pending) questions never pruned.

No daemon, no ports, no pi processes: the process manager is an in-memory
fake that records every outbound delivery.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional

from fastapi.testclient import TestClient

from hive.agent_graph import AgentGraph
from hive.config import HiveConfig
from hive.models import AgentNode, AgentProfile
from hive.qa import QuestionStore
from hive.server import ApiContext, EventBroadcaster, create_api_app

# ---------------------------------------------------------------------------
# fixtures / helpers
# ---------------------------------------------------------------------------
PRIMARY = "prim-0000"
CHILD1 = "child-one"
CHILD2 = "child-two"
GRAND = "grand-child"


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
    """prim -> (child-one -> grand-child, child-two): direct + cross levels."""
    return AgentGraph(nodes=[
        _node(PRIMARY, "primary"),
        _node(CHILD1, "subagent", parent=PRIMARY),
        _node(CHILD2, "subagent", parent=PRIMARY),
        _node(GRAND, "subagent", parent=CHILD1),
    ])


class FakeProcessManager:
    """Duck-typed ProcessManager: records every outbound delivery attempt.

    ``live`` holds the ids of agents considered to have a live process
    (``get`` returns a truthy sentinel for them, None otherwise — the exact
    liveness proxy the real ProcessManager exposes).
    """

    def __init__(self, live: Iterable[str] = ()) -> None:
        self.live = set(live)
        self.send_command_calls: List[Dict[str, Any]] = []
        self.send_calls: List[Dict[str, Any]] = []
        self.followups: List[Dict[str, Any]] = []

    def get(self, node_id: str) -> Optional[object]:
        return object() if node_id in self.live else None

    async def send_command(self, agent_id: str, cmd: Dict[str, Any]) -> None:
        self.send_command_calls.append({"agent": agent_id, "cmd": cmd})

    async def send(self, agent_id: str, cmd: Dict[str, Any], **kw: Any) -> None:
        self.send_calls.append({"agent": agent_id, "cmd": cmd})
        return None

    async def followup_subagent(self, node_id: str, prompt: str) -> Dict[str, Any]:
        self.followups.append({"agent": node_id, "prompt": prompt})
        return {"ok": True, "id": node_id}


def _client(
    graph: Optional[AgentGraph] = None,
    processes: Optional[FakeProcessManager] = None,
    qa: Optional[QuestionStore] = None,
    live: Iterable[str] = (),
):
    graph = graph if graph is not None else _tree()
    processes = processes if processes is not None else FakeProcessManager(live=live)
    ctx = ApiContext(
        graph=graph,
        processes=processes,  # type: ignore[arg-type]
        config=build_config(),
        broadcaster=EventBroadcaster(),
        qa=qa,
    )
    return TestClient(create_api_app(ctx)), ctx, processes


def _steers_for(pm: FakeProcessManager, agent: str) -> List[Dict[str, Any]]:
    return [
        e for e in pm.send_command_calls
        if e["agent"] == agent and e["cmd"].get("type") == "steer"
    ]


def _ask(client: TestClient, frm: str, question: str, to: Optional[str] = None):
    body: Dict[str, Any] = {"from": frm, "question": question}
    if to is not None:
        body["to"] = to
    return client.post("/hive/agent/ask", json=body)


def _answer(client: TestClient, frm: str, question_id: str, text: str):
    return client.post("/hive/agent/answer", json={
        "from": frm, "questionId": question_id, "text": text,
    })


def _status(client: TestClient, frm: str, question_id: str):
    return client.post("/hive/agent/question_status", json={
        "from": frm, "questionId": question_id,
    })


def _pending(client: TestClient, frm: str):
    return client.post("/hive/agent/pending_questions", json={"from": frm})


# ---------------------------------------------------------------------------
# (a) scope
# ---------------------------------------------------------------------------
def test_ask_downward_parent_to_child_succeeds():
    client, _, _ = _client()
    resp = _ask(client, PRIMARY, "What is the build status?", to=CHILD1)
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    qid = body["questionId"]
    # Opaque hive-unique id: uuid4 hex.
    assert isinstance(qid, str) and len(qid) == 32
    int(qid, 16)  # hex-parseable


def test_ask_upward_subagent_resolves_addressee_from_parent():
    client, _, _ = _client()
    resp = _ask(client, CHILD1, "Which file should I edit?")  # no `to`
    assert resp.json()["ok"] is True
    qid = resp.json()["questionId"]
    st = _status(client, CHILD1, qid).json()
    assert st["ok"] is True
    assert st["question"]["to"] == PRIMARY  # hive resolved parentId
    assert st["question"]["from"] == CHILD1


def test_ask_upward_grandchild_resolves_its_own_parent():
    client, _, _ = _client()
    resp = _ask(client, GRAND, "Upward from a nested subagent?")
    assert resp.json()["ok"] is True
    st = _status(client, GRAND, resp.json()["questionId"]).json()
    assert st["question"]["to"] == CHILD1  # its DIRECT parent, not the primary


def test_ask_sibling_rejected():
    client, _, _ = _client()
    resp = _ask(client, CHILD1, "Sibling question?", to=CHILD2)
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is False
    assert body.get("questionId") in (None, "")


def test_ask_grandparent_to_grandchild_rejected():
    client, _, _ = _client()
    resp = _ask(client, PRIMARY, "Cross-level downward?", to=GRAND)
    assert resp.json()["ok"] is False


def test_ask_grandchild_to_grandparent_rejected():
    client, _, _ = _client()
    resp = _ask(client, GRAND, "Cross-level upward?", to=PRIMARY)
    assert resp.json()["ok"] is False


def test_ask_unknown_agent_rejected():
    client, _, _ = _client()
    # Unknown asker.
    resp = _ask(client, "ghost", "Who are you?", to=CHILD1)
    assert resp.json()["ok"] is False
    # Unknown addressee.
    resp = _ask(client, PRIMARY, "Anyone there?", to="ghost")
    assert resp.json()["ok"] is False


def test_primary_asking_upward_without_parent_rejected():
    client, _, _ = _client()
    resp = _ask(client, PRIMARY, "Do I have a parent?")  # no `to`
    assert resp.json()["ok"] is False


# ---------------------------------------------------------------------------
# (b) exactly-once answers
# ---------------------------------------------------------------------------
def test_first_answer_wins_second_rejected_with_existing():
    client, _, _ = _client(live=(CHILD1,))
    qid = _ask(client, PRIMARY, "Q1", to=CHILD1).json()["questionId"]

    resp = _answer(client, CHILD1, qid, "the first answer")
    assert resp.json()["ok"] is True

    resp = _answer(client, CHILD1, qid, "a later answer")
    body = resp.json()
    assert body["ok"] is False
    assert body["error"] == "already answered"
    assert body["answer"] == "the first answer"
    assert body["questionId"] == qid


def test_answer_by_non_addressee_rejected():
    client, _, _ = _client(live=(CHILD1,))
    qid = _ask(client, PRIMARY, "For child-one only", to=CHILD1).json()["questionId"]
    # CHILD2 is a sibling of the addressee; PRIMARY is the asker: neither may answer.
    for impostor in (CHILD2, PRIMARY):
        resp = _answer(client, impostor, qid, "not mine to answer")
        assert resp.json()["ok"] is False
        assert "addressee" in resp.json()["error"]
    # The question is still pending for the real addressee afterwards.
    st = _status(client, CHILD1, qid).json()
    assert st["question"]["status"] == "pending"


def test_answer_unknown_question_id_rejected():
    client, _, _ = _client()
    resp = _answer(client, CHILD1, "f" * 32, "text")
    assert resp.json()["ok"] is False


# ---------------------------------------------------------------------------
# (c) retrieval
# ---------------------------------------------------------------------------
def test_question_status_by_asker_and_by_addressee():
    client, _, _ = _client()
    qid = _ask(client, PRIMARY, "Status check?", to=CHILD1).json()["questionId"]

    for asker_view in (PRIMARY, CHILD1):
        body = _status(client, asker_view, qid).json()
        assert body["ok"] is True
        assert body["question"]["id"] == qid
        assert body["question"]["from"] == PRIMARY
        assert body["question"]["to"] == CHILD1
        assert body["question"]["question"] == "Status check?"
        assert body["question"]["status"] == "pending"
        assert "answer" not in body["question"]
        assert body["question"]["askedAt"] > 0

    # After answering, the record carries the answer + answeredAt.
    _answer(client, CHILD1, qid, "all good")
    body = _status(client, PRIMARY, qid).json()
    assert body["question"]["status"] == "answered"
    assert body["question"]["answer"] == "all good"
    assert body["question"]["answeredAt"] >= body["question"]["askedAt"]
    # The answer is also surfaced as a top-level convenience key.
    assert body["answer"] == "all good"


def test_question_status_by_unrelated_agent_rejected():
    client, _, _ = _client()
    qid = _ask(client, PRIMARY, "Private?", to=CHILD1).json()["questionId"]
    resp = _status(client, CHILD2, qid)  # sibling: neither asker nor addressee
    assert resp.json()["ok"] is False
    resp = _status(client, CHILD2, "f" * 32)  # unknown id
    assert resp.json()["ok"] is False


def test_pending_questions_lists_only_askers_pending():
    client, _, _ = _client()
    q1 = _ask(client, PRIMARY, "Q1", to=CHILD1).json()["questionId"]
    q2 = _ask(client, PRIMARY, "Q2", to=CHILD2).json()["questionId"]
    _ask(client, CHILD1, "Upward Q", )  # asked by CHILD1, not PRIMARY

    body = _pending(client, PRIMARY).json()
    assert body["ok"] is True
    ids = [q["id"] for q in body["questions"]]
    assert ids == [q1, q2]  # only PRIMARY's own questions, still pending

    # Answering q2 removes it from the pending list; q1 remains.
    _answer(client, CHILD2, q2, "done with Q2")
    body = _pending(client, PRIMARY).json()
    assert [q["id"] for q in body["questions"]] == [q1]


# ---------------------------------------------------------------------------
# (d) delivery
# ---------------------------------------------------------------------------
def test_running_asker_gets_steer_containing_question_id():
    client, _, pm = _client(live=(PRIMARY, CHILD1))
    qid = _ask(client, PRIMARY, "Tell me the answer to everything", to=CHILD1).json()["questionId"]
    _answer(client, CHILD1, qid, "42")

    steers = _steers_for(pm, PRIMARY)
    assert len(steers) == 1
    msg = steers[0]["cmd"]["message"]
    assert qid in msg
    assert "42" in msg


def test_idle_asker_gets_nothing_no_wake():
    # Asker has NO live process: the answer must stay in the store only —
    # no steer, no follow-up, nothing that would wake the idle asker.
    client, _, pm = _client(live=(CHILD1,))  # PRIMARY idle
    qid = _ask(client, PRIMARY, "Anybody?", to=CHILD1).json()["questionId"]
    pm.send_command_calls.clear()  # drop the question-delivery steer noise
    resp = _answer(client, CHILD1, qid, "here you go")
    assert resp.json()["ok"] is True
    assert all(e["agent"] != PRIMARY for e in pm.send_command_calls)
    assert all(e["agent"] != PRIMARY for e in pm.followups)
    assert all(e["agent"] != PRIMARY for e in pm.send_calls)
    # The answer is nonetheless retrievable from the store.
    body = _status(client, PRIMARY, qid).json()
    assert body["question"]["answer"] == "here you go"


def test_idle_addressee_is_woken_with_prompt():
    client, _, pm = _client(live=(PRIMARY,))  # CHILD1 idle / not loaded
    qid = _ask(client, PRIMARY, "Wake up and answer", to=CHILD1).json()["questionId"]

    # No live process -> no steer; the wake path delivered a prompt.
    assert _steers_for(pm, CHILD1) == []
    assert all(e["agent"] != CHILD1 for e in pm.send_command_calls)
    assert len(pm.followups) == 1
    wake = pm.followups[0]
    assert wake["agent"] == CHILD1
    assert qid in wake["prompt"]
    assert "agent_answer" in wake["prompt"]
    assert "Wake up and answer" in wake["prompt"]
    assert PRIMARY in wake["prompt"]  # the asker is identified


def test_running_addressee_gets_steer_with_question_id():
    client, _, pm = _client(live=(CHILD1,))
    qid = _ask(client, PRIMARY, "While you run, please answer", to=CHILD1).json()["questionId"]

    steers = _steers_for(pm, CHILD1)
    assert len(steers) == 1
    msg = steers[0]["cmd"]["message"]
    assert qid in msg
    assert "agent_answer" in msg
    assert "While you run, please answer" in msg
    assert PRIMARY in msg  # the asker is identified
    # A live addressee is never woken via the follow-up path.
    assert pm.followups == []


# ---------------------------------------------------------------------------
# (e) store pruning (bounded retention of ANSWERED records only)
# ---------------------------------------------------------------------------
def test_store_prunes_answered_beyond_cap_never_pending():
    clock = iter(range(1_000_000_000, 1_000_100_000))  # monotonic fake clock
    store = QuestionStore(max_answered=2, now_ms=lambda: next(clock))

    q1 = store.create(frm=PRIMARY, to=CHILD1, question="q1")["id"]
    q2 = store.create(frm=PRIMARY, to=CHILD1, question="q2")["id"]
    q3 = store.create(frm=PRIMARY, to=CHILD1, question="q3")["id"]
    q_open = store.create(frm=PRIMARY, to=CHILD2, question="open")["id"]

    ok, _, err = store.answer(q1, "a1")
    assert ok and not err
    store.answer(q2, "a2")
    # Third answer exceeds the cap of 2: the OLDEST answered record (q1) is
    # pruned, the two most recent answered stay, pending never pruned.
    store.answer(q3, "a3")

    assert store.get(q1) is None
    assert store.get(q2)["answer"] == "a2"
    assert store.get(q3)["answer"] == "a3"
    assert store.get(q_open)["status"] == "pending"
    assert [q["id"] for q in store.pending_asked_by(PRIMARY)] == [q_open]


def test_store_prune_keeps_at_least_the_cap_when_idle():
    # Pruning only runs on answer; a quiet store with pending questions is
    # never touched by prune().
    store = QuestionStore(max_answered=1)
    for i in range(5):
        store.create(frm=PRIMARY, to=CHILD1, question=f"q{i}")
    assert store.prune() == 0  # nothing answered -> nothing to drop
    assert len(store.pending_asked_by(PRIMARY)) == 5


def test_store_answer_exactly_once_and_unknown_id():
    store = QuestionStore()
    rec = store.create(frm=CHILD1, to=PRIMARY, question="up?")
    ok, answered, err = store.answer(rec["id"], "yes")
    assert ok is True
    assert answered["answer"] == "yes"
    assert answered["status"] == "answered"
    assert answered["answeredAt"] >= answered["askedAt"]

    ok, existing, err = store.answer(rec["id"], "second try")
    assert ok is False
    assert err == "already answered"
    assert existing["answer"] == "yes"

    ok, none, err = store.answer("deadbeef", "x")
    assert ok is False and none is None and err
