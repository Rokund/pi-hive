"""Generic agent-settle long-poll: ProcessManager.wait_for_agent +
POST /hive/agent/wait (issue #5).

Covers the acceptance criteria:

  * immediate return for terminal nodes (done/failed/aborted) and for
    unloaded / reaped nodes (idle primary, no live state) — never wakes
    or materializes the agent;
  * early return when the agent settles well before ``wait_time`` (elapsed
    measured, asserted far below the bound — no full sleep);
  * timeout while running returns ``{ok, status:"running", progress}`` via
    the existing anti-stall ``_progress`` signals;
  * unknown id rejected with a clear error;
  * works for a PRIMARY node, not just subagents: a settled primary is
    reported with the graph node's ``idle`` status (primaries settle to
    ``idle``, subagents to ``done``).
"""

from __future__ import annotations

import asyncio
import time
from typing import Dict, Optional

from fastapi.testclient import TestClient

from hive.agent_graph import AgentGraph
from hive.config import HiveConfig
from hive.models import AgentNode, AgentProfile
from hive.process_manager import AgentRuntime, ProcessManager, _AgentState
from hive.server import ApiContext, EventBroadcaster, create_api_app

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------
PRIMARY = "prim-0000"
CHILD = "child-one"


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
    return AgentGraph(nodes=[
        _node(PRIMARY, "primary"),
        _node(CHILD, "subagent", parent=PRIMARY),
    ])


class WaitPM(ProcessManager):
    """Real ProcessManager with the process layer faked out.

    States are injected into ``_runtimes`` directly so the REAL
    ``wait_for_terminal`` / ``_progress`` / ``_result_payload`` machinery is
    exercised — only the pi process itself is absent.
    """

    def __init__(self, graph: AgentGraph) -> None:
        super().__init__(on_event=lambda *a, **k: None,
                         config=build_config(), graph=graph)

    def get(self, node_id: str) -> Optional[object]:
        # No live pi process for any synthetic agent.
        return None


def _attach(pm: WaitPM, node_id: str,
            status: str = "running", final_text: str = "") -> _AgentState:
    st = _AgentState()
    st.status = status
    st.finalText = final_text
    if status in ("done", "failed", "aborted"):
        st.finishedAt = 1700000000000
        st._settled.set()
    pm._runtimes[node_id] = AgentRuntime(proc=None, state=st)
    return st


# ---------------------------------------------------------------------------
# terminal + not-loaded -> immediate return
# ---------------------------------------------------------------------------
async def _test_terminal_subagent_returns_immediately() -> None:
    graph = _tree()
    graph.update_node(CHILD, status="done")
    pm = WaitPM(graph)
    _attach(pm, CHILD, status="done", final_text="the answer")

    t0 = time.monotonic()
    res = await pm.wait_for_agent(CHILD, wait_time_ms=10000)
    elapsed = time.monotonic() - t0

    assert elapsed < 1.0  # returned immediately, did not wait 10s
    assert res["ok"] is True
    assert res["id"] == CHILD
    assert res["status"] == "done"
    assert res["result"]["finalText"] == "the answer"


async def _test_unloaded_primary_returns_node_status_immediately() -> None:
    """A known node with NO live state (reaped / never streamed in this hive
    process): report the current node status at once, never wake."""
    graph = _tree()  # PRIMARY is "idle", no runtime attached
    pm = WaitPM(graph)

    t0 = time.monotonic()
    res = await pm.wait_for_agent(PRIMARY, wait_time_ms=5000)
    elapsed = time.monotonic() - t0

    assert elapsed < 1.0
    assert res == {"ok": True, "id": PRIMARY, "status": "idle"}


async def _test_settled_primary_reports_idle_not_done() -> None:
    """Primaries settle to ``idle``: the response carries the graph node's
    status, mirroring get_agent_glimpse."""
    graph = _tree()
    graph.update_node(PRIMARY, status="idle")
    pm = WaitPM(graph)
    _attach(pm, PRIMARY, status="done", final_text="final text")

    res = await pm.wait_for_agent(PRIMARY, wait_time_ms=0)

    assert res["ok"] is True
    assert res["status"] == "idle"  # node status, not the state's "done"
    assert res["result"]["finalText"] == "final text"


async def _test_unknown_id_rejected() -> None:
    graph = _tree()
    pm = WaitPM(graph)

    res = await pm.wait_for_agent("ghost", wait_time_ms=0)

    assert res["ok"] is False
    assert "unknown agent id" in res["error"]


# ---------------------------------------------------------------------------
# running -> wait (early return / timeout)
# ---------------------------------------------------------------------------
async def _test_settle_well_before_wait_returns_early() -> None:
    """Settle at ~200ms with a 10s bound: must return at the settle, not at
    the bound (early return, not a full sleep)."""
    graph = _tree()
    graph.update_node(CHILD, status="running")
    pm = WaitPM(graph)
    st = _attach(pm, CHILD, status="running")

    async def settle() -> None:
        await asyncio.sleep(0.2)
        st.status = "done"
        st.finalText = "hi there"
        st.finishedAt = 1700000000001
        st._settled.set()
        graph.update_node(CHILD, status="done")

    task = asyncio.create_task(settle())
    t0 = time.monotonic()
    res = await pm.wait_for_agent(CHILD, wait_time_ms=10000)
    elapsed = time.monotonic() - t0
    await task

    assert elapsed < 5.0, f"waited {elapsed:.2f}s; expected ~0.2s early return"
    assert res["ok"] is True
    assert res["status"] == "done"
    assert res["result"]["finalText"] == "hi there"


async def _test_timeout_while_running_returns_running_with_progress() -> None:
    graph = _tree()
    graph.update_node(CHILD, status="running")
    pm = WaitPM(graph)
    _attach(pm, CHILD, status="running")  # never settles

    t0 = time.monotonic()
    res = await pm.wait_for_agent(CHILD, wait_time_ms=300)
    elapsed = time.monotonic() - t0

    assert elapsed >= 0.25  # did wait the full bound
    assert res["ok"] is True
    assert res["id"] == CHILD
    assert res["status"] == "running"
    assert "progress" in res
    # Existing anti-stall signals, not a new protocol.
    assert "recentlyActive" in res["progress"]
    assert "streaming" in res["progress"]


async def _test_zero_wait_on_running_returns_now() -> None:
    """wait_time default (0) is a snapshot, not a wait."""
    graph = _tree()
    graph.update_node(CHILD, status="running")
    pm = WaitPM(graph)
    _attach(pm, CHILD, status="running")

    t0 = time.monotonic()
    res = await pm.wait_for_agent(CHILD)
    elapsed = time.monotonic() - t0

    assert elapsed < 1.0
    assert res["status"] == "running"
    assert "progress" in res


# ---------------------------------------------------------------------------
# route wiring: POST /hive/agent/wait
# ---------------------------------------------------------------------------
def _client(graph: AgentGraph, pm: WaitPM) -> TestClient:
    ctx = ApiContext(graph=graph, processes=pm, config=build_config(),
                     broadcaster=EventBroadcaster())
    return TestClient(create_api_app(ctx))


def test_route_terminal_subagent_returns_result() -> None:
    graph = _tree()
    graph.update_node(CHILD, status="done")
    pm = WaitPM(graph)
    _attach(pm, CHILD, status="done", final_text="route result")
    client = _client(graph, pm)

    resp = client.post("/hive/agent/wait", json={"id": CHILD})

    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    assert body["status"] == "done"
    assert body["result"]["finalText"] == "route result"


def test_route_default_wait_time_is_zero() -> None:
    graph = _tree()
    pm = WaitPM(graph)
    client = _client(graph, pm)

    # Omitted wait_time -> snapshot now; unloaded primary returns at once.
    body = client.post("/hive/agent/wait", json={"id": PRIMARY}).json()
    assert body["ok"] is True
    assert body["status"] == "idle"


def test_route_unknown_id_rejected() -> None:
    graph = _tree()
    pm = WaitPM(graph)
    client = _client(graph, pm)

    body = client.post("/hive/agent/wait", json={"id": "ghost", "wait_time": 0}).json()
    assert body["ok"] is False
    assert "unknown agent id" in body["error"]


# ---------------------------------------------------------------------------
# pytest entry points
# ---------------------------------------------------------------------------
def test_terminal_subagent_returns_immediately() -> None:
    asyncio.run(_test_terminal_subagent_returns_immediately())


def test_unloaded_primary_returns_node_status_immediately() -> None:
    asyncio.run(_test_unloaded_primary_returns_node_status_immediately())


def test_settled_primary_reports_idle_not_done() -> None:
    asyncio.run(_test_settled_primary_reports_idle_not_done())


def test_unknown_id_rejected() -> None:
    asyncio.run(_test_unknown_id_rejected())


def test_settle_well_before_wait_returns_early() -> None:
    asyncio.run(_test_settle_well_before_wait_returns_early())


def test_timeout_while_running_returns_running_with_progress() -> None:
    asyncio.run(_test_timeout_while_running_returns_running_with_progress())


def test_zero_wait_on_running_returns_now() -> None:
    asyncio.run(_test_zero_wait_on_running_returns_now())
