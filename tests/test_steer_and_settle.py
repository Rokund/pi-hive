"""Truthful steer delivery + self-describing settle events.

Two driver-ergonomics fixes, both learned from the issue #4 run:

* ``steer_subagent`` used to answer ``{ok: true, status: "steered"}`` for a
  settled-but-loaded target (process present, no live turn) — pi silently
  drops that steer, so the answer was a lie and callers built real bugs on
  it (the Q&A channel shipped exactly this bug before its e2e caught it).
* ``agent_settled`` events were pi-raw: drivers had to memorize that
  subagents settle to ``done`` (terminal) while primaries settle to
  ``idle`` (a restable conversation) — the distinction lived only in the
  graph. The forwarded event now carries a ``settled`` block.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import asyncio

from hive.agent_graph import AgentGraph
from hive.config import HiveConfig
from hive.main import Hive
from hive.models import AgentNode, AgentProfile
from hive.process_manager import ProcessManager
from hive.server import EventBroadcaster

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
    return AgentGraph(nodes=[_node(PRIMARY, "primary"), _node(CHILD, "subagent", parent=PRIMARY)])


class SteeringPM(ProcessManager):
    """Real ProcessManager with the process layer faked out.

    ``live``/``streaming`` model the two decoupled liveness dimensions (a
    settled agent keeps its process until the reaper collects it), and every
    outbound RPC command is recorded instead of reaching a real pi process.
    """

    def __init__(self, graph: AgentGraph, live: List[str], streaming: List[str]) -> None:
        super().__init__(on_event=lambda *a, **k: None, config=build_config(), graph=graph)
        self.live = set(live)
        self.streaming = set(streaming)
        self.commands: List[Dict[str, Any]] = []

    def get(self, node_id: str) -> Optional[object]:
        return object() if node_id in self.live else None

    def is_streaming(self, agent_id: str) -> bool:
        return agent_id in self.streaming

    async def send_command(self, agent_id: str, rpc_command: Dict[str, Any]) -> None:
        self.commands.append({"id": agent_id, **rpc_command})


# ---------------------------------------------------------------------------
# steer truthfulness
# ---------------------------------------------------------------------------
async def _test_steer_running_target_is_delivered() -> None:
    graph = _tree()
    graph.update_node(CHILD, status="running")
    pm = SteeringPM(graph, live=[CHILD], streaming=[CHILD])

    res = await pm.steer_subagent(CHILD, "focus on the sources")

    assert res == {"ok": True, "id": CHILD, "status": "steered", "delivered": True}
    assert pm.commands == [{"id": CHILD, "type": "steer", "message": "focus on the sources"}]


async def _test_steer_streaming_target_delivered_even_when_node_status_lags() -> None:
    """The event-layer streaming flag leads the node status — either is enough."""
    graph = _tree()
    graph.update_node(CHILD, status="idle")  # lagging
    pm = SteeringPM(graph, live=[CHILD], streaming=[CHILD])

    res = await pm.steer_subagent(CHILD, "nudge")

    assert res["status"] == "steered" and res["delivered"] is True


async def _test_steer_done_but_loaded_target_is_skipped_not_lied_about() -> None:
    """The e2e bug: process present, agent settled → steer would be silently dropped."""
    graph = _tree()
    graph.update_node(CHILD, status="done")
    pm = SteeringPM(graph, live=[CHILD], streaming=[])

    res = await pm.steer_subagent(CHILD, "wrong path")

    assert res["ok"] is True
    assert res["status"] == "skipped"
    assert res["delivered"] is False
    assert "followup" in res["reason"]
    assert pm.commands == []  # nothing was sent — nothing can be silently lost


async def _test_steer_target_without_process_is_an_error() -> None:
    graph = _tree()
    graph.update_node(CHILD, status="running")  # status says running, process gone
    pm = SteeringPM(graph, live=[], streaming=[])

    res = await pm.steer_subagent(CHILD, "anyone there?")

    assert res["ok"] is False
    assert "not running" in res["error"]


# ---------------------------------------------------------------------------
# self-describing agent_settled
# ---------------------------------------------------------------------------
class RecordingBroadcaster(EventBroadcaster):
    def __init__(self) -> None:
        super().__init__()
        self.frames: List[Dict[str, Any]] = []

    async def publish(self, obj: Dict[str, Any]) -> None:
        self.frames.append(obj)


def _make_hive() -> Hive:
    # Hive.__init__ wires graph/processes/broadcaster but spawns nothing,
    # so this is safe offline (same pattern as tests/test_primary_resolution).
    return Hive(build_config(), default_cwd="/tmp")


async def _test_subagent_settle_event_carries_terminal_done() -> None:
    hive = _make_hive()
    hive.graph = _tree()
    hive.graph.update_node(CHILD, status="running")
    b = RecordingBroadcaster()
    hive.broadcaster = b

    await hive._on_event(CHILD, {"type": "agent_settled"})

    forwarded = [f for f in b.frames if f.get("type") == "hive:event"]
    assert forwarded, "agent_settled must be forwarded on the event stream"
    settled = forwarded[0]["event"]["settled"]
    assert settled == {"kind": "subagent", "status": "done", "terminal": True}
    # and the node itself settled to done
    assert hive.graph.get_node(CHILD).status == "done"


async def _test_primary_settle_event_carries_nonterminal_idle() -> None:
    hive = _make_hive()
    hive.graph = _tree()
    hive.graph.update_node(PRIMARY, status="running")
    b = RecordingBroadcaster()
    hive.broadcaster = b

    await hive._on_event(PRIMARY, {"type": "agent_settled"})

    forwarded = [f for f in b.frames if f.get("type") == "hive:event"]
    settled = forwarded[0]["event"]["settled"]
    assert settled == {"kind": "primary", "status": "idle", "terminal": False}
    assert hive.graph.get_node(PRIMARY).status == "idle"


async def _test_settle_enrichment_survives_aborted_terminal_state() -> None:
    """abort sets status before the settle arrives; the enrichment must not rewrite it."""
    hive = _make_hive()
    hive.graph = _tree()
    hive.graph.update_node(CHILD, status="aborted")
    b = RecordingBroadcaster()
    hive.broadcaster = b

    await hive._on_event(CHILD, {"type": "agent_settled"})

    forwarded = [f for f in b.frames if f.get("type") == "hive:event"]
    settled = forwarded[0]["event"]["settled"]
    assert settled == {"kind": "subagent", "status": "aborted", "terminal": True}


async def _test_settle_for_unknown_agent_forwarded_without_enrichment() -> None:
    hive = _make_hive()
    hive.graph = _tree()
    b = RecordingBroadcaster()
    hive.broadcaster = b

    await hive._on_event("ghost-agent", {"type": "agent_settled"})

    forwarded = [f for f in b.frames if f.get("type") == "hive:event"]
    assert "settled" not in forwarded[0]["event"]


# -- sync runners (repo test style: no async plugin) -----------------------
def test_steer_running_target_is_delivered() -> None:
    asyncio.run(_test_steer_running_target_is_delivered())

def test_steer_streaming_target_delivered_even_when_node_status_lags() -> None:
    asyncio.run(_test_steer_streaming_target_delivered_even_when_node_status_lags())

def test_steer_done_but_loaded_target_is_skipped_not_lied_about() -> None:
    asyncio.run(_test_steer_done_but_loaded_target_is_skipped_not_lied_about())

def test_steer_target_without_process_is_an_error() -> None:
    asyncio.run(_test_steer_target_without_process_is_an_error())

def test_subagent_settle_event_carries_terminal_done() -> None:
    asyncio.run(_test_subagent_settle_event_carries_terminal_done())

def test_primary_settle_event_carries_nonterminal_idle() -> None:
    asyncio.run(_test_primary_settle_event_carries_nonterminal_idle())

def test_settle_enrichment_survives_aborted_terminal_state() -> None:
    asyncio.run(_test_settle_enrichment_survives_aborted_terminal_state())

def test_settle_for_unknown_agent_forwarded_without_enrichment() -> None:
    asyncio.run(_test_settle_for_unknown_agent_forwarded_without_enrichment())
