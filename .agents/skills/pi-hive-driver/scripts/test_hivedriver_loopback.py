"""LOOPBACK integration tests for hivedriver.py — real WebSocket end to end.

Unlike the unit tests (which stub the transport and inject frames in-process),
these run the driver against a REAL WebSocket server on 127.0.0.1 that speaks
the actual pi-hive protocol over the wire: it echoes `reqId` verbatim on every
response, answers `get_tree`, spawns a new primary node and streams its
`agent_updated` for a bare prompt, and lets the test push `message_end` /
`agent_settled` frames back through the real socket.

This proves the driver's correctness under real transport: bytes -> JSON ->
reqId-attributed ack -> spawn discovery from the event stream -> per-agent
routing -> settle gating — not just on stubbed in-memory calls.

The fake server is hosted in-process with the DEV-ONLY `websockets` package
(asyncio, background thread); the driver under test stays on
`websocket-client`, exactly as in production.

Run from the repo root:
    .venv/bin/python -m pytest -q .agents/skills/pi-hive-driver/scripts/test_hivedriver_loopback.py
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import threading
import time

import pytest

websockets = pytest.importorskip("websockets")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from test_python_client import upd, message_end, settle_event  # noqa: E402
from hivedriver import HiveDriver  # noqa: E402


# --------------------------------------------------------------------------- fake hive over a REAL socket
class LoopbackHive:
    """A protocol-faithful pi-hive fake served over a real WebSocket.

    * Answers `get_tree` with the current primary tree.
    * A bare `prompt` spawns a NEW primary id (p1, p2, ...), immediately acks
      with the echoed `reqId`, then streams that node's `agent_updated`.
    * A targeted `prompt` (agentId) just acks.
    * `send(frame)` lets the test push any frame back to the connected driver
      over the real socket (e.g. `message_end`, `agent_settled`), so event
      timing is controlled while the transport stays real.
    """

    def __init__(self):
        self.primaries: dict[str, dict] = {}
        self.received: list[dict] = []
        self.seq = 0
        self._lock = threading.Lock()
        self._listening = threading.Event()
        self._connected = threading.Event()
        self.port = None
        self._loop = None
        self._ws = None
        threading.Thread(target=self._serve, daemon=True).start()
        assert self._listening.wait(8.0), "loopback hive failed to start"

    # ------------------------------------------------------------------ server
    def _serve(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._main())
        except Exception:  # pragma: no cover
            raise

    async def _main(self) -> None:
        async def handler(ws):
            self._loop = asyncio.get_running_loop()
            self._ws = ws
            self._connected.set()
            try:
                async for raw in ws:
                    try:
                        frame = json.loads(raw)
                    except Exception:
                        continue
                    await self._dispatch(ws, frame)
            except websockets.ConnectionClosed:
                pass

        async with websockets.serve(handler, "127.0.0.1", 0) as server:
            self.port = server.sockets[0].getsockname()[1]
            self._listening.set()
            await asyncio.get_running_loop().create_future()  # serve forever

    # --------------------------------------------------------------- protocol
    async def _dispatch(self, ws, frame) -> None:
        ctype = frame.get("type")
        rq = frame.get("reqId")
        with self._lock:
            self.received.append(frame)
        if ctype == "get_tree":
            with self._lock:
                tree = [dict(n) for n in self.primaries.values()]
            await ws.send(json.dumps({
                "type": "response", "command": "get_tree", "success": True,
                "reqId": rq, "data": {"tree": tree},
            }))
        elif ctype == "prompt":
            aid = frame.get("agentId")
            if not aid:
                with self._lock:
                    self.seq += 1
                    nid = f"p{self.seq}"
                node = {"id": nid, "kind": "primary", "status": "running"}
                with self._lock:
                    self.primaries[nid] = node
                await ws.send(json.dumps({
                    "type": "response", "command": "prompt", "success": True,
                    "reqId": rq,
                }))
                await ws.send(json.dumps(upd(node)))
            else:
                await ws.send(json.dumps({
                    "type": "response", "command": "prompt", "success": True,
                    "reqId": rq,
                }))
        elif ctype in ("steer", "follow_up", "abort"):
            await ws.send(json.dumps({
                "type": "response", "command": ctype, "success": True,
                "reqId": rq,
            }))

    # ---------------------------------------------------------- control surface
    def send(self, frame: dict) -> None:
        """Push a frame to the connected driver over the real socket (thread-safe)."""
        if not self._connected.wait(3.0):
            raise RuntimeError("driver never connected")
        loop, ws = self._loop, self._ws
        fut = asyncio.run_coroutine_threadsafe(
            ws.send(json.dumps(frame, ensure_ascii=False)), loop)
        fut.result(timeout=3.0)

    def spawned(self) -> list[str]:
        with self._lock:
            return sorted(i for i in self.primaries if i.startswith("p"))


def make_driver(hive: LoopbackHive) -> HiveDriver:
    drv = HiveDriver(ws_url=f"ws://127.0.0.1:{hive.port}/ws", recv_timeout=0.5)
    drv.set_log(None)
    drv.connect(daemon=False)
    assert drv._online.wait(5.0), "driver did not go online"
    time.sleep(0.1)  # let the handshake + handler registration settle
    return drv


def wait_armed(drv: HiveDriver, agent_id: str, timeout: float = 5.0) -> bool:
    """Wait until a turn for `agent_id` exists AND has passed its ack barrier.

    A targeted `prompt` returns before the driver processes its ack, so a
    caller must wait for arming before it pushes turn-specific events (in real
    usage the agent's events never precede the prompt ack; this just mirrors
    that ordering over the loopback).
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        with drv._lock:
            t = drv._by_agent.get(agent_id)
            if t is not None and t.acked:
                return True
        time.sleep(0.005)
    return False


# ----------------------------------------------------------------------- tests
def test_real_socket_multi_spawn_isolated():
    """Two bare spawns over a real socket get distinct ids and settle
    independently; neither agent's text leaks into the other's transcript."""
    hive = LoopbackHive()
    drv = make_driver(hive)
    try:
        # Spawn A -> discovers p1 over the real event stream.
        ra = drv.prompt("task A", cwd=None, timeout_s=60, label="A",
                        wait_discovery=5.0)
        assert ra["ok"] is True and ra["agentId"] == "p1" and ra["spawned"] is True
        hive.send(message_end("p1", "Answer A"))

        # Spawn B while A is STILL in flight; B's pre-tree already contains p1.
        rb = drv.prompt("task B", cwd=None, timeout_s=60, label="B",
                        wait_discovery=5.0)
        assert rb["ok"] is True and rb["agentId"] == "p2" and rb["spawned"] is True
        assert ra["agentId"] != rb["agentId"]
        assert set(hive.spawned()) == {"p1", "p2"}

        # Both turns are concurrently in flight. Settle A only.
        hive.send(settle_event("p1", status="idle"))
        wa = drv.wait(ra["cmdId"], timeout_s=5.0)
        assert wa["settled"] is True and wa["final_text"] == "Answer A"
        assert wa["transcript"] == ["Answer A"]
        # B must NOT have been completed by A's settle.
        assert drv.status(rb["cmdId"])["settled"] is False

        # Now settle B; its transcript must only ever hold B's own text.
        hive.send(message_end("p2", "Answer B"))
        hive.send(settle_event("p2", status="idle"))
        wb = drv.wait(rb["cmdId"], timeout_s=5.0)
        assert wb["settled"] is True and wb["final_text"] == "Answer B"
        assert wb["transcript"] == ["Answer B"]   # A's text did not leak into B
    finally:
        try:
            drv._ws.close()
        except Exception:
            pass


def test_real_socket_targeted_continue_does_not_leak_prior_turn():
    """Continuing a settled agent over a real socket starts a fresh turn: the
    new turn collects only its own post-ack text, not the prior turn's answer."""
    hive = LoopbackHive()
    drv = make_driver(hive)
    try:
        r1 = drv.prompt("first", cwd=None, timeout_s=60, label="first",
                        wait_discovery=5.0)
        assert r1["ok"] is True and r1["agentId"] == "p1"
        hive.send(message_end("p1", "First answer"))
        hive.send(settle_event("p1", status="idle"))
        w1 = drv.wait(r1["cmdId"], timeout_s=5.0)
        assert w1["settled"] is True and w1["final_text"] == "First answer"

        # Continue the SAME agent: a fresh turn. A targeted prompt returns
        # before the driver processes its ack, so wait for arming before
        # pushing this turn's events (mirrors real event ordering).
        r2 = drv.prompt("second", agent_id="p1", timeout_s=60, label="second")
        assert r2["ok"] is True and r2["agentId"] == "p1"
        assert wait_armed(drv, "p1"), "new turn never armed"
        hive.send(message_end("p1", "Second answer"))
        hive.send(settle_event("p1", status="idle"))
        w2 = drv.wait(r2["cmdId"], timeout_s=5.0)
        assert w2["settled"] is True and w2["final_text"] == "Second answer"
        # The new turn must not have inherited the first turn's answer text.
        assert w2["transcript"] == ["Second answer"]
        # The first turn's result is unchanged by the continued conversation.
        assert drv.wait(r1["cmdId"], timeout_s=1.0)["final_text"] == "First answer"
    finally:
        try:
            drv._ws.close()
        except Exception:
            pass
