"""Dev-only tests for python_client.py against an in-process fake hive.

Run from the repo root:

    .venv/bin/python -m pytest -q .agents/skills/pi-hive-driver/scripts/test_python_client.py

The fake server is hosted in-process with the DEV-ONLY `websockets` package
(asyncio, background thread); the client under test stays synchronous on
`websocket-client`, exactly as in production. No runtime dependency is added
to python_client.py itself.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import threading
import time

import pytest
import websockets

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from python_client import HiveClient, HiveError  # noqa: E402


# --------------------------------------------------------------------------- fake hive
class FakeHive:
    """Scripted WS server standing in for the real pi-hive.

    Per connection, keyed by the client's first message:
      * get_tree -> one canned `{"type":"response","command":"get_tree"}` frame
        whose `data.tree` is `script["tree"]`.
      * prompt   -> `pre_ack` frames, the prompt response-ack (unless
        `ack=False`), then `post_ack` frames; afterwards the socket is either
        held open or dropped (`drop=True`).
    """

    def __init__(self, *, tree=None, pre_ack=(), post_ack=(), ack=True, drop=False):
        self.tree = tree if tree is not None else []
        self.pre_ack = list(pre_ack)
        self.post_ack = list(post_ack)
        self.ack = ack
        self.drop = drop
        self.port = None
        self._ready = threading.Event()
        threading.Thread(target=self._serve, daemon=True).start()
        assert self._ready.wait(5.0), "fake hive failed to start"

    def _serve(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._main())
        except Exception:  # pragma: no cover - server thread crash
            raise

    async def _main(self) -> None:
        async def handler(ws):
            try:
                await self._handle(ws)
            except websockets.ConnectionClosed:
                pass  # client hung up (check_online / post-settle close)

        async with websockets.serve(handler, "127.0.0.1", 0) as server:
            self.port = server.sockets[0].getsockname()[1]
            self._ready.set()
            await asyncio.get_running_loop().create_future()  # serve forever

    async def _handle(self, ws) -> None:
        msg = json.loads(await ws.recv())
        if msg.get("type") == "get_tree":
            await ws.send(json.dumps({
                "type": "response", "command": "get_tree", "success": True,
                "data": {"tree": self.tree},
            }))
            await ws.wait_closed()
            return
        if msg.get("type") == "prompt":
            for frame in self.pre_ack:
                await ws.send(json.dumps(frame))
            if self.ack:
                await ws.send(json.dumps({
                    "type": "response", "command": "prompt", "success": True,
                }))
            for frame in self.post_ack:
                await ws.send(json.dumps(frame))
            if self.drop:
                await ws.close()
                return
            await ws.wait_closed()

    def client(self, **kw) -> HiveClient:
        kw.setdefault("recv_timeout", 0.5)
        return HiveClient(ws_url=f"ws://127.0.0.1:{self.port}/ws", **kw)


# --------------------------------------------------------------------------- frame helpers
def ev(agent_id: str, event: dict) -> dict:
    return {"type": "hive:event", "agentId": agent_id, "ts": 0, "event": event}


def upd(agent: dict) -> dict:
    return {"type": "hive:agent_updated", "agent": agent, "ts": 0}


def settle_event(agent_id: str, kind: str = "primary", status: str = "idle",
                 terminal: bool = False) -> dict:
    return ev(agent_id, {"type": "agent_settled",
                         "settled": {"kind": kind, "status": status, "terminal": terminal}})


def message_end(agent_id: str, text: str, role: str = "assistant") -> dict:
    return ev(agent_id, {"type": "message_end",
                         "message": {"role": role,
                                     "content": [{"type": "text", "text": text}]}})


def tool_end(agent_id: str, tool_call_id: str, name: str) -> dict:
    return ev(agent_id, {"type": "tool_execution_end",
                         "toolCallId": tool_call_id, "toolName": name})


# --------------------------------------------------------------------------- tests
def test_normal_settle_completes_with_settle_status():
    hive = FakeHive(post_ack=[
        upd({"id": "a1", "kind": "primary", "status": "running"}),  # lagging snapshot
        message_end("other", "noise from another conversation"),
        message_end("a1", "Hello from the target"),
        settle_event("a1", kind="primary", status="idle"),
    ])
    res = hive.client().drive(prompt="hi", agent_id="a1", wall_timeout=10)
    assert res["settled"] is True
    assert res["agent_id"] == "a1"
    assert res["status"] == "idle"  # from the settle payload, NOT "running"
    assert res["final_text"] == "Hello from the target"
    assert res["transcript"] == ["Hello from the target"]  # other agent didn't leak


def test_stale_settle_before_ack_does_not_complete():
    # Previous turn's settle + done snapshot are queued BEFORE our prompt's ack
    # -> they must NOT complete the drive; it waits out its wall timeout.
    hive = FakeHive(
        pre_ack=[
            settle_event("a1"),
            upd({"id": "a1", "kind": "primary", "status": "idle",
                 "lastResult": {"finalText": "old answer"}}),
        ],
        post_ack=[],  # nothing after the ack; server holds the socket open
    )
    t0 = time.monotonic()
    res = hive.client().drive(prompt="hi", agent_id="a1", wall_timeout=1.0)
    assert res["settled"] is False
    assert time.monotonic() - t0 >= 0.9  # waited out the wall timeout, no early complete


def test_disconnect_during_drive_raises_quickly():
    hive = FakeHive(post_ack=[upd({"id": "a1", "kind": "primary", "status": "running"})],
                    drop=True)
    t0 = time.monotonic()
    with pytest.raises(HiveError, match="connection closed"):
        hive.client().drive(prompt="hi", agent_id="a1", wall_timeout=30)
    assert time.monotonic() - t0 < 5  # aborted, not spun until wall_timeout


def test_disconnect_before_ack_raises_quickly():
    hive = FakeHive(ack=False, drop=True)  # server drops right after reading the prompt
    with pytest.raises(HiveError, match="connection closed"):
        hive.client().drive(prompt="hi", agent_id="a1", wall_timeout=30)


def test_tool_name_captured_from_toolName():
    hive = FakeHive(post_ack=[
        tool_end("a1", "t1", "bash"),
        tool_end("a1", "t1", "bash"),  # duplicate toolCallId -> deduped
        tool_end("a1", "t2", "read"),
        tool_end("other", "x", "bash"),  # other agent's tool -> ignored
        ev("a1", {"type": "tool_execution_end", "toolCallId": "t3", "name": "legacy"}),
        settle_event("a1"),
    ])
    res = hive.client().drive(prompt="hi", agent_id="a1", wall_timeout=10)
    assert res["settled"] is True
    assert res["tool_calls"] == [
        {"agentId": "a1", "name": "bash"},
        {"agentId": "a1", "name": "read"},
        {"agentId": "a1", "name": "legacy"},  # old `name` field still works as fallback
    ]


def test_get_tree_envelope_parse():
    hive = FakeHive(tree=[
        {"id": "p1", "kind": "primary", "status": "idle"},
        {"id": "s1", "kind": "subagent", "status": "running"},
    ])
    client = hive.client()
    assert [n["id"] for n in client.get_tree()] == ["p1", "s1"]
    assert client._primary_ids() == {"p1"}


def test_bare_prompt_discovers_new_primary():
    hive = FakeHive(
        tree=[{"id": "old", "kind": "primary", "status": "idle"}],
        post_ack=[
            upd({"id": "new1", "kind": "primary", "status": "running"}),
            message_end("new1", "Fresh answer"),
            settle_event("new1"),
        ],
    )
    res = hive.client().drive(prompt="hi", wall_timeout=10)
    assert res["agent_id"] == "new1"
    assert res["settled"] is True
    assert res["final_text"] == "Fresh answer"


def test_http_base_derived_from_ws_url():
    client = HiveClient(ws_url="ws://10.0.0.7:4242/ws")
    assert client._http_base() == "http://10.0.0.7:4242"
    assert HiveClient(ws_url="wss://hive.example.com/ws")._http_base() == "https://hive.example.com"
