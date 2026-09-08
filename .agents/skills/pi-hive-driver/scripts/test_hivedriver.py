"""Dev-only tests for hivedriver.py's asynchronous multi-turn driver.

These test the driver's in-process logic — spawn discovery, per-agent routing,
the reqId-attributed ack barrier, busy semantics, and per-turn settlement — by
stubbing the transport (`_send`/`get_tree`) and injecting scripted WS frames
through the real `_on_message` path. No real hive, socket, or third-party deps
beyond what hivedriver already imports.

Run from the repo root:
    .venv/bin/python -m pytest -q .agents/skills/pi-hive-driver/scripts/test_hivedriver.py
"""
from __future__ import annotations

import json
import os
import sys
import threading
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from test_python_client import ev, upd, settle_event, message_end, tool_end  # noqa: E402
from hivedriver import HiveDriver  # noqa: E402


# --------------------------------------------------------------------------- helpers
def make_driver(send_fail=False):
    drv = HiveDriver(ws_url="ws://127.0.0.1:1/ws")
    drv._online.set()
    sent = []
    if send_fail:
        drv._send = lambda payload: False
    else:
        def fake_send(payload):
            sent.append(payload)
            return True
        drv._send = fake_send
    drv.get_tree = lambda timeout_s=8.0: []   # no pre-existing primaries
    drv.sent = sent
    return drv


def push(drv, frame: dict) -> None:
    drv._on_message(None, json.dumps(frame))


def ack_frame(req_id):
    return {"type": "response", "command": "prompt", "success": True, "reqId": req_id}


def pending_reqids(drv, n, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        ks = list(drv._reqid_turn.keys())
        if len(ks) >= n:
            return ks
        time.sleep(0.001)
    return list(drv._reqid_turn.keys())


def start_spawn(drv, text, label="L", wait_discovery=5.0):
    out = {}
    def run():
        out["res"] = drv.prompt(text=text, cwd=None, timeout_s=120,
                                label=label, wait_discovery=wait_discovery)
    th = threading.Thread(target=run, daemon=True)
    th.start()
    return th, out


# --------------------------------------------------------------------------- single spawn
def test_bare_spawn_discover_and_settle():
    drv = make_driver()
    th, out = start_spawn(drv, "task")
    reqs = pending_reqids(drv, 1)
    push(drv, ack_frame(reqs[0]))
    push(drv, upd({"id": "new1", "kind": "primary", "status": "running"}))
    th.join(3.0)
    assert not th.is_alive(), "bare prompt should have resolved"
    r = out["res"]
    assert r["ok"] is True and r["agentId"] == "new1" and r["spawned"] is True

    push(drv, message_end("new1", "Fresh answer"))
    push(drv, settle_event("new1", status="idle"))
    res = drv.wait(r["cmdId"], timeout_s=2.0)
    assert res["settled"] is True
    assert res["status"] == "idle"
    assert res["final_text"] == "Fresh answer"


def test_bare_spawn_discovery_timeout_returns_error():
    drv = make_driver()
    th, out = start_spawn(drv, "task", wait_discovery=0.4)
    reqs = pending_reqids(drv, 1)
    push(drv, ack_frame(reqs[0]))            # acked but never discovered
    th.join(3.0)
    r = out["res"]
    assert r["ok"] is True and r["agentId"] is None
    assert r["error"] == "discovery_timed_out"


# ------------------------------------------------ multiple concurrent spawns
def test_multi_spawn_distinct_and_isolated():
    """THE concurrency requirement: two bare prompts in flight never collide.

    Each gets its own new primary id, each settles independently, and neither
    agent's text leaks into the other's transcript.
    """
    drv = make_driver()
    th1, out1 = start_spawn(drv, "task A", "A")
    th2, out2 = start_spawn(drv, "task B", "B")
    reqs = pending_reqids(drv, 2)

    # Arm both (reqId-attributed acks).
    for rq in reqs:
        push(drv, ack_frame(rq))
    # Spawns complete in FIFO order -> new1 -> spawn A, new2 -> spawn B.
    push(drv, upd({"id": "new1", "kind": "primary", "status": "running"}))
    push(drv, upd({"id": "new2", "kind": "primary", "status": "running"}))
    th1.join(3.0)
    th2.join(3.0)
    assert not th1.is_alive() and not th2.is_alive()
    r1, r2 = out1["res"], out2["res"]
    assert r1["ok"] and r2["ok"]
    assert r1["agentId"] == "new1" and r2["agentId"] == "new2"
    assert r1["agentId"] != r2["agentId"]

    # Stream text for both, then settle A; text must stay isolated per agent.
    push(drv, message_end("new1", "Answer A"))
    push(drv, message_end("new2", "Answer B"))
    push(drv, settle_event("new1", status="idle"))
    w1 = drv.wait(r1["cmdId"], timeout_s=2.0)
    assert w1["settled"] is True and w1["final_text"] == "Answer A"
    # B must NOT have been completed by A's settle, nor polluted by A's text.
    assert drv.status(r2["cmdId"])["settled"] is False

    push(drv, settle_event("new2", status="idle"))
    w2 = drv.wait(r2["cmdId"], timeout_s=2.0)
    assert w2["settled"] is True and w2["final_text"] == "Answer B"
    assert w2["transcript"] == ["Answer B"]   # A's text did not leak into B


def test_same_agent_twice_is_busy():
    drv = make_driver()
    r1 = drv.prompt(text="a", agent_id="a1")   # in flight (no frames yet)
    r2 = drv.prompt(text="b", agent_id="a1")
    assert r1["ok"] is True
    assert r2["ok"] is False and r2["error"] == "busy"
    assert r2["active_cmdId"] == r1["cmdId"]


# ------------------------------------------------------------------- ack barrier
def test_stale_settle_before_ack_does_not_complete():
    """A prior turn's settle for the same agent must not complete the new turn.

    Frames before THIS turn's reqId-attributed ack are dropped (barrier), so a
    stale settle cannot complete it prematurely.
    """
    drv = make_driver()
    r = drv.prompt(text="hi", agent_id="a1")
    push(drv, settle_event("a1"))                      # stale, pre-ack
    push(drv, upd({"id": "a1", "kind": "primary", "status": "idle"}))  # stale snapshot
    reqs = pending_reqids(drv, 1)
    push(drv, ack_frame(reqs[0]))                      # ack; no further frames
    res = drv.wait(r["cmdId"], timeout_s=0.6)
    assert res["settled"] is False


def test_targeted_settle_after_ack_completes():
    drv = make_driver()
    r = drv.prompt(text="hi", agent_id="a1")
    reqs = pending_reqids(drv, 1)
    push(drv, ack_frame(reqs[0]))
    push(drv, message_end("a1", "Hello"))
    push(drv, settle_event("a1", status="idle"))
    res = drv.wait(r["cmdId"], timeout_s=2.0)
    assert res["settled"] is True and res["status"] == "idle"
    assert res["final_text"] == "Hello"


# ------------------------------------------------------------------- dedupe/filter
def test_tool_dedupe_and_target_filter():
    drv = make_driver()
    r = drv.prompt(text="hi", agent_id="a1")
    reqs = pending_reqids(drv, 1)
    push(drv, ack_frame(reqs[0]))
    push(drv, tool_end("a1", "t1", "bash"))
    push(drv, tool_end("a1", "t1", "bash"))      # duplicate id -> deduped
    push(drv, tool_end("other", "x", "bash"))    # other agent -> ignored
    push(drv, message_end("a1", "done"))
    push(drv, settle_event("a1", status="idle"))
    res = drv.wait(r["cmdId"], timeout_s=2.0)
    assert res["tool_calls"] == [{"agentId": "a1", "name": "bash"}]


# ------------------------------------------------------------------- failures
def test_send_failure_fails_turn():
    drv = make_driver(send_fail=True)
    r = drv.prompt(text="hi", agent_id="a1")
    assert r["ok"] is False and r["error"] == "send_failed"
    st = drv.status(r["cmdId"])
    assert st["settled"] is False and st["status"] == "driver_error"


def test_wait_unknown_cmd_id():
    drv = make_driver()
    res = drv.wait("nope", timeout_s=0.2)
    assert res["ok"] is False and res["error"] == "unknown cmdId"


def test_ws_close_pre_ack_fails_fast():
    drv = make_driver()
    r = drv.prompt(text="hi", agent_id="a1")
    reqs = pending_reqids(drv, 1)          # issued, ack not yet fed
    drv._on_close(None, 1000, "gone")       # closed before ack
    res = drv.wait(r["cmdId"], timeout_s=0.5)
    assert res["settled"] is False and res["status"] == "driver_error"
    assert "connection closed before prompt ack" in (res.get("error") or "")
