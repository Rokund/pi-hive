#!/usr/bin/env python3
"""hivedriver — a correct, controllable async driver for pi-hive.

This is the async, harness-friendly driver: a persistent daemon that owns one
WebSocket to the hive and accepts commands over a FIFO, streaming JSON-lines to
a log. It is the counterpart to the blocking, single-shot reference client
`python_client.py` (same directory): where that client holds the socket for the
whole of one turn (so the outer agent can't steer mid-turn or do other work),
this daemon lets an agent harness — where every tool call is a FRESH process
with no memory — launch a turn, get a `cmdId` immediately, and then poll /
`wait` / `steer` / `abort` across separate invocations.

Correctness is inherited from ONE shared source, `hive_protocol.py` (also in
this directory), which both drivers use: completion gated on `agent_settled`
(authoritative) behind the response-ack barrier, final text only from
`message_end` (never `message_update` deltas), events filtered to the target
agent, bare-prompt spawn discovery that never grabs a pre-existing conversation,
and tool-call dedupe. The protocol logic lives there and is NOT re-implemented
here — the daemon owns only transport, routing, and the FIFO/log surface.

Unlike a single-turn client, this daemon tracks MANY concurrent turns:

  * Each turn is keyed by its target agent id (resolved after discovery for a
    bare spawn). Frames stream for ALL agents on the socket; the driver routes
    each frame to the turn whose agent id it carries, so different in-flight
    agents can never contaminate each other's transcript or settlement.
  * A `prompt` with no `agentId` spawns a NEW primary. When several bare
    prompts are in flight at once they are disambiguated by spawn order (FIFO)
    plus a claimed-id set: each brand-new primary id is assigned to the oldest
    unresolved spawn whose pre-prompt tree snapshot did not already contain it,
    and never assigned twice. Different spawns get distinct ids and can be
    driven independently.
  * The response-ack barrier is per-turn and attributed by `reqId` (the server
    echoes each command's unique `reqId` back verbatim on its response — see
    SKILL.md §2). A settle/message for a turn is only honored after THAT turn's
    prompt has been acked, so a stale signal from a previous conversation on the
    same agent never completes a new turn. Ack matching is by `reqId`, never by
    arrival order.
  * At most one in-flight turn is allowed per agent id (a second prompt to a
    busy agent returns an immediate `busy` ack); this keeps the ack barrier for
    that agent airtight. Distinct agents run concurrently without limit.

On purpose this driver is WS-only (no HTTP /api/primary/spawn). Only
dependency: `websocket-client`.
"""
from __future__ import annotations

import argparse
import json
import os
import threading
import time
from typing import Any, Callable, Optional

import websocket  # websocket-client  (pip install websocket-client)

from hive_protocol import (TurnTracker, candidate_primary_ids,
                             primary_ids_from_tree)


class DriverError(Exception):
    pass


# Retention bounds for completed-turn results kept around so `wait`/`status`
# can return a cached result after a turn settled.
MAX_RETAINED = 10000          # hard cap on _by_cmd entries
RETENTION_S = 3600.0          # drop completed results older than this


class _Turn:
    """Per-turn state for one in-flight (or recently settled) conversation.

    Each turn owns its `TurnTracker`; the driver routes frames to it by agent
    id. Completion is recorded once and cached so `wait`/`status` can return it.
    """

    __slots__ = ("cmdId", "reqId", "agentId", "label", "t0", "timeout_s",
                 "acked", "discovered", "tracker", "done_ev", "result",
                 "finished_at")

    def __init__(self, cmd_id: str, req_id: str, agent_id: Optional[str],
                 pre_primary_ids, timeout_s: float, label: str = ""):
        self.cmdId = cmd_id
        self.reqId = req_id
        self.agentId = agent_id          # None until a bare spawn is discovered
        self.label = label
        self.t0 = time.time()
        self.timeout_s = timeout_s
        self.acked = False               # our prompt's response-ack barrier
        self.discovered = not (agent_id is None)  # bare prompts need discovery first
        self.tracker = TurnTracker(pre_primary_ids=pre_primary_ids)
        if agent_id:
            self.tracker.claim(agent_id)
        self.done_ev = threading.Event()
        self.result: Optional[dict] = None
        self.finished_at: Optional[float] = None

    def finish(self, status: str, kind: Optional[str], source: str,
               settled: bool = True, error: Optional[str] = None) -> None:
        self.result = {
            "cmdId": self.cmdId,
            "agentId": self.agentId,
            "label": self.label,
            "settled": settled,
            "status": status,
            "kind": kind,
            "source": source,
            "error": error,
            "final_text": (self.tracker.final_texts[-1]
                           if self.tracker.final_texts else ""),
            "transcript": list(self.tracker.final_texts),
            "tool_calls": list(self.tracker.tool_calls),
            "duration_s": round(time.time() - self.t0, 1),
        }
        self.finished_at = time.time()
        self.done_ev.set()

    def summary(self) -> dict:
        return {
            "cmdId": self.cmdId, "agentId": self.agentId, "label": self.label,
            "t0": self.t0, "timeout_s": self.timeout_s, "acked": self.acked,
            "discovered": self.discovered, "settled": self.result is not None,
            "partial_texts": len(self.tracker.final_texts),
            "elapsed_s": round(time.time() - self.t0, 1),
        }


class HiveDriver:
    """Owns one WebSocket and tracks MANY concurrent turns.

    Turns are keyed by agent id (for routing) and by cmdId (for `status`/
    `wait`); at most one in-flight turn per agent id. Public commands and the
    WS callbacks are guarded by an internal lock. State is held so an outer
    process can drive the daemon across separate tool calls via the FIFO/log
    interface (see `daemon_main`).
    """

    def __init__(self, ws_url: str = "ws://127.0.0.1:3001/ws", recv_timeout: float = 10.0):
        self.ws_url = ws_url
        self.recv_timeout = recv_timeout
        self._lock = threading.RLock()
        self._send_lock = threading.Lock()
        self._ws: Optional[websocket.WebSocketApp] = None
        self._online = threading.Event()
        self.last_error: Optional[str] = None
        self._seq = 0
        # Registries.
        self._by_agent: dict[str, _Turn] = {}   # agentId -> turn (routing)
        self._by_cmd: dict[str, _Turn] = {}     # cmdId -> turn (status/wait)
        self._unresolved: list[_Turn] = []      # bare spawns awaiting discovery (FIFO)
        self._reqid_turn: dict[str, _Turn] = {}  # pending prompt acks: reqId -> turn
        self._claimed: set = set()              # new-primary ids already assigned
        self._tree: Optional[list] = None
        self._tree_ev = threading.Event()
        self._sink: Optional[Callable[[dict], None]] = None

    # ------------------------------------------------------------- observability
    def set_log(self, sink) -> None:
        """Attach a log sink `fn(dict)` — the daemon writes these to the log file."""
        self._sink = sink

    def _log(self, obj: dict) -> None:
        obj["ts"] = time.time()
        obj["driver"] = "hivedriver"
        if self._sink is not None:
            try:
                self._sink(obj)
            except Exception:
                pass

    # ---------------------------------------------------------------- liveness
    def check_online(self) -> bool:
        try:
            ws = websocket.create_connection(self.ws_url, timeout=self.recv_timeout)
            ws.close()
            return True
        except Exception:
            return False

    def connect(self, daemon: bool = False) -> None:
        """Connect the WebSocketApp. `daemon=True` reconnects automatically."""
        self._ws = websocket.WebSocketApp(
            self.ws_url,
            on_open=self._on_open, on_message=self._on_message,
            on_error=self._on_error, on_close=self._on_close,
        )
        kwargs = {"reconnect": 2} if daemon else {}
        threading.Thread(target=self._ws.run_forever, kwargs=kwargs,
                         daemon=True).start()

    # ------------------------------------------------------------- WS callbacks
    def _on_open(self, _ws):
        self._online.set()
        self._log({"ev": "ws_open", "ws": self.ws_url})

    def _on_error(self, _ws, err):
        self._online.clear()
        self.last_error = str(err)
        self._log({"ev": "ws_error", "error": str(err)})

    def _on_close(self, _ws, code, msg):
        self._online.clear()
        self._log({"ev": "ws_closed", "code": code, "msg": msg})
        # Fail fast: a prompt whose ack we never saw may well have been lost in
        # transit. Surface partial text; the agent can re-prompt the same id
        # (the conversation persists server-side) — never let it spin to the
        # wall timeout. Acknowledged in-flight turns are left pending: after an
        # auto-reconnect their events resume streaming and may still settle.
        with self._lock:
            victims = [t for t in self._by_cmd.values()
                       if not t.acked and t.result is None]
        for t in victims:
            self._finalize(t, "driver_error", None, "ws_closed_pre_ack",
                           settled=False,
                           error=f"connection closed before prompt ack: {msg}")

    def _on_message(self, _ws, raw):
        try:
            frame = json.loads(raw)
        except Exception:
            return
        if self._sink is not None:
            try:
                self._sink({"ev": "frame", "frame": frame})
            except Exception:
                pass
        ftype = frame.get("type")
        if ftype == "response":
            self._handle_response(frame)
        elif ftype in ("hive:agent_updated", "hive:event", "hive:tree"):
            self._route(frame)
        elif ftype == "hive:error":
            self._log({"ev": "hive_error", "message": frame.get("message")})

    # ----------------------------------------------------------- send helper
    def _send(self, payload: dict) -> bool:
        with self._send_lock:
            if self._ws is None or not self._online.is_set():
                return False
            try:
                self._ws.send(json.dumps(payload, ensure_ascii=False))
                return True
            except Exception as exc:
                self._log({"ev": "ws_send_failed", "error": str(exc)})
                self._online.clear()
                return False

    def _next_id(self, prefix: str) -> str:
        with self._lock:
            self._seq += 1
            return f"{prefix}-{int(time.time() * 1000)}-{self._seq}"

    # ------------------------------------------------------------- responses
    def _handle_response(self, frame: dict):
        cmd = frame.get("command")
        ok = bool(frame.get("success"))
        req_id = frame.get("reqId")
        with self._lock:
            t = self._reqid_turn.pop(req_id, None) if req_id else None
        if cmd == "prompt" and t is not None:
            # Per-turn ack barrier (attributed by reqId, never by ordering):
            # this turn only starts accepting signals once ITS prompt is acked.
            if ok:
                t.acked = True
                t.discovered = t.agentId is not None
            else:
                self._finalize(t, "driver_error", None, "prompt_rejected",
                               settled=False,
                               error=frame.get("error") or "prompt rejected")
            self._log({"ev": "prompt_ack", "cmdId": t.cmdId,
                       "agentId": t.agentId, "success": ok,
                       "error": frame.get("error")})
        elif cmd == "get_tree":
            self._tree = (frame.get("data") or {}).get("tree", [])
            self._tree_ev.set()
            self._log({"ev": "tree", "nodes": len(self._tree), "success": ok})
        else:
            self._log({"ev": "cmd_ack", "command": cmd, "success": ok,
                       "reqId": req_id, "error": frame.get("error")})

    # ---------------------------------------------------------------- routing
    def _route(self, frame: dict):
        """Route one inbound frame to its own turn (spawn-discovery first).

        Frames stream for ALL agents. Identity assignment (bare-spawn
        discovery) is done here BEFORE feeding, and — unlike settlement — is
        NOT gated on the ack, because the server spawns synchronously and the
        new node's `agent_updated` can legitimately precede our prompt ack.
        Pre-existing ids are excluded by each spawn's pre-prompt tree snapshot,
        and a claimed-id set keeps one new id from being assigned twice.
        """
        ftype = frame.get("type")
        aid = None
        if ftype == "hive:agent_updated":
            ag = frame.get("agent") or {}
            aid = ag.get("id")
        elif ftype == "hive:event":
            aid = frame.get("agentId")
        self._try_assign(frame)
        if aid:
            self._feed(aid, frame)

    def _try_assign(self, frame: dict):
        """Assign brand-new primary ids to outstanding bare spawns, FIFO."""
        with self._lock:
            if not self._unresolved:
                return
            for cid in candidate_primary_ids(frame):
                if cid in self._claimed:
                    continue
                for t in self._unresolved:
                    if cid not in t.tracker.pre:
                        self._assign_locked(t, cid)
                        break

    def _assign_locked(self, t: _Turn, cid: str) -> None:
        t.agentId = cid
        t.discovered = True
        t.tracker.claim(cid)
        self._claimed.add(cid)
        self._unresolved.remove(t)
        self._by_agent[cid] = t
        self._log({"ev": "discovered_primary", "cmdId": t.cmdId, "agentId": cid})

    def _feed(self, aid: str, frame: dict):
        """Fold a frame for agent `aid` into its turn's tracker, if armed.

        Armed = the turn's prompt ack has been received for THIS agent id. A
        signal observed before arming belongs to a previous turn on the same
        agent and must not complete (or contaminate) the new turn.
        """
        with self._lock:
            t = self._by_agent.get(aid)
        if t is None or not t.acked:
            return
        signal = t.tracker.observe(frame)
        if signal and signal.get("agent_id") == aid:
            self._finalize(t, signal.get("status"), signal.get("kind"),
                           signal.get("source"), settled=True)

    # ------------------------------------------------------------- completion
    def _finalize(self, t: _Turn, status: str, kind: Optional[str], source: str,
                  settled: bool = True, error: Optional[str] = None) -> None:
        with self._lock:
            if t.result is not None:
                return  # already finalized (idempotent)
            t.finish(status, kind, source, settled=settled, error=error)
            self._detach_locked(t)
            self._prune_locked()
        self._log({"ev": "turn_result", "cmdId": t.cmdId,
                   "agentId": t.agentId, "settled": settled,
                   "status": status, "source": source, "error": error,
                   "final_text": t.result["final_text"],
                   "tool_calls": list(t.tracker.tool_calls)})

    def _detach_locked(self, t: _Turn) -> None:
        if t.agentId:
            self._by_agent.pop(t.agentId, None)
        if t in self._unresolved:
            self._unresolved.remove(t)
        self._reqid_turn.pop(t.reqId, None)
        # `t` stays in _by_cmd so wait()/status() can return its cached result.

    def _prune_locked(self) -> None:
        now = time.time()
        by_age = [cid for cid, t in self._by_cmd.items()
                  if t.finished_at is not None
                  and now - t.finished_at > RETENTION_S]
        for cid in by_age:
            del self._by_cmd[cid]
        # Hard cap: if still over, drop the oldest completed results.
        while len(self._by_cmd) > MAX_RETAINED:
            oldest = min(
                (t for t in self._by_cmd.values() if t.finished_at is not None),
                key=lambda t: t.finished_at, default=None)
            if oldest is None:
                break
            self._by_cmd.pop(oldest.cmdId, None)

    # ---------------------------------------------------------------- commands
    def get_tree(self, timeout_s: float = 8.0) -> list:
        self._tree_ev.clear()
        if not self._send({"type": "get_tree"}):
            raise DriverError("not online")
        if not self._tree_ev.wait(timeout_s):
            raise DriverError("get_tree: no response within timeout")
        return self._tree or []

    def _pre_spawn_primary_ids(self) -> set:
        try:
            return primary_ids_from_tree(self.get_tree())
        except Exception:
            return set()

    def prompt(self, text: str, agent_id: Optional[str] = None, cwd: Optional[str] = None,
               timeout_s: float = 2400.0, label: str = "",
               wait_discovery: float = 10.0) -> dict:
        """Launch a turn and return with a `cmdId`.

        * `agent_id` None  -> spawn a NEW primary (bare prompt). The new id is
          discovered from the event stream and, if found within
          `wait_discovery`, returned here.
        * `agent_id` given -> continue that conversation. If the agent already
          has an in-flight turn, returns an immediate `busy` ack.

        Returns `{ok, cmdId, agentId, spawned, error?}` — NOT the result. Use
        `wait()`/`status()` for settlement. Many distinct agents may be in
        flight concurrently; at most one turn per agent id.
        """
        with self._lock:
            if agent_id and self._by_agent.get(agent_id) is not None:
                active = self._by_agent[agent_id]
                if active.result is None:
                    return {"ok": False, "error": "busy",
                            "active_cmdId": active.cmdId}

        pre = self._pre_spawn_primary_ids() if agent_id is None else set()
        cmd_id = self._next_id("cmd")
        req_id = self._next_id("req")
        t = _Turn(cmd_id, req_id, agent_id, pre, timeout_s, label)
        with self._lock:
            self._by_cmd[cmd_id] = t
            self._reqid_turn[req_id] = t
            if agent_id is None:
                self._unresolved.append(t)
            else:
                self._by_agent[agent_id] = t

        payload: dict = {"type": "prompt", "text": text, "reqId": req_id}
        if agent_id:
            payload["agentId"] = agent_id
        if cwd:
            payload["cwd"] = cwd
        if not self._send(payload):
            with self._lock:
                in_by_cmd = self._by_cmd.get(cmd_id)
            if in_by_cmd is t:
                self._finalize(t, "driver_error", None, "send_failed",
                               settled=False, error="send_failed")
            return {"ok": False, "error": "send_failed", "cmdId": cmd_id}

        if agent_id is None:
            # Block (bounded) until the new primary id is discovered so the
            # caller can steer/abort it immediately. If we can't resolve it in
            # time, still return ok — settle will surface its true id later.
            deadline = time.time() + wait_discovery
            while time.time() < deadline:
                with self._lock:
                    if t.agentId is not None:
                        return {"ok": True, "cmdId": cmd_id,
                                "agentId": t.agentId, "spawned": True}
                t.done_ev.wait(0.1)
            return {"ok": True, "cmdId": cmd_id, "spawned": True,
                    "agentId": t.agentId, "error": "discovery_timed_out"}

        return {"ok": True, "cmdId": cmd_id, "agentId": agent_id, "spawned": False}

    def steer(self, agent_id: str, text: str) -> bool:
        return self._send({"type": "steer", "agentId": agent_id, "text": text,
                           "reqId": self._next_id("req")})

    def follow_up(self, agent_id: str, text: str) -> bool:
        return self._send({"type": "follow_up", "agentId": agent_id, "text": text,
                           "reqId": self._next_id("req")})

    def abort(self, agent_id: str, reason: str = "driver abort") -> bool:
        return self._send({"type": "abort", "agentId": agent_id,
                           "reason": reason, "by": "external",
                           "reqId": self._next_id("req")})

    # ------------------------------------------------------------ turn polling
    def status(self, cmd_id: str) -> Optional[dict]:
        with self._lock:
            t = self._by_cmd.get(cmd_id)
            if t is None:
                return {"cmdId": cmd_id, "settled": False, "error": "unknown cmdId"}
            return dict(t.result) if t.result is not None else t.summary()

    def wait(self, cmd_id: str, timeout_s: float = 60.0) -> dict:
        """Block up to `timeout_s` for `cmd_id`'s structured result.

        Non-destructive: calling again returns the same cached result. Returns
        `{ok, settled, ...result}` or `{ok:False, settled:False, timeout}`.
        """
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            with self._lock:
                t = self._by_cmd.get(cmd_id)
            if t is None:
                return {"cmdId": cmd_id, "ok": False, "settled": False,
                        "error": "unknown cmdId"}
            if t.result is not None:
                return {"ok": True, "settled": True, **t.result}
            t.done_ev.wait(timeout=min(0.5, deadline - time.time()))
        t = self._by_cmd.get(cmd_id)
        return {"cmdId": cmd_id, "ok": False, "settled": False,
                "timeout": True, "status": "in_progress",
                "partial": t.summary() if t else None}


# --------------------------------------------------------------------------- #
# FIFO / log daemon interface                                                  #
# --------------------------------------------------------------------------- #
def daemon_main(ws_url: str, fifo_path: str, log_path: str) -> None:
    """Run the persistent driver: reads commands as JSON-lines from a FIFO,
    appends JSON-lines to a log, keeps the WS alive with auto-reconnect."""
    import json as _json

    def log_sink(obj: dict):
        line = _json.dumps(obj, ensure_ascii=False, default=str)
        try:
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(line + "\n")
                f.flush()
        except Exception:
            pass
        try:
            print(line, flush=True)
        except Exception:
            pass

    drv = HiveDriver(ws_url=ws_url)
    drv.set_log(log_sink)
    drv.connect(daemon=True)

    for p in (fifo_path,):
        if os.path.exists(p):
            os.unlink(p)
    os.mkfifo(fifo_path)
    log_sink({"ev": "driver_start", "ws": ws_url, "fifo": fifo_path,
              "log": log_path, "pid": os.getpid()})

    def handle(cmd: dict):
        op = cmd.get("op")
        if op == "ping":
            with drv._lock:
                active = [t.cmdId for t in drv._by_cmd.values()
                          if t.result is None]
            log_sink({"ev": "pong", "online": drv._online.is_set(),
                      "active": active})
        elif op == "prompt":
            res = drv.prompt(
                text=cmd.get("text", ""), agent_id=cmd.get("agentId"),
                cwd=cmd.get("cwd"), timeout_s=cmd.get("timeout_s", 2400.0),
                label=cmd.get("label", ""),
                wait_discovery=cmd.get("wait_discovery", 10.0))
            log_sink({"ev": "cmd_ack", "op": "prompt", **res})
        elif op == "steer":
            ok = drv.steer(cmd.get("agentId", ""), cmd.get("text", ""))
            log_sink({"ev": "cmd_ack", "op": "steer", "ok": ok})
        elif op == "follow_up":
            ok = drv.follow_up(cmd.get("agentId", ""), cmd.get("text", ""))
            log_sink({"ev": "cmd_ack", "op": "follow_up", "ok": ok})
        elif op == "abort":
            ok = drv.abort(cmd.get("agentId", ""), cmd.get("reason", "driver abort"))
            log_sink({"ev": "cmd_ack", "op": "abort", "ok": ok})
        elif op == "get_tree":
            try:
                tree = drv.get_tree()
                log_sink({"ev": "cmd_ack", "op": "get_tree", "ok": True,
                          "nodes": len(tree), "tree": tree})
            except Exception as exc:
                log_sink({"ev": "cmd_ack", "op": "get_tree", "ok": False,
                          "error": str(exc)})
        elif op == "status":
            log_sink({"ev": "cmd_ack", "op": "status",
                      "data": drv.status(cmd.get("cmdId", ""))})
        elif op == "wait":
            res = drv.wait(cmd.get("cmdId", ""),
                           timeout_s=cmd.get("timeout_s", 60.0))
            log_sink({"ev": "wait_result", **res})
        else:
            log_sink({"ev": "cmd_ack", "op": op, "ok": False,
                      "error": f"unknown op {op}"})

    # Dispatch each command on its own thread so a blocking `wait`/`prompt`
    # never stalls the FIFO reader (steer/abort/get_tree stay responsive while
    # a wait is active).
    while True:
        try:
            with open(fifo_path, "r", encoding="utf-8") as fifo:
                for line in fifo:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        cmd = _json.loads(line)
                    except Exception:
                        log_sink({"ev": "bad_command", "raw": line[:200]})
                        continue
                    threading.Thread(target=handle, args=(cmd,), daemon=True).start()
        except Exception as exc:
            log_sink({"ev": "fifo_error", "error": str(exc)})
            time.sleep(0.5)
        log_sink({"ev": "fifo_eof_reopening"})


def demo(ws_url: str = "ws://127.0.0.1:4101/ws") -> None:
    """In-process smoke test: spawn two fresh primaries concurrently, wait for
    each to settle, then drive one of them again (a targeted prompt)."""
    import json as _json

    def sink(obj):
        print(_json.dumps(obj, ensure_ascii=False, default=str), flush=True)

    drv = HiveDriver(ws_url=ws_url)
    drv.set_log(sink)
    drv.connect(daemon=False)   # no auto-reconnect in a short-lived demo
    if not drv._online.wait(5):
        print("NOT ONLINE:", ws_url)
        return
    time.sleep(0.5)  # let the socket settle

    spawns = []
    for i, q in enumerate(["Reply with exactly one sentence about the Eiffel Tower.",
                           "Reply with exactly one sentence about zebras."]):
        r = drv.prompt(q, cwd=None, timeout_s=120, label=f"demo-spawn-{i}")
        print(f"\nSPAWN{i}:", _json.dumps(r))
        spawns.append(r)

    for r in spawns:
        if not r.get("ok") or not r.get("agentId"):
            continue
        res = drv.wait(r["cmdId"], timeout_s=120)
        print(f"\nRESULT {r['agentId']}:",
              _json.dumps({k: res.get(k) for k in
                           ("agentId", "settled", "status", "final_text")},
                          ensure_ascii=False))

    agent = next((r.get("agentId") for r in spawns if r.get("agentId")), None)
    if agent:
        turn = drv.prompt("Reply with exactly one sentence about mountains.",
                          agent_id=agent, timeout_s=120, label="demo-follow")
        r2 = drv.wait(turn["cmdId"], timeout_s=120)
        print("\nRESULT2:", _json.dumps(
            {k: r2.get(k) for k in ("agentId", "settled", "status", "final_text")},
            ensure_ascii=False))


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="hivedriver — async multi-turn pi-hive driver")
    ap.add_argument("--daemon", action="store_true",
                    help="run the persistent FIFO/log daemon (harness-facing)")
    ap.add_argument("--ws", default=os.environ.get("HIVEDRIVER_WS", "ws://127.0.0.1:3001/ws"))
    ap.add_argument("--fifo", default=os.environ.get("HIVEDRIVER_FIFO", "/tmp/hivedriver.in"))
    ap.add_argument("--log", default=os.environ.get("HIVEDRIVER_LOG", "/tmp/hivedriver.log"))
    args = ap.parse_args()
    if args.daemon:
        daemon_main(args.ws, args.fifo, args.log)
    else:
        demo(args.ws)
