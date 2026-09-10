"""Tests for hive_cli.py against an in-process fake HTTP hive.

Run from the repo root:

    .venv/bin/python -m pytest -q tests/test_hive_cli.py

The fake server hosts the bare-`{ok,...}` HTTP endpoints the client calls
(spawn / prompt / abort / wait / events / tree), driven by scripted state, so
the client's drive/watch-loop logic is exercised against real HTTP transport —
no WebSocket anywhere. Only stdlib is used by the client under test.
"""
from __future__ import annotations

import json
import os
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

sys.path.insert(0, os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    ".agents", "skills", "pi-hive-driver", "scripts",
))
from hive_cli import HiveClient, HiveError  # noqa: E402


class CountdownHive:
    """Scripted HTTP hive: an agent that needs N waits before it settles.

    Routes:
      POST /api/primary/spawn        -> {ok, id: "a1", ...}
      POST /api/prompt               -> {ok}
      POST /api/abort                -> {ok}
      POST /hive/agent/wait          -> counts down waits; {ok,id,status:"running"}
                                        until exhausted, then a settled payload
                                        {ok,id,status:"idle",result:{finalText:...}}
      GET /api/agent/{id}/events     -> a message_end backlog
      GET /api/health                -> {ok:True}
      GET /api/agent/{id}/questions  -> {ok, questions:[]}
      GET /api/tree                  -> {ok, tree:[...]}
    """

    def __init__(self, waits_before_settle: int = 0, final_text: str = "done",
                 events_empty_until: int = 0):
        self.waits_before_settle = waits_before_settle
        self.final_text = final_text
        # Emulate the real racer: /hive/agent/wait can signal settle a beat
        # before the message_end is queryable. Events return EMPTY for the
        # first `events_empty_until` reads, then the backlog appears.
        self.events_empty_until = events_empty_until
        self.events_reads = 0
        self.wait_calls = 0
        self.prompt_calls = 0
        self.spawn_calls = 0
        self.port = None
        self._ready = threading.Event()
        self._server = None
        threading.Thread(target=self._serve, daemon=True).start()
        assert self._ready.wait(5.0), "fake hive failed to start"

    def _serve(self) -> None:
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), self._handler())
        self._server.hive = self  # allow handlers to reach scripted state
        self.port = self._server.server_address[1]
        self._ready.set()
        self._server.serve_forever()

    def _handler(self):
        class H(BaseHTTPRequestHandler):
            def log_message(self, *a):  # quiet
                pass

            def _send(self, obj, code=200):
                payload = json.dumps(obj).encode("utf-8")
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def _body(self):
                length = int(self.headers.get("Content-Length", 0))
                if not length:
                    return {}
                return json.loads(self.rfile.read(length).decode("utf-8"))

            def do_GET(self):
                h = self.server.hive
                if self.path.startswith("/api/health"):
                    return self._send({"ok": True})
                if self.path.startswith("/api/agent/") and "/events" in self.path:
                    import re
                    m = re.match(r"/api/agent/([^/]+)/events", self.path)
                    h.events_reads += 1
                    if h.events_reads <= h.events_empty_until:
                        return self._send({"ok": True, "agentId": m.group(1),
                                           "events": [], "latest": 0})
                    return self._send({
                        "ok": True, "agentId": m.group(1), "events": [
                            {"agentId": m.group(1), "event": {
                                "type": "message_end",
                                "message": {"role": "assistant", "content": [
                                    {"type": "text", "text": h.final_text}]}},
                             "seq": 1},
                        ],
                    })
                if self.path.startswith("/api/agent/") and "/questions" in self.path:
                    return self._send({"ok": True, "questions": []})
                if self.path.startswith("/api/tree"):
                    return self._send({"ok": True, "tree": [
                        {"id": "a1", "kind": "primary", "status": "idle"}]})
                return self._send({"ok": True})

            def do_POST(self):
                h = self.server.hive
                body = self._body()
                if self.path == "/api/primary/spawn":
                    h.spawn_calls += 1
                    return self._send({"ok": True, "id": "a1", "model": "m", "error": ""})
                if self.path == "/api/prompt":
                    h.prompt_calls += 1
                    if body.get("agent") == "ghost":
                        return self._send({"ok": False, "error": "agent not found: ghost"})
                    return self._send({"ok": True})
                if self.path == "/api/steer":
                    return self._send({"ok": True})
                if self.path == "/api/follow_up":
                    return self._send({"ok": True})
                if self.path == "/api/abort":
                    return self._send({"ok": True})
                if self.path == "/hive/agent/wait":
                    h.wait_calls += 1
                    if h.wait_calls <= h.waits_before_settle:
                        return self._send({"ok": True, "id": "a1", "status": "running",
                                           "progress": {"recentlyActive": True, "streaming": True}})
                    return self._send({"ok": True, "id": "a1", "status": "idle",
                                       "result": {"finalText": h.final_text}})
                return self._send({"ok": True})

        return H

    def client(self, **kw) -> HiveClient:
        kw.setdefault("timeout", 5.0)
        if "api_base" not in kw and not (kw.get("host") and kw.get("port")):
            kw["api_base"] = f"http://127.0.0.1:{self.port}"
            kw["host"] = kw["port"] = None
        return HiveClient(**kw)


def test_immediate_settle_completes():
    h = CountdownHive(waits_before_settle=0, final_text="Hello")
    res = h.client().drive(prompt="hi", wall_timeout=20)
    assert res["settled"] is True
    assert res["agent_id"] == "a1"
    assert res["status"] == "idle"
    assert res["final_text"] == "Hello"
    assert res["transcript"] == ["Hello"]


def test_drive_waits_until_settle():
    h = CountdownHive(waits_before_settle=2, final_text="after two waits")
    res = h.client().drive(prompt="hi", wall_timeout=20)
    assert res["settled"] is True
    assert h.wait_calls >= 3  # one for each running wait + settle
    assert res["final_text"] == "after two waits"


def test_drive_reads_final_text_despite_event_lag():
    # Real racer: settle signals before message_end is readable. The client
    # must retry the event backlog (not give up after a single empty read).
    h = CountdownHive(waits_before_settle=0, final_text="laggy answer",
                      events_empty_until=2)
    res = h.client().drive(prompt="hi", wall_timeout=20)
    assert res["settled"] is True
    assert res["final_text"] == "laggy answer"
    assert h.events_reads >= 3  # empty reads then the real one


def test_spawn_via_primary_spawn_then_prompt():
    h = CountdownHive(waits_before_settle=0, final_text="x")
    res = h.client().drive(prompt="task", wall_timeout=20)
    assert h.spawn_calls == 1
    assert h.prompt_calls == 1


def test_prompt_error_raises():
    h = CountdownHive()
    with pytest.raises(HiveError, match="prompt"):
        h.client().prompt("ghost", "hi")


def test_steer_and_abort_noop_success():
    h = CountdownHive()
    c = h.client()
    c.steer("a1", "focus")  # should not raise
    c.abort("a1")           # should not raise


def test_get_tree_parses_bare_payload():
    h = CountdownHive()
    assert [n["id"] for n in h.client().get_tree()] == ["a1"]


def test_questions_readonly():
    h = CountdownHive()
    assert h.client().questions("a1") == []


def test_check_online_when_down():
    client = HiveClient(api_base="http://127.0.0.1:1", timeout=1.5)  # closed port
    assert client.check_online() is False


def test_port_not_hardcoded_via_host_port():
    """A caller can target a hive on any port via host/port (issue #12: the
    reference client must not hard-code the API port)."""
    h = CountdownHive(waits_before_settle=0, final_text="p")
    c = h.client(host="127.0.0.1", port=h.port)
    assert c.api_base == f"http://127.0.0.1:{h.port}"
    res = c.drive(prompt="hi", wall_timeout=10)
    assert res["settled"] is True


def test_api_base_env_override(monkeypatch):
    h = CountdownHive(waits_before_settle=0, final_text="e")
    monkeypatch.setenv("PI_HIVE_API_BASE", f"http://127.0.0.1:{h.port}")
    c = HiveClient()  # no args -> env var drives the base
    assert c.api_base == f"http://127.0.0.1:{h.port}"
    assert c.check_online() is True


def test_host_port_env_override(monkeypatch):
    h = CountdownHive(waits_before_settle=0, final_text="e2")
    monkeypatch.setenv("PI_HIVE_API_HOST", "127.0.0.1")
    monkeypatch.setenv("PI_HIVE_API_PORT", str(h.port))
    c = HiveClient()
    assert c.api_base == f"http://127.0.0.1:{h.port}"


def test_drive_locally_unreachable_raises():
    client = HiveClient(api_base="http://127.0.0.1:1", timeout=1.5)
    with pytest.raises(HiveError, match="not reachable"):
        client.drive(prompt="hi", wall_timeout=5)
