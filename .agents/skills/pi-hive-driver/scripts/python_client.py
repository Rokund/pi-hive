"""Reference Python client for driving a pi-hive over its WebSocket API.

This is the SKILL-reference client: it implements exactly what an AI agent
driving the hive by WebSocket needs, and nothing else. The hive is assumed to
already be running; this code does NOT start, configure, or query it over HTTP.

It speaks only the WebSocket protocol documented in SKILL.md:
  * commands  -> {"type": prompt|steer|follow_up|abort|get_tree|get_agent|subscribe}
  * responses -> {"type":"response", "command":..., "success":...}
  * output    -> {"type":"hive:event", "agentId":..., "event":{...}} (streams)

Only one third-party dependency: `websocket-client` (pip install websocket-client),
plus Python's stdlib for everything else. No `requests`, no external constants.
The correctness-critical frame-folding and spawn-discovery logic is shared with
the async daemon in `hive_protocol.py` (this directory) so the two drivers can
never drift apart on protocol invariants.

The `subagent_*` tools (spawn/result/abort/steer/followup/glimpse) are HTTP
endpoints that the PRIMARY calls via its extension — a WS-only driver normally
never touches them. The one exception documented here is peeking at an agent's
live output: it has NO WS command, only the HTTP endpoint
`POST /hive/agent/glimpse`, so the optional helper `HiveClient.agent_glimpse()`
below (alias `subagent_glimpse`) uses stdlib `urllib` to mirror the extension's
exact call shape. It is purely optional — driving primaries over WS never
needs it.

Deltas vs final: `message_update` frames are streamed deltas; the authoritative
final text of a turn is the assistant `message_end`. Completion is signaled by
the `agent_settled` event (authoritative) or a node status of idle (primary) /
done (subagent). Never treat `message_update` as a final answer. A dropped
connection aborts the drive with `HiveError` instead of spinning until the
wall timeout.
"""
from __future__ import annotations

import json
import time
from typing import Any, Optional
from urllib.parse import urlsplit

import websocket  # websocket-client  (pip install websocket-client)

from hive_protocol import TurnTracker, primary_ids_from_tree


class HiveError(Exception):
    """Raised for protocol / connection / timeout failures."""


class HiveClient:
    """Thin, WS-only driver for one hive. Thread-safety not guaranteed."""

    def __init__(self, ws_url: str = "ws://127.0.0.1:3001/ws", recv_timeout: float = 10.0):
        self.ws_url = ws_url
        self.recv_timeout = recv_timeout

    # ------------------------------------------------------------------ liveness
    def check_online(self) -> bool:
        """Best-effort liveness: opening a WebSocket is the online check."""
        ws = None
        try:
            ws = websocket.create_connection(self.ws_url, timeout=self.recv_timeout)
            return True
        except Exception:
            return False
        finally:
            if ws is not None:
                try:
                    ws.close()
                except Exception:
                    pass

    # ------------------------------------------------------------- WS primitives
    def _connect(self) -> websocket.WebSocket:
        return websocket.create_connection(self.ws_url, timeout=self.recv_timeout)

    @staticmethod
    def _send(ws: websocket.WebSocket, payload: dict) -> None:
        ws.send(json.dumps(payload, ensure_ascii=False))

    @staticmethod
    def _next_frame(ws: websocket.WebSocket):
        """Read one JSON frame; returns the parsed dict or None on recv timeout.

        None means "no frame arrived this poll" — callers may keep waiting. A
        CLOSED connection raises `HiveError` instead, so a drive loop aborts
        immediately rather than busy-spinning until its wall timeout. A frame
        that is not valid JSON is skipped (data is flowing, so no spin risk).
        """
        try:
            raw = ws.recv()
        except websocket.WebSocketTimeoutException:
            return None
        except (websocket.WebSocketConnectionClosedException, ConnectionError) as exc:
            raise HiveError(f"connection closed: {exc}") from exc
        if not raw:
            raise HiveError("connection closed: empty frame")
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return None

    # -------------------------------------------------------------------- tree
    def get_tree(self, idle_grace: float = 5.0) -> list[dict]:
        """Return the current node tree (send get_tree, read its response)."""
        ws = self._connect()
        try:
            self._send(ws, {"type": "get_tree"})
            deadline = time.time() + idle_grace
            while time.time() < deadline:
                frame = self._next_frame(ws)
                if frame and frame.get("type") == "response" and frame.get("command") == "get_tree":
                    return (frame.get("data") or {}).get("tree", [])
            raise HiveError("get_tree: no response within idle_grace")
        finally:
            try:
                ws.close()
            except Exception:
                pass

    def _primary_ids(self) -> set[str]:
        try:
            return primary_ids_from_tree(self.get_tree())
        except Exception:
            return set()

    # ------------------------------------------------------------------- drive
    def drive(
        self,
        prompt: str,
        agent_id: Optional[str] = None,
        cwd: Optional[str] = None,
        wall_timeout: float = 1800.0,
        collect: Optional[list] = None,
    ) -> dict:
        """Send a prompt and stream until the agent settles.

        * `agent_id` omitted -> starts a NEW primary conversation (bare prompt)
          and discovers its id from the event stream.
        * `agent_id` given   -> continues that conversation (prompt/steer).
        * `cwd`              -> working directory for a newly spawned primary
          (ignored by a targeted prompt; cwd is fixed at spawn).
        * `collect`          -> optional list every raw frame is appended to
          (for external audit trails).

        Returns a summary dict:
          {agent_id, settled, status, final_text (last assistant answer),
           transcript [all assistant texts], tool_calls (deduped), frame_count,
           duration_s}

        Completion is gated behind the response-ack barrier: settles and done
        node snapshots read BEFORE the `{"type":"response","command":"prompt"}`
        ack for OUR send belong to the previous turn and never complete this
        drive; only signals observed after the ack may. A dropped connection
        raises `HiveError` immediately; only recv timeouts are retried.

        Note: `settled` is the authoritative completion signal (backed by the
        agent's `agent_settled` event or a done node snapshot). `status` comes
        from the completing `agent_settled` payload (`{kind, status, terminal}`)
        — or, when the settle event carried no payload, from the completing
        node snapshot / last snapshot seen — so it no longer lags behind the
        settle signal.

        Frame folding (settle signals, `message_end` extraction, tool dedupe)
        and new-primary discovery are delegated to `TurnTracker` in
        `hive_protocol.py` — the shared, single source for those invariants.
        """
        if not self.check_online():
            raise HiveError("pi-hive not reachable at " + self.ws_url)

        pre = self._primary_ids()
        target: Optional[str] = agent_id
        tracker = TurnTracker(pre_primary_ids=pre)
        if target:
            tracker.claim(target)
        frames: list = []

        ws = self._connect()
        start = time.time()
        settled = False
        settle_signal: Optional[dict] = None
        acked = False
        try:
            payload: dict = {"type": "prompt", "text": prompt}
            if target:
                payload["agentId"] = target
            if cwd:
                payload["cwd"] = cwd
            self._send(ws, payload)

            while time.time() - start < wall_timeout:
                frame = self._next_frame(ws)
                if frame is None:
                    # Recv timeout only — a closed connection raises HiveError
                    # in _next_frame instead of busy-spinning here.
                    continue
                frames.append(frame)
                if collect is not None:
                    collect.append(frame)

                # Response-ack barrier: our prompt is only live once the
                # server has acked it on this socket.
                if frame.get("type") == "response" and frame.get("command") == "prompt":
                    acked = True

                signal = tracker.observe(frame)
                # Discover the freshly spawned primary if we asked for a new one.
                if target is None:
                    new = tracker.discover(frame, target)
                    if new is not None:
                        target = new
                        tracker.claim(new)
                # Completion: authoritative settled signal, or done node
                # status — but only from frames read after our prompt's ack.
                if acked and target and signal and signal.get("agent_id") == target:
                    settled = True
                    settle_signal = signal
                    break
        finally:
            try:
                ws.close()
            except Exception:
                pass

        status: Optional[str] = None
        if settle_signal is not None:
            status = settle_signal.get("status")
        if status is None:
            status = (tracker.observed.get(target) or {}).get("status")

        return {
            "agent_id": target,
            "settled": settled,
            "status": status,
            "final_text": tracker.final_texts[-1] if tracker.final_texts else "",
            "transcript": tracker.final_texts,
            "tool_calls": tracker.tool_calls,
            "frame_count": len(frames),
            "duration_s": round(time.time() - start, 1),
        }

    # ------------------------------------------- optional HTTP peek (glimpse) --
    # The glimpse endpoint is HTTP and exposed for ANY agent (primary or
    # subagent).  There is no WS command for a peek, so this optional helper
    # uses stdlib urllib (no third-party dependency).
    def _http_base(self) -> str:
        """HTTP API base derived from ws_url (same host/port; ws->http, wss->https)."""
        parts = urlsplit(self.ws_url)
        scheme = "https" if parts.scheme == "wss" else "http"
        return f"{scheme}://{parts.netloc}"

    def agent_glimpse(
        self,
        agent_id: str,
        n: int = 1024,
        api_base: Optional[str] = None,
    ) -> dict:
        """Peek at the tail of ANY agent's live produced text (HTTP-only).

        Works for both primaries and subagents. `api_base` defaults to the HTTP
        twin of `self.ws_url` (same host and port).

        Returns the hive payload: ``{ok, status, phase, complete, truncated,
        totalChars, text}`` (on transport/HTTP failure, ``{ok: False, error}``).
        * ``complete: False`` -> the text is a LIVE fragment (thinking or
          in-flight tool-call arguments), not a final answer.
        * ``n`` is clamped server-side to [1, 1024]; the server trims ``text``
          to the last ``n`` characters.
        * ``complete`` is the authoritative is-this-done signal — rely on it.
          ``status`` is only a hint and can lag the live state.
        * ``truncated: True`` only means the 8KB tail window exceeds ``n``; it
          does NOT mean the answer is cut off. For the full text, use the WS
          ``message_end`` event or ``subagent_result``'s ``result.finalText``.
        * ``totalChars`` is a monotonic per-process counter (== the
          subagent_result ``progress.liveOutputChars``), not the length of the
          returned text.
        * Works for any node id; a known-but-never-streamed node returns an
          empty text with a ``note``.
        """
        import urllib.request  # stdlib

        if api_base is None:
            api_base = self._http_base()
        url = f"{api_base}/hive/agent/glimpse"
        body = {"id": agent_id, "n": int(n)}
        req = urllib.request.Request(
            url,
            data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=self.recv_timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception as exc:  # HTTPError / URLError / JSONDecodeError
            return {"ok": False, "error": f"agent_glimpse failed: {exc}"}

    # Backward-compatible alias for the old method name ONLY: both names call
    # the canonical POST /hive/agent/glimpse endpoint (the legacy
    # /hive/subagent/glimpse alias exists server-side, not here).
    subagent_glimpse = agent_glimpse


def demo() -> None:
    """Minimal usage: drive a fresh primary in a target directory."""
    client = HiveClient()
    result = client.drive(
        prompt="Reply with exactly one short sentence about the Eiffel Tower.",
        # cwd is OPTIONAL. Omit it to let the hive's own startup default apply
        # (the daemon was launched with --cwd <dir> or PI_HIVE_CWD). Passing a
        # path here only takes effect when it spawns a brand-new primary, so
        # this client intentionally stays location-independent (no hardcoded
        # absolute path).
        cwd=None,
        wall_timeout=120.0,
    )
    print("agent_id:", result["agent_id"])
    print("settled:", result["settled"])
    print("status:", result["status"])
    print("final_text:", result["final_text"])
    print("tool_calls:", result["tool_calls"])


if __name__ == "__main__":
    demo()
