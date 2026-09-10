"""pi-hive driver CLI — drive a running pi-hive over plain HTTP (no WebSocket).

This is THE tool (issue #12) for an AI agent to drive the hive over HTTP:

  * spawn a new primary conversation,
  * prompt / steer / follow_up / abort an agent,
  * wait for an agent's turn to settle (long-poll, no sleep-loop),
  * read the conversation output (events) and peek at live output,
  * read an agent's in-flight / recent Q&A (read-only, issue #9).

The webSocket channel of pi-hive is reserved for the web GUI (Port 1 mirror).
External callers NEVER touch a socket: they only issue one-shot HTTP requests
and use the long-poll `POST /hive/agent/wait` to block for completion. No
connection lifecycle to maintain on the caller's side — every call opens and
closes its own request.

Only Python's stdlib is used (`urllib`). No third-party dependency
(`requests`, `httpx`, or `websocket-client` are all absent). Thread-safety is
not guaranteed.

The external-driving response dialect is the bare `{ok, ...}` form, requested
via the request header `Accept: application/vnd.hive.bare+json` on the `/api`
command endpoints (issue #8 decision). The client always sends that header, so
it never has to parse the WS `{type:"response"}` envelope.
"""
from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

#: Header used to opt the /api command endpoints into the bare {ok,...} dialect.
BARE_ACCEPT = "application/vnd.hive.bare+json"


def _env_int(name: str) -> Optional[int]:
    """Read a positive-int env var, or None if unset/invalid."""
    raw = os.environ.get(name)
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


class HiveError(Exception):
    """Raised for transport / protocol / timeout failures."""


def _req_json(url: str, *, method: str = "GET", body: Any = None,
              timeout: float = 30.0) -> Any:
    """Issue one HTTP request and parse the JSON response. Raises HiveError on
    transport or HTTP errors, or when the body is not valid JSON."""
    headers = {
        "Content-Type": "application/json",
        "Accept": BARE_ACCEPT,
    }
    data = None if body is None else json.dumps(body, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError,
            ConnectionError) as exc:
        raise HiveError(f"{method} {url} failed: {exc}") from exc
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise HiveError(f"{method} {url}: invalid JSON response: {exc}") from exc


class HiveClient:
    """HTTP-only driver for one hive. Thread-safety not guaranteed.

    The hive's API port is configurable (`hive.config.json` -> ``server.apiPort``)
    and is NOT hard-coded here so this client is never tied to one deployment.
    Pass `host`/`port` explicitly (or `api_base`), or omit them to fall back, in
    order, to the `PI_HIVE_API_HOST` / `PI_HIVE_API_PORT` env vars and then the
    `PI_HIVE_API_BASE` env var.  If none are set, the CLI flag is ``--api-base``.
    """

    def __init__(
        self,
        api_base: Optional[str] = None,
        *,
        host: Optional[str] = None,
        port: Optional[int] = None,
        timeout: float = 30.0,
    ):
        if api_base is None:
            base = os.environ.get("PI_HIVE_API_BASE") or ""
            if base:
                api_base = base
            else:
                h = host or os.environ.get("PI_HIVE_API_HOST") or "127.0.0.1"
                p = port or _env_int("PI_HIVE_API_PORT") or 3001
                api_base = f"http://{h}:{p}"
        self.api_base = api_base.rstrip("/")
        self.timeout = timeout

    # ------------------------------------------------------------------ liveness
    def check_online(self) -> bool:
        """Best-effort liveness: GET /api/health succeeds."""
        try:
            return bool(_req_json(f"{self.api_base}/api/health", timeout=self.timeout))
        except Exception:
            return False

    # -------------------------------------------------------------------- tree
    def get_tree(self) -> List[Dict[str, Any]]:
        """Current node tree (bare `/api/tree`, `{ok, tree: [...]}`)."""
        body = _req_json(f"{self.api_base}/api/tree", timeout=self.timeout)
        if not body.get("ok"):
            raise HiveError(f"get_tree failed: {body.get('error')}")
        return body.get("tree", [])

    def get_agent(self, agent_id: str) -> Dict[str, Any]:
        """One node's current state (bare `/api/agent/{id}`, `{ok, ...node}`)."""
        body = _req_json(f"{self.api_base}/api/agent/{agent_id}", timeout=self.timeout)
        if not body.get("ok"):
            raise HiveError(f"get_agent {agent_id} failed: {body.get('error')}")
        return body

    # ------------------------------------------------------------------- spawn
    def spawn(self, *, cwd: Optional[str] = None, agent: Optional[str] = None,
              model: Optional[str] = None) -> Dict[str, Any]:
        """Start a NEW primary conversation (a new root in the agent tree).

        Returns the created node (must contain its `id`). ``agent`` optionally
        names a primary-eligible profile to run; ``model``/``cwd`` are optional
        overrides honored only at spawn.
        """
        payload: Dict[str, Any] = {}
        if cwd is not None:
            payload["cwd"] = cwd
        if agent is not None:
            payload["agent"] = agent
        if model is not None:
            payload["model"] = model
        body = _req_json(
            f"{self.api_base}/api/primary/spawn", method="POST",
            body=payload, timeout=self.timeout,
        )
        if not body.get("ok") or not body.get("id"):
            raise HiveError(f"spawn failed: {body.get('error')}")
        return body

    # ------------------------------------------------------------ send/turn cmds
    def prompt(self, agent_id: str, message: str,
               images: Optional[List[Dict[str, Any]]] = None) -> None:
        """Send a task / continue an existing conversation (bare `/api/prompt`)."""
        payload: Dict[str, Any] = {"agent": agent_id, "message": message}
        if images:
            payload["images"] = images
        body = _req_json(
            f"{self.api_base}/api/prompt", method="POST", body=payload,
            timeout=self.timeout,
        )
        if not body.get("ok"):
            raise HiveError(f"prompt failed: {body.get('error')}")

    def steer(self, agent_id: str, message: str) -> None:
        """Mid-stream guidance to a running agent."""
        body = _req_json(
            f"{self.api_base}/api/steer", method="POST",
            body={"agent": agent_id, "message": message}, timeout=self.timeout,
        )
        if not body.get("ok"):
            raise HiveError(f"steer failed: {body.get('error')}")

    def follow_up(self, agent_id: str, message: str) -> None:
        """Queue a follow-up (delivered once the agent finishes)."""
        body = _req_json(
            f"{self.api_base}/api/follow_up", method="POST",
            body={"agent": agent_id, "message": message}, timeout=self.timeout,
        )
        if not body.get("ok"):
            raise HiveError(f"follow_up failed: {body.get('error')}")

    def abort(self, agent_id: str, reason: Optional[str] = None) -> None:
        """Abort a running (or done/idle/aborted/failed) agent — no-op if terminal."""
        payload: Dict[str, Any] = {"agent": agent_id}
        if reason:
            payload["reason"] = reason
        body = _req_json(
            f"{self.api_base}/api/abort", method="POST", body=payload,
            timeout=self.timeout,
        )
        if not body.get("ok"):
            raise HiveError(f"abort failed: {body.get('error')}")

    # -------------------------------------------------------- wait / completion
    def wait(self, agent_id: str, wait_time_ms: int = 0) -> Dict[str, Any]:
        """Long-poll `POST /hive/agent/wait` for an agent's turn to settle.

        0 = return the current state immediately. A still-running agent returns
        `{ok, id, status:"running", progress:{...}}`; a settled one returns the
        node status + result payload. Use this instead of sleep-polling
        `get_agent` — it blocks server-side up to `wait_time_ms` and is the
        ONLY sanctioned way to detect completion over HTTP.
        """
        body = _req_json(
            f"{self.api_base}/hive/agent/wait", method="POST",
            body={"id": agent_id, "wait_time": int(wait_time_ms)},
            timeout=self.timeout,
        )
        if not body.get("ok"):
            raise HiveError(f"wait {agent_id} failed: {body.get('error')}")
        return body

    def drive(
        self,
        prompt: str,
        agent_id: Optional[str] = None,
        cwd: Optional[str] = None,
        wall_timeout: float = 1800.0,
        wait_step_ms: int = 2000,
    ) -> Dict[str, Any]:
        """Spawn (or target) an agent, send a task, and block until it settles.

        * `agent_id` omitted -> spawns a NEW primary (via `/api/primary/spawn`)
          in `cwd` (if given) and targets it.
        * `agent_id` given   -> targets that existing conversation.
        * completion is detected with the long-poll `/hive/agent/wait` in a
          loop (each call blocks up to `wait_step_ms`, returns early on
          settle; NO busy sleep-poll).
        * wall_timeout bounds the whole drive.

        Returns a summary dict:
          {agent_id, settled, status, final_text, transcript, frame_count,
           duration_s}
        """
        if not self.check_online():
            raise HiveError("pi-hive not reachable at " + self.api_base)

        target = agent_id
        frames: list = []

        if target is None:
            node = self.spawn(cwd=cwd)
            target = node["id"]

        # Send the task to the (new or existing) primary.
        self.prompt(target, prompt)
        start = time.time()
        final_text = ""
        transcript: List[str] = []
        status: Optional[str] = None
        settled = False

        while time.time() - start < wall_timeout:
            # Check tool call: long-poll waits on the agent; returns early on settle.
            st = self.wait(target, wait_step_ms)
            frames.append(st)
            node_status = st.get("status")
            status = node_status or status
            if st.get("ok") and node_status != "running":
                settled = True
                result = st.get("result") or {}
                final_text = result.get("finalText") or final_text
                break
            if not st.get("ok"):
                raise HiveError(f"wait {target} failed: {st.get('error')}")

        if settled:
            # Pull the authoritative transcripts from the event backlog.
            # On the real hive the settle signal can beat the event-record
            # flush by a moment, so poll briefly for the message_end(s) rather
            # than trusting a single read. The events endpoint is the only
            # sanctioned source of final text over HTTP.
            transcript = self._read_final_texts_with_retry(
                target, deadline=start + wall_timeout
            )
            if final_text not in transcript:
                # Prefer the settled final text when the event replay lacks it.
                transcript = [final_text] if final_text else transcript
                final_text = transcript[-1] if transcript else final_text
            else:
                final_text = transcript[-1] if transcript else final_text

        return {
            "agent_id": target,
            "settled": settled,
            "status": status,
            "final_text": final_text,
            "transcript": transcript,
            "frame_count": len(frames),
            "duration_s": round(time.time() - start, 1),
        }

    def _read_final_texts_with_retry(self, agent_id: str, *, deadline: float,
                                     retry_delay: float = 0.5) -> List[str]:
        """Read the agent's final assistant texts, retrying briefly.

        The settle signal (from /hive/agent/wait) can arrive just before the
        corresponding message_end is recorded/queryable in the event backlog.
        Poll the events endpoint until it returns at least one message_end or
        ``deadline`` passes. Returns the collected texts (possibly empty if
        the backlog never produced a message_end).
        """
        texts: List[str] = []
        while True:
            texts = self._final_texts(agent_id)
            if texts:
                return texts
            if time.time() >= deadline:
                return texts
            time.sleep(retry_delay)

    def _final_texts(self, agent_id: str, since: int = 0) -> List[str]:
        """Assistant `message_end` texts from the event backlog, in order."""
        qs = urlencode({"since": since})
        body = _req_json(
            f"{self.api_base}/api/agent/{agent_id}/events?{qs}",
            timeout=self.timeout,
        )
        if not body.get("ok"):
            return []
        texts: List[str] = []
        for ev in body.get("events", []):
            event: Dict[str, Any] = ev.get("event", {})
            if event.get("type") == "message_end":
                message = event.get("message", {})
                if message.get("role") in (None, "assistant"):
                    content = message.get("content") or []
                    for part in content:
                        if part.get("type") == "text":
                            texts.append(part.get("text", ""))
        return texts

    # ------------------------------------------------------------ optional peek
    def agent_glimpse(self, agent_id: str, n: int = 1024) -> Dict[str, Any]:
        """Peek at the tail of ANY agent's live produced text (HTTP-only).

        Returns ``{ok, status, phase, complete, truncated, totalChars, text}``.
        ``complete:false`` = live fragment, never a final answer; rely on
        ``complete``, not ``status``. ``n`` is clamped server-side to [1,1024].
        """
        body = _req_json(
            f"{self.api_base}/hive/agent/glimpse", method="POST",
            body={"id": agent_id, "n": int(n)}, timeout=self.timeout,
        )
        if not body.get("ok"):
            raise HiveError(f"glimpse {agent_id} failed: {body.get('error')}")
        return body

    # Backward-compatible alias: the old name hits the same canonical endpoint.
    subagent_glimpse = agent_glimpse

    # ------------------------------------------------------------- Q&A (read)
    def questions(self, agent_id: str) -> List[Dict[str, Any]]:
        """Read-only visibility into the questions an agent ASKED (issue #9):
        pending first, then recently-answered, bounded by store retention.
        Returns a copy list; the driver never writes here.
        """
        body = _req_json(
            f"{self.api_base}/api/agent/{agent_id}/questions", timeout=self.timeout,
        )
        if not body.get("ok"):
            raise HiveError(f"questions {agent_id} failed: {body.get('error')}")
        return body.get("questions", [])


def demo() -> None:
    """CLI entry point — the one tool for driving a pi-hive over HTTP.

    This is THE driver (not a "reference"/"demo" to adapt): every action has a
    subcommand with concrete parameters that just works against the configured
    hive. Subcommands:

      drive      spawn (or target) an agent, send a task, block until it
                 settles, print the answer.
      spawn      start a NEW primary conversation.
      prompt     send a task / continue an existing conversation.
      steer      mid-stream guidance to a running agent.
      abort      abort an agent.
      wait       long-poll for an agent's turn to settle.
      tree       list the agent tree.
      agent      show one node.
      glimpse    peek at an agent's live output.
      questions  list the Q&A an agent asked (read-only).

    Host/port are never hard-coded: pass ``--host``/``--port``/``--api-base`` or
    set PI_HIVE_API_HOST / PI_HIVE_API_PORT / PI_HIVE_API_BASE.
    """
    import argparse

    parser = argparse.ArgumentParser(
        prog="pi-hive-driver",
        description="Drive a running pi-hive over HTTP.",
    )
    parser.add_argument("--api-base", help="full base URL, e.g. http://host:port")
    parser.add_argument("--host", default=None, help="hive API host (default 127.0.0.1)")
    parser.add_argument("--port", type=int, default=None, help="hive API port")
    parser.add_argument("--json", action="store_true", help="print structured JSON output")
    sub = parser.add_subparsers(dest="command", required=True)

    p_drive = sub.add_parser("drive", help="send a task and block for the answer")
    p_drive.add_argument("--prompt", required=True)
    p_drive.add_argument("--id", help="target an existing agent; omit to spawn a new primary")
    p_drive.add_argument("--cwd", default=None)
    p_drive.add_argument("--wall-timeout", type=float, default=1800.0)

    p_spawn = sub.add_parser("spawn", help="start a new primary conversation")
    p_spawn.add_argument("--cwd", default=None)
    p_spawn.add_argument("--agent", default=None, help="primary-eligible profile to run")
    p_spawn.add_argument("--model", default=None)

    p_prompt = sub.add_parser("prompt", help="send a task / continue a conversation")
    p_prompt.add_argument("--id", required=True)
    p_prompt.add_argument("--message", required=True)

    p_steer = sub.add_parser("steer", help="mid-stream guidance to a running agent")
    p_steer.add_argument("--id", required=True)
    p_steer.add_argument("--message", required=True)

    p_abort = sub.add_parser("abort", help="abort an agent")
    p_abort.add_argument("--id", required=True)
    p_abort.add_argument("--reason", default=None)

    p_wait = sub.add_parser("wait", help="long-poll for an agent to settle")
    p_wait.add_argument("--id", required=True)
    p_wait.add_argument("--wait-time-ms", type=int, default=0)

    sub.add_parser("tree", help="list the agent tree")

    p_agent = sub.add_parser("agent", help="show one node")
    p_agent.add_argument("--id", required=True)

    p_glimpse = sub.add_parser("glimpse", help="peek at an agent's live output")
    p_glimpse.add_argument("--id", required=True)
    p_glimpse.add_argument("--n", type=int, default=1024)

    p_questions = sub.add_parser("questions", help="list Q&A an agent asked (read-only)")
    p_questions.add_argument("--id", required=True)

    args = parser.parse_args()
    client = HiveClient(
        api_base=args.api_base,
        host=args.host,
        port=args.port,
    )

    if args.command == "drive":
        result = client.drive(
            prompt=args.prompt,
            agent_id=args.id,
            cwd=args.cwd,
            wall_timeout=args.wall_timeout,
        )
        _emit(args, result)
    elif args.command == "spawn":
        _emit(args, client.spawn(cwd=args.cwd, agent=args.agent, model=args.model))
    elif args.command == "prompt":
        client.prompt(args.id, args.message)
        _emit(args, {"ok": True, "id": args.id})
    elif args.command == "steer":
        client.steer(args.id, args.message)
        _emit(args, {"ok": True, "id": args.id})
    elif args.command == "abort":
        client.abort(args.id, reason=args.reason)
        _emit(args, {"ok": True, "id": args.id})
    elif args.command == "wait":
        _emit(args, client.wait(args.id, args.wait_time_ms))
    elif args.command == "tree":
        _emit(args, {"tree": client.get_tree()})
    elif args.command == "agent":
        _emit(args, client.get_agent(args.id))
    elif args.command == "glimpse":
        _emit(args, client.agent_glimpse(args.id, n=args.n))
    elif args.command == "questions":
        _emit(args, {"questions": client.questions(args.id)})


def _emit(args: Any, payload: Dict[str, Any]) -> None:
    """Print a result: structured JSON with --json, else a compact human line."""
    if getattr(args, "json", False):
        print(json.dumps(payload, ensure_ascii=False, default=str))
        return
    if isinstance(payload, dict) and payload.get("ok") is False:
        print(f"ERROR: {payload.get('error')}")
        return
    if args.command == "drive":
        print("agent_id:", payload.get("agent_id"))
        print("settled:", payload.get("settled"))
        print("status:", payload.get("status"))
        print("final_text:", payload.get("final_text"))
        return
    print(json.dumps(payload, ensure_ascii=False, default=str))


if __name__ == "__main__":
    demo()
