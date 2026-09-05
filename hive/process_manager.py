"""Subprocess lifecycle for pi RPC agents.

`PiSubprocess` spawns a single `pi --mode rpc` child and speaks the JSONL RPC
protocol over stdin/stdout (see `pi/packages/coding-agent/docs/rpc.md`):

* Framing: records are delimited by `\\n` only; a trailing `\\r` is stripped;
  partial lines are buffered.  We do **not** split on U+2028/U+2029.
* Non-JSON / broken lines are skipped with a warning instead of killing the reader.
* `extension_ui_request` dialog events (including pi's own project-trust prompt) are
  auto-responded so the subprocess never blocks waiting for a GUI (M4 may forward them).
* After spawn we send `get_state` and store the *real* `sessionId` / `sessionFile`.

Executable resolution: `shutil.which("pi")`.  On Windows the npm shim is a
`.cmd`/`.ps1` batch file that `Popen(["pi", ...])` cannot launch via
CreateProcess, so we spawn through `cmd.exe /c` (via COMSPEC) instead.
We read **bytes** and decode UTF-8 ourselves because `asyncio.create_subprocess_exec`
does not accept an `encoding=` kwarg on CPython.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import shutil
import sys
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Optional, Set

from .agent_graph import name_allowed
from .config import HiveConfig
from .models import AgentNode, AgentProfile

logger = logging.getLogger("hive.process_manager")

# Callback type: on_event(agent_id: str, event: dict) -> None
EventCallback = Callable[[str, Dict[str, Any]], Awaitable[None] | None]

# The subagent extension registered into every pi child (absolute path, since
# subagent cwds differ from the hive cwd).  See hive/extensions/subagent.ts.
EXTENSION_SUBAGENT = str(Path(__file__).resolve().parent / "extensions" / "subagent.ts")

# Extension tools that any agent able to spawn subagents must have active.  pi
# gates tools registered by extensions behind the `--tools` allowlist, so these
# two names are merged into the CSV for any agent whose `agent_allowlist` is
# non-empty (i.e. that may itself spawn subagents — M5 nesting).
# Note: an EMPTY agent_allowlist means deny-all (leaf node), so those agents
# do NOT get the subagent tools; ["*"] means allow-all.
SUBAGENT_TOOLS = ("subagent_spawn", "subagent_result", "subagent_abort", "subagent_followup", "subagent_steer", "subagent_glimpse")

# Standard locations searched when resolving a configured skill NAME into a
# real directory path that pi will accept (`--skill <path>`). pi does not
# resolve names — it requires an existing path — so the hive maps them.
USER_SKILL_DIRS = (
    os.path.join(os.path.expanduser("~"), ".pi", "agent", "skills"),
    os.path.join(os.path.expanduser("~"), ".agents", "skills"),
)
PROJECT_SKILL_SUBDIRS = ((".pi", "skills"), (".agents", "skills"))

DEFAULT_HIVE_API_BASE = "http://127.0.0.1:3001"


# Upper bound on the in-memory live-output tail kept per subagent (only the
# last this-many chars are retained; enough for subagent_glimpse's 1K peek and
# for a meaningful liveOutputChars while keeping memory constant).
LIVE_TEXT_MAX = 8192

# Best-effort labels of what a subagent is currently doing, derived from the
# event stream. Supplementary to the moving counters, never a substitute for
# them (see docs/adr and subagent_result's progressive fields).
PHASE_GENERATING = "generating"
PHASE_THINKING = "thinking"
PHASE_TOOLCALLING = "toolcalling"
PHASE_TOOL_RUNNING = "tool_running"


def _append_live_text(st: "_AgentState", text: str) -> None:
    """Append streamed output to a subagent's bounded live tail + char counter."""
    if not text:
        return
    st.liveText = (st.liveText + text)[-LIVE_TEXT_MAX:]
    st.liveOutputChars += len(text)


def _usage_is_noise(usage: Any) -> bool:
    """True when a usage snapshot has no positive counter anywhere.

    Some providers/servers (notably local OpenAI-compatible / vLLM endpoints)
    report a zero-filled usage object on every streamed snapshot and only fill
    real numbers on the final ``done`` event. Emitting that all-zero object in
    ``subagent_result.progress`` reads as "stalled" while the subagent is
    actually producing, so callers only learn about ``usage`` once it is
    informative. None / non-dict / non-numeric values are treated as noise.
    """
    if not isinstance(usage, dict):
        return True
    stack = list(usage.values())
    while stack:
        v = stack.pop()
        if isinstance(v, dict):
            stack.extend(v.values())
        elif isinstance(v, (list, tuple)):
            stack.extend(v)
        elif isinstance(v, (int, float)) and v > 0:
            return False
    return True


# enabled whenever any MCP server is allowed (ADR-0002).
MCP_GATEWAY_TOOLS = ("mcp", "mcpScript")

# Where the user's MCP servers are defined (pi-mcp-adapter reads the same
# file). The hive treats its keys as the single source of truth when validating
# a per-agent ``mcp`` allowlist; pi itself adds no server provisioning.
MCP_CONFIG_CANDIDATES = (Path.home() / ".pi" / "agent" / "mcp.json",)


def load_mcp_servers() -> Set[str]:
    """Names of the MCP servers configured for this user (keys of
    ~/.pi/agent/mcp.json). Empty set when none / unreadable."""
    for p in MCP_CONFIG_CANDIDATES:
        try:
            if p.is_file():
                data = json.loads(p.read_text(encoding="utf-8"))
                servers = (data or {}).get("mcpServers") or {}
                return set(servers.keys())
        except Exception:  # noqa: BLE001
            logger.debug("could not read MCP config %s", p)
    return set()


def _mcp_proxy_name(server: str) -> str:
    """Replicate pi-mcp-adapter's ``namespaceProxyName`` so the exact
    ``--tools`` entry pi registers matches (ADR-0002). "-" collapses to "_";
    any character outside [A-Za-z0-9_] is hex-encoded behind the ``_mcpns_``
    marker, mirroring the adapter's TS implementation."""
    normalized = server.replace("-", "_")
    marker = "_mcpns_"
    if (
        normalized
        and re.fullmatch(r"[A-Za-z0-9_]+", normalized)
        and not normalized.startswith(marker)
    ):
        return f"mcp__{normalized}"
    part = marker + "_".join(f"{ord(c):x}" for c in normalized)
    return f"mcp__{part}"


def mcp_tools_for(mcp_allowlist: List[str], servers: Set[str]) -> List[str]:
    """Concrete tool names to merge into ``--tools`` for an ``mcp`` allowlist.

    ``"*"`` in the allowlist enables EVERY configured MCP server (mirroring the
    allow-all sentinel used by ``tools`` / ``agent_allowlist``). Otherwise each
    entry is a server name; unknown names are logged (warning) and skipped
    rather than failing startup, so a stale/misspelled entry cannot brick an
    agent launch. Note this is distinct from ``tools: ["*"]``, which in pi
    means "every tool is visible" — that already unlocks all MCP tools without
    any ``mcp`` entry.
    """
    if not mcp_allowlist:
        return []
    names: List[str] = []
    if "*" in mcp_allowlist:
        # Allow-all: every configured server. Explicit entries are redundant
        # under "*" (results are deduplicated downstream by the caller).
        for s in sorted(servers):
            names.append(_mcp_proxy_name(s))
    else:
        for s in mcp_allowlist:
            s = (s or "").strip()
            if not s:
                continue
            if s in servers:
                names.append(_mcp_proxy_name(s))
            else:
                logger.warning(
                    "agent mcp allowlist references unknown server %r "
                    "(not in %s); skipping",
                    s, MCP_CONFIG_CANDIDATES[0],
                )
    if names:
        names += list(MCP_GATEWAY_TOOLS)
    return names


def _now_ms() -> int:
    return int(time.time() * 1000)


def is_windows() -> bool:
    return sys.platform == "win32" or os.name == "nt"


def resolve_pi() -> str:
    """Locate the `pi` executable on PATH."""
    exe = shutil.which("pi")
    if not exe:
        raise RuntimeError(
            "pi executable not found on PATH. Install pi or add it to PATH."
        )
    return exe


def resolve_skill_path(entry: str, cwd: Optional[str]) -> Optional[str]:
    """Resolve a configured skill entry to a path pi can load via ``--skill``.

    pi's ``loadSkills`` requires an EXISTING path; a bare name is resolved
    against the cwd and silently skipped if absent (no name search, no regex).
    So the hive maps each entry before injecting:

    - an existing absolute / cwd-relative path is passed through;
    - otherwise the entry is treated as a NAME and looked up in the standard
      user (~/.pi/agent/skills, ~/.agents/skills) and project
      (<cwd>/.pi/skills, <cwd>/.agents/skills) directories;
    - an unresolvable entry returns None (caller warns and skips it).
    """
    entry = (entry or "").strip()
    if not entry or entry == "*":
        return entry if entry == "*" else None
    cwd_ = cwd or os.getcwd()
    if os.path.isabs(entry):
        return entry if os.path.exists(entry) else None
    rel = os.path.join(cwd_, entry)
    if os.path.exists(rel):
        return rel
    # Name lookup in the standard locations.
    candidates: List[str] = [
        os.path.join(d, entry) for d in USER_SKILL_DIRS
    ]
    if cwd:
        candidates += [os.path.join(cwd, *sub, entry) for sub in PROJECT_SKILL_SUBDIRS]
    for cand in candidates:
        if os.path.isdir(cand) and os.path.isfile(os.path.join(cand, "SKILL.md")):
            return cand
    return None


def build_rpc_args(profile: AgentProfile, node_id: str, session_file: Optional[str] = None, cwd: Optional[str] = None) -> List[str]:
    """Build the `pi --mode rpc ...` argument list for a profile.

    Flags map from the profile per SPEC §8: model -> --model, thinking ->
    --thinking, tools -> --tools (csv), systemPrompt -> --system-prompt,
    session id (hive id) -> --session-id.  We deliberately do NOT pass
    --no-session: session files must persist for restore (SPEC §7.1).

    When `session_file` is given (resurrecting a persisted node), `--session
    <file>` is passed instead so pi loads the recorded history and the
    conversation continues with full context; pi keeps the same session id
    (== hive node id), so the tree stays consistent across restarts.

    The subagent extension is always injected via `--extension`, and the
    subagent tools are merged into the `--tools` allowlist for any agent with
    a non-empty `agent_allowlist` (including the primary), so nesting (M5)
    does not silently break: extension-registered tools are gated behind
    `--tools`.
    """
    args: List[str] = ["--mode", "rpc"]

    if profile.model:
        args += ["--model", profile.model]
    if profile.thinking:
        args += ["--thinking", profile.thinking]

    # ---- tools allowlist ("*" = allow all, [] = deny all, values = exact
    #      names passed straight through) ---------------------------------
    raw_tools = profile.tools or []
    if "*" in raw_tools:
        # allow-all: omit so pi keeps every tool (incl. extension tools).
        pass
    else:
        tools = [t for t in raw_tools if t]
        # Keep subagent nesting working: an agent that may spawn subagents
        # must also have the subagent extension tools active.
        if profile.agent_allowlist:
            for tool in SUBAGENT_TOOLS:
                if tool not in tools:
                    tools.append(tool)
        # MCP visibility (ADR-0002): merge the concrete tool names for every
        # allowed MCP server (the stable "mcp__<server>" namespace proxy) plus
        # the mcp/mcpScript gateway helpers into the allowlist. pi gates
        # extension/MCP tools behind exact-name allowlists, so this is the sole
        # mechanism that makes an allowed MCP server visible to this agent.
        for tool in mcp_tools_for(list(profile.mcp or []), load_mcp_servers()):
            if tool not in tools:
                tools.append(tool)
        if tools:
            args += ["--tools", ",".join(tools)]
        else:
            args += ["--no-tools"]

    # ---- skills allowlist ("*" = allow all, [] = deny all, values = the
    #      only skills loaded, injected as resolved paths) ----------------
    raw_skills = profile.skills or []
    if "*" in raw_skills:
        # allow-all: rely on pi's default skill discovery.
        pass
    elif not raw_skills:
        # deny-all: no skills discovered.
        args += ["--no-skills"]
    else:
        args += ["--no-skills"]
        resolved = 0
        for s in raw_skills:
            if not s or s == "*":
                continue
            p = resolve_skill_path(s, cwd)
            if p is None:
                logger.warning(
                    "agent %s: skill %r could not be resolved; skipping",
                    profile.name, s,
                )
                continue
            args += ["--skill", p]
            resolved += 1
        if resolved == 0:
            # Every entry failed to resolve — fall back to allow-all rather
            # than silently handing pi an agent with zero skills.
            args.remove("--no-skills")

    if profile.systemPrompt:
        # Collapse all newlines/whitespace to single spaces. On Windows `pi` is a
        # .CMD shim, so the child is spawned as `cmd /c <pi> ... --system-prompt
        # ... --extension ...`. cmd.exe treats a literal newline inside that
        # command line as a command separator (list2cmdline does not escape it),
        # which truncates the command AT the newline and silently drops every
        # later flag — notably `--extension <subagent.ts>`. The subagent tools
        # then never register and the agent reports it has no subagent ability.
        # A system prompt's line breaks are cosmetic, so collapsing them onto one
        # line keeps every flag on the same cmd line with unchanged meaning.
        prompt = " ".join(profile.systemPrompt.split())
        args += ["--system-prompt", prompt]
    if session_file:
        # Restore mode: reload the persisted history. pi adopts the session
        # id from the file (which equals our node id), keeping ids stable.
        args += ["--session", session_file]
    else:
        args += ["--session-id", node_id]
    if profile.name != "primary":
        args += ["--name", profile.name]
    args += ["--extension", EXTENSION_SUBAGENT]
    return args


def build_spawn_argv(pi_exe: str, rpc_args: List[str]) -> List[str]:
    """Wrap the pi executable argv for cross-platform launching of the shim."""
    if is_windows() and pi_exe.lower().endswith((".cmd", ".bat", ".ps1")):
        comspec = os.environ.get("COMSPEC") or "cmd.exe"
        return [comspec, "/c", pi_exe, *rpc_args]
    return [pi_exe, *rpc_args]


class PiSubprocess:
    """One pi RPC child process + JSONL reader + writer."""

    def __init__(
        self,
        node: AgentNode,
        *,
        on_event: EventCallback,
        pi_exe: Optional[str] = None,
        hive_api_base: str = DEFAULT_HIVE_API_BASE,
        subagents_env: str = "",
    ) -> None:
        self.node = node
        self.on_event = on_event
        self.pi_exe = pi_exe or resolve_pi()
        self.hive_api_base = hive_api_base
        # JSON list of the named subagents THIS agent may spawn (from its
        # agent_allowlist + the hive config), injected as PI_HIVE_SUBAGENTS so the
        # subagent extension can advertise valid names in its tool schema.
        self.subagents_env = subagents_env

        self.proc: Optional[asyncio.subprocess.Process] = None
        self._reader_task: Optional[asyncio.Task] = None
        self._stderr_task: Optional[asyncio.Task] = None
        self._buffer = bytearray()
        self._write_lock = asyncio.Lock()
        self._pending: Dict[str, asyncio.Future] = {}
        # Wired by ProcessManager: invoked once when stdout closes WITHOUT a
        # deliberate close() (i.e. the child crashed / was killed externally)
        # so the hive can fail the node instead of leaving it running forever.
        self.on_exit: Optional[Any] = None
        self._spawned = asyncio.Event()
        self._closed = False

    # -- lifecycle --------------------------------------------------------
    async def spawn(self) -> "PiSubprocess":
        # Restore mode: when the node already carries a persisted sessionFile,
        # pass --session <file> so pi reloads the recorded history (the
        # conversation continues with full context; ids stay stable).
        session_file = self.node.sessionFile or None
        argv = build_spawn_argv(
            self.pi_exe,
            build_rpc_args(self.node.profile, self.node.id, session_file, self.node.cwd),
        )
        env = dict(os.environ)
        env.setdefault("PI_HIVE_API_BASE", self.hive_api_base)
        if self.subagents_env:
            env.setdefault("PI_HIVE_SUBAGENTS", self.subagents_env)
        logger.debug("spawning: %s", " ".join(argv))
        self.proc = await asyncio.create_subprocess_exec(
            *argv,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=self.node.cwd or None,
            env=env,
        )
        self._spawned.set()
        self._reader_task = asyncio.create_task(self._read_loop())
        self._stderr_task = asyncio.create_task(self._stderr_loop())
        # After spawn, learn the real session id/file (SPEC §7.1 — do not guess).
        try:
            await asyncio.wait_for(self._query_state(), timeout=15)
        except Exception as exc:  # noqa: BLE001
            logger.warning("get_state for %s failed: %s", self.node.id, exc)
        # The process is now live/loaded. For a lazily-loaded restored session
        # this flips node.loaded True, marking it as materialized in the tree.
        self.node.loaded = True
        return self

    async def _query_state(self) -> None:
        resp = await self.send({"type": "get_state"}, expect_response=True)
        data = (resp or {}).get("data") or {}
        session_id = data.get("sessionId")
        session_file = data.get("sessionFile")
        if session_id:
            self.node.id = session_id
        if session_file:
            self.node.sessionFile = session_file
        logger.info(
            "agent %s -> sessionId=%s sessionFile=%s",
            self.node.id, session_id, session_file,
        )

    # -- I/O write --------------------------------------------------------
    async def send(
        self, cmd: Dict[str, Any], expect_response: bool = False
    ) -> Optional[Dict[str, Any]]:
        """Write a command object as one JSONL record (UTF-8 + `\\n`)."""
        if self.proc is None or self.proc.stdin is None:
            raise RuntimeError("subprocess not spawned")
        fut: Optional[asyncio.Future] = None
        if expect_response:
            cid = cmd.get("id") or uuid.uuid4().hex
            cmd["id"] = cid
            fut = asyncio.get_running_loop().create_future()
            self._pending[cid] = fut

        payload = (json.dumps(cmd, ensure_ascii=False) + "\n").encode("utf-8")
        async with self._write_lock:
            self.proc.stdin.write(payload)
            await self.proc.stdin.drain()

        if fut is not None:
            try:
                return await fut
            finally:
                self._pending.pop(cmd["id"], None)
        return None

    # -- read loop --------------------------------------------------------
    async def _read_loop(self) -> None:
        assert self.proc is not None and self.proc.stdout is not None
        try:
            while True:
                chunk = await self.proc.stdout.read(65536)
                if not chunk:
                    break
                self._buffer.extend(chunk)
                self._flush_lines()
        except asyncio.CancelledError:
            pass
        except Exception as exc:  # noqa: BLE001
            logger.warning("stdout reader error for %s: %s", self.node.id, exc)
        finally:
            # Drain any remaining partial line, then mark process exit.
            self._flush_lines(final=True)
            logger.info("stdout closed for agent %s", self.node.id)
            # Fail every in-flight RPC wait so callers get errors immediately
            # instead of hanging until their timeouts.
            for fut in list(self._pending.values()):
                if not fut.done():
                    fut.set_exception(RuntimeError("subprocess stdout closed"))
            self._pending.clear()
            if not self._closed and self.on_exit is not None:
                try:
                    asyncio.ensure_future(self.on_exit())
                except RuntimeError:
                    pass

    def _flush_lines(self, final: bool = False) -> None:
        """Split buffer on `\\n` only, strip a trailing `\\r`, parse JSONL."""
        while True:
            idx = self._buffer.find(b"\n")
            if idx == -1:
                break
            line = bytes(self._buffer[:idx])
            del self._buffer[: idx + 1]
            self._handle_line(line)
        if final and self._buffer:
            self._handle_line(bytes(self._buffer))
            self._buffer.clear()

    def _handle_line(self, raw: bytes) -> None:
        line = raw
        if line.endswith(b"\r"):
            line = line[:-1]
        try:
            text = line.decode("utf-8", errors="replace")
            obj = json.loads(text)
        except (ValueError, UnicodeDecodeError):
            logger.warning("skipping non-JSON line from %s: %r", self.node.id, line[:120])
            return
        if not isinstance(obj, dict):
            return
        self._dispatch(obj)

    def _dispatch(self, obj: Dict[str, Any]) -> None:
        ev_type = obj.get("type")

        # Command ack: correlate via optional id.
        if ev_type == "response" and obj.get("id") in self._pending:
            fut = self._pending[obj["id"]]
            if not fut.done():
                fut.set_result(obj)

        # Extension UI dialog requests block the subprocess until answered.
        if ev_type == "extension_ui_request":
            self._auto_respond_ui(obj)
            return

        # Forward to the callback (optionally await) without blocking reader.
        cb = self.on_event(self.node.id, obj)
        if cb is not None:
            try:
                asyncio.ensure_future(cb)
            except RuntimeError:
                pass

    def _auto_respond_ui(self, req: Dict[str, Any]) -> None:
        """Auto-answer blocking dialog requests so the agent never hangs."""
        rid = req.get("id")
        if not rid:
            return
        method = req.get("method")
        if method == "confirm":
            response = {"type": "extension_ui_response", "id": rid, "confirmed": True}
        else:  # select / input / editor / unknown -> cancel
            response = {"type": "extension_ui_response", "id": rid, "cancelled": True}
        asyncio.ensure_future(self.send(response))

    async def _stderr_loop(self) -> None:
        assert self.proc is not None and self.proc.stderr is not None
        try:
            while True:
                chunk = await self.proc.stderr.read(65536)
                if not chunk:
                    break
                if chunk.strip():
                    logger.debug("agent %s stderr: %s", self.node.id, chunk.decode("utf-8", "replace").rstrip())
        except asyncio.CancelledError:
            pass
        except Exception as exc:  # noqa: BLE001
            logger.debug("stderr reader ended for %s: %s", self.node.id, exc)

    # -- teardown ---------------------------------------------------------
    async def close(self, *, abort: bool = True, timeout: float = 5.0) -> None:
        if self._closed:
            return
        self._closed = True
        if self.proc is None:
            return

        try:
            if abort and self.proc.returncode is None:
                try:
                    await self.send({"type": "abort"})
                except Exception:  # noqa: BLE001
                    pass

            if self.proc.returncode is None:
                self.proc.terminate()
                try:
                    await asyncio.wait_for(self.proc.wait(), timeout=timeout)
                except asyncio.TimeoutError:
                    self.proc.kill()
                    try:
                        await self.proc.wait()
                    except Exception:  # noqa: BLE001
                        pass

            # Let the readers hit EOF now that the process has exited, then
            # explicitly close each transport so Windows asyncio does not emit
            # "unclosed transport / I/O on closed pipe" ResourceWarnings.
            reader_tasks = [
                t for t in (self._reader_task, self._stderr_task) if t is not None
            ]
            for t in reader_tasks:
                try:
                    await asyncio.wait_for(asyncio.shield(t), timeout=1.0)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    t.cancel()
            await asyncio.gather(*reader_tasks, return_exceptions=True)

            self._close_transports()
        finally:
            for t in (self._reader_task, self._stderr_task):
                if t is not None and not t.done():
                    t.cancel()
            await asyncio.gather(
                *(t for t in (self._reader_task, self._stderr_task) if t is not None),
                return_exceptions=True,
            )

    def _close_transports(self) -> None:
        if self.proc is None:
            return
        for stream in (self.proc.stdin, self.proc.stdout, self.proc.stderr):
            transport = getattr(stream, "_transport", None) if stream is not None else None
            if transport is not None:
                try:
                    transport.close()
                except Exception:  # noqa: BLE001
                    pass


class _AgentState:
    """Lightweight runtime tracking of one agent (primary or subagent).

    Not persisted; lives only while the hive process is up.  The authoritative
    graph node is in `AgentGraph`; this is purely the result/status side used
    by the `subagent_result` / `subagent_abort` endpoints plus the shared
    live-output tail used by `get_agent_glimpse`.
    """

    def __init__(self) -> None:
        self.status: str = "running"  # running / done / failed / aborted
        self.finalText: str = ""
        self.usage: Optional[Dict[str, Any]] = None
        self.error: Optional[str] = None
        self.abortBy: Optional[str] = None
        self.abortReason: Optional[str] = None
        self.finishedAt: Optional[int] = None
        self._prompted = False
        self._settled = asyncio.Event()
        # --- live output tracking (item: anti-stall liveness) ---------------
        # Bounded tail of the streamed output (text + thinking + tool-call
        # arguments) for subagent_glimpse; authoritative final text replaces it
        # on message_end so the tail never drifts from what pi actually emitted.
        self.liveText: str = ""
        # Monotonic count of chars streamed via the live tail. A moving number:
        # flat while the model is silent, growing while it emits. Feeds
        # subagent_result.progress to counter the "stalled" misread.
        # NOTE: intentionally NEVER reset at message_end — it is a per-process
        # liveness counter, not "chars in the current answer". (The bounded
        # liveText above IS replaced wholesale on message_end; these two are
        # deliberately different.)
        self.liveOutputChars: int = 0
        # Provider-reported usage forwarded on message_update, kept current
        # rather than only at completion.
        self.liveUsage: Optional[Dict[str, Any]] = None
        # Best-effort current activity label (see PHASE_* constants).
        self.phase: str = "running"
        # True while an assistant message is being streamed (message_start..
        # message_end); used to label glimpse output as authoritative vs live.
        self._in_flight: bool = False

    async def wait_for_terminal(self, timeout_ms: int) -> bool:
        """Block up to timeout_ms for a terminal state. Returns True if terminal."""
        try:
            await asyncio.wait_for(self._settled.wait(), timeout=timeout_ms / 1000.0)
        except asyncio.TimeoutError:
            return False
        return self.status in ("done", "failed", "aborted")


class AgentRuntime:
    """Runtime record pairing one agent's live pi process with its state.

    `proc` is None when the process has been reaped/dropped but the record
    (and its live-output state) is kept for late polls. Bundling the two halves
    into one object is what lets a rekey move both atomically instead of having
    to keep two dictionaries in lockstep.
    """

    def __init__(self, proc: Optional["PiSubprocess"] = None,
                 state: Optional["_AgentState"] = None) -> None:
        self.proc: Optional["PiSubprocess"] = proc
        self.state: Optional["_AgentState"] = state


class ProcessManager:
    """Owns all PiSubprocess instances, keyed by node id."""

    def __init__(
        self,
        *,
        on_event: EventCallback,
        pi_exe: Optional[str] = None,
        config: Optional[HiveConfig] = None,
        graph: Optional[Any] = None,
        default_cwd: Optional[str] = None,
    ) -> None:
        self._on_event = on_event
        self.pi_exe = pi_exe or resolve_pi()
        self.config = config
        self.graph = graph
        # Hive-level working-directory default (from --cwd / PI_HIVE_CWD at
        # startup). Used only as the LAST-resort cwd for spawned agents when a
        # per-call cwd, the profile cwd, and the parent cwd are all unset, so
        # driving programs do not have to run next to the hive's own start dir.
        self.default_cwd = default_cwd or os.getcwd()
        # One runtime record per agent: its live pi process (or None when the
        # process has been reaped/dropped but its live-output state is kept) plus
        # its live-output state. Keeping proc + state in ONE record means a rekey
        # or a reap moves/clears both halves atomically — the "two dicts must be
        # moved together" failure mode goes away.
        self._runtimes: Dict[str, "AgentRuntime"] = {}
        self._streaming: Set[str] = set()  # agent ids currently streaming
        # Last activity timestamp (ms) per agent, for idle reaping.
        self._last_activity: Dict[str, int] = {}
        self._lock = threading.RLock()
        # Wired by Hive.__init__: (old_id, new_id) -> awaitable; moves durable
        # state + broadcaster buffers when a node's id changes after spawn.
        self.on_rekey: Optional[Any] = None

    # -- hive api base (env var injected into every child) -----------------
    @property
    def hive_api_base(self) -> str:
        if self.config is not None:
            bind = self.config.server.bind
            port = self.config.server.apiPort
            if bind and port:
                return f"http://{bind}:{port}"
        return DEFAULT_HIVE_API_BASE

    def _subagents_env(self, node: AgentNode) -> str:
        """JSON description of the named subagents `node` may spawn.

        Sourced from the node's agent_allowlist (regex patterns; "*" = any),
        expanded to the concrete agent names defined in the hive config, and
        enriched with each profile's systemPrompt (as a short purpose
        description) so the LLM can pick the right name without guessing.
        Empty string when nothing allowed.
        """
        patterns = list(node.profile.agent_allowlist or [])
        if not patterns:
            return ""
        if self.config is not None:
            names = [a.name for a in self.config.agents
                     if name_allowed(a.name, patterns)]
        else:
            names = [p for p in patterns if p != "*"]
        if not names:
            return ""
        entries: List[Dict[str, Any]] = []
        for name in names:
            desc = ""
            max_conc: Optional[int] = None
            if self.config is not None:
                p = self.config.profile_by_name(name)
                if p is not None:
                    if p.systemPrompt:
                        desc = " ".join(p.systemPrompt.split())
                    # M9: when the subagent's model has a structured LLM
                    # capability profile, append it to the description so the
                    # primary (which reads this tool schema) can tell a subagent
                    # that is slow WITHIN REAL CONSTRAINTS from one that is
                    # stuck, instead of force-stopping it.
                    llm = self.config.llm_for_model(p.model)
                    if llm is not None:
                        note = llm.describe()
                        desc = ((desc + " ") if desc else "") + note
                    max_conc = p.max_concurrency
            entry: Dict[str, Any] = {"name": name}
            if max_conc is not None:
                entry["max_concurrency"] = max_conc
            if desc:
                # Inject the FULL subagent prompt (not truncated) so the primary
                # sees each subagent's complete capability profile (model, coding
                # level, vision, token/quota status, ctx, throughput) and can route
                # tasks correctly. The primary is the orchestrator and tolerates
                # the extra context; subagent descriptions are collapsed to a
                # single line (newlines are already flattened above).
                entry["description"] = desc
            entries.append(entry)
        return json.dumps(entries, ensure_ascii=False)

    def _make(self, node: AgentNode) -> PiSubprocess:
        proc = PiSubprocess(
            node,
            on_event=self._route_event,
            pi_exe=self.pi_exe,
            hive_api_base=self.hive_api_base,
            subagents_env=self._subagents_env(node),
        )
        # Crash watchdog: when the child's stdout closes without a deliberate
        # close(), fail the node/subagent state instead of leaving it running.
        proc.on_exit = self._make_on_proc_exit(node.id)
        return proc

    def _make_on_proc_exit(self, node_id: str):
        """Build the unexpected-exit handler for one agent (SPEC §7/§9)."""

        async def _on_proc_exit() -> None:
            # Subagent bookkeeping: settle the state so `subagent_result`
            # reports failed instead of running forever.
            st = self.get_state(node_id)
            if st is not None and st.status == "running":
                st.status = "failed"
                st.error = "subagent process exited unexpectedly"
                st.finishedAt = _now_ms()
                st._settled.set()
            with self._lock:
                self._streaming.discard(node_id)
                # Drop the dead process so ensure_loaded can lazily respawn
                # it (--session) when the user next opens the conversation.
                # The runtime record (and its live-output state) is kept so a
                # late glimpse/result poll still has something to report.
                rt = self._runtimes.get(node_id)
                if rt is not None and rt.proc is not None:
                    try:
                        if rt.proc.proc.returncode is None:
                            pass  # still terminating; leave cleanup to close()
                        else:
                            rt.proc = None
                    except Exception:  # noqa: BLE001
                        rt.proc = None
                elif rt is not None:
                    rt.proc = None
            # Notify the hive layer; main._on_event turns process_exited into a
            # graph status change + hive:agent_updated broadcast.
            cb = self._route_event(
                node_id,
                {"type": "process_exited", "reason": "stdout closed unexpectedly"},
            )
            if cb is not None:
                try:
                    await cb
                except Exception:  # noqa: BLE001
                    logger.exception("process_exited handling failed for %s", node_id[:8])

        return _on_proc_exit

    # -- event routing / subagent ingestion -------------------------------
    def _route_event(self, agent_id: str, event: Dict[str, Any]) -> Awaitable[None] | None:
        self._last_activity[agent_id] = _now_ms()
        self._track_streaming(agent_id, event)
        self._ingest(agent_id, event)
        return self._on_event(agent_id, event)

    def _track_streaming(self, agent_id: str, event: Dict[str, Any]) -> None:
        """Drive per-agent streaming state from the RPC event stream.

        `turn_start` marks an agent as streaming; `turn_end` / `agent_settled`
        clear it.  The M4 routing layer uses `is_streaming` to decide whether a
        `prompt` must carry `streamingBehavior` (pi rejects a bare `prompt`
        while the agent is streaming).
        """
        ev_type = event.get("type")
        if ev_type == "turn_start":
            with self._lock:
                self._streaming.add(agent_id)
        elif ev_type in ("turn_end", "agent_settled"):
            with self._lock:
                self._streaming.discard(agent_id)

    def is_streaming(self, agent_id: str) -> bool:
        """True if `agent_id` is currently mid-turn (streaming)."""
        with self._lock:
            return agent_id in self._streaming

    def _ingest(self, agent_id: str, event: Dict[str, Any]) -> None:
        """Record subagent lifecycle + live output from the RPC event stream.

        In addition to the terminal-state bookkeeping this also accumulates the
        streamed assistant output (text, thinking, tool-call arguments) into a
        bounded live tail + monotonic char counter, and keeps the latest
        provider-reported usage, so `subagent_result.progress` and
        `subagent_glimpse` reflect what the subagent is ACTUALLY producing right
        now instead of flat message_end-only snapshots.
        """
        st = self.get_state(agent_id)
        if st is None:
            return
        ev_type = event.get("type")
        if ev_type == "agent_start":
            st._settled.clear()
            st.phase = PHASE_GENERATING
        elif ev_type == "message_start":
            msg = event.get("message") or {}
            if msg.get("role") == "assistant":
                st._in_flight = True
        elif ev_type == "message_update":
            if event.get("usage"):
                st.liveUsage = event["usage"]
            ame = event.get("assistantMessageEvent") or {}
            ame_type = ame.get("type") if isinstance(ame, dict) else None
            if ame_type == "text_delta" or ame_type == "thinking_delta" or ame_type == "toolcall_delta":
                delta = ame.get("delta") if isinstance(ame, dict) else None
                if isinstance(delta, str):
                    _append_live_text(st, delta)
            if ame_type in ("text_start", "text_delta"):
                st.phase = PHASE_GENERATING
            elif ame_type in ("thinking_start", "thinking_delta"):
                st.phase = PHASE_THINKING
            elif ame_type in ("toolcall_start", "toolcall_delta"):
                st.phase = PHASE_TOOLCALLING
        elif ev_type == "message_end":
            msg = event.get("message") or {}
            if msg.get("role") == "assistant":
                content = msg.get("content") or []
                text = "".join(
                    p.get("text", "") for p in content if isinstance(p, dict) and p.get("type") == "text"
                )
                if text:
                    st.finalText = text
                    # Authoritative: the accumulated tail must never drift from
                    # what pi actually finalized, so replace it wholesale.
                    st.liveText = text[-LIVE_TEXT_MAX:]
                if msg.get("usage"):
                    st.usage = msg["usage"]
                    st.liveUsage = msg["usage"]
                st._in_flight = False
        elif ev_type == "tool_execution_start" or ev_type == "tool_execution_update" or ev_type == "tool_execution_end":
            # The model turn has handed off to a tool; liveness during this
            # window is driven by the phase + recentlyActive heartbeat, not the
            # output tail (tool output is not ``produced by the model``).
            st.phase = PHASE_TOOL_RUNNING
        elif ev_type == "agent_settled":
            st._in_flight = False
            if st.status in ("done", "failed", "aborted"):
                st.phase = st.status
            else:
                st.phase = "idle"
            # A terminal state (done/failed/aborted) is never overwritten by a
            # later settle.  ``aborted`` in particular is set by
            # ``abort_subagent`` immediately, and the aborted run STILL emits a
            # settle — without this guard every abort would be rewritten to
            # ``done`` (ADR-0001).  Only a live ``running`` subagent may
            # transition to ``done`` here.
            if st.status == "running" and st._prompted:
                st.status = "done"
                st.finishedAt = _now_ms()
                st.phase = "done"
            st._settled.set()

    # -- lifecycle ---------------------------------------------------------
    async def spawn_primary(self, node: AgentNode) -> PiSubprocess:
        return await self.spawn(node)

    async def spawn(self, node: AgentNode) -> PiSubprocess:
        original_id = node.id
        proc = self._make(node)
        with self._lock:
            # One runtime record holds BOTH the live process and the
            # live-output state. Created for every agent (primary included) so
            # the glimpse/live-tail machinery always has something to
            # accumulate into. setdefault semantics: a caller may have
            # pre-seeded the state (e.g. spawn_subagent's fresh state) — never
            # clobber it.
            rt = self._runtimes.get(original_id)
            rt_created = rt is None
            if rt_created:
                rt = AgentRuntime()
                self._runtimes[original_id] = rt
            rt.proc = proc
            if rt_created:
                rt.state = _AgentState()
            self._last_activity[original_id] = _now_ms()
        try:
            await proc.spawn()
        except BaseException:
            with self._lock:
                if rt_created:
                    # Fresh runtime: drop the whole record.
                    self._runtimes.pop(original_id, None)
                else:
                    # Pre-seeded state (e.g. spawn_subagent's) must survive so
                    # a failed spawn still reports "failed" via subagent_result.
                    rt.proc = None
            raise
        # pi may report a real sessionId different from the hive-assigned id
        # (_query_state mutates node.id). The single runtime record (proc +
        # state) must move to the new id together, otherwise lookups by the
        # serialized (new) id fail with "agent not found".
        new_id = node.id
        if new_id != original_id:
            with self._lock:
                if original_id in self._runtimes:
                    self._runtimes[new_id] = self._runtimes.pop(original_id)
                if original_id in self._last_activity:
                    self._last_activity[new_id] = self._last_activity.pop(original_id)
            if self.graph is not None and self.graph.has_node(original_id):
                self.graph.rekey_node(original_id, new_id)
            if self.on_rekey is not None:
                try:
                    await self.on_rekey(original_id, new_id)
                except Exception:  # noqa: BLE001
                    logger.exception("rekey %s -> %s failed", original_id[:8], new_id[:8])
            logger.info("agent %s rekeyed to pi sessionId %s", original_id[:8], new_id[:8])
        return proc

    def get(self, node_id: str) -> Optional[PiSubprocess]:
        """Return the live PiSubprocess for an agent, or None if reaped/absent.

        A reaped agent still has a runtime record (its live-output state is
        kept), so this intentionally returns None when only the state remains —
        callers keep using it as a "is a live process attached?" check.
        """
        with self._lock:
            rt = self._runtimes.get(node_id)
            return rt.proc if rt is not None else None

    def get_state(self, node_id: str) -> Optional["_AgentState"]:
        """Live-output state for an agent, independent of process liveness."""
        rt = self._runtimes.get(node_id)
        return rt.state if rt is not None else None

    def forget(self, *node_ids: str) -> None:
        """Drop runtime tracking (process + subagent state) for removed agents.

        Used when a session is deleted: close the process first, then call this
        so the manager no longer references it. Safe to call for ids that don't
        exist.
        """
        with self._lock:
            for nid in node_ids:
                self._runtimes.pop(nid, None)

    async def reap_idle(self, max_idle_ms: int) -> List[str]:
        """Reclaim the process of any agent that has finished and been idle.

        Only agents that reached a resting state (not ``running``) AND have had
        no event activity for at least ``max_idle_ms`` are reaped. The node and
        its durable ``hive.state.json`` record are kept (``node.loaded`` is set
        False) so pi-hive can lazily restore the session from its
        ``.jsonl`` when the user opens it again — this is what lets a finished
        conversation free its memory instead of lingering forever.

        Subagent result state is intentionally kept so a late
        ``subagent_result`` poll still returns the saved outcome. Returns the
        list of reaped agent ids.
        """
        now = _now_ms()
        with self._lock:
            ids = list(self._runtimes.keys())
        reaped: List[str] = []
        for aid in ids:
            node = None
            if self.graph is not None and self.graph.has_node(aid):
                node = self.graph.get_node(aid)
            if node is None:
                continue
            if node.status == "running":
                continue
            last = self._last_activity.get(aid, 0)
            if not last or last > now:
                continue
            if now - last < max_idle_ms:
                continue
            proc = self.get(aid)
            if proc is None:
                continue
            try:
                await proc.close(abort=True)
            except Exception:  # noqa: BLE001
                logger.warning("reap close failed for %s", aid[:8])
            # Drop the process, keep the (done) state for late polls.
            with self._lock:
                rt = self._runtimes.get(aid)
                if rt is not None:
                    rt.proc = None
            node.loaded = False
            logger.info(
                "reaped idle agent %s (%s) after %dms idle",
                aid[:8], node.name, now - last,
            )
            reaped.append(aid)
        return reaped

    async def send(self, node_id: str, cmd: Dict[str, Any], **kw) -> Optional[Dict[str, Any]]:
        proc = self.get(node_id)
        if proc is None:
            raise KeyError(node_id)
        return await proc.send(cmd, **kw)

    async def get_history(self, agent_id: str) -> List[Dict[str, Any]]:
        """Fetch the agent's FULL conversation from its pi process.

        Uses the RPC `get_messages` command — the same source the interactive
        UI replays from — so after a hive restart (session restored via
        --session) the GUI can rebuild the transcript without parsing
        session files. Returns a list of AgentMessage dicts (possibly empty).
        """
        resp = await self.send(agent_id, {"type": "get_messages"},
                               expect_response=True)
        data = (resp or {}).get("data") or {}
        msgs = data.get("messages")
        return msgs if isinstance(msgs, list) else []

    async def send_command(self, agent_id: str, rpc_command: dict) -> None:
        """Write a newline-terminated JSON command to the subprocess stdin.

        RPC commands sent to pi stdin include:
        - {"type": "prompt", "message": "..."}
        - {"type": "steer", "message": "..."}
        - {"type": "follow_up", "message": "..."}
        - {"type": "abort"}

        The actual stdin write happens in `PiSubprocess.send`: it frames the
        command as UTF-8 JSONL and ``await``s the drain before returning, so
        concurrent writers are serialized via the per-process write lock.
        """
        process = self.get(agent_id)
        if process is None or process.proc is None or process.proc.stdin is None:
            raise ValueError(f"Process for agent {agent_id} not found or stdin closed")
        await process.send(rpc_command)

    # -- subagent orchestration (SPEC §5.1) --------------------------------
    async def spawn_subagent(
        self,
        name: str,
        prompt: str,
        cwd: Optional[str],
        parentId: str,
    ) -> str:
        """Spawn a named-agent subagent and hand it a task prompt.

        CWD fallback priority: tool-cwd > profile-cwd > parent-cwd.  Returns the
        hive subagent id.  Never raises for a crashed child — the node is marked
        `failed` instead (see `subagent_result`).
        """
        if self.config is None:
            raise ValueError("process manager has no config; cannot spawn subagents")
        profile = self.config.profile_by_name(name)
        if profile is None:
            raise ValueError(f"unknown agent profile: {name}")

        parent_cwd: Optional[str] = None
        if self.graph is not None and self.graph.has_node(parentId):
            parent_cwd = self.graph.get_node(parentId).cwd
        eff_cwd = cwd or profile.cwd or parent_cwd or self.default_cwd

        node = AgentNode(
            id=uuid.uuid4().hex,
            kind="subagent",
            name=profile.name,
            parentId=parentId,
            status="running",
            profile=profile,
            cwd=eff_cwd,
            sessionFile="",
            createdAt=_now_ms(),
        )
        if self.graph is not None:
            self.graph.add_node(node)

        st = _AgentState()
        with self._lock:
            rt = self._runtimes.setdefault(node.id, AgentRuntime())
            rt.state = st

        try:
            await self.spawn(node)
        except BaseException as exc:  # noqa: BLE001
            st.status = "failed"
            st.error = f"subagent failed to start: {exc}"
            st.finishedAt = _now_ms()
            st._settled.set()
            if self.graph is not None and self.graph.has_node(node.id):
                self.graph.update_node(node.id, status="failed")
            raise

        st._prompted = True
        try:
            await self.send(node.id, {"type": "prompt", "message": prompt})
        except Exception as exc:  # noqa: BLE001
            # The child may have died between spawn and prompt; mark failed but
            # keep the node/hive alive.
            st.status = "failed"
            st.error = f"could not deliver prompt: {exc}"
            st.finishedAt = _now_ms()
            st._settled.set()
            if self.graph is not None and self.graph.has_node(node.id):
                self.graph.update_node(node.id, status="failed")

        return node.id

    async def get_subagent_result(self, node_id: str, wait_time_ms: int = 0) -> Dict[str, Any]:
        """Poll (optionally blocking up to wait_time_ms) for a subagent result.

        While the subagent is still running, the response carries a
        `progress` object (token usage so far, ms since the last RPC event,
        current session-file size in bytes) so the caller can see the
        subagent is alive and making headroom instead of guessing it hung.
        """
        st = self.get_state(node_id)
        if st is None:
            if self.get(node_id) is not None:
                return {"ok": True, "status": "running", "progress": self._progress(node_id)}
            return {"ok": False, "error": f"unknown subagent id: {node_id}"}

        if st.status in ("done", "failed", "aborted"):
            return self._result_payload(st, node_id)

        if wait_time_ms and wait_time_ms > 0:
            terminal = await st.wait_for_terminal(wait_time_ms)
            if not terminal or st.status == "running":
                return {"ok": True, "status": "running", "progress": self._progress(node_id)}
        return self._result_payload(st, node_id)

    def _progress(self, node_id: str) -> Dict[str, Any]:
        """Liveness/progress signals for a still-running subagent.

        The authoritative anti-stall signal is the event-layer heartbeat:
        `recentlyActive` (true while events are still arriving), `lastEventAgeMs`
        and `streaming`. `phase` labels best-effort what the agent is doing.

        The numeric counters are emitted only when they carry information, so a
        healthy-but-quiet subagent never reads as "stalled zeros":
          * `usage` is present only when the provider sent a non-zero token
            counter (some local OpenAI-compatible/vLLM servers report a
            zero-filled usage object on every streamed snapshot and only fill
            real numbers on the final `done` event);
          * `liveOutputChars` is present only once it has moved (> 0). It is a
            monotonic count of chars the model streamed (text + thinking +
            tool-call arguments) and stays 0 during silent prefix/TTFT windows
            and for the whole of a tool run (tool output is not model text).
        Neither counter is a substitute for the heartbeat: a working subagent
        that is thinking or running tools legitimately reports neither.
        """
        progress: Dict[str, Any] = {}
        st = self.get_state(node_id)
        if st is not None:
            live_usage = st.liveUsage or st.usage
            if not _usage_is_noise(live_usage):
                progress["usage"] = live_usage
            if st.liveOutputChars:
                progress["liveOutputChars"] = st.liveOutputChars
            progress["phase"] = st.phase
        with self._lock:
            last = self._last_activity.get(node_id)
            streaming = node_id in self._streaming
        if last:
            age = max(0, _now_ms() - last)
            progress["lastEventAgeMs"] = age
            # Server-side liveness verdict: events are still arriving. The
            # caller should NOT abort while this is True — a thinking subagent
            # streams deltas without consuming visible tokens or disk bytes.
            progress["recentlyActive"] = age < 30_000
        else:
            progress["recentlyActive"] = False
        progress["streaming"] = streaming
        return progress

    def _result_payload(self, st: _AgentState, node_id: str = "") -> Dict[str, Any]:
        if st.status == "done":
            return {
                "ok": True,
                "status": "done",
                "result": {
                    "finalText": st.finalText,
                    "usage": st.usage or st.liveUsage,
                },
                "finishedAt": st.finishedAt,
            }
        if st.status == "aborted":
            return {
                "ok": True,
                "status": "aborted",
                "abortBy": st.abortBy,
                "abortReason": st.abortReason or st.error,
            }
        if st.status == "failed":
            return {"ok": True, "status": "failed", "error": st.error or "subagent failed"}
        return {"ok": True, "status": "running", "progress": self._progress(node_id)}

    async def get_agent_glimpse(self, node_id: str, n: int = 1024) -> Dict[str, Any]:
        """Return the last N characters of what an agent is producing.

        Works for ANY agent (primary or subagent). This is the cheap,
        non-blocking "peek" — it never waits for completion and never starts
        the agent.  It returns the tail of the *live recorded* output stream:
        text, thinking, and tool-call arguments as they stream in (through the
        bounded server-side accumulator), or the authoritative final text once a
        message settles (message_end overwrites the tail so it never drifts from
        pi's final output).  The payload labels whether the returned text is
        authoritative (`complete`) and what the agent was doing (`phase`), so a
        caller can tell a live fragment from a final answer.

        N is clamped to [1, 1024] (the parent-visible "1K" cap).

        Caller contract (read before interpreting the payload):
          * `complete` is the AUTHORITATIVE "is this a final answer?" signal.
            Rely on it; it is derived from streaming state + node status.
          * `status` is a REFERENCE label only — primaries settle to "idle" while
            subagents settle to "done", and it is maintained by a separate
            (async) graph path, so it can momentarily disagree with the in-memory
            state. Do NOT treat `status` alone as a completeness check.
          * `totalChars` is a MONOTONIC per-process counter of every character
            streamed since the process started (identical to
            `subagent_result.progress.liveOutputChars`). It is intentionally NOT
            reset at message_end, so it can exceed the 8K tail and exceed `n`.
          * `truncated` only means "the 8K tail is longer than the requested
            n" — it describes the PEEK WINDOW, not that the answer is cut off.
            A `complete: true` + `truncated: true` pair is normal and means
            "settled, but I'm showing only the last n chars". For the FULL
            final text, use the WS `message_end` event or
            `subagent_result`'s `result.finalText`, NOT glimpse.
        """
        # Strict clamp to [1, 1024]: an explicit 0/negative is not "default", it
        # is a boundary the contract says to clamp (0 -> 1). Only an absent value
        # (None, which the route never sends) falls back to the default.
        n = max(1, min(int(n) if n is not None else 1024, 1024))
        st = self.get_state(node_id)
        node = None
        if self.graph is not None and self.graph.has_node(node_id):
            node = self.graph.get_node(node_id)
        if st is None:
            if node is not None:
                # Known node, but no live capture in this hive process (e.g. a
                # metadata-restored or never-streamed agent).
                return {
                    "ok": True,
                    "id": node_id,
                    "status": node.status,
                    "phase": "idle",
                    "complete": True,
                    "truncated": False,
                    "totalChars": 0,
                    "text": "",
                    "note": "no live output recorded for this agent in this hive process",
                }
            return {"ok": False, "error": f"unknown agent id: {node_id}"}

        # The graph node's status is authoritative for BOTH primaries and
        # subagents (maintained by main._on_event), whereas the in-memory
        # state.status is only reliably driven for subagents. Prefer the node.
        status = node.status if node is not None else st.status
        complete = status in ("done", "failed", "aborted") or not self.is_streaming(node_id)
        text = st.liveText[-n:]
        return {
            "ok": True,
            "id": node_id,
            "status": status,
            "phase": st.phase,
            "complete": complete,
            "truncated": len(st.liveText) > len(text),
            "totalChars": st.liveOutputChars,
            "text": text,
        }

    def _fresh_state(self, node_id: str) -> _AgentState:
        """Replace the agent's result state with a fresh `running` one.

        Used when reusing a finished agent (follow-up): the previous
        settled outcome is discarded and the new run owns the state so
        `subagent_result` polls the *follow-up* run, not the stale one.
        """
        st = _AgentState()
        st.status = "running"
        with self._lock:
            rt = self._runtimes.setdefault(node_id, AgentRuntime())
            rt.state = st
        return st

    async def followup_subagent(self, node_id: str, prompt: str) -> Dict[str, Any]:
        """Reuse a previously completed subagent with a follow-up task.

        Sends `prompt` to the SAME subagent conversation so it continues with
        its full recorded context instead of starting fresh.  If the subagent's
        process was reaped (idle cleanup), it is lazily respawned from its
        persisted ``--session`` file first (same node id, same context), then
        the prompt is delivered.  The outcome is polled via `subagent_result`
        as usual.

        Returns ``{ok: True, id}`` on success, or ``{ok: False, error}``.
        """
        node = None
        if self.graph is not None and self.graph.has_node(node_id):
            node = self.graph.get_node(node_id)
        if node is None:
            return {"ok": False, "error": f"unknown subagent id: {node_id}"}

        # The process may still be live (not yet reaped) or already closed.
        # If closed, respawn it from the persisted session so context survives.
        if self.get(node_id) is None:
            if not node.sessionFile:
                return {
                    "ok": False,
                    "error": "subagent has no persisted session to reuse",
                }
            # Fresh running state before respawn so any settle emitted during
            # reload cannot prematurely mark the follow-up run as done.
            st = self._fresh_state(node_id)
            st._prompted = False
            try:
                await self.spawn(node)
            except BaseException as exc:  # noqa: BLE001
                st.status = "failed"
                st.error = f"could not restart subagent for follow-up: {exc}"
                st.finishedAt = _now_ms()
                st._settled.set()
                if self.graph is not None and self.graph.has_node(node_id):
                    self.graph.update_node(node_id, status="failed")
                return {"ok": False, "error": st.error}
            node.loaded = True
        else:
            st = self._fresh_state(node_id)
            st._prompted = False

        try:
            await self.send(node_id, {"type": "prompt", "message": prompt})
        except Exception as exc:  # noqa: BLE001
            st.status = "failed"
            st.error = f"could not deliver follow-up prompt: {exc}"
            st.finishedAt = _now_ms()
            st._settled.set()
            if self.graph is not None and self.graph.has_node(node_id):
                self.graph.update_node(node_id, status="failed")
            return {"ok": False, "error": str(exc)}

        st._prompted = True
        if self.graph is not None and self.graph.has_node(node_id):
            self.graph.update_node(
                node_id, status="running", abortBy=None, abortReason=None
            )
        logger.info("follow-up prompt delivered to reused subagent %s", node_id[:8])
        return {"ok": True, "id": node_id}

    async def abort_subagent(
        self,
        node_id: str,
        reason: Optional[str] = None,
        by: str = "parent",
        grace_ms: int = 5000,
    ) -> Dict[str, Any]:
        """Abort a subagent: cooperative RPC abort, then a hard stop (ADR-0001).

        Sends the RPC abort first so the agent can stop cleanly when it is able.
        pi's RPC abort only interrupts model generation, NOT an in-flight tool
        (it does not even call its own ``abortBash``), so a long-running tool can
        keep the subagent going for minutes while it pins a concurrency slot. If
        the run has not ended within ``grace_ms``, the pi child process is killed
        (``proc.close(abort=True)``) and dropped from the live process table so
        the slot frees at once. The persisted ``.jsonl`` session stays on disk, so
        the aborted subagent can still be reused via ``followup`` or lazily
        reloaded from ``--session``.

        ``aborted`` is terminal: a later ``agent_settled`` / ``message_end`` /
        ``process_exited`` never overwrites it (see ``_ingest`` and hive
        ``_on_event``). Aborting an already-terminal subagent is a no-op.
        """
        proc = self.get(node_id)
        st = self.get_state(node_id)

        # Aborting an already-terminal subagent keeps its terminal state
        # (documented contract: abort of a done/failed/aborted agent is a no-op).
        if st is not None and st.status != "running":
            return {"ok": True, "id": node_id, "status": st.status}
        if st is None and proc is None:
            return {"ok": False, "id": node_id, "error": f"unknown subagent id: {node_id}"}

        if proc is not None:
            try:
                await proc.send({"type": "abort"})
            except Exception:  # noqa: BLE001
                pass

        if st is not None:
            st.status = "aborted"
            st.abortBy = by
            st.abortReason = reason
            st.error = reason
            st.finishedAt = _now_ms()
            st._settled.set()
        if self.graph is not None and self.graph.has_node(node_id):
            self.graph.update_node(
                node_id,
                status="aborted",
                abortBy=by,
                abortReason=reason,
            )

        # Hard-stop watchdog: pi abort only stops generation; an in-flight tool
        # may keep the run alive. If the run has not ended within the grace, kill
        # the pi child so the concurrency slot frees (ADR-0001).
        try:
            await asyncio.wait_for(
                self._wait_not_streaming(node_id), timeout=grace_ms / 1000.0
            )
        except asyncio.TimeoutError:
            proc = self.get(node_id)
            if proc is not None:
                try:
                    await proc.close(abort=True)
                except Exception:  # noqa: BLE001
                    logger.warning("abort close failed for %s", node_id[:8])
                with self._lock:
                    rt = self._runtimes.get(node_id)
                    if rt is not None:
                        rt.proc = None
                if self.graph is not None and self.graph.has_node(node_id):
                    node = self.graph.get_node(node_id)
                    if node is not None:
                        # No live process: the node is restorable from its
                        # persisted session, not currently materialized.
                        node.loaded = False
                        try:
                            self.graph.update_node(
                                node_id, status="aborted", abortBy=by, abortReason=reason
                            )
                        except Exception:  # noqa: BLE001
                            pass
                logger.info(
                    "aborted subagent %s by hard stop (still running after %dms)",
                    node_id[:8], grace_ms,
                )
            else:
                logger.info(
                    "aborted subagent %s (no live process to stop)", node_id[:8]
                )
        else:
            logger.info(
                "aborted subagent %s (cooperative stop settled within %dms)",
                node_id[:8], grace_ms,
            )
        return {"ok": True, "id": node_id, "status": "aborted"}

    async def _wait_not_streaming(self, node_id: str) -> None:
        """Return once the agent is no longer mid-turn."""
        while self.is_streaming(node_id):
            await asyncio.sleep(0.1)

    async def steer_subagent(self, node_id: str, message: str) -> Dict[str, Any]:
        """Steer a running subagent mid-execution (SPEC §5.1).

        Sends an RPC `steer` to the subagent's pi process. pi delivers the
        steering message after the subagent's current assistant turn finishes
        executing its tool calls, before its next model call — so this redirects
        a subagent that is already working (down the wrong path, or to call one
        of its own tools) WITHOUT aborting it; use `abort_subagent` to stop it
        instead. Steer only reaches a live process: a reaped/idle subagent has no
        live turn to steer, so the caller should use `followup_subagent` to
        continue an already-finished one.
        """
        try:
            await self.send_command(node_id, {"type": "steer", "message": message})
        except ValueError as exc:
            return {"ok": False, "error": f"subagent not running: {exc}"}
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "error": f"could not steer subagent: {exc}"}
        return {"ok": True, "id": node_id, "status": "steered"}

    async def shutdown(self, *, timeout: float = 5.0) -> None:
        with self._lock:
            procs = [rt.proc for rt in self._runtimes.values() if rt.proc is not None]
        await asyncio.gather(
            *(p.close(abort=True, timeout=timeout) for p in procs),
            return_exceptions=True,
        )
        with self._lock:
            for rt in self._runtimes.values():
                rt.proc = None
