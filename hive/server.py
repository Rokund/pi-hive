"""FastAPI / WebSocket transport layer.

Two servers (run concurrently by `hive.main`):

* Port 1 (GUI):   WebSocket at `/ws`. Forwards every pi RPC event as
  ``{"type":"hive:event","agentId":...,"ts":...,"event":{...}}``.
* Port 2 (API):   HTTP + WebSocket (Prompt/Steer API). Accepts
  `prompt`, `steer`, `follow_up`, `abort`, `get_tree`, `get_agent`, `subscribe`.

Transport only — all state lives in the graph / process manager supplied via
`ApiContext`.
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from .agent_graph import AgentGraph, NodeNotFoundError
from .config import HiveConfig
from .process_manager import ProcessManager

logger = logging.getLogger("hive.server")


# ---------------------------------------------------------------------------
# Broadcast hive
# ---------------------------------------------------------------------------
class EventBroadcaster:
    """Fan out forwardable events to all connected WebSocket subscribers.

    Also keeps a bounded in-memory transcript log per agent (keyed by
    agentId, tagged with a global monotonic `seq`), so a GUI that connects
    or switches to a node late can fetch the backlog via
    `GET /api/agent/{id}/events?since=<seq>` and see the full conversation
    while the hive is still running.  (Replaying from the pi session files
    after a hive restart is a separate, future feature.)
    """

    MAX_EVENTS_PER_AGENT = 4000

    def __init__(self) -> None:
        self._subscribers: Set[asyncio.Queue] = set()
        self._log: Dict[str, List[Dict[str, Any]]] = {}
        self._seq = 0
        self._log_lock = threading.Lock()
        self._history_pulled: Set[str] = set()

    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue()
        self._subscribers.add(q)
        return q

    def unsubscribe(self, q: asyncio.Queue) -> None:
        self._subscribers.discard(q)

    async def publish(self, obj: Dict[str, Any]) -> None:
        if obj.get("type") == "hive:event" and obj.get("agentId"):
            with self._log_lock:
                self._seq += 1
                obj = {**obj, "seq": self._seq}
                buf = self._log.setdefault(obj["agentId"], [])
                buf.append(obj)
                overflow = len(buf) - self.MAX_EVENTS_PER_AGENT
                if overflow > 0:
                    del buf[:overflow]
        dead: List[asyncio.Queue] = []
        for q in list(self._subscribers):
            try:
                q.put_nowait(obj)
            except Exception:  # noqa: BLE001
                dead.append(q)
        for q in dead:
            self._subscribers.discard(q)

    def events_since(self, agent_id: str, since: int) -> List[Dict[str, Any]]:
        """Buffered events for `agent_id` with `seq` strictly greater than `since`."""
        with self._log_lock:
            buf = self._log.get(agent_id, [])
            return [e for e in buf if e.get("seq", 0) > since]

    def has_history(self, agent_id: str) -> bool:
        """True once the pi-side history was already folded into the log."""
        with self._log_lock:
            return agent_id in self._history_pulled

    def mark_history(self, agent_id: str) -> None:
        with self._log_lock:
            self._history_pulled.add(agent_id)

    def drop_events(self, agent_id: str) -> None:
        """Discard the buffered event log + history flag for a deleted agent."""
        with self._log_lock:
            self._log.pop(agent_id, None)
            self._history_pulled.discard(agent_id)

    def rekey(self, old_id: str, new_id: str) -> None:
        """Move an agent's buffered log/history flag to a new agent id.

        Pairs with AgentGraph.rekey_node when pi reports a different real
        sessionId after spawn; without this the transcript backlog would be
        stranded under the old key and the GUI would see an empty history.
        """
        if old_id == new_id:
            return
        with self._log_lock:
            buf = self._log.pop(old_id, None)
            if buf is not None:
                existing = self._log.get(new_id, [])
                merged = existing + [e for e in buf if e not in existing]
                self._log[new_id] = merged[-self.MAX_EVENTS_PER_AGENT:]
                for e in self._log[new_id]:
                    e["agentId"] = new_id
            if old_id in self._history_pulled:
                self._history_pulled.discard(old_id)
                self._history_pulled.add(new_id)

    @property
    def subscriber_count(self) -> int:
        return len(self._subscribers)


# ---------------------------------------------------------------------------
# Context handed to the API / GUI apps
# ---------------------------------------------------------------------------
class ApiContext:
    def __init__(
        self,
        graph: AgentGraph,
        processes: ProcessManager,
        config: HiveConfig,
        broadcaster: EventBroadcaster,
        spawn_primary: Optional[Any] = None,
    ) -> None:
        self.graph = graph
        self.processes = processes
        self.config = config
        self.broadcaster = broadcaster
        # Async callable () -> AgentNode, wired by Hive.__init__; lets the
        # API start a new primary conversation (new tree root) on demand.
        self.spawn_primary = spawn_primary
        # Async callable (agent_id: str) -> bool, wired by Hive.__init__. When a
        # client targets a persisted-but-unloaded conversation (clicked in the
        # GUI / prompted), this lazily spawns its pi subprocess (lazy load).
        self.ensure_loaded: Optional[Any] = None
        # Async callable (agent_id: str) -> dict, wired by Hive.__init__. Deletes
        # a session/conversation: removes the node + its subtree from the graph
        # and durable state (leaving pi session files on disk) and stops the
        # process.
        self.delete_session: Optional[Any] = None
        # Async callable (agent_id: str, new_name: str) -> dict, wired by Hive.__init__.
        self.rename_session: Optional[Any] = None
        # Async callable (agent_id: str, model: str) -> dict, wired by Hive.__init__.
        self.set_agent_model: Optional[Any] = None
        # Per-agent asyncio locks that serialize the (re)build of an agent's
        # transcript backlog (see api_agent_events). Without this, two repeated
        # or concurrent requests can each pull the full history and re-publish
        # every message with fresh seqs, which the GUI seq-dedup cannot
        # collapse -> the conversation is shown twice.
        self._history_locks: Dict[str, asyncio.Lock] = {}

    def history_lock(self, agent_id: str) -> asyncio.Lock:
        """Return the per-agent lock guarding transcript backlog (re)build."""
        lock = self._history_locks.get(agent_id)
        if lock is None:
            lock = asyncio.Lock()
            self._history_locks[agent_id] = lock
        return lock


# ---------------------------------------------------------------------------
# Command dispatch (shared by HTTP + WS on Port 2)
# ---------------------------------------------------------------------------
class PromptIn(BaseModel):
    agent: str
    message: str
    images: Optional[List[Dict[str, Any]]] = None
    streamingBehavior: Optional[str] = None


class SteerIn(BaseModel):
    agent: str
    message: str
    images: Optional[List[Dict[str, Any]]] = None


class FollowUpIn(BaseModel):
    agent: str
    message: str
    images: Optional[List[Dict[str, Any]]] = None


class AbortIn(BaseModel):
    agent: str
    reason: Optional[str] = None


class RenameIn(BaseModel):
    name: str


class SpawnPrimaryIn(BaseModel):
    """Optional overrides for a newly spawned primary conversation."""
    label: Optional[str] = None
    model: Optional[str] = None
    cwd: Optional[str] = None


class SetModelIn(BaseModel):
    model: str


class SubagentSpawnIn(BaseModel):
    name: str
    prompt: str
    cwd: Optional[str] = None
    parentId: str


class SubagentResultIn(BaseModel):
    id: str
    wait_time: int = 0


class SubagentAbortIn(BaseModel):
    id: str
    reason: Optional[str] = None
    by: Optional[str] = None


class SubagentSteerIn(BaseModel):
    id: str
    message: str
    by: Optional[str] = None


class SubagentFollowupIn(BaseModel):
    id: str
    prompt: str


class AgentGlimpseIn(BaseModel):
    id: str
    # Max characters of the agent's live produced text to return (clamped to
    # [1, 1024] server-side; the parent-visible "1K" cap). Optional.
    n: int = 1024


def _wrap_event(agent_id: str, event: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "type": "hive:event",
        "agentId": agent_id,
        "ts": int(time.time() * 1000),
        "event": event,
    }


def _build_api_response(command: str, payload: Dict[str, Any] | None = None,
                        error: Optional[str] = None) -> Dict[str, Any]:
    return {
        "type": "response",
        "command": command,
        "success": error is None,
        **({"data": payload} if payload is not None else {}),
        **({"error": error} if error is not None else {}),
    }


def _resolve_agent(ctx: ApiContext, payload: Optional[Dict[str, Any]]) -> Optional[str]:
    """Resolve the target agent id for a routing command.

    Prefers an explicit `agentId` (M4 WS schema) or `agent` (M2 HTTP schema);
    when neither is given, defaults to the primary agent's id.
    """
    if payload:
        agent = payload.get("agentId") or payload.get("agent")
        if agent:
            return agent
    # With multiple conversations, an implicit target means "the one I'm
    # working in" — the most recently created primary.
    return ctx.graph.latest_primary_id()


def _command_message(payload: Optional[Dict[str, Any]]) -> str:
    """Extract the prompt text, accepting either the M4 `text` or M2 `message` key."""
    if not payload:
        return ""
    return payload.get("text") or payload.get("message") or ""


async def _handle_command(
    ctx: ApiContext,
    command: str,
    payload: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """Dispatch one Hive API command and return a response dict."""
    try:
        if command in ("prompt", "steer", "follow_up"):
            explicit = (payload or {}).get("agentId") or (payload or {}).get("agent")
            if command == "prompt" and not explicit:
                # A bare prompt over WS/HTTP starts a NEW conversation: spawn a
                # fresh primary to carry it instead of piling onto the oldest
                # root. (Explicit agent ids always target that agent.)
                if ctx.spawn_primary is None:
                    return _build_api_response(command, error="hive not wired")
                node = await ctx.spawn_primary(
                    cwd=(payload or {}).get("cwd") or None
                )
                explicit = node.id
            agent = explicit or ctx.graph.latest_primary_id()
            if not agent:
                return _build_api_response(command, error="no agent id given and no primary agent")
            if not ctx.graph.has_node(agent):
                return _build_api_response(command, error=f"agent not found: {agent}")
            # Lazily materialize a persisted-but-unloaded conversation so the
            # prompt/steer reaches a live process (lazy load on open). This is
            # a cheap no-op for already-spawned agents.
            if ctx.ensure_loaded is not None:
                await ctx.ensure_loaded(agent)

            message = _command_message(payload)
            rpc_cmd: Dict[str, Any] = {"type": command, "message": message}
            if payload and payload.get("images"):
                rpc_cmd["images"] = payload["images"]

            if command == "prompt":
                # RPC protocol: a `prompt` while the agent is streaming MUST
                # carry streamingBehavior or pi rejects it.  Honor an explicit
                # one if provided, otherwise auto-insert "steer" for a
                # streaming target; an idle target gets a plain prompt.
                explicit = payload.get("streamingBehavior") if payload else None
                if explicit:
                    rpc_cmd["streamingBehavior"] = explicit
                elif ctx.processes.is_streaming(agent):
                    rpc_cmd["streamingBehavior"] = "steer"

            await ctx.processes.send_command(agent, rpc_cmd)
            return _build_api_response(command)

        if command == "abort":
            agent = _resolve_agent(ctx, payload)
            if not agent:
                return _build_api_response("abort", error="no agent id given and no primary agent")
            node = ctx.graph.get_node(agent) if ctx.graph.has_node(agent) else None
            # SPEC §7: abort on a done/idle/aborted/failed node is a no-op.
            if node and node.status not in ("done", "idle", "aborted", "failed"):
                by = (payload or {}).get("by", "user")
                reason = (payload or {}).get("reason")
                ctx.graph.update_node(
                    agent, status="aborted", abortBy=by, abortReason=reason
                )
                # RPC abort carries no reason itself.
                await ctx.processes.send_command(agent, {"type": "abort"})
                await ctx.broadcaster.publish({
                    "type": "hive:agent_updated",
                    "agent": ctx.graph.get_node(agent).model_dump(mode="json"),
                    "ts": int(time.time() * 1000),
                })
            return _build_api_response("abort")

        if command == "get_tree":
            nodes = ctx.graph.get_tree()
            tree = [n.model_dump(mode="json") for n in nodes]
            return _build_api_response("get_tree", {"tree": tree})

        if command == "get_agent":
            agent = payload.get("agent") if payload else None
            if not agent:
                raise ValueError("agent is required")
            node = ctx.graph.get_node(agent)
            return _build_api_response(
                "get_agent", node.model_dump(mode="json")
            )

        if command == "subscribe":
            # Subscribe happens over the WS connection itself; nothing to do here.
            return _build_api_response("subscribe")

        return _build_api_response(command, error=f"unknown command: {command}")
    except NodeNotFoundError as exc:
        return _build_api_response(command, error=f"agent not found: {exc}")
    except Exception as exc:  # noqa: BLE001
        logger.exception("command %s failed", command)
        return _build_api_response(command, error=str(exc))


# ---------------------------------------------------------------------------
# Port 2 (API) app
# ---------------------------------------------------------------------------
def create_api_app(ctx: ApiContext) -> FastAPI:
    app = FastAPI(title="pi-hive API", version="0.1.0")
    # The GUI is served from Port 1 (3000) but issues HTTP calls to this API
    # on Port 2 (3001) — allow that cross-origin access.
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.get("/api/health")
    async def health() -> Dict[str, Any]:
        return {"ok": True, "agents": ctx.graph.__len__()}

    @app.get("/api/tree")
    async def api_get_tree() -> Dict[str, Any]:
        return await _handle_command(ctx, "get_tree", None)

    @app.get("/api/agent/{agent_id}")
    async def api_get_agent(agent_id: str) -> Dict[str, Any]:
        # Resolving an agent by id is the GUI's "select/click" signal — lazily
        # materialize the persisted session if it isn't loaded yet.
        if ctx.ensure_loaded is not None and ctx.graph.has_node(agent_id):
            await ctx.ensure_loaded(agent_id)
        return await _handle_command(ctx, "get_agent", {"agent": agent_id})

    @app.get("/api/agent/{agent_id}/events")
    async def api_agent_events(agent_id: str, since: int = 0) -> Dict[str, Any]:
        """Transcript backlog for an agent.

        Serves the in-memory event log so the GUI can populate a node clicked
        after its events already streamed. When the hive was restarted (memory
        log wiped) and the agent was resurrected from its session file, the
        backlog is rebuilt from the pi process itself via the RPC
        `get_messages` command — the same source the interactive UI uses —
        wrapped into `hive:event`-shaped `message_end` entries the GUI reducer
        already understands. `since` is the last `seq` applied; 0 = all.
        """
        # The GUI calls this endpoint when the user clicks a node — this is the
        # lazy-load trigger: materialize the persisted session (spawn pi with
        # --session <file>) if it hasn't been loaded yet, so get_history below
        # returns the full conversation.
        load_error: Optional[str] = None
        if ctx.ensure_loaded is not None and ctx.graph.has_node(agent_id):
            try:
                if not await ctx.ensure_loaded(agent_id):
                    load_error = "session could not be materialized (no live process)"
            except Exception as exc:  # noqa: BLE001
                logger.warning("ensure_loaded(%s) failed: %s", agent_id, exc)
                load_error = f"lazy load failed: {exc}"

        # Rebuild the transcript backlog only ONCE per agent. Serialize via a
        # per-agent lock: if a second request (re-click, reconnect retry, or a
        # concurrent client) arrives while/after the first pull, it skips the
        # rebuild and serves the already-published events. This keeps every
        # message in the conversation unique — the previous lack of a guard let
        # two pulls re-publish the whole history with fresh seqs, duplicating
        # the answers in the GUI.
        async with ctx.history_lock(agent_id):
            events = ctx.broadcaster.events_since(agent_id, since)
            # Pull the full conversation from the pi process when the memory
            # log holds no actual messages (fresh hive after restart, resurrected
            # session). The log may contain only control responses (get_state
            # etc.), so test for message_end entries, not for emptiness.
            has_msgs = any(
                e.get("event", {}).get("type") == "message_end"
                for e in events
            )
            # Additionally, only pull while the agent is NOT actively streaming.
            # If a turn is in flight, its message_end will be published to the
            # live WS stream with its real seq momentarily; re-publishing it
            # here under a NEW seq would hand the GUI a second, distinct copy
            # of the in-progress (final) message that the seq guard cannot
            # deduplicate — the source of intermittent duplicated conclusions.
            # The pull is still performed for genuinely cold/restored sessions
            # (agent idle, resurrected from disk), which is the only case where
            # it is actually needed.
            streaming = ctx.processes.is_streaming(agent_id)
            if (not has_msgs and ctx.graph.has_node(agent_id)
                    and not ctx.broadcaster.has_history(agent_id)
                    and not streaming):
                # First request after restart: pull the full conversation from pi.
                # Retry briefly: the pi child may have JUST been spawned by
                # ensure_loaded and not be ready to answer get_messages yet;
                # a single failed attempt here would leave the GUI with an
                # empty transcript until the next click / page refresh.
                msgs: List[Dict[str, Any]] = []
                pull_exc: Optional[str] = None
                for attempt in range(3):
                    try:
                        msgs = await asyncio.wait_for(
                            ctx.processes.get_history(agent_id), timeout=15
                        )
                        pull_exc = None
                    except Exception as exc:  # noqa: BLE001
                        logger.warning(
                            "get_history(%s) attempt %d failed: %s",
                            agent_id, attempt + 1, exc,
                        )
                        pull_exc = str(exc) or exc.__class__.__name__
                        msgs = []
                    if msgs:
                        break
                    await asyncio.sleep(1.0)
                if msgs:
                    for m in msgs:
                        await ctx.broadcaster.publish({
                            "type": "hive:event",
                            "agentId": agent_id,
                            "ts": int(time.time() * 1000),
                            "event": {"type": "message_end", "message": m},
                        })
                    ctx.broadcaster.mark_history(agent_id)
                    events = ctx.broadcaster.events_since(agent_id, since)
                # msgs == [] alone is NOT an error: a brand-new conversation
                # simply has no history yet. But when every pull attempt
                # RAISED (dead process, RPC broken), surface it so the GUI
                # shows why the transcript is empty instead of staying
                # silently blank.
                if not msgs and pull_exc and load_error is None:
                    load_error = f"history pull failed: {pull_exc}"
        return {
            "ok": load_error is None,
            "agentId": agent_id,
            "events": events,
            "latest": events[-1]["seq"] if events else since,
            **({"error": load_error} if load_error else {}),
        }

    @app.post("/api/prompt")
    async def api_prompt(body: PromptIn) -> Dict[str, Any]:
        return await _handle_command(
            ctx, "prompt", body.model_dump(exclude_none=True)
        )

    @app.post("/api/steer")
    async def api_steer(body: SteerIn) -> Dict[str, Any]:
        return await _handle_command(
            ctx, "steer", body.model_dump(exclude_none=True)
        )

    @app.post("/api/follow_up")
    async def api_follow_up(body: FollowUpIn) -> Dict[str, Any]:
        return await _handle_command(
            ctx, "follow_up", body.model_dump(exclude_none=True)
        )

    @app.post("/api/abort")
    async def api_abort(body: AbortIn) -> Dict[str, Any]:
        return await _handle_command(
            ctx, "abort", body.model_dump(exclude_none=True)
        )

    @app.post("/api/subscribe")
    async def api_subscribe() -> Dict[str, Any]:
        return await _handle_command(ctx, "subscribe", None)

    @app.post("/api/primary/spawn")
    async def api_primary_spawn(body: SpawnPrimaryIn | None = None) -> Dict[str, Any]:
        """Start a NEW primary conversation (a new root in the agent tree).

        Accepts an optional JSON body `{label?, model?, cwd?}` so the GUI can
        pick which model the new conversation runs on (and, optionally, which
        working directory to launch it in). Existing primaries and
        their subagents are left untouched, so the GUI sidebar can host
        multiple parallel conversations.
        """
        if ctx.spawn_primary is None:
            return {"ok": False, "id": "", "error": "hive not wired"}
        try:
            node = await ctx.spawn_primary(
                label=body.label if body else None,
                model=(body.model.strip() or None) if body and body.model else None,
                cwd=(body.cwd.strip() or None) if body and body.cwd else None,
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("primary spawn failed")
            return {"ok": False, "id": "", "error": str(exc)}
        return {"ok": True, "id": node.id, "model": node.profile.model, "error": ""}

    @app.get("/api/agent/{agent_id}/stats")
    async def api_agent_stats(agent_id: str) -> Dict[str, Any]:
        """Session stats from the live pi process (get_session_stats RPC).

        Returns token/cost totals and the current context-window usage
        (contextUsage: tokens / contextWindow / percent) for the GUI usage
        display. Fails softly when the process is not materialized.
        """
        if not ctx.graph.has_node(agent_id):
            return {"ok": False, "error": f"agent not found: {agent_id}"}
        if ctx.processes.get(agent_id) is None:
            return {"ok": False, "error": "process not loaded"}
        try:
            resp = await asyncio.wait_for(
                ctx.processes.send(
                    agent_id, {"type": "get_session_stats"}, expect_response=True
                ),
                timeout=10,
            )
            data = (resp or {}).get("data") or {}
            return {"ok": True, "stats": data, "error": ""}
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "error": str(exc)}

    @app.post("/api/agent/{agent_id}/model")
    async def api_agent_set_model(agent_id: str, body: SetModelIn) -> Dict[str, Any]:
        """Change which model an agent runs on (GUI combobox in node-meta).

        Updates the node's profile snapshot and persists it, and when a pi
        process is live, hot-swaps the model in-place via the RPC
        `set_model` command so the next prompt already uses the new model.
        If no process is materialized yet, the new model applies at spawn.
        """
        if ctx.set_agent_model is None:
            return {"ok": False, "error": "hive not wired"}
        try:
            return await ctx.set_agent_model(agent_id, body.model.strip())
        except Exception as exc:  # noqa: BLE001
            logger.exception("set agent model failed")
            return {"ok": False, "error": str(exc)}

    @app.get("/api/models")
    async def api_models() -> Dict[str, Any]:
        """Model choices the GUI can offer when starting a conversation.

        Sourced from hive.config.json: an explicit top-level `models` list if
        present, otherwise the distinct models already used by the configured
        primary/agents.
        """
        models = list(getattr(ctx.config, "models", []) or [])
        if not models:
            seen: List[str] = []
            candidates = [ctx.config.primary.model] + [
                a.model for a in ctx.config.agents
            ]
            for m in candidates:
                if m and m not in seen:
                    seen.append(m)
            models = seen
        default_model = ctx.config.primary.model
        return {
            "ok": True,
            "models": models,
            "default": default_model,
            "error": "",
        }

    @app.post("/api/agent/{agent_id}/delete")
    async def api_agent_delete(agent_id: str) -> Dict[str, Any]:
        """Delete a session/conversation from the hive.

        Removes the agent and its entire subagent subtree from the live graph
        and from the durable `hive.state.json` record. The pi session (.jsonl)
        files are left on disk untouched. Stops any running process and
        broadcasts the updated tree so connected GUIs drop the session.
        """
        if ctx.delete_session is None:
            return {"ok": False, "deleted": [], "error": "hive not wired"}
        try:
            return await ctx.delete_session(agent_id)
        except Exception as exc:  # noqa: BLE001
            logger.exception("session delete failed")
            return {"ok": False, "deleted": [], "error": str(exc)}

    @app.post("/api/agent/{agent_id}/rename")
    async def api_agent_rename(agent_id: str, body: RenameIn) -> Dict[str, Any]:
        """Rename a session/conversation (any node in the tree).

        Sets a manual title that the auto-titler won't overwrite, persists it
        to `hive.state.json`, and broadcasts the updated tree.
        """
        if ctx.rename_session is None:
            return {"ok": False, "name": "", "error": "hive not wired"}
        try:
            return await ctx.rename_session(agent_id, body.name)
        except Exception as exc:  # noqa: BLE001
            logger.exception("session rename failed")
            return {"ok": False, "name": "", "error": str(exc)}

    @app.post("/hive/subagent/spawn")
    async def hive_subagent_spawn(body: SubagentSpawnIn) -> Dict[str, Any]:
        """Spawn a named-agent subagent (SPEC §5.1).

        Validates `name` against the parent's agent_allowlist and the global
        maxConcurrentSubagents cap (via AgentGraph.check_spawn — SPEC §8/§9),
        plus the per-agent-type `max_concurrency` ceiling when one is
        configured.  When a constraint is violated, returns
        `{ok: false, error: "..."}` with a specific reason (not allowed /
        global cap reached / per-agent cap reached).

        When the name is not allowed, returns the exact SPEC §9 error
        `{ok: false, error: "not allowed"}`.
        """
        ok, err = ctx.graph.check_spawn(body.parentId, body.name, ctx.config)
        if not ok:
            return {"ok": False, "id": "", "error": err}

        try:
            sid = await ctx.processes.spawn_subagent(
                name=body.name,
                prompt=body.prompt,
                cwd=body.cwd,
                parentId=body.parentId,
            )
        except ValueError as exc:
            return {"ok": False, "id": "", "error": str(exc)}
        except Exception as exc:  # noqa: BLE001
            logger.exception("subagent spawn failed")
            return {"ok": False, "id": "", "error": str(exc)}

        await ctx.broadcaster.publish(
            {"type": "hive:event", "agentId": body.parentId, "ts": int(time.time() * 1000),
             "event": {"type": "subagent_spawned", "id": sid, "name": body.name}}
        )
        # Push a fresh tree so connected GUIs learn about the new node
        # (the frontend sidebar only populates from hive:tree messages).
        await ctx.broadcaster.publish({
            "type": "hive:tree",
            "tree": [n.model_dump(mode="json") for n in ctx.graph.get_tree()],
            "ts": int(time.time() * 1000),
        })
        return {"ok": True, "id": sid, "error": ""}

    @app.post("/hive/subagent/result")
    async def hive_subagent_result(body: SubagentResultIn) -> Dict[str, Any]:
        """Poll a subagent's outcome (SPEC §5.1)."""
        try:
            return await ctx.processes.get_subagent_result(body.id, body.wait_time or 0)
        except Exception as exc:  # noqa: BLE001
            logger.exception("subagent result failed")
            return {"ok": False, "error": str(exc)}

    @app.post("/hive/subagent/abort")
    async def hive_subagent_abort(body: SubagentAbortIn) -> Dict[str, Any]:
        """Abort a running subagent (SPEC §5.1).

        Hard stop (ADR-0001): cooperative RPC abort, then a grace, then kill the
        subagent's pi process if it did not settle, so its concurrency slot
        frees even when an in-flight tool refuses to stop. After an abort the
        node/state stays ``aborted`` (terminal) and the refreshed agent/tree is
        broadcast so connected GUIs reflect it (a hard-killed subagent emits no
        further events to do so).
        """
        by = body.by or "parent"
        try:
            result = await ctx.processes.abort_subagent(body.id, body.reason, by)
        except Exception as exc:  # noqa: BLE001
            logger.exception("subagent abort failed")
            return {"ok": False, "error": str(exc)}
        # Surface the terminal/aborted state to connected GUIs even when a hard
        # kill suppressed the normal agent_settled event stream.
        if ctx.graph is not None and ctx.graph.has_node(body.id):
            try:
                await ctx.broadcaster.publish({
                    "type": "hive:agent_updated",
                    "agent": ctx.graph.get_node(body.id).model_dump(mode="json"),
                    "ts": int(time.time() * 1000),
                })
                await ctx.broadcaster.publish({
                    "type": "hive:tree",
                    "tree": [n.model_dump(mode="json") for n in ctx.graph.get_tree()],
                    "ts": int(time.time() * 1000),
                })
            except Exception:  # noqa: BLE001
                pass
        return result

    @app.post("/hive/subagent/steer")
    async def hive_subagent_steer(body: SubagentSteerIn) -> Dict[str, Any]:
        """Steer a running subagent mid-execution (SPEC §5.1).

        Delivered to the subagent after its current tool calls finish and before
        its next model call, so a parent can redirect it without aborting it.
        """
        try:
            return await ctx.processes.steer_subagent(body.id, body.message)
        except Exception as exc:  # noqa: BLE001
            logger.exception("subagent steer failed")
            return {"ok": False, "error": str(exc)}

    @app.post("/hive/subagent/followup")
    async def hive_subagent_followup(body: SubagentFollowupIn) -> Dict[str, Any]:
        """Send a follow-up task to a previously completed subagent, reusing
        its persisted session for context (SPEC §5.1 reuse).  If the subagent
        process was reaped, it is lazily respawned from its ``--session`` file
        so the conversation continues instead of starting fresh.  The new
        outcome is polled via `subagent_result`.
        """
        try:
            return await ctx.processes.followup_subagent(body.id, body.prompt)
        except Exception as exc:  # noqa: BLE001
            logger.exception("subagent followup failed")
            return {"ok": False, "error": str(exc)}

    @app.post("/hive/agent/glimpse")
    async def hive_agent_glimpse(body: AgentGlimpseIn) -> Dict[str, Any]:
        """Peek at the tail of any agent's (primary or subagent) live produced text.

        Non-blocking: returns the last N (<= 1024) characters of what the agent
        is currently producing — including thinking and tool-call arguments as
        they stream in — plus phase/complete labels so the caller can tell a live
        fragment from a final answer. Never starts or waits.
        """
        try:
            return await ctx.processes.get_agent_glimpse(body.id, body.n)
        except Exception as exc:  # noqa: BLE001
            logger.exception("agent glimpse failed")
            return {"ok": False, "error": str(exc)}

    @app.post("/hive/subagent/glimpse")
    async def hive_subagent_glimpse(body: AgentGlimpseIn) -> Dict[str, Any]:
        """Backward-compatible alias for `/hive/agent/glimpse`.

        Kept so existing callers (the `subagent_glimpse` tool, the python
        reference client, docs) that still POST here keep working.
        """
        return await hive_agent_glimpse(body)

    @app.websocket("/ws")
    async def api_ws(websocket: WebSocket) -> None:
        await websocket.accept()
        queue = ctx.broadcaster.subscribe()
        try:
            async def pump() -> None:
                while True:
                    obj = await queue.get()
                    await websocket.send_json(obj)

            pump_task = asyncio.create_task(pump())
            try:
                while True:
                    raw = await websocket.receive_text()
                    try:
                        msg = json.loads(raw)
                    except json.JSONDecodeError:
                        await websocket.send_json(
                            _build_api_response("parse", error="invalid JSON")
                        )
                        continue
                    if isinstance(msg, dict) and msg.get("type"):
                        resp = await _handle_command(ctx, msg["type"], msg)
                        await websocket.send_json(resp)
                    else:
                        await websocket.send_json(
                            _build_api_response("parse", error="malformed message")
                        )
            finally:
                pump_task.cancel()
                try:
                    await pump_task
                except asyncio.CancelledError:
                    pass
        except WebSocketDisconnect:
            pass
        finally:
            ctx.broadcaster.unsubscribe(queue)

    return app


# ---------------------------------------------------------------------------
# Port 1 (GUI) app
# ---------------------------------------------------------------------------
def create_gui_app(
    broadcaster: EventBroadcaster,
    graph: "AgentGraph | None" = None,
    *,
    api_port: int = 3001,
    gui_port: int = 3000,
) -> FastAPI:
    app = FastAPI(title="pi-hive GUI", version="0.1.0")

    @app.get("/health")
    async def health() -> Dict[str, Any]:
        return {"ok": True, "subscribers": broadcaster.subscriber_count}

    def _tree_snapshot() -> Dict[str, Any] | None:
        """Current agent tree as a hive:tree push message (or None w/o graph)."""
        if graph is None:
            return None
        return {
            "type": "hive:tree",
            "tree": [n.model_dump(mode="json") for n in graph.get_tree()],
            "ts": int(time.time() * 1000),
        }

    @app.websocket("/ws")
    async def gui_ws(websocket: WebSocket) -> None:
        await websocket.accept()
        queue = broadcaster.subscribe()
        try:
            async def pump() -> None:
                while True:
                    obj = await queue.get()
                    await websocket.send_json(obj)

            pump_task = asyncio.create_task(pump())
            try:
                while True:
                    raw = await websocket.receive_text()
                    # The frontend sends {"type":"subscribe"} on connect;
                    # respond with an immediate tree snapshot so the sidebar
                    # populates without waiting for the next event.
                    try:
                        msg = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    if isinstance(msg, dict) and msg.get("type") == "subscribe":
                        snapshot = _tree_snapshot()
                        if snapshot is not None:
                            await websocket.send_json(snapshot)
            finally:
                pump_task.cancel()
                try:
                    await pump_task
                except asyncio.CancelledError:
                    pass
        except WebSocketDisconnect:
            pass
        finally:
            broadcaster.unsubscribe(queue)

    # Serve the built frontend (hive/gui/dist) at the root.  Routes defined
    # above (/health, /ws) take precedence over the mount.
    gui_dist = Path(__file__).resolve().parent / "gui" / "dist"
    if gui_dist.is_dir():
        index = gui_dist / "index.html"

        # The frontend must learn the real apiPort (Port 2) and guiPort (Port 1)
        # without hardcoding them, since both are configurable via hive.config.json
        # `server.*Port`. Inject a tiny runtime config script into the served page:
        # the bundle reads `window.__PI_HIVE_CONFIG__` and derives the GUI
        # WebSocket from its own origin (same port as this page).
        def _config_script() -> str:
            payload = json.dumps(
                {"apiPort": api_port, "guiPort": gui_port},
                separators=(",", ":"),
            ).replace("</", "<\\/")
            return (
                "<script>window.__PI_HIVE_CONFIG__="
                + payload
                + ";</script>"
            )

        @app.get("/", include_in_schema=False)
        async def gui_index() -> Response:
            html = index.read_text(encoding="utf-8")
            script = _config_script()
            head_end = "</head>"
            if head_end in html:
                return HTMLResponse(html.replace(head_end, script + head_end, 1))
            # No </head>? Prepend the config so it always runs before the bundle.
            return HTMLResponse(script + html)

        app.mount("/", StaticFiles(directory=str(gui_dist), html=True), name="gui")
    else:
        import logging
        logging.getLogger("hive.server").warning(
            "GUI dist not found at %s - run npm build in hive/gui", gui_dist)

    return app
