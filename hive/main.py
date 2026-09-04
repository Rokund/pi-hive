"""pi-hive entrypoint.

Bootstraps configuration, the agent graph, the process manager, spawns the
primary pi subprocess, and runs the Port 1 (GUI) + Port 2 (API) servers.
Handles SIGINT/SIGTERM for graceful subprocess + server shutdown.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
import time
import uuid
from typing import Any, Dict, List, Optional

import uvicorn
from pathlib import Path

from .agent_graph import AgentGraph
from .config import HiveConfig, load_config
from .models import AgentNode
from .process_manager import ProcessManager
from .server import ApiContext, EventBroadcaster, create_api_app, create_gui_app, _wrap_event
from .state import HiveState

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("hive.main")


def _now_ms() -> int:
    return int(time.time() * 1000)


class Hive:
    """Central wiring object shared across servers."""

    def __init__(self, config: HiveConfig, default_cwd: Optional[str] = None) -> None:
        self.config = config
        # Hive-level default working directory for spawned agents. Precedence
        # is per-call cwd > profile.cwd > parent cwd > this hive default > cwd.
        # Sourced from `python -m hive.main --cwd <dir>` or PI_HIVE_CWD so an
        # external driver does not have to live next to the hive's start dir.
        self.default_cwd = default_cwd or os.getcwd()
        self.state = HiveState().load()
        self.graph = AgentGraph()
        self.broadcaster = EventBroadcaster()
        self.processes = ProcessManager(
            on_event=self._on_event,
            config=config,
            graph=self.graph,
            default_cwd=self.default_cwd,
        )
        self.api_context = ApiContext(
            graph=self.graph,
            processes=self.processes,
            config=config,
            broadcaster=self.broadcaster,
        )
        # Let the API layer create new primary conversations on demand.
        self.api_context.spawn_primary = self.start_primary
        # Let the API layer lazily load a persisted conversation only when the
        # user actually opens it (clicks it in the GUI / prompts it), instead
        # of loading every session at server startup.
        self.api_context.ensure_loaded = self.ensure_loaded
        # Let the API layer delete a session/conversation on demand.
        self.api_context.delete_session = self.delete_session
        # Let the API layer rename a session/conversation on demand.
        self.api_context.rename_session = self.rename_session
        # Let the API layer switch which model an agent runs on (GUI combobox).
        self.api_context.set_agent_model = self.set_agent_model
        # When pi reports a real sessionId differing from the hive-assigned id,
        # ProcessManager rekeys runtime maps + graph; this hook moves the
        # durable state record and buffered transcript log to the new key.
        self.processes.on_rekey = self._rekey_agent

    async def _rekey_agent(self, old_id: str, new_id: str) -> None:
        node = self.graph.get_node(new_id)
        self.state.remove_node(old_id)
        self.state.restore_node(node)
        self.broadcaster.rekey(old_id, new_id)

    # -- restore from persisted state (SPEC §7.1) -------------------------
    async def restore_metadata(self) -> int:
        """Reload conversation *metadata* (not the sessions) after a restart.

        Reads hive.state.json and re-creates an ``AgentNode`` for every recorded
        conversation so the tree/sidebar lists it, but does NOT spawn a pi
        subprocess per node and does NOT reload its `.jsonl` history. The
        actual ``--session <file>`` load is deferred until the user opens that
        conversation (lazy load via :meth:`ensure_loaded`), keeping startup
        fast.  Nodes whose session file no longer exists are dropped from the
        durable state.  Returns the number of restored (metadata-only) nodes.
        """
        from .models import AgentProfile

        restored = 0
        agents = self.state.agents  # id -> persisted dict
        for node_id, rec in list(agents.items()):
            session_file = rec.get("sessionFile") or ""
            if not session_file or not Path(session_file).is_file():
                logger.warning(
                    "restore %s (%s): session file missing, skipping",
                    node_id, rec.get("name"),
                )
                self.state.remove_node(node_id)
                continue
            try:
                profile = AgentProfile(**(rec.get("configSnapshot") or {}))
            except Exception as exc:  # noqa: BLE001
                logger.warning("restore %s: bad profile snapshot (%s)", node_id, exc)
                continue
            node = AgentNode(
                id=node_id,
                kind=rec.get("kind", "subagent"),
                name=rec.get("name") or node_id,
                parentId=rec.get("parentId"),
                status="idle",
                profile=profile,
                cwd=rec.get("cwd") or self.default_cwd,
                sessionFile=session_file,
                createdAt=rec.get("createdAt") or _now_ms(),
                finishedAt=rec.get("finishedAt"),
                titled=bool(rec.get("titled")),
                loaded=False,  # metadata only — no process yet
            )
            self.graph.add_node(node)
            restored += 1
            logger.info("restored conversation metadata %s (%s)", node.name, node_id[:8])
        # Second pass: wire parent/child edges. add_node only links a child
        # into its parent's childrenIds when the parent is ALREADY in the
        # graph, and state records restore in arbitrary dict order — without
        # this pass, any child restored before its parent stays unwired and
        # the whole subtree flattens/disappears in the GUI after a restart.
        for snapshot in self.graph.get_tree():
            pid = snapshot.parentId
            if pid and self.graph.has_node(pid) and not self.graph.has_node(snapshot.id):
                continue  # orphaned child (parent pruned); leave as-is
            if pid and self.graph.has_node(pid):
                parent = self.graph.get_node(pid)
                if snapshot.id not in parent.childrenIds:
                    parent.childrenIds.append(snapshot.id)
        return restored

    async def ensure_loaded(self, agent_id: str) -> bool:
        """Lazily spawn the pi subprocess backing a persisted conversation.

        Restored conversations are kept as metadata-only in the graph until the
        user actually opens one (clicks it in the GUI / sends a prompt).  This
        spawns the real ``pi --mode rpc`` child with ``--session <file>`` so pi
        reloads the recorded history at that point — never eagerly at startup.

        Returns True if the agent is backed by a live process afterwards.
        """
        if self.processes.get(agent_id) is not None:
            return True
        if not self.graph.has_node(agent_id):
            return False
        node = self.graph.get_node(agent_id)
        if not node.sessionFile:
            # No persisted history to load (e.g. a brand-new on-demand node
            # whose process was never started) — nothing to materialize.
            return False
        try:
            await self.processes.spawn(node)
        except Exception as exc:  # noqa: BLE001
            logger.warning("lazy load %s failed (%s)", agent_id, exc)
            return False
        node.loaded = True
        logger.info("lazily loaded conversation %s (%s)", node.name, node.id[:8])
        # Broadcast the refreshed tree so connected GUIs drop the stale
        # "archived" badge immediately instead of waiting for the next
        # unrelated tree broadcast (reap/rename/status change).
        await self.broadcaster.publish({
            "type": "hive:tree",
            "tree": [n.model_dump(mode="json") for n in self.graph.get_tree()],
            "ts": _now_ms(),
        })
        return True

    async def delete_session(self, agent_id: str) -> Dict[str, Any]:
        """Delete a conversation/session from the hive.

        Removes the agent and its entire subagent subtree from the live graph
        and from the durable `hive.state.json` record. The pi session (.jsonl)
        files are deliberately left on disk untouched. Running processes are
        stopped and their buffered event logs dropped so the GUI stops showing
        the session. Returns ``{"ok": True, "deleted": [ids...]}``.
        """
        if not self.graph.has_node(agent_id):
            return {"ok": False, "deleted": [], "error": f"agent not found: {agent_id}"}

        # Collect the whole subtree (the node plus every descendant).
        deleted: List[str] = []
        stack = [agent_id]
        while stack:
            nid = stack.pop()
            deleted.append(nid)
            node = self.graph.get_node(nid)
            for cid in node.childrenIds:
                stack.append(cid)

        # Stop any live processes and drop their runtime tracking.
        for nid in deleted:
            proc = self.processes.get(nid)
            if proc is not None:
                try:
                    await proc.close(abort=True)
                except Exception:  # noqa: BLE001
                    pass
            self.processes.forget(nid)

        # Drop the node from the graph, durable state, and buffered event log.
        for nid in deleted:
            self.graph.remove_node(nid)
            self.state.remove_node(nid)
            self.broadcaster.drop_events(nid)

        logger.info("deleted session %s (subtree %s)", agent_id[:8], deleted)
        await self.broadcaster.publish({
            "type": "hive:tree",
            "tree": [n.model_dump(mode="json") for n in self.graph.get_tree()],
            "ts": _now_ms(),
        })
        return {"ok": True, "deleted": deleted, "error": ""}

    async def rename_session(self, agent_id: str, new_name: str) -> Dict[str, Any]:
        """Rename a conversation/session (any node in the tree).

        Sets a manual title on the node and claims the `titled` slot so the
        auto-titler never overwrites it, persists the change to
        `hive.state.json`, mirrors the name into pi's session metadata, and
        broadcasts the updated tree.
        """
        if not self.graph.has_node(agent_id):
            return {"ok": False, "name": "", "error": f"agent not found: {agent_id}"}
        name = (new_name or "").strip()
        if not name:
            return {"ok": False, "name": "", "error": "name is empty"}
        name = name[:60]
        self.graph.update_node(agent_id, name=name, titled=True)
        self.state.restore_node(self.graph.get_node(agent_id))
        # Mirror the new name into pi's session metadata (no-op if process is
        # not currently materialized).
        try:
            await self.processes.send_command(
                agent_id, {"type": "set_session_name", "name": name})
        except Exception:  # noqa: BLE001
            pass
        await self.broadcaster.publish({
            "type": "hive:tree",
            "tree": [n.model_dump(mode="json") for n in self.graph.get_tree()],
            "ts": _now_ms(),
        })
        logger.info("renamed session %s -> %r", agent_id[:8], name)
        return {"ok": True, "id": agent_id, "name": name, "error": ""}

    async def set_agent_model(self, agent_id: str, model: str) -> Dict[str, Any]:
        """Switch the model for an agent (GUI combobox in node-meta).

        Updates the node's profile snapshot and durable state (so the choice
        survives re-spawn / hive restart), and — when a pi process is live —
        hot-swaps the model in-place via the RPC `set_model` command, so the
        very next prompt already runs on the new model. If the process is
        not currently materialized, the new model applies at the next spawn.
        """
        if not model:
            return {"ok": False, "error": "model is empty"}
        if not self.graph.has_node(agent_id):
            return {"ok": False, "error": f"agent not found: {agent_id}"}
        node = self.graph.get_node(agent_id)
        old_model = node.profile.model
        if model == old_model:
            return {"ok": True, "id": agent_id, "model": model, "error": ""}
        node.profile = node.profile.model_copy(update={"model": model})
        self.state.restore_node(node)
        live = self.processes.get(agent_id) is not None
        if live:
            # pi RPC: {"type":"set_model","provider":"...","modelId":"..."}
            provider, _, model_id = model.partition("/")
            try:
                await self.processes.send(
                    agent_id,
                    {"type": "set_model", "provider": provider, "modelId": model_id},
                    expect_response=True,
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "live set_model failed for %s (%s); new model %r applies on next spawn",
                    agent_id[:8], exc, model,
                )
        await self.broadcaster.publish({
            "type": "hive:agent_updated",
            "agent": node.model_dump(mode="json"),
            "ts": _now_ms(),
        })
        logger.info(
            "agent %s model %r -> %r%s", agent_id[:8], old_model, model,
            "" if live else " (applies on next spawn)",
        )
        return {"ok": True, "id": agent_id, "model": model, "error": ""}

    async def reap_idle_agents(self) -> List[str]:
        """Reclaim processes for conversations that have finished and gone idle.

        Because finished sessions can be lazily restored from their ``.jsonl``
        by :meth:`ensure_loaded`, a concluded conversation no longer needs to
        hold a live pi process in memory. This reaps any agent (primary or
        subagent) whose process has rested idle for ``maxSubagentIdleMs`` and
        broadcasts the refreshed tree/node state so the GUI can reflect it.
        """
        max_idle = self.config.server.maxSubagentIdleMs
        try:
            reaped = await self.processes.reap_idle(max_idle)
        except Exception:  # noqa: BLE001
            logger.exception("idle reap failed")
            return []
        if not reaped:
            return []
        for aid in reaped:
            if self.graph.has_node(aid):
                await self.broadcaster.publish({
                    "type": "hive:agent_updated",
                    "agent": self.graph.get_node(aid).model_dump(mode="json"),
                    "ts": _now_ms(),
                })
        await self.broadcaster.publish({
            "type": "hive:tree",
            "tree": [n.model_dump(mode="json") for n in self.graph.get_tree()],
            "ts": _now_ms(),
        })
        return reaped

    # -- primary bootstrap -------------------------------------------------
    def _resolve_primary_profile(self, agent: Optional[str]) -> AgentProfile:
        """Resolve the profile for a new primary conversation.

        When `agent` names an agent, its profile is used and must exist and be
        primary-eligible (a clear error otherwise). When `agent` is absent, the
        configured `default_primary` (via the legacy shim when present) is used.
        """
        if agent is not None:
            profile = self.config.profile_by_name(agent)
            if profile is None:
                raise ValueError(
                    f"cannot spawn primary: unknown agent {agent!r}; "
                    f"known names: {sorted(self.config.known_names())}"
                )
            if not self.config.is_primary_eligible(agent):
                raise ValueError(
                    f"cannot spawn primary: agent {agent!r} is not primary-eligible "
                    f"(allow_as_primary is not True, while other agents are flagged)"
                )
            return profile
        return self.config.default_primary_profile()

    def make_primary_node(self, label: Optional[str] = None,
                          model: Optional[str] = None,
                          cwd: Optional[str] = None,
                          agent: Optional[str] = None) -> AgentNode:
        profile = self._resolve_primary_profile(agent)
        # GUI model selection: override the configured default for this
        # conversation only (the node's own profile snapshot keeps it).
        if model:
            profile = profile.model_copy(update={"model": model})
        # M9: the primary should know its OWN LLM's real constraints (context
        # window, pricing, vision, speed) so it does not misjudge a slow-but-
        # legitimate subagent. Inject the structured capability profile derived
        # from the ACTUAL resolved model (after any `model` override above) into
        # its own system prompt. It never appears in PI_HIVE_SUBAGENTS, which
        # only describes the subagents the primary can spawn. profile_by_name
        # returns a fresh copy, so mutating it is safe.
        own_llm = self.config.llm_for_model(profile.model)
        if own_llm is not None:
            note = own_llm.describe()
            base = profile.systemPrompt or ""
            profile.systemPrompt = (base.rstrip() + ("\n\n" if base else "") + note)
        return AgentNode(
            id=uuid.uuid4().hex,
            kind="primary",
            name=label or profile.name,
            status="idle",
            profile=profile,
            cwd=cwd or profile.cwd or self.default_cwd,
            sessionFile="",
            createdAt=_now_ms(),
        )

    async def start_primary(self, label: Optional[str] = None,
                            model: Optional[str] = None,
                            cwd: Optional[str] = None,
                            agent: Optional[str] = None) -> AgentNode:
        # Distinguish multiple conversations: first keeps the profile name,
        # later ones get primary-2, primary-3, ...  (node.profile is stored
        # on the node, so spawn-agent_allowlist checks are unaffected by the label.
        # The auto-titler replaces these placeholders with a summary once the
        # first exchange settles.)
        base_name = self._resolve_primary_profile(agent).name
        if label is None:
            existing = [n for n in self.graph.get_tree() if n.kind == "primary"]
            if existing:
                label = f"{base_name}-{len(existing) + 1}"
        node = self.make_primary_node(label, model=model, cwd=cwd, agent=agent)
        self.graph.add_node(node)
        await self.processes.spawn(node)
        self.state.restore_node(node)
        logger.info("primary agent %s started", node.id)
        # Announce the initial tree so any already-connected GUI populates.
        await self.broadcaster.publish({
            "type": "hive:tree",
            "tree": [n.model_dump(mode="json") for n in self.graph.get_tree()],
            "ts": _now_ms(),
        })
        return node

    # -- event handler -----------------------------------------------------
    async def _on_event(self, agent_id: str, event: Dict[str, Any]) -> None:
        """Update node status and fan the event out to GUI/API subscribers."""
        ev_type = event.get("type")

        status_changed = False
        if agent_id and self.graph.has_node(agent_id):
            node = self.graph.get_node(agent_id)
            old_status = node.status
            if ev_type == "agent_start":
                node.status = "running"
            elif ev_type == "process_exited":
                # Child died without a deliberate close() — mark it failed so
                # the GUI stops showing running and the idle reaper can reap.
                # Deliberate closes (reap/abort/delete) go through close(),
                # which suppresses the on_exit callback entirely.
                node.status = "failed"
                if node.finishedAt is None:
                    node.finishedAt = _now_ms()
            elif ev_type == "agent_settled":
                # Terminal states (done/failed/aborted) are never overwritten by
                # a settle.  ``aborted`` is set by the abort endpoint at once and
                # the aborted run still emits a settle — without this guard the
                # node would flip back to done/idle and the abort would look
                # like it never happened (ADR-0001).
                if node.status == "aborted":
                    pass
                elif node.kind == "subagent":
                    node.status = "done"
                else:
                    node.status = "idle"
                if node.finishedAt is None:
                    node.finishedAt = _now_ms()
            status_changed = node.status != old_status
            # M5 state persistence: flush the durable record whenever a node
            # reaches a terminal state (complete/fail/abort), using the real
            # sessionFile returned by get_state at spawn (SPEC §7.1).
            if node.status in ("done", "failed", "aborted"):
                try:
                    self.state.restore_node(node)
                except Exception:  # noqa: BLE001
                    logger.exception("failed to persist node %s", node.id)
                logger.warning(
                    "agent %s (%s) -> %s%s",
                    node.id[:8], node.name, node.status,
                    f" ({event.get('reason')})" if event.get("reason") else "",
                )

        await self.broadcaster.publish(_wrap_event(agent_id, event))
        # Keep the GUI sidebar in sync: the tree only refreshes node fields
        # via hive:agent_updated, so push it whenever a status transition
        # actually happened.
        if status_changed and self.graph.has_node(agent_id):
            await self.broadcaster.publish({
                "type": "hive:agent_updated",
                "agent": self.graph.get_node(agent_id).model_dump(mode="json"),
                "ts": int(time.time() * 1000),
            })

        # Auto-title: once the first real exchange settles, ask the same pi
        # process (which holds the full conversation context) for a short
        # title and rename the node. One-shot per node, runs detached.
        if ev_type == "agent_settled" and self.graph.has_node(agent_id):
            node = self.graph.get_node(agent_id)
            if not getattr(node, "titled", False) and node.kind == "primary":
                node.titled = True  # claim immediately; avoid double-runs
                asyncio.create_task(self._generate_title(node))

    async def _generate_title(self, node: AgentNode) -> None:
        """Generate a conversation title with a THROWAWAY pi process.

    Runs `pi --no-session -p` (ephemeral, nothing persisted) and feeds it the
    recent conversation text extracted via get_messages, so the main session
    is never polluted with a title round-trip.
    """
        try:
            msgs = await self.processes.get_history(node.id)
            # Condense the conversation into a transcript for the titler.
            # Use only the EARLY turns: the opening exchange establishes the
            # conversation's subject, while later turns are often trivial acks
            # (e.g. 'OK') that would mislead the titler. Ground realism on the
            # first user prompt.
            lines: list[str] = []
            for m in msgs:
                role = m.get("role")
                if role not in ("user", "assistant"):
                    continue
                content = m.get("content")
                text = "".join(
                    p.get("text", "") for p in content
                    if isinstance(p, dict) and p.get("type") == "text"
                ) if isinstance(content, list) else str(content or "")
                text = " ".join(text.split())
                if not text:
                    continue
                lines.append(f"{'User' if role == 'user' else 'Assistant'}: {text}")
                if len(lines) >= 4:          # first two exchanges only
                    break
            if not lines:
                node.titled = False
                return
            transcript = "\n".join(lines)[:8000]

            import subprocess as _sp
            import tempfile as _tf
            pi_exe = self.processes.pi_exe if hasattr(self.processes, "pi_exe") else "pi"
            prompt_text = (
                "You are given a chat transcript. Read it fully, then invent a "
                "very short title (3-6 words, plain text, no quotes) that "
                "summarizes the SUBJECT of the conversation. Reply with the "
                "title ONLY, written in the same language as the user's "
                "messages.\n\n--- TRANSCRIPT ---\n" + transcript + "\n--- END ---"
            )
            # Write the prompt to a temp file and pass it via @file: avoids
            # argv-length and multiline-truncation issues (cmd /c wrappers
            # silently truncate at newlines on Windows).
            fd, tmp = _tf.mkstemp(suffix=".md", prefix="pi-title-")
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    f.write(prompt_text)
                argv = [pi_exe, "--no-session", "--no-context-files", "-p",
                        "--model", node.profile.model, "--thinking", "off",
                        "@" + tmp]
                proc = await asyncio.create_subprocess_exec(
                    *argv,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.DEVNULL,
                    cwd=node.cwd or None,
                )
                try:
                    out, _ = await asyncio.wait_for(proc.communicate(), timeout=90)
                except asyncio.TimeoutError:
                    proc.kill()
                    node.titled = False
                    return
            finally:
                try:
                    os.unlink(tmp)
                except OSError:
                    pass
            title = (out or b"").decode("utf-8", errors="replace").strip()
            # The one-shot may include thinking or extra prose; take the last
            # non-empty line of a reasonable length as the title.
            cand = [ln.strip().strip('"\u201c\u201d').strip() for ln in title.splitlines()]
            cand = [c for c in cand if c and len(c) <= 80]
            if not cand:
                node.titled = False
                return
            node.name = " ".join(cand[-1].split())[:60]
            self.state.restore_node(node)
            logger.info("titled conversation %s -> %r", node.id[:8], node.name)
            # Mirror the title into pi's own session metadata so the name
            # also shows up in `pi -r` session listings.
            try:
                await self.processes.send_command(
                    node.id, {"type": "set_session_name", "name": node.name})
            except Exception:  # noqa: BLE001
                pass
            await self.broadcaster.publish({
                "type": "hive:agent_updated",
                "agent": node.model_dump(mode="json"),
                "ts": int(time.time() * 1000),
            })
        except Exception:  # noqa: BLE001
            node.titled = False
            logger.exception("title generation failed for %s", node.id[:8])

    # -- shutdown ----------------------------------------------------------
    async def shutdown(self) -> None:
        # Flush every in-memory node into the durable state file (SPEC §7.1),
        # then send RPC abort to every subprocess, close stdin, and wait.
        try:
            self.state.restore_all(self.graph)
        except Exception:  # noqa: BLE001
            logger.exception("failed to flush graph state on shutdown")
        await self.processes.shutdown()
async def _run_servers(hive: Hive, stop: asyncio.Event) -> None:
    gui_port = hive.config.server.guiPort
    api_port = hive.config.server.apiPort
    bind = hive.config.server.bind

    gui_server = uvicorn.Server(
        uvicorn.Config(
            create_gui_app(hive.broadcaster, graph=hive.graph, api_port=api_port, gui_port=gui_port),
            host=bind, port=gui_port, log_level="info",
        )
    )
    api_server = uvicorn.Server(
        uvicorn.Config(create_api_app(hive.api_context), host=bind, port=api_port, log_level="info")
    )

    gui_task = asyncio.create_task(gui_server.serve())
    api_task = asyncio.create_task(api_server.serve())

    logger.info("GUI server on ws://%s:%s/ws", bind, gui_port)
    logger.info("API server on ws://%s:%s/ws", bind, api_port)

    try:
        await stop.wait()
    finally:
        gui_server.should_exit = True
        api_server.should_exit = True
        await asyncio.gather(gui_task, api_task, return_exceptions=True)


def _install_signal_handlers(loop: asyncio.AbstractEventLoop, stop: asyncio.Event) -> None:
    def _request_stop(*_: Any) -> None:
        logger.info("shutdown signal received")
        stop.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _request_stop)
        except (NotImplementedError, RuntimeError):
            # On Windows SIGTERM is not supported by add_signal_handler.
            if sig == signal.SIGINT:
                try:
                    signal.signal(sig, lambda *a, **k: _request_stop())
                except Exception:  # noqa: BLE001
                    pass

    # Windows console-close delivers SIGBREAK (not SIGINT); register it so a
    # user closing the console window still triggers a graceful shutdown.
    if hasattr(signal, "SIGBREAK"):
        try:
            loop.add_signal_handler(signal.SIGBREAK, _request_stop)
        except (NotImplementedError, RuntimeError):
            try:
                signal.signal(signal.SIGBREAK, lambda *a, **k: _request_stop())
            except Exception:  # noqa: BLE001
                pass


async def _run_idle_reaper(hive: "Hive", interval: float = 30.0) -> None:
    """Periodically reclaim idle processes for finished conversations.

    Runs detached until the hive stops; errors are logged and never crash the
    loop. The interval is deliberately coarse, because reaping is a best-effort
    memory optimization, not latency-critical.
    """
    while True:
        try:
            await asyncio.sleep(interval)
            await hive.reap_idle_agents()
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            logger.exception("idle reaper iteration failed")


async def run(default_cwd: Optional[str] = None) -> None:
    config = load_config()
    hive = Hive(config, default_cwd=default_cwd)
    stop = asyncio.Event()
    _install_signal_handlers(asyncio.get_running_loop(), stop)

    # Restore ONLY the conversation *metadata* (sidebar shows every session,
    # but no subprocess is spawned and no .jsonl history is loaded). Each
    # session is materialized lazily via ensure_loaded when the user opens it.
    # A fresh primary is started only when there are no sessions to show.
    restored = await hive.restore_metadata()
    if restored == 0:
        await hive.start_primary()
    else:
        logger.info(
            "restored %d conversation(s) from previous run (lazy-load on open)",
            restored,
        )

    # Background idle-reaper: reclaim processes for conversations that have
    # finished and sat idle (restorable later via lazy-load).
    reaper_task = asyncio.create_task(_run_idle_reaper(hive))
    try:
        await _run_servers(hive, stop)
    finally:
        reaper_task.cancel()
        try:
            await reaper_task
        except asyncio.CancelledError:
            pass
        await hive.shutdown()
        logger.info("shutdown complete")


def _resolve_default_cwd(argv: Optional[List[str]] = None) -> Optional[str]:
    """Resolve the hive's default working directory from `--cwd` or PI_HIVE_CWD.

    Returns an absolute path, or None to fall back to os.getcwd().
    """
    args = list(sys.argv[1:] if argv is None else argv)
    val: Optional[str] = None
    try:
        i = args.index("--cwd")
        if i + 1 < len(args):
            val = args[i + 1]
    except ValueError:
        pass
    if not val:
        val = os.environ.get("PI_HIVE_CWD")
    if val:
        val = os.path.abspath(os.path.expanduser(val))
        if not os.path.isdir(val):
            logger.warning(
                "hive default cwd %r is not a directory; falling back to current dir",
                val,
            )
            val = None
    return val


def main() -> None:
    try:
        asyncio.run(run(default_cwd=_resolve_default_cwd()))
    except KeyboardInterrupt:
        logger.info("interrupted")


if __name__ == "__main__":
    main()
