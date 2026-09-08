"""Shared, dependency-free protocol logic for the pi-hive WS drivers.

This module is the ONE place where the correctness-critical frame folding and
spawn-discovery logic lives, used by BOTH drivers in this directory:

  * ``python_client.py`` — the blocking, single-shot reference client; and
  * ``hivedriver.py``    — the persistent async daemon for agent harnesses.

Extracting these into a single module is deliberate: the original two files
each carried their own copy of the same invariant-bearing code (final-answer
extraction, settle-signal detection, tool-call dedupe, new-primary discovery)
and any protocol change risked silent drift between them. Keeping the pure
logic here, with transport/call-flow left to the drivers, makes correctness
enumerable and testable in exactly one place.

Everything here is PURE: no sockets, threads, I/O or logging. The pieces that
are genuinely different between a blocking single-shot client and an async
multi-turn daemon — connection lifecycle, the response-ack barrier, per-turn
routing, the FIFO/daemon interface — deliberately stay in the drivers.

Protocol invariants enforced by this module (never do these in the drivers):
  * Authoritative final text comes from ``message_end`` assistant blocks, never
    from ``message_update`` deltas.
  * Completion is signaled by ``agent_settled`` (authoritative) or a done node
    snapshot (status idle/done); both are returned as a structured settle
    signal, which a driver may still gate behind its own ack barrier.
  * A fresh bare-prompt primary is discovered by id (kind==primary) that was
    NOT present in the tree before the prompt — never a pre-existing
    conversation.
  * Tool calls are recorded at most once per id.
"""

from __future__ import annotations

import time
from typing import Any, Dict, List, Optional, Sequence


# --------------------------------------------------------------------------- #
# Pure helpers                                                                 #
# --------------------------------------------------------------------------- #
def extract_block_texts(message: Optional[dict]) -> list[str]:
    """Extract final assistant text blocks from a ``message_end`` payload.

    Accepts the assistant message shapes used by pi: ``content`` as a list of
    blocks (``{type:"text", text}``) or a plain string. Returns only non-empty
    text. This is the authoritative answer source — never use ``message_update``
    deltas for final text.
    """
    if not isinstance(message, dict) or message.get("role") != "assistant":
        return []
    content = message.get("content")
    out: list[str] = []
    if isinstance(content, list):
        for c in content:
            if isinstance(c, dict) and c.get("type") == "text" and c.get("text"):
                out.append(str(c["text"]))
    elif isinstance(content, str) and content.strip():
        out.append(content)
    return out


def primary_ids_from_tree(tree: Sequence[dict]) -> set[str]:
    """The set of node ids of kind ``primary`` in a ``get_tree`` payload."""
    return {n.get("id") for n in tree if n.get("kind") == "primary"}


def candidate_primary_ids(frame: dict) -> list[str]:
    """Primary ids this frame may reveal as brand-new (spawn discovery).

    A bare-prompt spawn surfaces its new root via a ``hive:agent_updated``
    (agent.kind == primary) or ``hive:tree`` frame. Return just the candidate
    ids; the caller decides (against its pre-prompt tree snapshot) which are
    genuinely new.
    """
    cands: list[str] = []
    ftype = frame.get("type")
    if ftype == "hive:agent_updated":
        ag = frame.get("agent") or {}
        if ag.get("kind") == "primary" and ag.get("id"):
            cands.append(ag["id"])
    elif ftype == "hive:tree":
        for n in frame.get("tree") or []:
            if n.get("kind") == "primary" and n.get("id"):
                cands.append(n["id"])
    return cands


# --------------------------------------------------------------------------- #
# TurnTracker                                                                  #
# --------------------------------------------------------------------------- #
class TurnTracker:
    """Fold a stream of WS frames into ONE turn's observed result.

    Stateless with respect to transport: call :meth:`observe` for each frame
    (the driver routes frames to the right tracker by agent id) and read the
    accumulated fields. Completion arbitration (which settle signal may
    complete the drive, behind an ack barrier) is the driver's job.

    A tracker also helps resolve a *bare* prompt (no agent id): it knows the
    primary ids that existed before the prompt and will only ever accept a
    brand-new id as its target via :meth:`discover` / :meth:`claim`.
    """

    def __init__(self, pre_primary_ids: Sequence[str] = ()):
        self.pre: frozenset = frozenset(pre_primary_ids)
        self.target: Optional[str] = None          # resolved target id (None until discovered)
        self.final_texts: list[str] = []           # assistant message_end text, target only
        self.tool_calls: list[dict] = []           # deduped {agentId, name}
        self.observed: Dict[str, dict] = {}        # id -> {status, done, final_text}
        self._seen_tool_keys: set = set()

    # --------------------------------------------------------------- discovery
    def discover(self, frame: dict, current: Optional[str]) -> Optional[str]:
        """Return a candidate new-primary id from this frame, or None.

        Only ids never seen before the prompt (not in ``pre``) and not equal to
        ``current`` qualify — a brand-new bare-prompt spawn, not a
        pre-existing conversation.
        """
        for cid in candidate_primary_ids(frame):
            if cid not in self.pre and cid != current:
                return cid
        return None

    def claim(self, agent_id: str) -> None:
        """Adopt ``agent_id`` as this turn's resolved target (after discovery)."""
        self.target = agent_id

    # ---------------------------------------------------------------- folding
    def observe(self, frame: dict) -> Optional[dict]:
        """Fold one frame into this turn.

        Returns a structured settle signal ``{agent_id, status, kind, source}``
        when THIS frame marks the target settled/done — an ``agent_settled``
        event (source ``"settled"``) or a done node snapshot (source
        ``"snapshot"``) — else None. The caller gates completion on the signal
        matching its target and passing its ack barrier.

        ``message_end`` and ``tool_execution_end`` frames are collected ONLY
        for the resolved target (``self.target``); other agents' frames never
        leak into this turn's transcript/tool list.
        """
        ftype = frame.get("type")
        if ftype == "hive:agent_updated":
            ag = frame.get("agent") or {}
            aid = ag.get("id")
            if aid:
                snap = self._snapshot(ag)
                self.observed[aid] = snap
                if snap["done"]:
                    return {"agent_id": aid, "status": snap["status"],
                            "kind": ag.get("kind"), "source": "snapshot"}
            return None
        if ftype != "hive:event":
            return None
        ev = frame.get("event") or {}
        aid = frame.get("agentId")
        etype = ev.get("type")
        if etype == "agent_settled" and aid:
            s = ev.get("settled") or {}
            return {"agent_id": aid, "status": s.get("status"),
                    "kind": s.get("kind"), "source": "settled"}
        if etype == "message_end" and aid and aid == self.target:
            self.final_texts.extend(extract_block_texts(ev.get("message")))
            return None
        if etype == "tool_execution_end" and aid and aid == self.target:
            self._record_tool(aid, ev)
            return None
        return None

    # ---------------------------------------------------------------- internals
    @staticmethod
    def _snapshot(node: dict) -> dict:
        st = node.get("status")
        done = st in ("idle", "done")
        last = node.get("lastResult") or {}
        return {"status": st, "done": done,
                "final_text": (last.get("finalText") or last.get("final_text") or "")}

    def _record_tool(self, aid: str, ev: dict) -> None:
        name = ev.get("toolName") or ev.get("name")
        # Dedupe by the tool's own id; fall back to a unique stamp only when a
        # server omitted both id fields, so two distinct nameless calls are not
        # collapsed into one.
        key = (aid, ev.get("toolCallId") or ev.get("id")
               or f"{aid}:{name}:{time.time()}")
        if key in self._seen_tool_keys:
            return
        self._seen_tool_keys.add(key)
        self.tool_calls.append({"agentId": aid, "name": name})


__all__ = [
    "TurnTracker",
    "extract_block_texts",
    "primary_ids_from_tree",
    "candidate_primary_ids",
]
