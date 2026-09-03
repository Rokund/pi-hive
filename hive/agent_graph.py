"""In-memory management of `AgentNode`s.

Mirrors SPEC §4.1: a single-parent tree rooted at the primary.  Each node owns
its `childrenIds`; the graph enforces one parent per node.  DAG sharing is out
of scope for this version.
"""

from __future__ import annotations

import re
import threading
from typing import Dict, List, Optional, Tuple

from .config import HiveConfig
from .models import AgentNode


class NodeNotFoundError(KeyError):
    pass


class DuplicateNodeError(ValueError):
    pass


# SPEC §9 error strings for spawn validation.  Keep them exact so the model
# (and tests) can match on them.
NOT_ALLOWED_ERROR = "not allowed"
MAX_CONCURRENT_ERROR = "max concurrent subagents reached"
MAX_PER_AGENT_CONCURRENT_ERROR = "max concurrent subagents of this type reached"
UNKNOWN_PARENT_ERROR = "unknown parent"

#: Sentinel in an agent_allowlist meaning "may spawn any defined agent".
ALLOW_ALL = "*"


def name_allowed(requested_name: str, agent_allowlist) -> bool:
    """True if `requested_name` may be spawned under `agent_allowlist`.

    Semantics (SPEC §8):
      - empty / None  -> deny all (leaf node, may not spawn anything)
      - any `"*"`     -> allow all defined agents
      - other entries -> regex patterns matched against the requested name;
        a plain name (e.g. "reviewer") is a valid pattern matching that name.
    """
    patterns = agent_allowlist or []
    for pattern in patterns:
        if pattern == ALLOW_ALL:
            return True
        try:
            if re.search(pattern, requested_name):
                return True
        except re.error:
            # Defensive: config validation should have caught a bad pattern;
            # fall back to an exact-string match so a typo can't crash spawn.
            if pattern == requested_name:
                return True
    return False


class AgentGraph:
    """Thread-safe wrapper around a node-id -> AgentNode dict."""

    def __init__(self, nodes: Optional[List[AgentNode]] = None) -> None:
        self._nodes: Dict[str, AgentNode] = {}
        self._lock = threading.RLock()
        for n in nodes or []:
            self._nodes[n.id] = n

    # -- basic access -----------------------------------------------------
    def get_node(self, node_id: str) -> AgentNode:
        with self._lock:
            node = self._nodes.get(node_id)
            if node is None:
                raise NodeNotFoundError(node_id)
            return node

    def has_node(self, node_id: str) -> bool:
        with self._lock:
            return node_id in self._nodes

    def rekey_node(self, old_id: str, new_id: str) -> Optional[AgentNode]:
        """Change a node's id, preserving all edges (SPEC §7.1 id sync).

        Used when pi reports its real sessionId after spawn and it differs
        from the hive-assigned id: the graph dict key AND every parent/child
        reference must move together, otherwise lookups by the new id fail
        (agent not found) while tree serialization shows only the new id.
        """
        if old_id == new_id:
            return self.get_node(old_id)
        with self._lock:
            node = self._nodes.pop(old_id, None)
            if node is None:
                return None
            self._nodes[new_id] = node
            # Repoint children at the new parent id.
            for cid in node.childrenIds:
                child = self._nodes.get(cid)
                if child is not None and child.parentId == old_id:
                    child.parentId = new_id
            # Repoint the parent's childrenIds entry.
            if node.parentId:
                parent = self._nodes.get(node.parentId)
                if parent is not None:
                    parent.childrenIds = [
                        new_id if c == old_id else c for c in parent.childrenIds
                    ]
            return node

    def get_tree(self) -> List[AgentNode]:
        """Return a copy of all nodes (as mutable dicts for API serialization).

        Ordered newest-conversation-first so the GUI sidebar lists the most
        recent conversation at the top and older ones sink toward the bottom.
        The GUI rebuilds parent/child nesting from ids, so the flat order only
        decides root (primary conversation) placement.
        """
        with self._lock:
            nodes = [n.model_copy(deep=True) for n in self._nodes.values()]
        nodes.sort(key=lambda n: n.createdAt, reverse=True)
        return nodes

    def __len__(self) -> int:
        with self._lock:
            return len(self._nodes)

    # -- mutation ---------------------------------------------------------
    def add_node(self, node: AgentNode) -> AgentNode:
        """Add a node, atomically wiring up the parent/child edges."""
        with self._lock:
            if node.id in self._nodes:
                raise DuplicateNodeError(node.id)
            self._nodes[node.id] = node
            if node.parentId is not None and node.parentId in self._nodes:
                parent = self._nodes[node.parentId]
                if node.id not in parent.childrenIds:
                    parent.childrenIds.append(node.id)
            return node

    def remove_node(self, node_id: str) -> Optional[AgentNode]:
        """Remove a node and detach it from its parent's children list."""
        with self._lock:
            node = self._nodes.pop(node_id, None)
            if node is None:
                return None
            if node.parentId is not None and node.parentId in self._nodes:
                parent = self._nodes[node.parentId]
                if node_id in parent.childrenIds:
                    parent.childrenIds.remove(node_id)
            return node

    def update_node(self, node_id: str, **fields) -> AgentNode:
        """In-place update of a node. Fields are applied to the existing model."""
        with self._lock:
            node = self._nodes.get(node_id)
            if node is None:
                raise NodeNotFoundError(node_id)
            for key, value in fields.items():
                if key in ("parentId", "childrenIds"):
                    # Re-wire recursively handled below via explicit edge ops.
                    setattr(node, key, value)
                else:
                    setattr(node, key, value)
            return node

    def add_child_edge(self, parent_id: str, child_id: str) -> None:
        """Mark `child_id` as a direct child of `parent_id`."""
        with self._lock:
            parent = self._nodes.get(parent_id)
            child = self._nodes.get(child_id)
            if parent is None or child is None:
                raise NodeNotFoundError(
                    parent_id if parent is None else child_id
                )
            child.parentId = parent_id
            if child_id not in parent.childrenIds:
                parent.childrenIds.append(child_id)

    def children_of(self, node_id: str) -> List[AgentNode]:
        with self._lock:
            node = self._nodes.get(node_id)
            if node is None:
                return []
            return [
                self._nodes[cid]
                for cid in node.childrenIds
                if cid in self._nodes
            ]

    def root(self) -> Optional[AgentNode]:
        with self._lock:
            for n in self._nodes.values():
                if n.kind == "primary" and n.parentId is None:
                    return n
            return None

    def primary_id(self) -> Optional[str]:
        """Return the id of the first (oldest) primary agent, or None."""
        root = self.root()
        return root.id if root is not None else None

    def latest_primary_id(self) -> Optional[str]:
        """Id of the most recently created primary (newest conversation).

        With multiple conversations the implicit target for steer/follow_up/
        abort should be the one the user is actually working in — the latest —
        not the oldest root.
        """
        with self._lock:
            primaries = [
                n for n in self._nodes.values()
                if n.kind == "primary" and n.parentId is None
            ]
            if not primaries:
                return None
            return max(primaries, key=lambda n: n.createdAt).id

    # -- spawn validation (SPEC §8 / §9, M5) -------------------------------
    def running_subagent_count(self) -> int:
        """Number of subagent nodes currently in the `running` state.

        The primary root is not counted.  Used to enforce
        `server.maxConcurrentSubagents`.
        """
        with self._lock:
            return sum(
                1
                for n in self._nodes.values()
                if n.kind == "subagent" and n.status == "running"
            )

    def running_subagent_count_of_type(self, name: str) -> int:
        """Number of running subagent nodes whose profile name matches `name`.

        Used to enforce the per-agent-type ``max_concurrency`` ceiling from the
        hive config.  Only nodes with a subagent profile whose ``profile.name``
        equals `name` are counted; the primary root is never counted.
        """
        with self._lock:
            return sum(
                1
                for n in self._nodes.values()
                if n.kind == "subagent"
                and n.status == "running"
                and n.profile is not None
                and n.profile.name == name
            )

    def check_spawn(
        self, parent_id: str, requested_name: str, config: Optional[HiveConfig] = None
    ) -> Tuple[bool, str]:
        """Validate a `subagent_spawn(parent_id, requested_name)` request.

        Enforces both M5 constraints before any process is created:
          1. `requested_name` must be permitted by the parent's
             `profile.agent_allowlist` (SPEC §8/§9 — otherwise
             `{ok: false, error: "not allowed"}`).  Empty allowlist = deny
             all; `"*"` = allow all; other entries are regex patterns.
          2. The number of concurrently running subagents must be below
             `config.server.maxConcurrentSubagents` (SPEC §7).
          3. When the requested agent type has a per-agent
             `max_concurrency` configured, the number of concurrently
             running subagents of that type must be below it.  On a violation
             the returned error explains which agent type hit its ceiling and
             its limit, so the calling LLM knows the actual constraint.

        Returns ``(ok, error)``; ``error`` is empty when ``ok`` is True.
        """
        with self._lock:
            parent = self._nodes.get(parent_id)
            if parent is None:
                return False, f"{UNKNOWN_PARENT_ERROR}: {parent_id}"
            patterns = parent.profile.agent_allowlist or []
            if not name_allowed(requested_name, patterns):
                # Prefix stays the exact SPEC §9 string; the suffix tells the
                # calling LLM what IS allowed so its next attempt is correct
                # instead of another guess.
                hint = ", ".join(f"\"{p}\"" for p in patterns) if patterns else "(none)"
                return False, (
                    f'{NOT_ALLOWED_ERROR}: "{requested_name}" — '
                    f"this agent may only spawn: {hint}"
                )
        if config is not None:
            limit = config.server.maxConcurrentSubagents
            if self.running_subagent_count() >= limit:
                return False, MAX_CONCURRENT_ERROR
            # Per-agent-type ceiling (profile.max_concurrency).  When set,
            # refuse once the running instances of THIS type reach the cap,
            # returning a reason with the agent name, current count, and limit.
            profile = config.profile_by_name(requested_name)
            per_limit = profile.max_concurrency if profile is not None else None
            if per_limit is not None:
                type_count = self.running_subagent_count_of_type(profile.name)
                if type_count >= per_limit:
                    return False, (
                        f'{MAX_PER_AGENT_CONCURRENT_ERROR}: "{profile.name}" '
                        f"({type_count} running; limit {per_limit}). "
                        f"Each run counts while active (even a reused one via "
                        f"subagent_followup). Free a slot by waiting for one to "
                        f"settle (subagent_result) or aborting a currently-"
                        f"running one (subagent_abort)."
                    )
        return True, ""

    def can_spawn(self, parent_id: str, requested_name: str, config: Optional[HiveConfig] = None) -> bool:
        """Boolean form of :meth:`check_spawn` (M5 deliverable surface)."""
        ok, _ = self.check_spawn(parent_id, requested_name, config)
        return ok
