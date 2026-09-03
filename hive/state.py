"""Persistence of the agent graph + session-file mapping.

pi-hive only persists the minimal metadata needed for restorage (SPEC
§7.1): the graph relations and each node's *real* pi session `.jsonl` path.
The full transcript lives in pi's own session files; we never duplicate it.

State file: `hive/state/hive.state.json` (git-ignored).
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from .models import AgentNode

STATE_RELATIVE = Path("state") / "hive.state.json"


def default_state_dir() -> Path:
    return Path(__file__).resolve().parent / "state"


def default_state_file() -> Path:
    return default_state_dir() / STATE_RELATIVE.name


def _node_to_persisted(node: AgentNode) -> Dict[str, Any]:
    """Project an AgentNode onto the minimal persisted shape (SPEC §7.1)."""
    return {
        "titled": getattr(node, "titled", False),
        "id": node.id,
        "kind": node.kind,
        "name": node.name,
        "parentId": node.parentId,
        "cwd": node.cwd,
        "configSnapshot": node.profile.model_dump(),
        "sessionFile": node.sessionFile,
        "createdAt": node.createdAt,
        "finishedAt": node.finishedAt,
    }


class HiveState:
    """Load / save the persisted agent graph.

    `agents` maps node id -> persisted dict, mirroring SPEC §7.1.  This is not
    a full live graph store (that lives in `AgentGraph`); it is the durable
    association record for restore.
    """

    def __init__(self, path: Optional[Path | str] = None) -> None:
        self.path = Path(path) if path is not None else default_state_file()
        self.agents: Dict[str, Dict[str, Any]] = {}
        self.updated_at: Optional[str] = None

    # -- load -------------------------------------------------------------
    def load(self) -> "HiveState":
        if not self.path.is_file():
            return self
        data = json.loads(self.path.read_text(encoding="utf-8"))
        self.agents = {
            a["id"]: a for a in data.get("agents", []) if "id" in a
        }
        self.updated_at = data.get("updated_at")
        return self

    def restore_node(self, node: AgentNode) -> "HiveState":
        """Overwrite in-memory persistence record for a given node and flush."""
        self.agents[node.id] = _node_to_persisted(node)
        self.save()
        return self

    def restore_all(self, graph: Any) -> "HiveState":
        """Persist every node currently in the live graph.

        Mirrors SPEC §7.1 and is used to flush the full tree on shutdown.  Each
        node must expose the same fields as `AgentNode` (id/kind/name/parentId/
        cwd/profile/sessionFile/createdAt/finishedAt); the real `sessionFile`
        (set from `get_state` at spawn) is saved verbatim so `pi --session
        <file>` can restore it later.
        """
        for node in graph.get_tree():
            try:
                session_file = getattr(node, "sessionFile", None)
                if session_file and not Path(session_file).is_file():
                    # Dead session path (its .jsonl no longer exists): drop the
                    # record so the durable ledger never accumulates stale refs.
                    self.agents.pop(node.id, None)
                    continue
                self.agents[node.id] = _node_to_persisted(node)
            except Exception:  # noqa: BLE001
                continue
        self.save()
        return self

    def remove_node(self, node_id: str) -> "HiveState":
        self.agents.pop(node_id, None)
        self.save()
        return self

    # -- save -------------------------------------------------------------
    def save(self) -> "HiveState":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": 1,
            "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "agents": list(self.agents.values()),
        }
        tmp = self.path.with_suffix(".json.tmp")
        tmp.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        tmp.replace(self.path)
        self.updated_at = payload["updated_at"]
        return self


def load_state(path: Optional[Path] = None) -> HiveState:
    return HiveState(path=path).load()
