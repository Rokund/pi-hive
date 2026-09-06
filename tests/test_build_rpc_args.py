"""Regression tests for pi spawn argument construction.

Guards the "sending to an archived / never-persisted session jumps to an
empty page" bug: a node whose persisted session file is missing (e.g. a
brand-new conversation that never produced a turn and was reaped) must boot
pi with ``--session-id <node_id>`` so pi reuses the exact hive id instead of
minting a fresh id. A fresh id rekeys the node, orphans the GUI selection,
and drops the in-flight prompt.

pi refuses ``--session``` combined with ``--session-id``, so the two are
mutually exclusive and chosen by whether real history exists on disk.
"""

from __future__ import annotations

from hive.models import AgentProfile
from hive.process_manager import build_rpc_args


def _profile() -> AgentProfile:
    return AgentProfile(name="t", model="m")


def test_existing_session_file_uses_session_flag(tmp_path) -> None:
    f = tmp_path / "s.jsonl"
    f.write_text("{}")
    args = build_rpc_args(_profile(), "node123", str(f), str(tmp_path))
    assert "--session" in args
    assert str(f) in args
    assert "--session-id" not in args


def test_missing_session_file_pins_session_id(tmp_path) -> None:
    missing = tmp_path / "gone.jsonl"  # referenced but does not exist
    args = build_rpc_args(_profile(), "node123", str(missing), str(tmp_path))
    assert "--session-id" in args
    assert "node123" in args
    assert "--session" not in args


def test_no_session_file_pins_session_id() -> None:
    args = build_rpc_args(_profile(), "node123", None, "/tmp")
    assert "--session-id" in args
    assert "node123" in args
    assert "--session" not in args
