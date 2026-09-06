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
from hive.process_manager import QA_TOOLS, SUBAGENT_TOOLS, build_rpc_args


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


# ---------------------------------------------------------------------------
# Inter-agent Q&A tools merge (ADR-0001 / issue #4)
# ---------------------------------------------------------------------------
def _tools_csv(args) -> "list[str]":
    assert "--tools" in args, f"no --tools flag in {args}"
    return args[args.index("--tools") + 1].split(",")


def test_qa_tools_merge_for_every_profile_even_with_empty_agent_allowlist() -> None:
    # Q&A addressing is by relation (direct parent/child), NOT by spawn
    # agent_allowlist: an agent with an EMPTY allowlist (deny-all spawning,
    # a leaf node that gets no subagent tools) still gets the 4 QA tools.
    profile = AgentProfile(name="t", model="m", tools=["read"], agent_allowlist=[])
    names = _tools_csv(build_rpc_args(profile, "node123", None, "/tmp"))
    assert "read" in names
    for tool in QA_TOOLS:
        assert tool in names
    # ...and the subagent tools are NOT merged for an empty allowlist.
    for tool in SUBAGENT_TOOLS:
        assert tool not in names


def test_qa_tools_merge_alongside_subagent_tools() -> None:
    # A spawning agent (non-empty allowlist) gets BOTH tool sets.
    profile = AgentProfile(name="t", model="m", tools=["read"], agent_allowlist=["coder"])
    names = _tools_csv(build_rpc_args(profile, "node123", None, "/tmp"))
    for tool in QA_TOOLS:
        assert tool in names
    for tool in SUBAGENT_TOOLS:
        assert tool in names


def test_deny_all_tools_profile_still_gets_qa_tools() -> None:
    # tools: [] is deny-all in pi, but Q&A must remain available to EVERY
    # family member: the merged QA names turn the bare --no-tools into a
    # QA-only allowlist instead.
    profile = AgentProfile(name="t", model="m", tools=[], agent_allowlist=[])
    args = build_rpc_args(profile, "node123", None, "/tmp")
    assert "--no-tools" not in args
    assert _tools_csv(args) == list(QA_TOOLS)


def test_star_tools_profile_passes_no_tools_flag() -> None:
    # "*" (allow-all) profiles are unaffected: no --tools is passed at all,
    # so pi keeps every tool (QA tools included) visible.
    profile = AgentProfile(name="t", model="m", tools=["*"], agent_allowlist=[])
    args = build_rpc_args(profile, "node123", None, "/tmp")
    assert "--tools" not in args
    assert "--no-tools" not in args
