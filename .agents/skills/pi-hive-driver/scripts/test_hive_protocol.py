"""Dev-only tests for the shared protocol layer, hive_protocol.py.

Cover the correctness invariants that BOTH drivers (python_client.py and
hivedriver.py) rely on, so the shared source is itself pinned down:
final-text extraction, new-primary discovery, settle-signal detection, and
tool-call dedupe/agent filtering.

Run from the repo root:
    .venv/bin/python -m pytest -q .agents/skills/pi-hive-driver/scripts/test_hive_protocol.py
"""
from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from test_python_client import ev, upd, settle_event, message_end, tool_end  # noqa: E402
from hive_protocol import (TurnTracker, candidate_primary_ids,  # noqa: E402
                           extract_block_texts, primary_ids_from_tree)


# ------------------------------------------------------------- text extraction
def test_extract_block_texts_assistant_only_and_nonempty():
    assert extract_block_texts({"role": "assistant",
                                "content": [{"type": "text", "text": "hi"}]}) == ["hi"]
    assert extract_block_texts({"role": "user",
                                "content": [{"type": "text", "text": "hi"}]}) == []
    assert extract_block_texts({"role": "assistant",
                                "content": [{"type": "text", "text": ""}]}) == []
    assert extract_block_texts({"role": "assistant", "content": "plain"}) == ["plain"]
    assert extract_block_texts(None) == []


def test_primary_ids_from_tree():
    tree = [{"id": "p1", "kind": "primary"}, {"id": "s1", "kind": "subagent"}]
    assert primary_ids_from_tree(tree) == {"p1"}


# ---------------------------------------------------------------- discovery
def test_discover_only_new_not_pre():
    t = TurnTracker(pre_primary_ids=["old"])
    assert t.discover(upd({"id": "new1", "kind": "primary", "status": "running"}), None) == "new1"
    # pre-existing id is never a discovery candidate
    assert t.discover(upd({"id": "old", "kind": "primary", "status": "idle"}), None) is None
    # a subagent is never a primary-discovery candidate
    assert t.discover(upd({"id": "s1", "kind": "subagent", "status": "done"}), None) is None


def test_discovery_from_tree_frame():
    t = TurnTracker(pre_primary_ids=["old"])
    tree_frame = {"type": "hive:tree", "tree": [
        {"id": "old", "kind": "primary"}, {"id": "new1", "kind": "primary"}]}
    # candidate_primary_ids reports ALL primaries in the tree; discover()
    # filters out the pre-existing "old" against the turn's pre-set.
    assert set(candidate_primary_ids(tree_frame)) == {"old", "new1"}
    assert t.discover(tree_frame, None) == "new1"


# ------------------------------------------------------------- settle signals
def test_agent_settled_is_authoritative_signal():
    t = TurnTracker(pre_primary_ids=[])
    t.claim("a1")
    sig = t.observe(settle_event("a1", kind="primary", status="idle"))
    assert sig == {"agent_id": "a1", "status": "idle", "kind": "primary", "source": "settled"}


def test_done_snapshot_signal():
    t = TurnTracker(pre_primary_ids=[])
    t.claim("a1")
    sig = t.observe(upd({"id": "a1", "kind": "primary", "status": "idle"}))
    assert sig is not None and sig["source"] == "snapshot" and sig["status"] == "idle"
    # a non-done snapshot produces no signal
    assert t.observe(upd({"id": "a1", "kind": "primary", "status": "running"})) is None


# ------------------------------------------------------------- collection rules
def test_message_end_only_target_no_leak():
    t = TurnTracker(pre_primary_ids=[])
    t.claim("a1")
    t.observe(message_end("other", "noise"))
    t.observe(message_end("a1", "mine"))
    assert t.final_texts == ["mine"]


def test_tool_dedupe_and_filter():
    t = TurnTracker(pre_primary_ids=[])
    t.claim("a1")
    t.observe(tool_end("a1", "t1", "bash"))
    t.observe(tool_end("a1", "t1", "bash"))   # dup id
    t.observe(tool_end("other", "x", "read"))  # other agent
    assert t.tool_calls == [{"agentId": "a1", "name": "bash"}]


def test_message_update_not_collected():
    t = TurnTracker(pre_primary_ids=[])
    t.claim("a1")
    t.observe(ev("a1", {"type": "message_update",
                        "message": {"role": "assistant",
                                    "content": [{"type": "text", "text": "delta"}]}}))
    assert t.final_texts == []   # only message_end, never deltas
