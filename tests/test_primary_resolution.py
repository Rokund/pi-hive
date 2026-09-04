"""Focused tests for ticket #2 — primary resolution & eligibility (EXPAND).

These tests build HiveConfig objects from inline dictionaries and route them
through the same load/validate path as `load_config`, with NO dependency on the
on-disk `hive/hive.config.json` and NO running daemon. They cover primary
eligibility, registry-based name resolution, spawn selection (including the
clear spawn-time errors), the legacy-shape shim, and the derived /api/models
default/candidates.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

import pytest

from fastapi.testclient import TestClient

from hive.config import HiveConfig
from hive.main import Hive
from hive.models import AgentProfile
from hive.server import ApiContext, EventBroadcaster, create_api_app


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------
def build_config(raw: Dict[str, Any]) -> HiveConfig:
    """Mirror load_config's validation path without the on-disk file."""
    return HiveConfig.model_validate(raw).validate()


def _server(**kw: Any) -> Dict[str, Any]:
    base = {"bind": "127.0.0.1", "guiPort": 3100, "apiPort": 3101}
    base.update(kw)
    return base


def _agent(name: str, model: Optional[str] = None, **kw: Any) -> Dict[str, Any]:
    if model is None:
        model = f"m-{name}"
    return {"name": name, "model": model, **kw}


def _make_hive(cfg: HiveConfig) -> "Hive":
    # Hive.__init__ wires graph/processes/broadcaster but spawns nothing, so
    # this is safe offline. Config is not read from disk.
    return Hive(cfg, default_cwd="C:/work")


def _api_client(cfg: HiveConfig, spawn_primary=None):
    """A TestClient over the API app with a real config (+ optional spawn stub)."""
    ctx = ApiContext(
        graph=None,
        processes=None,
        config=cfg,
        broadcaster=EventBroadcaster(),
    )
    ctx.spawn_primary = spawn_primary if spawn_primary is not None else (
        lambda **kw: None
    )
    return TestClient(create_api_app(ctx)), ctx


# ---------------------------------------------------------------------------
# Eligibility rule
# ---------------------------------------------------------------------------
def test_eligibility_none_flagged_means_all_eligible():
    cfg = build_config({
        "server": _server(),
        "agents": [_agent("a"), _agent("b"), _agent("c")],
        "default_primary": "a",
    })
    assert cfg.primary_eligibility() == ["a", "b", "c"]
    assert cfg.is_primary_eligible("a")
    assert cfg.is_primary_eligible("c")


def test_eligibility_some_flagged_means_only_flagged_eligible():
    cfg = build_config({
        "server": _server(),
        "agents": [
            _agent("a"),
            _agent("b", allow_as_primary=True),
            _agent("c", allow_as_primary=False),
        ],
        "default_primary": "b",
    })
    assert cfg.primary_eligibility() == ["b"]
    assert cfg.is_primary_eligible("b")
    assert not cfg.is_primary_eligible("a")
    assert not cfg.is_primary_eligible("c")


def test_eligibility_flag_only_restricts_primary_not_subagents():
    cfg = build_config({
        "server": _server(),
        "agents": [
            _agent("a"),
            _agent("b", allow_as_primary=True),
            _agent("c", allow_as_primary=False),
        ],
        "default_primary": "b",
    })
    # `profile_by_name` resolves every agent regardless of the eligibility flag:
    # subagent spawnability is unaffected by allow_as_primary.
    for name in ("a", "b", "c"):
        assert cfg.profile_by_name(name) is not None


# ---------------------------------------------------------------------------
# Registry-based name resolution
# ---------------------------------------------------------------------------
def test_known_names_and_profile_derive_from_agents_only():
    cfg = build_config({
        "server": _server(),
        "agents": [_agent("a"), _agent("b")],
        "default_primary": "a",
    })
    assert cfg.known_names() == ["a", "b"]
    # The public registry accessor is the agents list, verbatim.
    assert [p.name for p in cfg.registry_profiles()] == ["a", "b"]
    # The legacy `_registry()` alias used by hive/server.py agrees with it.
    assert [p.name for p in cfg._registry()] == [p.name for p in cfg.registry_profiles()]
    for name in ("a", "b"):
        p = cfg.profile_by_name(name)
        assert p is not None and p.name == name
    assert cfg.profile_by_name("nope") is None


def test_legacy_shim_synthesizes_primary_as_default():
    raw = {
        "server": _server(),
        "primary": {
            "name": "primary",
            "model": "m-p",
            "agent_allowlist": ["tester"],
            "tools": ["*"],
        },
        "agents": [_agent("tester")],
    }
    cfg = build_config(raw)
    # Legacy config boots identical to today: known names == primary + agents.
    assert cfg.known_names() == ["primary", "tester"]
    # The primary entry is resolved from the registry and is the default.
    default = cfg.default_primary_profile()
    assert default.name == "primary"
    assert default.model == "m-p"
    assert cfg.profile_by_name("primary") is not None
    # Legacy primary is primary-eligible (no flags -> all eligible).
    assert cfg.is_primary_eligible("primary")


# ---------------------------------------------------------------------------
# Spawn selection
# ---------------------------------------------------------------------------
def test_spawn_explicit_eligible_agent_is_used():
    cfg = build_config({
        "server": _server(),
        "agents": [_agent("a"), _agent("b", allow_as_primary=True)],
        "default_primary": "b",
    })
    hive = _make_hive(cfg)
    node = hive.make_primary_node(agent="b")
    assert node.kind == "primary"
    assert node.profile.name == "b"
    assert node.profile.model == "m-b"


def test_spawn_omitting_agent_falls_back_to_default_primary():
    cfg = build_config({
        "server": _server(),
        "agents": [_agent("a"), _agent("b", allow_as_primary=True)],
        "default_primary": "b",
    })
    hive = _make_hive(cfg)
    node = hive.make_primary_node()  # no agent -> default_primary
    assert node.profile.name == "b"


def test_spawn_unknown_agent_raises_clear_error():
    cfg = build_config({
        "server": _server(),
        "agents": [_agent("a", allow_as_primary=True)],
        "default_primary": "a",
    })
    hive = _make_hive(cfg)
    with pytest.raises(ValueError, match="unknown agent"):
        hive.make_primary_node(agent="does-not-exist")


def test_spawn_non_eligible_agent_raises_clear_error():
    cfg = build_config({
        "server": _server(),
        "agents": [_agent("a"), _agent("b", allow_as_primary=True)],
        "default_primary": "b",
    })
    hive = _make_hive(cfg)
    with pytest.raises(ValueError, match="not primary-eligible"):
        hive.make_primary_node(agent="a")


def test_spawn_endpoint_threads_agent_selection():
    cfg = build_config({
        "server": _server(),
        "agents": [_agent("a"), _agent("b", allow_as_primary=True)],
        "default_primary": "b",
    })

    captured: Dict[str, Any] = {}

    async def fake_spawn_primary(**kw):
        captured.update(kw)
        node = _make_hive(cfg).make_primary_node(**kw)
        return node

    client, _ = _api_client(cfg, spawn_primary=fake_spawn_primary)

    # Explicit eligible agent threads through to spawn_primary(agent=...).
    resp = client.post("/api/primary/spawn", json={"agent": "b"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    assert captured.get("agent") == "b"

    # Omitting agent -> spawn_primary called with agent=None (falls back).
    resp = client.post("/api/primary/spawn", json={})
    assert resp.status_code == 200
    assert resp.json()["ok"] is True
    assert captured.get("agent") is None


def test_spawn_endpoint_returns_clear_error_for_bad_agent():
    cfg = build_config({
        "server": _server(),
        "agents": [_agent("a"), _agent("b", allow_as_primary=True)],
        "default_primary": "b",
    })

    async def fake_spawn_primary(**kw):
        return _make_hive(cfg).make_primary_node(**kw)

    hive = _make_hive(cfg)
    ctx = ApiContext(
        graph=hive.graph,
        processes=hive.processes,
        config=cfg,
        broadcaster=hive.broadcaster,
    )
    ctx.spawn_primary = fake_spawn_primary
    client = TestClient(create_api_app(ctx))

    resp = client.post("/api/primary/spawn", json={"agent": "does-not-exist"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is False
    assert "unknown agent" in body["error"]

    resp = client.post("/api/primary/spawn", json={"agent": "a"})
    body = resp.json()
    assert body["ok"] is False
    assert "not primary-eligible" in body["error"]


# ---------------------------------------------------------------------------
# New-shape config
# ---------------------------------------------------------------------------
def test_new_shape_config_loads_with_default_primary():
    cfg = build_config({
        "server": _server(),
        "agents": [_agent("a", allow_as_primary=True), _agent("b")],
        "default_primary": "a",
    })
    assert cfg.default_primary == "a"
    assert cfg.default_primary_profile().name == "a"


def test_new_shape_without_default_primary_loads_optional():
    # default_primary stays OPTIONAL at load in this ticket (T1/EXPAND) — a
    # config without it must load and remain resolvable via eligibility only.
    cfg = build_config({
        "server": _server(),
        "agents": [_agent("a")],
        # no primary block, no default_primary -> loads fine
    })
    assert cfg.default_primary is None
    assert cfg.known_names() == ["a"]
    assert cfg.primary_eligibility() == ["a"]


def test_default_primary_naming_non_eligible_agent_fails_validation():
    with pytest.raises(ValueError, match="not primary-eligible"):
        build_config({
            "server": _server(),
            "agents": [_agent("a"), _agent("b", allow_as_primary=True)],
            "default_primary": "a",  # a is not flagged -> ineligible
        })


def test_default_primary_naming_unknown_agent_fails_validation():
    # Optional at load, but STRICT when present: a default_primary naming an
    # agent that is not in the registry must fail validation with a clear
    # error instead of resolving to None at spawn time.
    with pytest.raises(ValueError, match="does not name any"):
        build_config({
            "server": _server(),
            "agents": [_agent("a", allow_as_primary=True)],
            "default_primary": "ghost",  # not in the agents registry
        })


# ---------------------------------------------------------------------------
# LLM capability injection (req 5) : derived from the ACTUAL spawned model
# ---------------------------------------------------------------------------
def test_llm_capability_note_uses_actually_spawned_model():
    raw = {
        "server": _server(),
        "agents": [_agent("a", model="model-x", allow_as_primary=True)],
        "default_primary": "a",
        "llm": [{
            "name": "model-x",
            "context_window": 100,
            "prices": "$",
            "capability": "c",
            "speed": "s",
        }],
    }
    cfg = build_config(raw)
    hive = _make_hive(cfg)
    node = hive.make_primary_node()
    assert node.profile.model == "model-x"
    assert "model=model-x" in node.profile.systemPrompt

    # Overriding the model re-derives the injected note from the ACTUAL model:
    # a model override keeps the injected note consistent with the real model.
    cfg2 = build_config({
        "server": _server(),
        "agents": [_agent("a", model="model-x", allow_as_primary=True)],
        "default_primary": "a",
        "llm": [
            {"name": "model-x", "context_window": 100, "prices": "$",
             "capability": "no-vision", "speed": "fast"},
        ],
    })
    hive2 = _make_hive(cfg2)
    node2 = hive2.make_primary_node(model="other-model")
    assert node2.profile.model == "other-model"
    # No matching llm entry for the overridden model -> no note injected.
    assert node2.profile.systemPrompt is None  # unchanged (no llm entry)

    # An llm entry that DOES match the overridden model must be the one injected.
    cfg3 = build_config({
        "server": _server(),
        "agents": [_agent("a", model="model-x", allow_as_primary=True)],
        "default_primary": "a",
        "llm": [
            {"name": "model-x", "context_window": 100, "prices": "$",
             "capability": "no-vision", "speed": "fast"},
            {"name": "model-y", "context_window": 200, "prices": "$$",
             "capability": "vision", "speed": "slow"},
        ],
    })
    hive3 = _make_hive(cfg3)
    node3 = hive3.make_primary_node(model="model-y")
    assert node3.profile.model == "model-y"
    assert "model=model-y" in node3.profile.systemPrompt
    assert "model=model-x" not in node3.profile.systemPrompt


# ---------------------------------------------------------------------------
# /api/models (req 6) : default + candidates from the registry
# ---------------------------------------------------------------------------
def test_api_models_derived_from_registry_new_shape():
    cfg = build_config({
        "server": _server(),
        "agents": [
            _agent("a", model="m-alpha", allow_as_primary=True),
            _agent("b", model="m-beta"),
        ],
        "default_primary": "a",
    })
    client, _ = _api_client(cfg)
    resp = client.get("/api/models")
    assert resp.status_code == 200
    body = resp.json()
    # Default = default_primary profile's model, never a separate primary block.
    assert body["default"] == "m-alpha"
    # Candidates = distinct models across the registry.
    assert set(body["models"]) == {"m-alpha", "m-beta"}


def test_api_models_without_default_primary_returns_null_default_not_500():
    # Graceful fallback (just implemented — lock it in): a config with NO
    # default_primary configured must keep GET /api/models at HTTP 200 with
    # "default": None (never a 500); candidates still derive from the
    # registry alone.
    cfg = build_config({
        "server": _server(),
        "agents": [_agent("a", model="m-one"), _agent("b", model="m-two")],
        # no default_primary configured
    })
    assert cfg.default_primary is None
    client, _ = _api_client(cfg)
    resp = client.get("/api/models")
    assert resp.status_code == 200  # explicitly NOT a 500
    body = resp.json()
    assert body["ok"] is True
    assert body["default"] is None
    assert set(body["models"]) == {"m-one", "m-two"}


def test_api_models_legacy_default_from_shimmed_primary():
    raw = {
        "server": _server(),
        "primary": {"name": "primary", "model": "m-legacy-primary",
                    "tools": ["*"], "agent_allowlist": ["tester"]},
        "agents": [_agent("tester", model="m-tester")],
    }
    cfg = build_config(raw)
    client, _ = _api_client(cfg)
    resp = client.get("/api/models")
    assert resp.status_code == 200
    body = resp.json()
    assert body["default"] == "m-legacy-primary"
    assert set(body["models"]) == {"m-legacy-primary", "m-tester"}


# ---------------------------------------------------------------------------
# Legacy shape: subagent resolution still works
# ---------------------------------------------------------------------------
def test_legacy_shape_subagent_resolution_still_works():
    raw = {
        "server": _server(),
        "primary": {"name": "primary", "model": "m-p", "tools": ["*"],
                    "agent_allowlist": ["tester"]},
        "agents": [_agent("tester", model="m-t")],
    }
    cfg = build_config(raw)
    # Every agent (primary included) resolves via the registry for subagents.
    for name in ("primary", "tester"):
        assert cfg.profile_by_name(name) is not None
    assert cfg.profile_by_name("primary").model == "m-p"
