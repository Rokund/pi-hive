"""Focused tests for tickets #2/#3 — primary resolution & eligibility (STRICT).

These tests build HiveConfig objects from inline dictionaries and route them
through the same load/validate path as `load_config`, with NO dependency on a
running daemon. Ticket #3 turned primary configuration STRICT and breaking:

  * the legacy top-level `primary` block is REJECTED at model-validate time
    (it must be an ordinary `agents` entry flagged `allow_as_primary: true`,
    selected via the top-level `default_primary` key);
  * `default_primary` is REQUIRED at `validate()` time — a missing/empty
    selector is a hard load error listing the primary-eligible agents;
  * a present `default_primary` must name an existing, primary-eligible agent.

The suite covers primary eligibility, registry-based name resolution, spawn
selection (including the clear spawn-time errors), the strict rejection of the
legacy shape, the derived /api/models default/candidates, and clean loading of
the shipped `hive/hive.config.json` and `hive/hive.config.json.example`.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

import pytest

from fastapi.testclient import TestClient

from hive.config import HiveConfig, load_config
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


def test_legacy_primary_block_rejected_at_load():
    # Ticket #3 STRICT: the legacy top-level `primary` block no longer loads.
    # The primary must live in the `agents` registry (allow_as_primary: true)
    # and be selected via the top-level `default_primary` key.
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
    with pytest.raises(ValueError, match="no longer supported") as excinfo:
        build_config(raw)
    msg = str(excinfo.value)
    assert "legacy" in msg
    assert "allow_as_primary" in msg
    assert "default_primary" in msg
    # A correct new-shape config (same intent) DOES load.
    cfg = build_config({
        "server": _server(),
        "agents": [_agent("primary", model="m-p", allow_as_primary=True),
                   _agent("tester", model="m-tester")],
        "default_primary": "primary",
    })
    default = cfg.default_primary_profile()
    assert default.name == "primary"
    assert default.model == "m-p"
    assert cfg.profile_by_name("primary") is not None
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


def test_spawn_none_flagged_all_agents_are_primary_eligible():
    # With NO allow_as_primary flag anywhere, every agent is primary-eligible —
    # both at the load/eligibility level and at spawn.
    cfg = build_config({
        "server": _server(),
        "agents": [_agent("a"), _agent("b")],
        "default_primary": "a",
    })
    assert cfg.primary_eligibility() == ["a", "b"]
    hive = _make_hive(cfg)
    node = hive.make_primary_node(agent="b")  # any registry agent spawnable
    assert node.profile.name == "b"
    # Unknown names still raise the clear spawn error.
    with pytest.raises(ValueError, match="unknown agent"):
        hive.make_primary_node(agent="ghost")


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


def test_missing_default_primary_is_hard_load_error():
    # Ticket #3 STRICT (rewrite of the T1 lenient test): `default_primary` is
    # REQUIRED at validate() time. With NO agent flagged, every agent is
    # primary-eligible, and the error must LIST those eligible names.
    with pytest.raises(ValueError, match="missing required") as excinfo:
        build_config({
            "server": _server(),
            "agents": [_agent("a"), _agent("b"), _agent("c")],
            # no default_primary -> hard load error
        })
    msg = str(excinfo.value)
    assert "default_primary" in msg
    assert "primary-eligible agents: ['a', 'b', 'c']" in msg


def test_missing_default_primary_lists_only_flagged_when_some_flagged():
    # When at least one agent IS flagged, only the flagged agents appear in the
    # eligible list of the missing-selector error.
    with pytest.raises(ValueError) as excinfo:
        build_config({
            "server": _server(),
            "agents": [
                _agent("a"),
                _agent("b", allow_as_primary=True),
                _agent("c"),
            ],
            # no default_primary -> hard load error
        })
    msg = str(excinfo.value)
    assert "missing required" in msg
    assert "primary-eligible agents: ['b']" in msg


def test_default_primary_naming_non_eligible_agent_fails_validation():
    with pytest.raises(ValueError, match="not primary-eligible"):
        build_config({
            "server": _server(),
            "agents": [_agent("a"), _agent("b", allow_as_primary=True)],
            "default_primary": "a",  # a is not flagged -> ineligible
        })


def test_default_primary_naming_unknown_agent_fails_validation():
    # STRICT when present: a default_primary naming an agent that is not in
    # the registry must fail validation with a clear error instead of
    # resolving to None at spawn time.
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
    # The /api/models endpoint is graceful by design: even a config with NO
    # default_primary must keep GET /api/models at HTTP 200 with "default":
    # None (never a 500); candidates still derive from the registry alone.
    #
    # NOTE: the LOAD-TIME contract (validate()) now REJECTS this shape as a
    # hard error, so we build the HiveConfig WITHOUT calling .validate() —
    # this test targets the endpoint's graceful-null behavior only, not the
    # requiredness check.
    cfg = HiveConfig.model_validate({
        "server": _server(),
        "agents": [_agent("a", model="m-one"), _agent("b", model="m-two")],
        # no default_primary configured
    })
    assert cfg.default_primary is None
    # Sanity: the same shape WOULD be rejected by the load-time contract —
    # this documents why we cannot use build_config() here.
    with pytest.raises(ValueError, match="missing required"):
        cfg.validate()
    client, _ = _api_client(cfg)
    resp = client.get("/api/models")
    assert resp.status_code == 200  # explicitly NOT a 500
    body = resp.json()
    assert body["ok"] is True
    assert body["default"] is None
    assert set(body["models"]) == {"m-one", "m-two"}


def test_api_models_default_from_default_primary_new_shape():
    # The legacy top-level `primary` block is rejected at load.
    with pytest.raises(ValueError, match="no longer supported"):
        build_config({
            "server": _server(),
            "primary": {"name": "primary", "model": "m-legacy-primary",
                        "tools": ["*"], "agent_allowlist": ["tester"]},
            "agents": [_agent("tester", model="m-tester")],
        })
    # New shape: the models endpoint reports the default from `default_primary`.
    cfg = build_config({
        "server": _server(),
        "agents": [
            _agent("primary", model="m-new-primary", allow_as_primary=True),
            _agent("tester", model="m-tester"),
        ],
        "default_primary": "primary",
    })
    client, _ = _api_client(cfg)
    resp = client.get("/api/models")
    assert resp.status_code == 200
    body = resp.json()
    assert body["default"] == "m-new-primary"
    assert set(body["models"]) == {"m-new-primary", "m-tester"}


# ---------------------------------------------------------------------------
# Legacy shape rejected; new-shape subagent resolution works
# ---------------------------------------------------------------------------
def test_legacy_shape_rejected_and_new_shape_resolution_works():
    # The legacy top-level `primary` block no longer loads.
    raw = {
        "server": _server(),
        "primary": {"name": "primary", "model": "m-p", "tools": ["*"],
                    "agent_allowlist": ["tester"]},
        "agents": [_agent("tester", model="m-t")],
    }
    with pytest.raises(ValueError, match="no longer supported"):
        build_config(raw)

    # New shape: every registry agent (primary included) resolves for subagents.
    cfg = build_config({
        "server": _server(),
        "agents": [_agent("primary", model="m-p", allow_as_primary=True),
                   _agent("tester", model="m-t")],
        "default_primary": "primary",
    })
    for name in ("primary", "tester"):
        assert cfg.profile_by_name(name) is not None
    assert cfg.profile_by_name("primary").model == "m-p"


# ---------------------------------------------------------------------------
# Shipped config files (ticket #3): must load cleanly under the strict contract
# ---------------------------------------------------------------------------
def test_shipped_hive_config_json_loads_and_resolves_default_primary():
    # Loads the REAL shipped `hive/hive.config.json` at its default path.
    cfg_path = Path(__file__).resolve().parent.parent / "hive" / "hive.config.json"
    if not cfg_path.is_file():
        # hive/hive.config.json is gitignored (machine-local, env-specific);
        # a fresh checkout without it must not fail the suite — the tracked
        # deliverable `.example` is covered by the explicit-path test below.
        pytest.skip("local hive/hive.config.json absent (gitignored); skipped")
    cfg = load_config()
    assert cfg.default_primary == "primary"
    # The shipped config legitimately flags several agents allow_as_primary
    # (primary + tester + coder1..3 + reviewer), so the eligible set is larger
    # than just "primary" — assert membership (robust to additional eligible
    # agents) rather than an exact list.
    assert "primary" in cfg.primary_eligibility()
    assert cfg.is_primary_eligible(cfg.default_primary)
    # The registry holds all 6 agents (primary + tester + coder1..3 + reviewer).
    assert len(cfg.known_names()) == 6
    assert cfg.default_primary_profile().name == "primary"


def test_shipped_example_config_loads_via_explicit_path():
    # The `.example` file lives at hive/hive.config.json.example (NOT the
    # default path), so it is loaded via an EXPLICIT path.
    example = Path(__file__).resolve().parent.parent / "hive" / "hive.config.json.example"
    cfg = load_config(path=example)
    assert cfg.default_primary == "primary"
    assert cfg.primary_eligibility() == ["primary"]  # non-empty, names the primary
    assert cfg.is_primary_eligible(cfg.default_primary)
    assert len(cfg.known_names()) == 4  # primary + tester + coder + reviewer
    # default_primary names the placeholder primary entry.
    assert cfg.default_primary_profile().name == "primary"
