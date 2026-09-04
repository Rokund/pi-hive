"""Typed configuration loading / validation for `hive.config.json`.

Follows SPEC section 8.  The raw `agents` blocks are mapped onto
`AgentProfile` (from `hive.models`); the `server` block onto `ServerConfig`.
There is no separate primary block: the primary is an ordinary `agents`
entry flagged with `allow_as_primary: true` and selected via the top-level
`default_primary` key.  The legacy top-level `primary` block is REJECTED
(ticket #3, breaking).
"""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Any, List, Optional

from pydantic import (
    AliasChoices,
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    model_validator,
)

from .models import AgentProfile

logger = logging.getLogger("hive.config")

#: Sentinel entry meaning "may spawn any defined agent".
ALLOW_ALL = "*"
#: Entries fully matching this are treated as plain agent names (typo-checked
#: against known names); anything else is treated as a regex pattern.
_PLAIN_NAME_RE = re.compile(r"[A-Za-z0-9_-]+\Z")


class ServerConfig(BaseModel):
    bind: str = "127.0.0.1"
    guiPort: int = 3000
    apiPort: int = 3001
    maxConcurrentSubagents: int = 20
    maxSubagentIdleMs: int = 120000


class LlmProfile(BaseModel):
    """Capability profile of one model, injected to the primary (SPEC §8/M9).

    `name` must equal a model id used by an ``agents[]`` entry, so the hive can
    look the profile up for a given agent. The other fields are free-form
    descriptions of the things the primary must know to
    judge whether a subagent is "slow" or simply working within real
    constraints (context window, pricing, vision/other capabilities, speed).
    """

    name: str
    context_window: int
    prices: str
    capability: str
    speed: str

    def describe(self) -> str:
        """Compact single-line description for injection into a prompt/tool
        schema (newlines are collapsed later; keep it one line here too)."""
        return (
            f"LLM capability profile: model={self.name}; "
            f"context_window={self.context_window} tokens; "
            f"prices={self.prices}; capability={self.capability}; "
            f"speed={self.speed}"
        )


class HiveConfig(BaseModel):
    server: ServerConfig
    # Shared agent registry — the SINGLE source of truth for name resolution
    # and primary eligibility. There is no separate primary identity: the
    # primary is an ordinary entry in this list, flagged with
    # `allow_as_primary: true` and selected via `default_primary`.
    agents: List[AgentProfile] = Field(default_factory=list)
    # Top-level selector naming the DEFAULT primary agent: the profile used
    # when a spawn request carries no explicit `agent`. REQUIRED at validate()
    # time — a config without it fails with an error listing the
    # primary-eligible agents. The field itself stays Optional purely so that
    # bespoke error can be raised from `validate()` instead of pydantic's
    # generic missing-field message. Must reference a primary-eligible agent.
    default_primary: Optional[str] = None
    # Optional explicit model choices for the GUI "New conversation" picker.
    # When absent, the GUI falls back to the distinct models of the registry.
    models: List[str] = Field(default_factory=list)
    # Capability profiles describing the models used by primary/agents, keyed by
    # exact model id. Injected to the primary (subagent entries through the
    # subagent_spawn tool schema, the primary's own through its system prompt)
    # so it can tell "working within limits" from "stuck" instead of
    # force-stopping a slow subagent.
    llm: List[LlmProfile] = Field(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def _reject_legacy_primary(cls, data: Any) -> Any:
        """REJECT the legacy top-level `primary` block (ticket #3, breaking).

        Legacy config files carried a top-level ``{"primary": {...}}`` block
        alongside (or instead of) the ``agents`` registry. The T1 shim used
        to fold that block into ``agents`` silently; the strict contract now
        rejects the shape outright so it can never load unnoticed. The
        primary is configured by giving an ``agents`` entry
        ``allow_as_primary: true`` and naming it via the top-level
        ``default_primary`` selector.

        ANY value under a top-level ``primary`` key (dict, string, null, …)
        is rejected. Pydantic surfaces the raised ValueError as a
        ``ValidationError`` (itself a ``ValueError`` subclass), so both
        ``HiveConfig.model_validate`` and ``load_config`` fail with this
        message.
        """
        if isinstance(data, dict) and "primary" in data:
            raise ValueError(
                "the legacy top-level `primary` block is no longer supported: "
                "move that agent into the `agents` registry with "
                "`allow_as_primary: true` and select it via the top-level "
                "`default_primary` key"
            )
        return data

    def llm_for_model(self, model: str) -> Optional[LlmProfile]:
        """The capability profile whose name matches `model`, if any."""
        if not model:
            return None
        for p in self.llm:
            if p.name == model:
                return p
        return None

    # -- shared agent registry -------------------------------------------
    # The `agents` list is the SINGLE source of truth for name resolution and
    # primary eligibility. There is no separate "primary" identity: the
    # primary is an `agents` entry flagged `allow_as_primary` and selected
    # through `default_primary`.

    def registry_profiles(self) -> List[AgentProfile]:
        """All agent profiles in the registry (the `agents` list).

        The complete set of resolvable profiles — the primary included, as
        an ordinary registry entry.
        """
        return list(self.agents)

    def _registry(self) -> List[AgentProfile]:
        """Legacy alias for :meth:`registry_profiles` (HTTP API compatibility).

        Retained because hive/server.py still calls ``_registry()``; it is
        just the agents list.
        """
        return self.registry_profiles()

    def known_names(self) -> List[str]:
        """All agent/profile names, derived solely from the `agents` registry
        (the primary included, as an ordinary registry entry)."""
        return [a.name for a in self.agents]

    def profile_by_name(self, name: str) -> Optional[AgentProfile]:
        """Look up an agent profile by name, from the registry only.
        Returns None when no such agent exists."""
        return next((a for a in self.agents if a.name == name), None)

    # -- primary eligibility / default selector ---------------------------
    def _primary_eligible_names(self) -> List[str]:
        """Names allowed to serve as a PRIMARY agent.

        Eligibility rule: if NO agent is flagged `allow_as_primary is True`,
        every agent in the registry is primary-eligible; if any agent is
        flagged True, ONLY the flagged agents are. The flag never restricts
        subagent spawnability (any agent can still be spawned as a subagent
        regardless of this flag).
        """
        flagged = [a.name for a in self.agents if a.allow_as_primary is True]
        if flagged:
            return flagged
        return [a.name for a in self.agents]

    def primary_eligibility(self) -> List[str]:
        """Sorted list of primary-eligible agent names."""
        return sorted(self._primary_eligible_names())

    def is_primary_eligible(self, name: str) -> bool:
        """Whether `name` may be used as a primary agent."""
        return name in self._primary_eligible_names()

    def default_primary_profile(self) -> AgentProfile:
        """The profile used when a spawn request carries no explicit `agent`.

        Resolves via the top-level `default_primary` selector and validates
        that it names an existing, primary-eligible agent. Raises ValueError
        when no `default_primary` is configured or it fails to resolve;
        callers (primary spawn bootstrap, HTTP API) require a resolvable
        profile here. (`validate()` already rejects configs without a
        `default_primary`, so the no-selector branch below is a defensive
        check for programmatically built configs.)
        """
        if not self.default_primary:
            raise ValueError(
                "no default_primary selector configured; cannot resolve a "
                "default primary profile"
            )
        profile = self.profile_by_name(self.default_primary)
        if profile is None:
            raise ValueError(
                f"default_primary {self.default_primary!r} does not name any "
                f"agent in the registry; known names: {sorted(self.known_names())}"
            )
        if not self.is_primary_eligible(self.default_primary):
            raise ValueError(
                f"default_primary {self.default_primary!r} is not primary-eligible "
                f"(allow_as_primary is not True, while other agents are flagged)"
            )
        return profile

    def validate(self) -> "HiveConfig":
        """Validate cross-references (SPEC §8 / M5).

        Every `agent_allowlist` entry (on every agent in the registry) is
        either the allow-all sentinel `"*"` or a regular-expression pattern
        matched against the requested agent name.  Plain-name entries (no
        regex metacharacters) must reference an agent/profile name that
        actually exists, so typos like `"researher"` fail at load time
        instead of silently mis-matching at runtime.  Regex entries are
        checked only for compilability.  An empty list means the agent may
        spawn no subagents (deny-all).

        Also enforces the strict primary-selection contract (ticket #3,
        breaking): the legacy top-level `primary` block is rejected at
        model-validate time by `_reject_legacy_primary`, `default_primary` is
        REQUIRED (a missing/empty selector is a hard error listing the
        primary-eligible agents), and a present `default_primary` must name
        an existing, primary-eligible agent.
        """
        known = set(self.known_names())
        for profile in self.registry_profiles():
            for entry in profile.agent_allowlist:
                if entry == ALLOW_ALL:
                    continue
                if _PLAIN_NAME_RE.fullmatch(entry):
                    if entry not in known:
                        raise ValueError(
                            f"agent_allowlist of agent {profile.name!r} references "
                            f"unknown agent/profile {entry!r}; known names: {sorted(known)}"
                        )
                else:
                    try:
                        re.compile(entry)
                    except re.error as exc:
                        raise ValueError(
                            f"agent_allowlist of agent {profile.name!r} entry "
                            f"{entry!r} is not a valid regular expression: {exc}"
                        ) from exc
        # M9: an ``llm`` capability entry should reference a model actually in
        # use, or it can never be injected (warning only — a stale entry must
        # not brick startup; it is simply inert).
        known_models = {p.model for p in self.registry_profiles() if p.model}
        for p in self.llm:
            if p.name not in known_models:
                logger.warning(
                    "llm entry %r does not match any agent/primary model; it will "
                    "not be injected. known models: %s",
                    p.name, sorted(known_models),
                )
        # Primary selection (strict, ticket #3): `default_primary` is
        # REQUIRED. The eligible set is computed by the exact same rule as
        # `primary_eligibility()` (single source of truth — this calls it):
        # when NO agent is flagged `allow_as_primary is True` every agent is
        # eligible; otherwise only the flagged ones are. A missing/empty
        # selector is a hard error listing the eligible agents; a present one
        # must name a known, primary-eligible agent.
        if not self.default_primary:
            raise ValueError(
                "missing required `default_primary`: name the default "
                "primary agent; primary-eligible agents: "
                f"{self.primary_eligibility()}"
            )
        if self.default_primary not in known:
            raise ValueError(
                f"default_primary {self.default_primary!r} does not name any "
                f"agent in the registry; known names: {sorted(known)}"
            )
        if not self.is_primary_eligible(self.default_primary):
            raise ValueError(
                f"default_primary {self.default_primary!r} is not primary-eligible "
                f"(allow_as_primary is not True, while other agents are flagged)"
            )
        return self


def default_config_path() -> Path:
    """Preferred config location, `hive/hive.config.json` (SPEC §8)."""
    return Path(__file__).resolve().parent / "hive.config.json"


def _config_candidates(explicit: Optional[Path]) -> List[Path]:
    """Resolve candidate config paths, most-preferred first.

    Prefers an explicit `path` argument, then the SPEC §8 deliverable location
    `hive/hive.config.json`, then the legacy repo-root location
    `<project>/hive.config.json` (still used by older tooling / git staging).
    The first file that exists wins.
    """
    if explicit is not None:
        return [explicit]
    hd = Path(__file__).resolve().parent
    return [hd / "hive.config.json", hd.parent / "hive.config.json"]


def load_config(path: Optional[str | Path] = None) -> HiveConfig:
    """Load and validate `hive.config.json`.

    Raises FileNotFoundError if no candidate exists and ValueError if the
    contents are invalid (schema, JSON, or agent_allowlist entries).
    """
    cfg_path = None
    for cand in _config_candidates(Path(path) if path is not None else None):
        if cand.is_file():
            cfg_path = cand
            break
    if cfg_path is None:
        raise FileNotFoundError(
            "hive.config.json not found; expected at hive/hive.config.json or repo root"
        )

    raw = json.loads(cfg_path.read_text(encoding="utf-8"))
    try:
        return HiveConfig.model_validate(raw).validate()
    except ValidationError as exc:
        raise ValueError(
            f"invalid hive.config.json ({cfg_path}): {exc}"
        ) from exc
