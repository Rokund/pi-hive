"""Typed configuration loading / validation for `hive.config.json`.

Follows SPEC section 8.  The raw `primary` / `agents` blocks are mapped onto
`AgentProfile` (from `hive.models`); the `server` block onto `ServerConfig`.
"""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Optional, List

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, ValidationError

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


class PrimaryConfig(BaseModel):
    # Accept the legacy `allowlist` key for old config files.
    model_config = ConfigDict(populate_by_name=True)

    name: str = "primary"
    model: str
    thinking: Optional[str] = None
    tools: List[str] = Field(default_factory=list)
    skills: List[str] = Field(default_factory=list)
    systemPrompt: Optional[str] = None
    # Regex/allow-all semantics identical to AgentProfile.agent_allowlist.
    agent_allowlist: List[str] = Field(
        default_factory=list,
        validation_alias=AliasChoices("agent_allowlist", "allowlist"),
    )
    # Named MCP servers (keys of ~/.pi/agent/mcp.json) this agent may see; none
    # by default. Folded into the primary profile (and thus --tools) at spawn.
    mcp: List[str] = Field(default_factory=list)


class LlmProfile(BaseModel):
    """Capability profile of one model, injected to the primary (SPEC §8/M9).

    `name` must equal a model id used by ``primary.model`` or an ``agents[]``
    entry, so the hive can look the profile up for a given agent. The other
    fields are free-form descriptions of the things the primary must know to
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
    primary: PrimaryConfig
    agents: List[AgentProfile] = Field(default_factory=list)
    # Optional explicit model choices for the GUI "New conversation" picker.
    # When absent, the GUI falls back to the distinct models of primary/agents.
    models: List[str] = Field(default_factory=list)
    # Capability profiles describing the models used by primary/agents, keyed by
    # exact model id. Injected to the primary (subagent entries through the
    # subagent_spawn tool schema, the primary's own through its system prompt)
    # so it can tell "working within limits" from "stuck" instead of
    # force-stopping a slow subagent.
    llm: List[LlmProfile] = Field(default_factory=list)

    def llm_for_model(self, model: str) -> Optional[LlmProfile]:
        """The capability profile whose name matches `model`, if any."""
        if not model:
            return None
        for p in self.llm:
            if p.name == model:
                return p
        return None

    def known_names(self) -> List[str]:
        """All resolvable agent/profile names, including the primary."""
        return [self.primary.name] + [a.name for a in self.agents]

    def validate(self) -> "HiveConfig":
        """Validate cross-references (SPEC §8 / M5).

        Every `agent_allowlist` entry (on the primary and on every agent) is
        either the allow-all sentinel `"*"` or a regular-expression pattern
        matched against the requested agent name.  Plain-name entries (no
        regex metacharacters) must reference an agent/profile name that
        actually exists, so typos like `"researher"` fail at load time
        instead of silently mis-matching at runtime.  Regex entries are
        checked only for compilability.  An empty list means the agent may
        spawn no subagents (deny-all).
        """
        known = set(self.known_names())
        for profile in self._all_profiles():
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
        known_models = {p.model for p in self._all_profiles() if p.model}
        for p in self.llm:
            if p.name not in known_models:
                logger.warning(
                    "llm entry %r does not match any agent/primary model; it will "
                    "not be injected. known models: %s",
                    p.name, sorted(known_models),
                )
        return self

    def _all_profiles(self) -> List[AgentProfile]:
        primary = AgentProfile(
            name=self.primary.name,
            model=self.primary.model,
            thinking=self.primary.thinking,
            tools=self.primary.tools,
            skills=self.primary.skills,
            systemPrompt=self.primary.systemPrompt,
            agent_allowlist=self.primary.agent_allowlist,
            max_concurrency=None,
            mcp=self.primary.mcp,
        )
        return [primary] + list(self.agents)

    def profile_by_name(self, name: str) -> Optional[AgentProfile]:
        if name == self.primary.name:
            return AgentProfile(
                name=self.primary.name,
                model=self.primary.model,
                thinking=self.primary.thinking,
                tools=self.primary.tools,
                skills=self.primary.skills,
                systemPrompt=self.primary.systemPrompt,
                agent_allowlist=self.primary.agent_allowlist,
                max_concurrency=None,
                mcp=self.primary.mcp,
            )
        for p in self.agents:
            if p.name == name:
                return p
        return None


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
