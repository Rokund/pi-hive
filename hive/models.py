"""Pydantic data models for pi-hive.

These mirror SPEC section 4 (AgentNode / AgentResult) and the RPC event
envelope forwarded to the GUI (HiveEvent).  The exact field set follows
`docs/SPEC.md` §4.1/§4.3 and the authoritative pi RPC docs
(`pi/packages/coding-agent/docs/rpc.md`).
"""

from pydantic import AliasChoices, BaseModel, ConfigDict, Field
from typing import Optional, List, Literal, Any, Dict


class AgentProfile(BaseModel):
    """Resolved profile for an agent, parsed from the config file."""

    # Accept the legacy `allowlist` key (old config files / persisted state
    # snapshots) while serializing the canonical `agent_allowlist` name.
    model_config = ConfigDict(populate_by_name=True)

    name: str
    model: str
    thinking: Optional[str] = None
    tools: Optional[List[str]] = Field(default_factory=list)
    skills: Optional[List[str]] = Field(default_factory=list)
    cwd: Optional[str] = None
    systemPrompt: Optional[str] = None
    # Names (or regex patterns) of agents this agent may spawn.
    # Empty list  -> no subagent spawning allowed (deny-all).
    # ["*"]       -> may spawn any defined agent (allow-all).
    # Other entries are treated as regular expressions matched against
    # the requested agent name; a plain name (e.g. "reviewer") remains
    # a valid pattern and matches that name.
    agent_allowlist: List[str] = Field(
        default_factory=list,
        validation_alias=AliasChoices("agent_allowlist", "allowlist"),
    )
    # Maximum number of concurrently-running instances of THIS agent type.
    # A value of None (unset) means no per-type cap beyond the global
    # ``server.maxConcurrentSubagents`` limit.  Advertised to the parent via
    # the subagent tools so the calling LLM knows the per-agent ceiling.
    max_concurrency: Optional[int] = Field(default=None)
    # Primary-eligibility flag (ticket #2). Optional; when set, determines
    # whether THIS agent may be used as a primary conversation root:
    #   - None (unset) on every agent  -> every agent is primary-eligible
    #   - at least one agent True      -> ONLY the agents flagged True are
    #     primary-eligible
    # It never restricts subagent spawnability (any agent can still be spawned
    # as a subagent regardless of this flag).
    allow_as_primary: Optional[bool] = Field(default=None)
    # Named MCP servers (keys of the user-level ~/.pi/agent/mcp.json) whose
    # tools this agent may see. An empty list allows none. The hive merges the
    # concrete tool names (the "mcp__<server>" namespace proxy plus the
    # mcp/mcpScript gateway helpers) into this agent's --tools allowlist
    # (ADR-0002); pi gates extension/MCP tools behind exact-name allowlists.
    mcp: List[str] = Field(default_factory=list)


class AgentResult(BaseModel):
    """Final result of a completed / failed / aborted agent run (SPEC §4.3)."""

    id: str
    status: Literal["done", "failed", "aborted"]
    finalText: Optional[str] = None
    partialText: Optional[str] = None
    usage: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    abortBy: Optional[Literal["user", "external", "parent"]] = None
    abortReason: Optional[str] = None
    finishedAt: int


class AgentNode(BaseModel):
    """A node in the agent tree (SPEC §4.1).

    `id` is the hive agent id which is *also* the pi session id (set via
    `--session-id` at spawn), so `sessionId == id`.  `sessionFile` is the real
    on-disk pi session `.jsonl` path returned by the `get_state` RPC.
    """

    id: str
    kind: Literal["primary", "subagent"]
    name: str
    parentId: Optional[str] = None
    childrenIds: List[str] = Field(default_factory=list)
    status: Literal["running", "idle", "done", "failed", "aborted"]
    abortBy: Optional[Literal["user", "external", "parent"]] = None
    abortReason: Optional[str] = None
    profile: AgentProfile
    cwd: str
    sessionFile: str
    createdAt: int
    finishedAt: Optional[int] = None
    lastResult: Optional[AgentResult] = None
    # True once an auto-generated conversation title replaced the default
    # name (primary / primary-2 / ...). Set by the hive after the first
    # exchange settles; persisted so restarts keep the title.
    titled: bool = False
    # Runtime flag, not persisted: True once this node's pi subprocess has
    # actually been spawned/loaded. Restored conversations are metadata-only
    # (loaded=False) until the user opens them (lazy load); it flips to True
    # in PiSubprocess.spawn. Defaults to False for newly created nodes, which
    # are always spawned eagerly on creation.
    loaded: bool = False


class HiveEvent(BaseModel):
    """Envelope wrapping a raw pi RPC event for GUI / API consumers.

    Forwarded shape: ``{"type": "hive:event", "agentId", "ts", "event"}``.
    """

    type: str
    agentId: str
    ts: int
    event: Dict[str, Any]
