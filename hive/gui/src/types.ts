/**
 * Type definitions for the pi-hive GUI.
 *
 * These mirror the authoritative Python backend models (`hive/models.py`) and
 * the pi RPC event envelope forwarded to the GUI. The event shapes follow
 * `pi/packages/coding-agent/docs/rpc.md` — in particular `message_update` has
 * NO cumulative `message` field; deltas arrive in `assistantMessageEvent`.
 */

/* ------------------------------------------------------------------ */
/* Agent model (mirrors hive/models.py)                                */
/* ------------------------------------------------------------------ */

export interface AgentProfile {
  name: string;
  model: string;
  thinking?: string | null;
  tools?: string[] | null;
  skills?: string[] | null;
  cwd?: string | null;
  systemPrompt?: string | null;
  agent_allowlist: string[];
}

export type AgentStatus = "running" | "idle" | "done" | "failed" | "aborted";

export type AgentKind = "primary" | "subagent";

export interface AgentResult {
  id: string;
  status: "done" | "failed" | "aborted";
  finalText?: string | null;
  partialText?: string | null;
  usage?: Record<string, unknown> | null;
  error?: string | null;
  abortBy?: "user" | "external" | "parent" | null;
  abortReason?: string | null;
  finishedAt: number;
}

export interface AgentNode {
  id: string;
  kind: AgentKind;
  name: string;
  parentId?: string | null;
  childrenIds: string[];
  status: AgentStatus;
  abortBy?: "user" | "external" | "parent" | null;
  abortReason?: string | null;
  profile: AgentProfile;
  cwd: string;
  sessionFile: string;
  createdAt: number;
  finishedAt?: number | null;
  lastResult?: AgentResult | null;
  /**
   * True once this conversation's pi subprocess has been spawned/loaded.
   * Restored (archived) sessions are metadata-only until the user opens them
   * (lazy load); the server never loads them eagerly at startup.
   */
  loaded?: boolean;
}

/* ------------------------------------------------------------------ */
/* pi RPC event shapes (forwarded inside `hive:event`)                 */
/* ------------------------------------------------------------------ */

export type AssistantMessageEventType =
  | "text_start"
  | "text_delta"
  | "text_end"
  | "thinking_start"
  | "thinking_delta"
  | "thinking_end"
  | "toolcall_start"
  | "toolcall_delta"
  | "toolcall_end";

export interface AssistantMessageEvent {
  type: AssistantMessageEventType;
  contentIndex?: number;
  /** text_delta / thinking_delta / toolcall_delta payload */
  delta?: string;
  /** text_end / thinking_end authoritative content */
  content?: string;
  /** toolcall_start / toolcall_delta id */
  id?: string;
  /** toolcall_start tool name */
  toolName?: string;
  /** toolcall_end completed tool call */
  toolCall?: Record<string, unknown>;
}

export interface MessageUpdateEvent {
  type: "message_update";
  /** Latest cumulative provider usage (may be 0 until completion). */
  usage?: {
    input?: number;
    output?: number;
    cacheRead?: number;
    cacheWrite?: number;
    totalTokens?: number;
    cost?: Record<string, unknown>;
    [k: string]: unknown;
  };
  assistantMessageEvent: AssistantMessageEvent;
}

export interface ToolExecutionStartEvent {
  type: "tool_execution_start";
  toolCallId: string;
  toolName: string;
  args?: unknown;
}

export interface ToolExecutionUpdateEvent {
  type: "tool_execution_update";
  toolCallId: string;
  toolName: string;
  args?: unknown;
  partialResult?: {
    content?: Array<Record<string, unknown>>;
    details?: unknown;
  };
}

export interface ToolExecutionEndEvent {
  type: "tool_execution_end";
  toolCallId: string;
  toolName: string;
  result?: {
    content?: Array<Record<string, unknown>>;
    details?: unknown;
  };
  isError?: boolean;
}

export interface MessageEndEvent {
  type: "message_end";
  message?: unknown;
  usage?: MessageUpdateEvent["usage"];
}

/** Any event we do not need to interpret structurally. */
export interface GenericPiEvent {
  type: string;
  [k: string]: unknown;
}

export type PiEvent =
  | MessageUpdateEvent
  | ToolExecutionStartEvent
  | ToolExecutionUpdateEvent
  | ToolExecutionEndEvent
  | MessageEndEvent
  | GenericPiEvent;

/* ------------------------------------------------------------------ */
/* Hive transport                                                      */
/* ------------------------------------------------------------------ */

/**
 * Envelope wrapping a raw pi RPC event for GUI consumers (mirrors
 * `hive.models.HiveEvent`). `{"type":"hive:event","agentId","ts","event"}`.
 */
export interface HiveEvent {
  type: string;
  agentId: string;
  ts: number;
  /** Global monotonic sequence assigned by the hive; used for backlog dedup. */
  seq?: number;
  event: PiEvent;
}

/** Incoming messages on the Port-1 GUI WebSocket (SPEC §5.4). */
export type HivePushMessage =
  | { type: "hive:tree"; tree: AgentNode[] }
  | HiveEvent
  | { type: "hive:agent_updated"; agent: AgentNode }
  | { type: "hive:error"; message: string };

/** Outgoing message sent on connection to subscribe to the stream. */
export interface SubscribeMessage {
  type: "subscribe";
}

export type ConnectionStatus = "connecting" | "open" | "closed" | "error";
