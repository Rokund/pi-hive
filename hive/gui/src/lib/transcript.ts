/**
 * Transcript reducer.
 *
 * Converts the forwarding `hive:event` stream for a single agent into a bounded
 * display model. Handles the pi RPC event semantics documented in
 * `pi/packages/coding-agent/docs/rpc.md`:
 *
 * - `message_update` carries deltas in `assistantMessageEvent` and has NO
 *   cumulative `message` field. Accumulate by `contentIndex`.
 * - `message_end.message` is authoritative; partials are finalized when it
 *   arrives.
 * - Tool calls are kept INLINE in the single ordered `blocks` stream: a
 *   `toolcall` block holds not just the call but also its execution output and
 *   status (`tool_execution_*` and `role:"toolResult"` messages update the
 *   matching block in place). Because tool results never live in a separate
 *   list, ordering is guaranteed by `blocks` order and a tool result can never
 *   drift ahead of a later message.
 * - Settlement is indicated by `agent_settled` (NOT `agent_end`, which may be
 *   followed by retries/compaction).
 *
 * This is a pure reducer over a SINGLE event so callers can feed it one event
 * at a time without replay/dedup concerns.
 */

import type {
  AssistantMessageEvent,
  HiveEvent,
  MessageUpdateEvent,
  ToolExecutionEndEvent,
  ToolExecutionStartEvent,
  ToolExecutionUpdateEvent,
} from "../types";

export type BlockKind = "text" | "thinking" | "toolcall" | "user";

export interface MessageBlock {
  key: string;
  contentIndex: number;
  kind: BlockKind;
  /** Accumulated text/thinking content, or the tool-call args preview for
   *  `toolcall` blocks. */
  text: string;
  toolName?: string;
  toolCallId?: string;
  info?: string;
  /** Tool-call execution state (only on `toolcall` blocks). */
  args?: unknown;
  output?: string;
  isError?: boolean;
  status?: "running" | "done";
  finalized: boolean;
}

/** Session-cumulative token/cost totals, accumulated per assistant message. */
export interface UsageTotals {
  input: number;
  output: number;
  cacheRead: number;
  cacheWrite: number;
  reasoning: number;
  totalTokens: number;
  costTotal: number;
}

export interface TranscriptState {
  /** Single, ordered conversation stream (user / text / thinking / toolcall). */
  blocks: MessageBlock[];
  usage: Record<string, unknown> | null;
  /** Accumulated across every assistant message so far (never resets). */
  totals: UsageTotals | null;
  /** Steering messages queued and pending delivery (pi `queue_update`). */
  steering: string[];
  /** Follow-up messages queued behind the running turn. */
  followUps: string[];
  settled: boolean;
  errorCount: number;
  settledAt: number | null;
  /** Live token throughput (tokens/second), 0 when not streaming. Computed
   *  from real `usage.output` deltas over wall-clock time, so it is accurate
   *  but only appears for providers that stream progressive usage. */
  tokensPerSec: number;
  /** Sampling state for the throughput estimate (last output-token count +
   *  the hive ts it was observed at). Cleared on each new message. */
  _tpsSample: { output: number; ts: number } | null;
}

export const emptyTranscript = (): TranscriptState => ({
  blocks: [],
  usage: null,
  totals: null,
  steering: [],
  followUps: [],
  settled: false,
  errorCount: 0,
  settledAt: null,
  tokensPerSec: 0,
  _tpsSample: null,
});

const num = (v: unknown): number =>
  typeof v === "number" && Number.isFinite(v) ? v : 0;

/** Merge one per-message usage report into the session totals. */
function accumulateUsage(
  totals: UsageTotals | null,
  usage: Record<string, unknown>,
): UsageTotals {
  const cost = (
    typeof usage.cost === "object" && usage.cost !== null
      ? (usage.cost as Record<string, unknown>)
      : {}
  );
  const t: UsageTotals = totals ?? {
    input: 0,
    output: 0,
    cacheRead: 0,
    cacheWrite: 0,
    reasoning: 0,
    totalTokens: 0,
    costTotal: 0,
  };
  return {
    input: t.input + num(usage.input),
    output: t.output + num(usage.output),
    cacheRead: t.cacheRead + num(usage.cacheRead),
    cacheWrite: t.cacheWrite + num(usage.cacheWrite),
    reasoning: t.reasoning + num(usage.reasoning),
    totalTokens: t.totalTokens + num(usage.totalTokens),
    costTotal: t.costTotal + num(cost.total),
  };
}

let seq = 0;
const nextKey = (prefix: string) => `${prefix}-${Date.now()}-${seq++}`;

const textFromContent = (content: unknown): string | null => {
  // User messages carry their prompt as a plain string, not a content array.
  if (typeof content === "string") return content;
  if (!Array.isArray(content)) return null;
  const parts: string[] = [];
  let any = false;
  for (const block of content) {
    if (block && typeof block === "object") {
      const b = block as Record<string, unknown>;
      if (b.type === "text" && typeof b.text === "string") {
        parts.push(b.text);
        any = true;
      } else if (b.type === "thinking" && typeof b.text === "string") {
        parts.push(b.text);
        any = true;
      }
    }
  }
  return any ? parts.join("\n") : null;
};

const extractText = (content: unknown): string | null => {
  if (!Array.isArray(content)) return null;
  const parts: string[] = [];
  let any = false;
  for (const block of content) {
    if (block && typeof block === "object") {
      const b = block as Record<string, unknown>;
      if (typeof b.text === "string") {
        parts.push(b.text);
        any = true;
      }
    }
  }
  return any ? parts.join("") : null;
};

/**
 * Estimate real token throughput (tokens/sec) from provider usage deltas.
 *
 * `usage.output` is session-cumulative; we take the delta of output tokens
 * over the wall-clock interval between consecutive updates and smooth it
 * exponentially. If the provider does not stream progressive usage (most do
 * not), `output` stays 0 and the readout simply shows nothing — we prefer that
 * over a fake estimate.
 */
function updateThroughput(
  tokensPerSec: number,
  sample: { output: number; ts: number } | null,
  usage: unknown,
  ts: number,
): { tokensPerSec: number; sample: { output: number; ts: number } | null } {
  if (!usage || typeof usage !== "object") {
    return { tokensPerSec, sample };
  }
  const out = num((usage as Record<string, unknown>).output);
  if (out <= 0) {
    return { tokensPerSec, sample };
  }
  // Output regressed (new message / provider reset) -> restart the window.
  if (sample && out < sample.output) {
    return { tokensPerSec: 0, sample: { output: out, ts } };
  }
  if (sample && ts > sample.ts) {
    const dt = (ts - sample.ts) / 1000;
    const dOut = out - sample.output;
    if (dt > 0 && dOut > 0) {
      const inst = dOut / dt;
      const next = tokensPerSec > 0 ? tokensPerSec * 0.8 + inst * 0.2 : inst;
      return { tokensPerSec: next, sample: { output: out, ts } };
    }
  }
  return { tokensPerSec, sample: { output: out, ts } };
}

function handleMessageUpdate(
  state: TranscriptState,
  ev: MessageUpdateEvent,
  ts: number,
): TranscriptState {
  const blocks = [...state.blocks];
  const usage = ev.usage ? { ...ev.usage } : state.usage;
  const ame: AssistantMessageEvent = ev.assistantMessageEvent;

  switch (ame.type) {
    case "text_start":
    case "thinking_start": {
      const idx = ame.contentIndex ?? 0;
      const existing = blocks.find(
        (b) => !b.finalized && b.contentIndex === idx && b.kind === (ame.type === "text_start" ? "text" : "thinking"),
      );
      if (!existing) {
        blocks.push({
          key: nextKey(ame.type === "text_start" ? "text" : "think"),
          contentIndex: idx,
          kind: ame.type === "text_start" ? "text" : "thinking",
          text: "",
          finalized: false,
        });
      }
      break;
    }
    case "text_delta":
    case "thinking_delta": {
      const idx = ame.contentIndex ?? 0;
      const kind: BlockKind = ame.type === "text_delta" ? "text" : "thinking";
      let found = blocks.find((b) => !b.finalized && b.contentIndex === idx && b.kind === kind);
      if (!found) {
        found = {
          key: nextKey(kind),
          contentIndex: idx,
          kind,
          text: "",
          finalized: false,
        };
        blocks.push(found);
      }
      const delta = typeof ame.delta === "string" ? ame.delta : "";
      found = { ...found, text: found.text + delta };
      const bi = blocks.findIndex((b) => b.key === found!.key);
      blocks[bi] = found;
      break;
    }
    case "text_end":
    case "thinking_end": {
      const idx = ame.contentIndex ?? 0;
      const kind: BlockKind = ame.type === "text_end" ? "text" : "thinking";
      const bi = blocks.findIndex((b) => !b.finalized && b.contentIndex === idx && b.kind === kind);
      if (bi >= 0) {
        const authoritative = typeof ame.content === "string" ? ame.content : null;
        blocks[bi] = {
          ...blocks[bi],
          text: authoritative ?? blocks[bi].text,
          finalized: true,
        };
      }
      break;
    }
    case "toolcall_start": {
      const idx = ame.contentIndex ?? 0;
      const existing = blocks.find((b) => !b.finalized && b.contentIndex === idx && b.kind === "toolcall");
      if (!existing) {
        blocks.push({
          key: nextKey("call"),
          contentIndex: idx,
          kind: "toolcall",
          text: "",
          toolName: typeof ame.toolName === "string" ? ame.toolName : undefined,
          toolCallId: typeof ame.id === "string" ? ame.id : undefined,
          info: typeof ame.toolName === "string" ? `toolcall: ${ame.toolName}` : "toolcall",
          finalized: false,
        });
      }
      break;
    }
    case "toolcall_delta": {
      const idx = ame.contentIndex ?? 0;
      let found = blocks.find((b) => !b.finalized && b.contentIndex === idx && b.kind === "toolcall");
      if (!found) {
        found = {
          key: nextKey("call"),
          contentIndex: idx,
          kind: "toolcall",
          text: "",
          toolCallId: typeof ame.id === "string" ? ame.id : undefined,
          finalized: false,
        };
        blocks.push(found);
      }
      found = { ...found, text: found.text + (typeof ame.delta === "string" ? ame.delta : "") };
      const bi = blocks.findIndex((b) => b.key === found!.key);
      blocks[bi] = found;
      break;
    }
    case "toolcall_end": {
      const idx = ame.contentIndex ?? 0;
      const bi = blocks.findIndex((b) => !b.finalized && b.contentIndex === idx && b.kind === "toolcall");
      if (bi >= 0) {
        const toolCall = ame.toolCall as Record<string, unknown> | undefined;
        const toolName = (toolCall?.name as string | undefined) ?? blocks[bi].toolName;
        // Stamp the id here too when the stream provides one (in practice
        // `toolcall_*` events usually arrive WITHOUT it — see the
        // reconciliation in handleMessageEnd for the reliable source).
        const endId = (toolCall?.id as string | undefined) ?? (ame.id as string | undefined);
        const args = (toolCall?.input as unknown | undefined) ?? (toolCall?.args as unknown | undefined);
        let text = blocks[bi].text;
        if (args && typeof args === "object") {
          text = JSON.stringify(args, null, 2);
        }
        blocks[bi] = {
          ...blocks[bi],
          text,
          info: toolName ? `toolcall: ${String(toolName)}` : "toolcall",
          toolName,
          toolCallId: blocks[bi].toolCallId ?? endId,
          finalized: true,
        };
      }
      break;
    }
    default:
      break;
  }

  const tps = updateThroughput(state.tokensPerSec, state._tpsSample, usage, ts);
  return {
    ...state,
    blocks,
    usage,
    tokensPerSec: tps.tokensPerSec,
    _tpsSample: tps.sample,
  };
}

/**
 * Reconcile ONE authoritative content part into `blocks` (used on every
 * assistant `message_end`). Returns the updated array.
 *
 * This is the linchpin of tool-result visibility: pi's streamed
 * `toolcall_start/delta/end` events do NOT carry the tool-call id (verified
 * against live RPC traffic), so the inline toolcall block would otherwise
 * keep `toolCallId === undefined` forever — and the later
 * `tool_execution_*` / `role:"toolResult"` events, which correlate ONLY by
 * `toolCallId`, would silently drop (no output ever rendered). The
 * authoritative message content DOES carry the id, so we stamp it here.
 *
 * Handles both paths:
 *   - live: a streaming partial exists for this contentIndex -> finalize it
 *     in place (fill in the id/args it lacks)
 *   - replay/history: nothing streamed -> append the part fresh
 */
function reconcilePart(blocks: MessageBlock[], part: unknown, i: number): MessageBlock[] {
  if (!part || typeof part !== "object") return blocks;
  const p = part as Record<string, unknown>;

  // --- text / thinking ----------------------------------------------------
  if (p.type === "text" || p.type === "thinking") {
    const kind: BlockKind = p.type === "text" ? "text" : "thinking";
    // pi uses `text` on text parts but `thinking` on thinking parts.
    const raw = p.type === "text" ? p.text : (p.thinking ?? p.text);
    const text = typeof raw === "string" ? raw : "";
    const bi = blocks.findIndex(
      (b) => !b.finalized && b.kind === kind && b.contentIndex === i,
    );
    if (bi >= 0) {
      const next = [...blocks];
      next[bi] = { ...next[bi], text: text || next[bi].text, finalized: true };
      return next;
    }
    // Nothing streamed (or already finalized): append unless the exact text
    // is already rendered (live+backlog double-delivery guard).
    if (!text) return blocks;
    const dup = blocks.some((b) => b.kind === kind && b.finalized && b.text === text);
    if (dup) return blocks;
    return [
      ...blocks,
      { key: nextKey(kind), contentIndex: i, kind, text, finalized: true },
    ];
  }

  // --- toolCall ------------------------------------------------------------
  if (p.type === "toolCall") {
    const id = typeof p.id === "string" ? p.id : undefined;
    const name = typeof p.name === "string" ? p.name : undefined;
    let argsText = "";
    if (p.arguments && typeof p.arguments === "object") {
      argsText = JSON.stringify(p.arguments, null, 2);
    } else if (typeof p.arguments === "string") {
      argsText = p.arguments;
    }
    // Match the streamed sibling: prefer a block at this contentIndex whose
    // id is still missing (the common case) or already equals the real id.
    // Finalized or not — toolcall_end fires before message_end.
    const bi = blocks.findIndex(
      (b) =>
        b.kind === "toolcall" &&
        b.contentIndex === i &&
        (b.toolCallId === undefined || b.toolCallId === id),
    );
    if (bi >= 0) {
      const next = [...blocks];
      const b = next[bi];
      next[bi] = {
        ...b,
        toolCallId: b.toolCallId ?? id,
        toolName: b.toolName ?? name,
        text: b.text || argsText,
        info: (b.toolName ?? name) ? `toolcall: ${String(b.toolName ?? name)}` : b.info,
        finalized: true,
      };
      return next;
    }
    // Replay path: no streamed block at all -> create one carrying the id.
    return [
      ...blocks,
      {
        key: nextKey("call"),
        contentIndex: i,
        kind: "toolcall",
        text: argsText,
        toolName: name,
        toolCallId: id,
        info: name ? `toolcall: ${String(name)}` : "toolcall",
        finalized: true,
      },
    ];
  }

  return blocks;
}

/** Find the inline `toolcall` block for a toolCallId and update it. */
function updateToolBlock(
  state: TranscriptState,
  toolCallId: unknown,
  fn: (b: MessageBlock) => MessageBlock,
): TranscriptState {
  if (typeof toolCallId !== "string") return state;
  let found = false;
  const blocks = state.blocks.map((b) => {
    if (b.kind === "toolcall" && b.toolCallId === toolCallId) {
      found = true;
      return fn(b);
    }
    return b;
  });
  if (!found) return state; // no matching call block (pathological); drop
  return { ...state, blocks };
}

function handleMessageEnd(state: TranscriptState, ev: { message?: unknown; usage?: unknown }): TranscriptState {
  let blocks = state.blocks;
  const msg = ev.message as Record<string, unknown> | undefined;
  if (msg?.role === "user") {
    // The user's prompt: append as its own block so the conversation reads
    // prompt -> (thinking/tool) -> response, top to bottom.
    const text = textFromContent(msg.content) ?? "";
    if (text) {
      return {
        ...state,
        blocks: [...state.blocks,
          { key: nextKey("user"), contentIndex: -1, kind: "user", text, finalized: true }],
      };
    }
    return state;
  }
  if (msg?.role === "assistant") {
    // Reconcile EVERY part of the authoritative content (not just text):
    // streaming partials get finalized in place, and toolCall parts get
    // their real id stamped (see reconcilePart — without it the tool result
    // can never attach and silently disappears from the UI).
    const content = Array.isArray(msg.content) ? msg.content : [];
    content.forEach((part, i) => {
      blocks = reconcilePart(blocks, part, i);
    });
  }
  if (msg?.role === "toolResult") {
    // Attach the tool result to its inline toolcall block (keeps ordering).
    const toolCallId = msg.toolCallId;
    const output = extractText(msg.content) ?? "";
    return updateToolBlock(state, toolCallId, (b) => ({
      ...b,
      output: output || b.output,
      isError: msg.isError === true,
      status: "done" as const,
      finalized: true,
    }));
  }
  blocks = blocks.map((b) => (b.finalized ? b : { ...b, finalized: true }));
  const usage =
    ev.usage && typeof ev.usage === "object"
      ? { ...(ev.usage as Record<string, unknown>) }
      : state.usage;
  // Session totals grow monotonically: fold each assistant message's usage
  // into the accumulator so the displayed numbers never reset mid-session.
  const totals =
    msg?.role === "assistant" && usage && typeof usage === "object"
      ? accumulateUsage(state.totals, usage)
      : state.totals;
  return { ...state, blocks, usage, totals };
}

function handleToolStart(state: TranscriptState, ev: ToolExecutionStartEvent): TranscriptState {
  return updateToolBlock(state, ev.toolCallId, (b) => ({
    ...b,
    args: ev.args ?? b.args,
    output: "",
    isError: false,
    status: "running" as const,
  }));
}

function handleToolUpdate(state: TranscriptState, ev: ToolExecutionUpdateEvent): TranscriptState {
  return updateToolBlock(state, ev.toolCallId, (b) => ({
    ...b,
    output: extractText(ev.partialResult?.content) ?? b.output,
  }));
}

function handleToolEnd(state: TranscriptState, ev: ToolExecutionEndEvent): TranscriptState {
  return updateToolBlock(state, ev.toolCallId, (b) => ({
    ...b,
    output: extractText(ev.result?.content) ?? b.output,
    isError: ev.isError === true,
    status: "done" as const,
    finalized: true,
  }));
}

/**
 * Reduce a single forwarded hive event into the transcript.
 * Returns the current state with new local changes discarded on error.
 */
export function reduceTranscript(
  state: TranscriptState,
  hiveEvent: HiveEvent,
): TranscriptState {
  const ev = hiveEvent.event;
  const type = ev?.type;

  switch (type) {
    case "message_update":
      return handleMessageUpdate(state, ev as MessageUpdateEvent, hiveEvent.ts ?? 0);
    case "message_end":
      return handleMessageEnd(state, ev as { message?: unknown; usage?: unknown });
    case "tool_execution_start":
      return handleToolStart(state, ev as ToolExecutionStartEvent);
    case "tool_execution_update":
      return handleToolUpdate(state, ev as ToolExecutionUpdateEvent);
    case "tool_execution_end":
      return handleToolEnd(state, ev as ToolExecutionEndEvent);
    case "message_start":
    case "turn_start":
      // A new message / turn is beginning: restart the token-throughput
      // estimate so it measures only the active generation, never idle gaps
      // or tool-execution time between messages.
      return { ...state, tokensPerSec: 0, _tpsSample: null };
    case "agent_settled":
      return {
        ...state,
        settled: true,
        settledAt: hiveEvent.ts,
        blocks: state.blocks.map((b) => (b.finalized ? b : { ...b, finalized: true })),
        // Streaming is over; drop the live estimate (UI gates on running).
        tokensPerSec: 0,
        _tpsSample: null,
      };
    case "queue_update": {
      // pi reports the pending steering / follow-up queues whenever they
      // change — surface them so the user sees what will be delivered.
      const q = ev as { steering?: unknown; followUp?: unknown };
      return {
        ...state,
        steering: Array.isArray(q.steering) ? q.steering.filter((s): s is string => typeof s === "string") : state.steering,
        followUps: Array.isArray(q.followUp) ? q.followUp.filter((s): s is string => typeof s === "string") : state.followUps,
      };
    }
    case "extension_error":
      return { ...state, errorCount: state.errorCount + 1 };
    default:
      return state;
  }
}
