import { useEffect, useRef, useState, type ReactNode } from "react";
import type { AgentNode } from "../types";
import type {
  MessageBlock,
  TranscriptState,
} from "../lib/transcript";
import Markdown from "./Markdown";
import { API_BASE } from "../lib/apiBase";

interface NodeDetailProps {
  node: AgentNode | null;
  transcript: TranscriptState;
  /** When true, autofocus the composer (a brand-new empty conversation). */
  focusComposer?: boolean;
  /** Called to delete this agent's session/conversation. */
  onDelete?: (agentId: string) => void;
  /** Selectable model choices (from /api/models) for the model combobox. */
  models?: string[];
}

/** Shared "show more / show less" footer used by every collapsible block. */
function CollapseFooter({ open, onToggle }: { open: boolean; onToggle: () => void }) {
  return (
    <button type="button" className="collapse-footer" onClick={onToggle} aria-expanded={open}>
      {open ? "▲ show less" : "▼ show more"}
    </button>
  );
}

/** Plain monospace block (user prompt / thinking) with expand/collapse when
 *  the content is long. Thinking blocks start EXPANDED (watching the model
 *  reason is half the fun); user prompts start collapsed to a preview. */
function PlainBlock({ kind, text }: { kind: "user" | "thinking"; text: string }) {
  const [open, setOpen] = useState(kind === "thinking");
  const shown = text || "…";
  const { body, excessive } = clampPreview(shown);
  return (
    <div className={`block ${kind}`}>
      <pre className="block-content">{open ? shown : body}</pre>
      {excessive ? (
        <CollapseFooter open={open} onToggle={() => setOpen((o) => !o)} />
      ) : null}
    </div>
  );
}

function BlockView({ block }: { block: MessageBlock }) {
  if (block.kind === "user") {
    return <PlainBlock kind="user" text={block.text} />;
  }
  if (block.kind === "thinking") {
    return <PlainBlock kind="thinking" text={block.text} />;
  }
  if (block.kind === "toolcall") {
    return <ToolCallBlock block={block} />;
  }
  // Never render a blank box for an empty text block (a `text_start` with no
  // actual content would otherwise show up as an empty dialog under the call).
  if (!block.text.trim()) return null;
  // Assistant output is Markdown — render headings/lists/tables properly,
  // collapsed to a preview when the answer is very long.
  return <TextBlockView text={block.text} />;
}

/** Assistant Markdown block. Long answers are collapsed to a preview and can
 *  be expanded/collapsed in place instead of stretching the transcript. */
const TEXT_PREVIEW_CHARS = 1200;
function TextBlockView({ text }: { text: string }) {
  const [open, setOpen] = useState(false);
  const isLong = text.length > TEXT_PREVIEW_CHARS;
  const shown = !open && isLong ? text.slice(0, TEXT_PREVIEW_CHARS).trimEnd() + " …" : text;
  return (
    <div className={`block text${open && isLong ? " expanded" : ""}`}>
      <div className="block-content">
        <Markdown text={shown} />
      </div>
      {isLong ? <CollapseFooter open={open} onToggle={() => setOpen((o) => !o)} /> : null}
    </div>
  );
}

/** Compute the COLLAPSED preview of a text. Uses BOTH a line cap and a
 *  per-line character cap so that even a single very long line (e.g. command
 *  output or JSON with no newlines) is truncated and gets a working
 *  expand/collapse toggle instead of an un-collapsible wall of text.
 *
 *  NOTE: deliberately independent of the current open/closed state — whether
 *  a block NEEDS a toggle must not change when it is expanded, otherwise the
 *  toggle would vanish and the content could never be collapsed again. */
const PREVIEW_LINES = 3;
const PREVIEW_LINE_CHARS = 200;
const clampPreview = (text: string): { body: string; excessive: boolean } => {
  const lines = text.split("\n");
  // Not excessive when there are few short lines.
  if (lines.length <= PREVIEW_LINES && text.length <= PREVIEW_LINE_CHARS * 2) {
    return { body: text, excessive: false };
  }
  // Cap both line count and per-line length, then mark as collapsible.
  const capped = lines
    .slice(0, PREVIEW_LINES)
    .map((l) => (l.length > PREVIEW_LINE_CHARS ? l.slice(0, PREVIEW_LINE_CHARS) + "…" : l));
  return { body: capped.join("\n") + "\n…", excessive: true };
};

/**
 * Tool call entry, rendered inline WITH its execution output/status so the
 * result is always shown right after the call — the ordering is guaranteed by
 * the transcript block stream, never by a separate tool list.
 */
function ToolCallBlock({ block }: { block: MessageBlock }) {
  const [open, setOpen] = useState(false);
  const running = block.status === "running";
  const hasArgs = block.text !== "";
  const hasOutput = (block.output ?? "") !== "";
  const argsClamped = hasArgs ? clampPreview(block.text) : null;
  const outClamped = hasOutput ? clampPreview(block.output ?? "") : null;
  // Long enough to need a toggle — computed on FULL content, independent of
  // `open`, so the collapse control never disappears once expanded.
  const excessive = (argsClamped?.excessive ?? false) || (outClamped?.excessive ?? false);

  // A tool call that was just made and is still running has no payload yet.
  if ((!hasArgs && !hasOutput) || running) {
    return (
      <div className="tool-exec-minimal">
        <span className="tool-name">{block.toolName ?? block.toolCallId ?? "tool"}</span>
        <span className="tool-status running">… running</span>
      </div>
    );
  }
  return (
    <div className={`tool-exec${block.isError ? " error" : ""}`}>
      <button
        type="button"
        className="tool-exec-toggle"
        onClick={() => setOpen((o) => !o)}
        aria-expanded={open}
        title={open ? "Collapse tool call / result" : "Expand tool call / result"}
      >
        <span className="tool-chevron">{open ? "▾" : "▸"}</span>
        <span className="tool-name">{block.toolName ?? block.toolCallId ?? "toolcall"}</span>
        <span className={`tool-status ${block.status ?? "done"}`}>
          {running ? "…" : block.isError ? "error" : "done"}
        </span>
      </button>
      {hasArgs ? (
        <pre className="tool-args">{open ? block.text : (argsClamped?.body ?? "")}</pre>
      ) : null}
      {hasOutput ? (
        <pre className="tool-output">{open ? block.output : (outClamped?.body ?? "")}</pre>
      ) : null}
      {excessive ? (
        <CollapseFooter open={open} onToggle={() => setOpen((o) => !o)} />
      ) : null}
    </div>
  );
}

/** Thousands-separated integer. */
const num = (v: unknown): number =>
  typeof v === "number" && Number.isFinite(v) ? v : 0;
const fmtInt = (n: number): string =>
  n >= 1000 ? n.toLocaleString("en-US") : String(Math.round(n));

/**
 * Usage line. Intentionally MINIMAL: only the live token throughput (t/s,
 * real usage deltas, shown while streaming) and the context-window occupancy
 * (ctx / total). No token/cost/turn chips — those flickered and cluttered the
 * view. Rendered unconditionally so nothing pops in/out; empty when no data.
 */
function UsageView({ running, context, tps }: {
  running: boolean;
  context?: { tokens?: number | null; contextWindow?: number | null; percent?: number | null };
  tps?: number;
}) {
  const chips: ReactNode[] = [];

  if (running && typeof tps === "number" && tps > 0) {
    chips.push(
      <span key="tps" className="usage-tps" title="real token throughput (provider usage deltas)">
        ⚡ {tps.toFixed(1)} t/s
      </span>,
    );
  }

  const ctxTokens = num(context?.tokens);
  const ctxWindow = num(context?.contextWindow);
  let ctxPct = typeof context?.percent === "number" ? context.percent : null;
  if (ctxPct === null && ctxTokens > 0 && ctxWindow > 0) {
    ctxPct = Math.min(100, (ctxTokens / ctxWindow) * 100);
  }
  if (ctxTokens > 0 && ctxWindow > 0 && ctxPct !== null) {
    const warn = ctxPct >= 80 ? "usage-ctx-hot" : ctxPct >= 60 ? "usage-ctx-warm" : "";
    chips.push(
      <span
        key="ctx"
        className={`usage-ctx${warn}`}
        title={`context window: ${fmtInt(ctxTokens)} / ${fmtInt(ctxWindow)} tokens`}
      >
        ctx {fmtInt(ctxTokens)} / {fmtInt(ctxWindow)} ({ctxPct.toFixed(0)}%)
      </span>,
    );
  }

  // Render the row unconditionally (stable, no flicker); it is empty when
  // there is nothing meaningful to show.
  return (
    <span className="usage-chips">{chips}</span>
  );
}

interface SessionStats {
  contextUsage?: { tokens?: number | null; contextWindow?: number | null; percent?: number | null };
}

async function fetchAgentStats(agentId: string): Promise<SessionStats | null> {
  try {
    const res = await fetch(`${API_BASE}/api/agent/${agentId}/stats`);
    const data = (await res.json()) as { ok: boolean; stats?: SessionStats };
    return data.ok && data.stats ? data.stats : null;
  } catch {
    return null;
  }
}

async function postJSON(path: string, body: unknown): Promise<void> {
  try {
    await fetch(`${API_BASE}${path}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
  } catch {
    // Transport errors surface via the reconnecting socket / empty result;
    // a silent catch keeps the composer from crashing the panel.
  }
}

/**
 * Bottom composer for the selected agent.
 *
 * State-aware dispatch (pi RPC semantics):
 *   - agent `running` -> send issues a STEER (injects guidance into the
 *     active stream); an abort button is offered alongside.
 *   - agent idle/done -> send issues a PROMPT. The pi RPC process keeps the
 *     session in memory, so a prompt continues the same conversation with
 *     full context.
 *
 * New conversations are started from the sidebar button (or a bare WS
 * prompt); the composer always targets the currently selected agent.
 */
function Composer({ node, focus, usage }: { node: AgentNode; focus?: boolean; usage?: ReactNode }) {
  const [text, setText] = useState("");
  const inputRef = useRef<HTMLTextAreaElement | null>(null);
  const running = node.status === "running";

  useEffect(() => {
    if (focus) inputRef.current?.focus();
  }, [focus]);

  const send = () => {
    const message = text.trim();
    if (!message) return;
    // idle -> "prompt" continues this session (explicit agent id, so the
    // server never spawns a new conversation here).
    const command = running ? "steer" : "prompt";
    void postJSON(`/api/${command}`, { agent: node.id, message });
    setText("");
  };

  const abort = () => {
    void postJSON("/api/abort", { agent: node.id, reason: "aborted from GUI" });
  };

  return (
    <div className="composer">
      <div className="composer-hint">
        <span>
          {running
            ? "steering the running agent"
            : "continue this conversation (same session)"}
        </span>
        {usage}
      </div>
      <div className="composer-row">
        <textarea
          ref={inputRef}
          className="composer-input"
          value={text}
          rows={2}
          placeholder={
            running
              ? "Steer the agent while it is working…"
              : "Continue this conversation…"
          }
          onChange={(e) => setText(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter" && !e.shiftKey) {
              e.preventDefault();
              send();
            }
          }}
        />
        <button
          type="button"
          className={`composer-send${running ? " steer" : ""}`}
          onClick={send}
          disabled={!text.trim()}
          title={running ? "Send as steer" : "Send as prompt (continues session)"}
        >
          {running ? "Steer" : "Send"}
        </button>
        {running && (
          <button
            type="button"
            className="composer-abort"
            onClick={abort}
            title="Abort this agent"
          >
            Abort
          </button>
        )}
      </div>
    </div>
  );
}

/**
 * Detail panel for the selected agent. Renders the reduced transcript
 * (streaming text/thinking, inline tool calls with their outputs) plus
 * settlement and usage metadata. Events are already filtered to this agent by
 * the reducer.
 */
export default function NodeDetail({ node, transcript, focusComposer, onDelete, models = [] }: NodeDetailProps) {
  const [showThinking, setShowThinking] = useState(true);
  const [stats, setStats] = useState<SessionStats | null>(null);
  const viewportRef = useRef<HTMLDivElement | null>(null);
  const stickToBottomRef = useRef(true);

  useEffect(() => {
    const el = viewportRef.current;
    if (!el) return;
    const onScroll = () => {
      const nearBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 80;
      stickToBottomRef.current = nearBottom;
    };
    el.addEventListener("scroll", onScroll);
    return () => el.removeEventListener("scroll", onScroll);
  }, []);

  // When a different agent is opened, start pinned at the bottom again and
  // pull its session stats (context-window usage) from the live process.
  useEffect(() => {
    stickToBottomRef.current = true;
    const el = viewportRef.current;
    if (el) el.scrollTop = el.scrollHeight;
    setStats(null);
    if (node?.id) {
      void fetchAgentStats(node.id).then((s) => {
        setStats((cur) => cur ?? s);
      });
    }
  }, [node?.id]);

  // Refresh stats when a turn settles (context usage only updates then).
  useEffect(() => {
    if (!transcript.settled || !node?.id) return;
    const t = setTimeout(() => {
      void fetchAgentStats(node.id).then((s) => {
        if (s) setStats(s);
      });
    }, 500);
    return () => clearTimeout(t);
  }, [transcript.settled, node?.id]);

  // Auto-scroll to the bottom as the conversation stream comes in.
  useEffect(() => {
    if (stickToBottomRef.current) {
      const el = viewportRef.current;
      if (el) el.scrollTop = el.scrollHeight;
    }
  }, [transcript.blocks]);

  if (!node) {
    return (
      <div className="node-detail empty">Select an agent to view its activity.</div>
    );
  }

  // The transcript is a SINGLE ordered block stream (tool results already live
  // inside their toolcall blocks), so we render it in order with no pairing.
  const transcriptItems: ReactNode[] = [];
  transcript.blocks.forEach((b) => {
    if (b.kind !== "thinking" || showThinking) {
      transcriptItems.push(<BlockView key={b.key} block={b} />);
    }
  });

  return (
    <div className="node-detail">
      <header className="node-header">
        <h2>{node.name}</h2>
        <span className={`node-status ${node.status}`}>{node.status}</span>
        {node.loaded === false && node.sessionFile ? (
          <span className="node-not-loaded" title="Session not loaded yet — opening it loads it from disk">
            not loaded
          </span>
        ) : null}
        {transcript.settled ? (
          <span className="node-settled" title="agent fully settled (no auto retry / compaction)">
            settled
          </span>
        ) : null}
        {onDelete ? (
          <button
            type="button"
            className="delete-session"
            onClick={() => onDelete(node.id)}
            title="Delete this conversation from the hive (session files are kept on disk)"
          >
            Delete
          </button>
        ) : null}
      </header>

      <div className="node-meta">
        <div className="model-field">
          <strong>model</strong>
          {models.length > 0 || node.profile.model ? (
            <select
              className="model-select"
              value={node.profile.model}
              onChange={(e) => {
                const model = e.target.value;
                if (model && model !== node.profile.model) {
                  void postJSON(`/api/agent/${node.id}/model`, { model });
                }
              }}
              title="Switch this agent's model (hot-swaps the live pi process; applies on next spawn if idle/unmaterialized)"
            >
              {!models.includes(node.profile.model) && node.profile.model ? (
                <option value={node.profile.model}>{node.profile.model}</option>
              ) : null}
              {models.map((m) => (
                <option key={m} value={m}>{m}</option>
              ))}
            </select>
          ) : (
            <span>{node.profile.model}</span>
          )}
        </div>
        <div>
          <strong>id</strong> {node.id}
        </div>
        {node.parentId ? (
          <div>
            <strong>parent</strong> {node.parentId}
          </div>
        ) : null}
        <div>
          <strong>cwd</strong> {node.cwd}
        </div>
        <div>
          <strong>session</strong> {node.sessionFile}
        </div>
      </div>

      {/* Scrollable conversation viewport; the composer below stays pinned
          to the bottom instead of scrolling away with the messages. */}
      <div className="transcript-viewport" ref={viewportRef}>
          {transcript.blocks.some((b) => b.kind === "thinking") && (
            <button
              type="button"
              className="thinking-toggle"
              onClick={() => setShowThinking((s) => !s)}
            >
              {showThinking ? "hide" : "show"} thinking (
              {transcript.blocks.filter((b) => b.kind === "thinking").length})
            </button>
          )}
          <section className="transcript">
            {transcriptItems}
          </section>

          {node.lastResult ? (
          <section className="last-result">
            <h3>last result</h3>
            <div className="result-status">{node.lastResult.status}</div>
            {node.lastResult.finalText ? (
              <div className="result-text">
                <Markdown text={node.lastResult.finalText} />
              </div>
            ) : null}
            {node.lastResult.error ? (
              <pre className="result-error">{node.lastResult.error}</pre>
            ) : null}
          </section>
        ) : null}
      </div>

      {/* Pending steering messages: queued, not yet delivered. pi's RPC has
          no command to remove/edit a queued steer, so these are display-only
          (they disappear automatically once delivered via queue_update). */}
      {transcript.steering.length > 0 && (
        <div className="pending-steer">
          <div className="pending-steer-label">
            pending steer ({transcript.steering.length}) — will be delivered after the current tool call finishes
          </div>
          {transcript.steering.map((s, i) => (
            <div key={i} className="pending-steer-item">{s}</div>
          ))}
        </div>
      )}

      <Composer node={node} focus={focusComposer} usage={
        <UsageView
          running={node.status === "running"}
          context={stats?.contextUsage}
          tps={transcript.tokensPerSec}
        />
      } />
    </div>
  );
}
