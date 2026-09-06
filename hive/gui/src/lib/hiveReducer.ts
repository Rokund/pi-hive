/**
 * App-level reducer for hive state.
 *
 * Maintains the agent tree plus a per-agent transcript derived from the
 * forwarding event stream. `hive:event` messages are fed one-at-a-time into the
 * transcript reducer so nothing is double-processed.
 */

import type { AgentNode, HiveEvent } from "../types";
import {
  emptyTranscript,
  reduceTranscript,
  type TranscriptState,
} from "./transcript";

export interface AgentView {
  node: AgentNode | null;
  transcript: TranscriptState;
}

export interface HiveState {
  /** Root-level agent tree (ordered by creation). */
  tree: AgentNode[];
  /** Selected agent id for the detail panel. */
  selectedId: string | null;
  /** Human-readable hive errors reported over the socket. */
  errors: string[];
  /** Transient per-agent views; keyed by agent id. */
  agentViews: Record<string, AgentView>;
  /** Highest hive event `seq` applied per agent (backlog/live dedup). */
  appliedSeq: Record<string, number>;
}

export type HiveAction =
  | { type: "tree"; tree: AgentNode[] }
  | { type: "event"; event: HiveEvent }
  | { type: "backlog"; events: HiveEvent[] }
  | { type: "agent_updated"; agent: AgentNode }
  | { type: "error"; message: string }
  | { type: "dismiss_error"; index: number }
  | { type: "select"; id: string | null };

export function initHiveState(): HiveState {
  return { tree: [], selectedId: null, errors: [], agentViews: {}, appliedSeq: {} };
}

/**
 * Apply one `hive:event` to the state, skipping any event whose `seq` was
 * already applied (live stream and backlog fetches overlap safely).
 */
function applyHiveEvent(state: HiveState, event: HiveEvent): HiveState {
  const { agentId, seq } = event;
  if (seq !== undefined && (state.appliedSeq[agentId] ?? -1) >= seq) {
    return state; // duplicate / out-of-order replay
  }
  const agentViews = { ...state.agentViews };
  const existing = agentViews[agentId] ?? { node: null, transcript: emptyTranscript() };
  agentViews[agentId] = {
    ...existing,
    transcript: reduceTranscript(existing.transcript, event),
  };
  const appliedSeq = { ...state.appliedSeq };
  if (seq !== undefined) appliedSeq[agentId] = seq;
  return { ...state, agentViews, appliedSeq };
}

export function hiveReducer(state: HiveState, action: HiveAction): HiveState {
  switch (action.type) {
    case "tree": {
      // Replace the tree and seed/refresh agent views for every known node.
      const agentViews = { ...state.agentViews };
      for (const node of action.tree) {
        const existing = agentViews[node.id];
        agentViews[node.id] = {
          node,
          transcript: existing?.transcript ?? emptyTranscript(),
        };
      }
      // Drop views for nodes that no longer exist.
      const known = new Set(action.tree.map((n) => n.id));
      for (const id of Object.keys(agentViews)) {
        if (!known.has(id)) delete agentViews[id];
      }
      return {
        ...state,
        tree: action.tree,
        agentViews,
        selectedId:
          state.selectedId && known.has(state.selectedId)
            ? state.selectedId
            : null,
      };
    }
    case "event":
      return applyHiveEvent(state, action.event);
    case "backlog": {
      // Before folding the backlog, clear each touched agent's seq watermark
      // when NOTHING of that agent has been rendered yet.
      //
      // Why: the watermark is bumped by EVERY hive:event, including no-op
      // control traffic (RPC `get_session_stats` / `get_state` / `abort`
      // responses each consume a global seq). A page can therefore hold a
      // very high watermark for an agent whose transcript is still EMPTY;
      // the click-time backlog then carries much LOWER seqs (its history was
      // logged earlier) and the guard would silently drop ALL of it -> a
      // permanently blank detail panel. With zero blocks rendered there is
      // nothing legitimate to deduplicate against, so resetting is always
      // safe; once blocks exist the guard keeps preventing double-render.
      const cleared = new Set<string>();
      for (const ev of action.events) {
        const aid = ev.agentId;
        if (
          !cleared.has(aid) &&
          state.appliedSeq[aid] !== undefined &&
          (state.agentViews[aid]?.transcript.blocks.length ?? 0) === 0
        ) {
          cleared.add(aid);
        }
      }
      const base: HiveState =
        cleared.size > 0
          ? (() => {
              const appliedSeq = { ...state.appliedSeq };
              for (const aid of cleared) delete appliedSeq[aid];
              return { ...state, appliedSeq };
            })()
          : state;
      // Events arrive oldest-first; fold them through the same seq-guarded
      // path so replays are no-ops and ordering is preserved.
      let next = base;
      for (const ev of action.events) {
        next = applyHiveEvent(next, ev);
      }
      return next;
    }
    case "agent_updated": {
      const agent = action.agent;
      // Update the node within the tree.
      const tree = state.tree.map((n) => (n.id === agent.id ? agent : n));
      const agentViews = { ...state.agentViews };
      const existing = agentViews[agent.id] ?? { node: null, transcript: emptyTranscript() };
      agentViews[agent.id] = { ...existing, node: agent };
      return { ...state, tree, agentViews };
    }
    case "error": {
      const errors = state.errors.length >= 50
        ? [...state.errors.slice(-49), action.message]
        : [...state.errors, action.message];
      return { ...state, errors };
    }
    case "dismiss_error": {
      return {
        ...state,
        errors: state.errors.filter((_, i) => i !== action.index),
      };
    }
    case "select":
      return { ...state, selectedId: action.id };
    default:
      return state;
  }
}
