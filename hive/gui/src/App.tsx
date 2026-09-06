import { useEffect, useReducer, useState } from "react";
import AgentTree from "./components/AgentTree";
import NodeDetail from "./components/NodeDetail";
import { useWebSocket } from "./hooks/useWebSocket";
import { hiveReducer, initHiveState } from "./lib/hiveReducer";
import { API_BASE } from "./lib/apiBase";
import type { HiveEvent, HivePushMessage } from "./types";

async function startNewConversation(model?: string): Promise<string | null> {
  // Returns the new primary's agent id, or null on failure. An explicit
  // model overrides the configured default for this conversation.
  try {
    const res = await fetch(`${API_BASE}/api/primary/spawn`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(model ? { model } : {}),
    });
    const data = (await res.json()) as { ok: boolean; id?: string };
    return data.ok && data.id ? data.id : null;
  } catch {
    return null;
  }
}

interface ModelsResponse {
  ok: boolean;
  models?: string[];
  default?: string;
}

async function fetchModels(): Promise<ModelsResponse> {
  try {
    const res = await fetch(`${API_BASE}/api/models`);
    return (await res.json()) as ModelsResponse;
  } catch {
    return { ok: false };
  }
}

async function fetchAgentBacklog(
  agentId: string,
): Promise<{ events: HiveEvent[]; error: string | null }> {
  try {
    const res = await fetch(`${API_BASE}/api/agent/${agentId}/events?since=0`);
    if (!res.ok) {
      return { events: [], error: `backlog HTTP ${res.status}` };
    }
    const data = (await res.json()) as {
      ok: boolean;
      events?: HiveEvent[];
      error?: string;
    };
    // Serve whatever events exist even when ok:false — the error flag is
    // informational; dropping a non-empty backlog would blank the panel.
    const events = Array.isArray(data.events) ? data.events : [];
    return { events, error: data.ok ? null : data.error || "backlog unavailable" };
  } catch (err) {
    return {
      events: [],
      error: err instanceof Error ? err.message : "backlog fetch failed",
    };
  }
}

/** How many message-bearing events a backlog fetch returned. */
const hasMessages = (events: HiveEvent[]): boolean =>
  events.some((e) => (e.event as { type?: string })?.type === "message_end");

export default function App() {
  const [state, dispatch] = useReducer(hiveReducer, undefined, initHiveState);
  const [starting, setStarting] = useState(false);
  const [models, setModels] = useState<string[]>([]);
  // Model chosen for the next new conversation (sidebar picker). Defaults to
  // the server-provided `default` so the plain button keeps today's behavior.
  const [newModel, setNewModel] = useState<string>("");

  // Load the selectable model list (from hive.config.json) once on mount.
  useEffect(() => {
    void fetchModels().then((m) => {
      if (m.ok && m.models && m.models.length > 0) {
        setModels(m.models);
        if (m.default) setNewModel(m.default);
      }
    });
  }, []);

  const connection = useWebSocket({
    onMessage: (msg: HivePushMessage) => {
      // Narrow the open union with `in` guards: HiveEvent.type is `string`, so
      // literal switches cannot exclude it. Each message type carries a unique
      // payload property that only it owns.
      if ("event" in msg && msg.type === "hive:event") {
        dispatch({ type: "event", event: msg });
      } else if ("tree" in msg) {
        dispatch({ type: "tree", tree: msg.tree });
      } else if ("agent" in msg) {
        dispatch({ type: "agent_updated", agent: msg.agent });
      } else if ("message" in msg) {
        dispatch({ type: "error", message: msg.message });
      }
    },
  });

  const selectAgent = (id: string | null) => {
    dispatch({ type: "select", id });
    // Pull the conversation backlog so a node clicked after its session
    // already finished still shows the full transcript (server-side log).
    // The first pull can legitimately come back empty when the server is
    // still lazily spawning the pi process — retry a few times instead of
    // leaving the panel blank until a manual page refresh.
    if (id) {
      const load = async (attempt: number): Promise<void> => {
        const { events, error } = await fetchAgentBacklog(id);
        if (!hasMessages(events) && attempt < 3 && !error) {
          await new Promise((r) => setTimeout(r, 1500));
          return load(attempt + 1);
        }
        if (events.length > 0) {
          dispatch({ type: "backlog", events });
        }
        // Hard failure (spawn/history pull broke server-side): stop retrying
        // and tell the user why the transcript is empty instead of showing a
        // silent blank panel that never recovers.
        if (!hasMessages(events) && error) {
          dispatch({ type: "error", message: `${id.slice(0, 8)}: ${error}` });
        }
      };
      void load(0);
    }
  };

  /** New conversation: spawn a fresh primary and select it in the main
   *  panel, where the (autofocused) composer takes the first prompt. */
  const startChat = async () => {
    if (starting) return;
    setStarting(true);
    try {
      // Pass the model selected in the sidebar (or the configured default
      // when none was chosen) so the new conversation runs on it.
      const id = await startNewConversation(newModel || undefined);
      if (id) selectAgent(id);
    } finally {
      setStarting(false);
    }
  };

  /** Delete a session/conversation from the hive. The server stops the agent,
   *  removes it (and its subagents) from the tree and durable state, leaving
   *  the pi session files on disk. The sidebar tree updates via the WS tree
   *  broadcast; we just clear the selection (if it was the deleted node) so
   *  the panel closes. */
  const deleteSession = async (agentId: string) => {
    const confirmed = window.confirm(
      `Delete this conversation${selectedNode?.name ? ` (${selectedNode.name})` : ""}?\n\nIt will be removed from the hive. Its session files are kept on disk.`,
    );
    if (!confirmed) return;
    try {
      if (state.selectedId === agentId) dispatch({ type: "select", id: null });
      await fetch(`${API_BASE}/api/agent/${agentId}/delete`, { method: "POST" });
    } catch {
      // Transport errors surface via the reconnecting socket / empty result.
    }
  };

  /** Rename a session/conversation via the hive. The sidebar updates through
   *  the WS tree broadcast, so a successful rename needs no local mutation. */
  const renameSession = async (agentId: string, name: string) => {
    try {
      await fetch(`${API_BASE}/api/agent/${agentId}/rename`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name }),
      });
    } catch {
      // Transport errors surface via the reconnecting socket / empty result.
    }
  };

  const selectedView = state.selectedId
    ? state.agentViews[state.selectedId]
    : undefined;
  const selectedNode = selectedView?.node ?? null;
  const selectedTranscript = selectedView?.transcript;
  // A node with no transcript blocks yet is a brand-new conversation ->
  // autofocus its composer so the user can type the first prompt right away.
  const freshConversation =
    selectedNode !== null &&
    selectedTranscript !== undefined &&
    selectedTranscript.blocks.length === 0;

  return (
    <div className="app">
      <header className="header">
        <h1>pi-hive</h1>
        <span className={`conn-indicator ${connection.status}`} />
        <span className="conn">{connection.status}</span>
        {connection.error ? (
          <span className="conn error-text">{connection.error}</span>
        ) : null}
      </header>

      <aside className="sidebar">
        {state.errors.map((err, i) => (
          <div key={i} className="error">
            <span>{err}</span>
            <button
              type="button"
              className="error-dismiss"
              onClick={() => dispatch({ type: "dismiss_error", index: i })}
            >
              ×
            </button>
          </div>
        ))}
        <button
          type="button"
          className="new-chat"
          disabled={starting}
          onClick={() => void startChat()}
        >
          ＋ New conversation
        </button>
        {models.length > 0 ? (
          <label className="new-chat-model">
            <span>model</span>
            <select
              className="model-select"
              value={newModel}
              onChange={(e) => setNewModel(e.target.value)}
              title="Model the next new conversation runs on"
            >
              {models.map((m) => (
                <option key={m} value={m}>{m}</option>
              ))}
            </select>
          </label>
        ) : null}
        <AgentTree
          tree={state.tree}
          selectedId={state.selectedId}
          onSelect={selectAgent}
          onDelete={deleteSession}
          onRename={renameSession}
        />
      </aside>

      <main className="main">
        {selectedNode && selectedTranscript ? (
          <NodeDetail
            node={selectedNode}
            transcript={selectedTranscript}
            focusComposer={freshConversation}
            onDelete={deleteSession}
            models={models}
          />
        ) : (
          <div className="empty">Select an agent to view its activity.</div>
        )}
      </main>
    </div>
  );
}
