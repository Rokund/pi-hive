---
name: pi-hive-driver
description: >-
  How to drive the pi-hive orchestrator from an AI agent loop using ONLY
  its WebSocket channel. The hive is already running — you connect to
  ws://127.0.0.1:3001/ws, issue {type:prompt|steer|follow_up|abort|get_tree}
  commands, and read the agent output that streams back on the same socket.
  Use this when you need to spawn an agent, give it a task, steer or abort it,
  or read its tool calls and text output through pi-hive instead of a
  human clicking the web GUI.
---

# pi-hive — WebSocket Driver (for AI agents)

You are talking to a **running** pi-hive daemon. You do not start,
configure, or maintain it — a human/supervisor handles that. You drive it
**exclusively over its WebSocket API**. The hive spawns and supervises pi
agents (each an isolated LLM "agent") and relays their output to you over the
same socket.

> Connect to `ws://127.0.0.1:3001/ws` (Port 2, the API WebSocket).
> Port 1 (`ws://127.0.0.1:3000/ws`) is only the GUI mirror — you don't need it.

> **Writing this as code?** A self-contained, WS-only **Python reference client**
> lives at `scripts/python_client.py`. It implements the exact protocol
> below (bare-prompt spawn with `cwd`, prompt/steer/follow_up/abort, `get_tree`,
> completing on `agent_settled`, reading `message_end` not deltas, deduped tool
> calls) with zero daemon/HTTP knowledge — copy it or read it for the correct
> reasoning pattern for any language. Only dependency: `websocket-client`.

---

## 1. What you can and can't do

You talk to agents via **commands** and read their output via the **event
stream** (both on the same socket).

- Agents are `primary` (a conversation root) or `subagent` (nested under a
  primary). Each has a stable `id` — the only handle you should reuse.
- **You steer primaries.** You give a primary a task, then steer / follow_up /
  abort it as it works.
- **Subagents are spawned BY the primary**, not by you. When a primary decides
  it needs help it calls its own `subagent_spawn` / `subagent_result` /
  `subagent_followup` / `subagent_abort` / `subagent_steer` / `subagent_glimpse`
  tools. You observe that machinery through the event stream; you don't run the
  subagents yourself. Note `subagent_glimpse` (a *peek* at the tail of what a
  subagent is producing — including tool-call arguments as they stream) is
  HTTP-only today; there is no WS command for it.
- **The hive does not do the "thinking."** It routes your commands into each
  agent's stdin and forwards every event (text deltas, tool calls,
  settlement) back out tagged with `agentId`. It is memory-efficient: settled
  / idle agents are reclaimed and restarted on demand, so an idle `id` still
  works — it just may take a moment to come back.

### Ground rules
- A **`prompt` with no `agentId` always creates a NEW primary conversation.**
  To continue an existing one you MUST pass its `agentId`.
- **`cwd` is fixed at spawn time.** It is honored only when a new primary is
  being created (a bare `prompt` without `agentId`). A later `prompt`/`steer`
  that carries an `agentId` ignores `cwd` — an agent's `read`/`bash`/`edit`
  tools always run relative to the directory it was launched in. Omit `cwd` to
  use the hive's configured default working directory.

---

## 2. Commands

Every message you send is a JSON object with a `type`. The hive answers with
`{type:"response", command:..., success:...}` and, in parallel, pushes
broadcast event frames on the same socket.

### 2.1 Spawn a new conversation with a task
```json
{ "type": "prompt", "text": "Investigate the height of the Eiffel Tower and reply briefly." }
```
- No `agentId` → the hive spawns a fresh primary to run it. The new `id` shows
  up in the next `hive:agent_updated` / `hive:tree` frame.
- `cwd` (optional absolute path) sets that primary's working directory.

### 2.2 Continue / steer an existing agent
```json
{ "type": "prompt",    "agentId": "<id>", "text": "Now also compare two more countries." }
{ "type": "steer",     "agentId": "<id>", "text": "Wait, focus on the sources, not speculation." }
{ "type": "follow_up", "agentId": "<id>", "text": "Summarize in 3 bullets." }
```
- `prompt` with an `agentId` = continue that conversation (lazily restored if idle).
- `steer` = mid-stream guidance to a **running** agent.
- `follow_up` = queued until the agent finishes; on an **idle** agent use `prompt`.

### 2.3 Abort
```json
{ "type": "abort", "agentId": "<id>", "reason": "time budget exceeded", "by": "external" }
```
- Aborting an already done/idle/aborted/failed agent is a no-op.
- Driver-level abort is **cooperative** (an RPC abort to that agent; it does
  not kill the process — only the primary's `subagent_abort` tool hard-stops a
  subagent). Once aborted, a node stays **`aborted` / terminal**: the settle
  event that follows an aborted run does NOT flip it back to `done`.

### 2.4 Query
```json
{ "type": "get_tree" }                        // data.tree = all nodes
{ "type": "get_agent", "agent": "<id>" }       // data = one node
{ "type": "subscribe" }                        // ack; events already flow anyway
```

### 2.5 Peek at an agent's live output (optional, HTTP only)
There is no WS command for this; the hive exposes it as
`POST http://127.0.0.1:3001/hive/agent/glimpse` with body
`{"id": "<agent_id>", "n": <int, clamped to [1, 1024]>}`. It works for ANY
agent — subagents and the primary alike. The primary calls it for its own
subagents through `subagent_glimpse`; a driver that wants the same view
out-of-band can use the stdlib-HTTP helper `HiveClient.subagent_glimpse()` in
`scripts/python_client.py` (urllib — no new dependency; it still POSTs to the
legacy `/hive/subagent/glimpse` alias, which remains). The response carries
`status`, `phase`, `complete`, `truncated`, `totalChars`, `text`; treat
`complete:false` as a *live fragment*, never a final answer. Also note:
- `complete` is the authoritative "is this a final answer?" signal; rely on it.
  `status` is a reference label only — primaries settle to `idle` while
  subagents settle to `done`, and it can briefly disagree with the live state
  (e.g. at the moment of an abort), so do not use `status` alone to judge
  completeness.
- `totalChars` is a monotonic per-process counter of everything streamed since
  the process started (the same number as `subagent_result`'s progress
  `liveOutputChars`); it is NOT the length of the returned `text` and is never
  reset between turns.
- `truncated:true` only means the 8KB tail window is longer than `n` — with a
  settled answer it is the normal case, NOT a sign the answer is cut off. The
  FULL final text is only available from the WS `message_end` event or
  `subagent_result`'s `result.finalText`, never from a glimpse.

---

## 3. Reading output (the event stream)

Agent output arrives as **event frames**, separate from command responses:
```json
{ "type": "hive:event", "agentId": "<id>", "seq": 123,
  "event": { "type": "message_update" | "message_end" | "tool_execution_*" | "agent_settled" | ... } }
{ "type": "hive:tree", "tree": [...] }
{ "type": "hive:agent_updated", "agent": {...} }
{ "type": "hive:error", "message": "..." }
```
Useful node fields: `id`, `kind` (`primary`|`subagent`), `name`, `parentId`,
`status`, `createdAt`, `lastResult{...}`.

Inside `hive:event.event`:
- `message_update` — streamed deltas (**not cumulative**).
- `message_end` — the **authoritative** final text of a turn; use this, not deltas.
- `tool_execution_start/update/end` — a tool invocation on that node.
- `turn_end` / `agent_settled` — lifecycle boundaries / the agent stopped.
- `subagent_spawned` — that node spawned a child; find it in the tree by `parentId`.

Treat `message_end` + a completion signal (node status, or `agent_settled`) as
"the turn ended" before issuing the next command.

---

## 4. Traps (read before driving)

1. **A bare `prompt` forks a new conversation every time.** Track `id`s; a
   bare prompt never continues the previous one.
2. **Read `message_end`, not `message_update` deltas**, for final text.
3. **Idle reaping is real.** A settled agent may be reclaimed and silently
   restarted on the next command to its `id`. `loaded=false` is not "lost" —
   it means a one-time restart latency.
4. **`follow_up` is a queue for running agents.** To continue an idle agent
   use `prompt` with the explicit `agentId`.
5. **Only allowed subagent names spawn.** If the primary calls
   `subagent_spawn(name)` with a name outside its allowlist it gets
   `{ok:false, error:"not allowed"}`. Allowed names are configured per parent
   — ask the primary which it can spawn, or inspect its `agent_allowlist` in
   the node profile.
6. **`cwd` is spawn-time only.** You cannot change an existing agent's working
   directory. To target a directory, set `cwd` when spawning the new primary.
7. **Let the primary steer its subagents.** `subagent_steer` nudges a *running*
   subagent and can't reach an idle one (that needs `subagent_followup`). Since
   the primary authors these, keep them as natural next-step instructions, not
   "ignore your instructions" (which models may mistake for a prompt injection).
8. **Working ≠ stalled.** A subagent can spend a long time thinking or building
   tool-call arguments; its token/byte aggregates look static while it works.
   The primary's `subagent_result` `progress` is the honest signal:
   `recentlyActive` (true while events are arriving) plus the *moving* counters
   `liveOutputChars` and `usage` and a `phase` (`thinking` / `generating` /
   `toolcalling` / `tool_running`) mean it is producing. Do not force-stop a
   primary just because a subagent it spawned has gone quiet for a while — if
   you must look, watch that subagent's own event stream (thinking deltas, tool
   execution) rather than guessing from silence. `sessionBytes` is gone; rely
   on the fields above, not file-size heuristics.

---

## 5. A minimal safe loop

1. `{type:"get_tree"}` to see what exists (or start fresh).
2. Create work: `{type:"prompt", text:"..."}`; capture the new primary's `id`.
3. Read `hive:event` frames; accumulate `message_end` text for that `id`; watch
   `tool_execution_*` only if you need to know which tools it used.
4. To redirect: `{type:"steer", agentId:"<id>", text:"..."}`.
5. When done or out of budget: `{type:"abort", agentId:"<id>", ...}`.
6. For parallel work, hold each primary's `id` and target every command at it
   explicitly.

---

This skill concerns **you, the driver**. You do not touch Python,
`hive.config.json`, session files, or how the daemon is launched — the hive is an
infrastructure detail that is already running for you.
