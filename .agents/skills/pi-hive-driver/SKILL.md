---
name: pi-hive-driver
description: >-
  How to drive the pi-hive orchestrator from an AI agent loop using ONLY
  its HTTP channel. The hive is already running — you make one-shot HTTP
  requests to the API (http://127.0.0.1:3001), issue prompt/steer/follow_up/
  abort/get_tree commands, and read agent output by polling the event backlog
  after the long-poll settle. Use this when you need to spawn an agent, give
  it a task, steer or abort it, or read its output through pi-hive instead of
  a human clicking the web GUI.
---

# pi-hive — HTTP Driver (for AI agents)

You are talking to a **running** pi-hive daemon. You do not start, configure,
or maintain it — a human/supervisor handles that. You drive it **exclusively
over its HTTP API**. The hive spawns and supervises pi agents (each an
isolated LLM "agent") and answers your HTTP requests with the results.

> Base URL: `http://127.0.0.1:3001` by default (Port 2, the API); the port is
> **configurable** and never hard-coded in the reference client — pass `host`/`port`
> (or `api_base`) to `HiveClient`, or set `PI_HIVE_API_HOST` / `PI_HIVE_API_PORT` /
> `PI_HIVE_API_BASE`. Port 1
> (`http://127.0.0.1:3000`) is only the web **GUI**, whose real-time view runs
> over a WebSocket mirror — you never touch a socket. The hive keeps a
> WebSocket only for that GUI; external drivers use HTTP only, with no
> connection lifecycle to maintain (each request is self-contained).

> **Writing this as code?** A self-contained, HTTP-only **Python reference
> client** lives at `scripts/http_client.py`. It implements the exact protocol
> below (spawn via `/api/primary/spawn`, prompt/steer/follow_up/abort, bare
> `{ok,...}` dialect, long-poll `/hive/agent/wait` completion, reading
> `message_end` from the event backlog) with only Python's stdlib (`urllib`) —
> no `requests`, no `httpx`, no `websocket-client`. Copy it or read it for the
> correct reasoning pattern for any language.

Every `/api` command endpoint returns the **bare `{ok, ...}`** dialect (no WS
envelope) when called with the request header
`Accept: application/vnd.hive.bare+json`. The reference client always sends
that header; you should too. Without it, the endpoint returns the legacy
`{type:"response",...}` envelope — both call the same handler, but for an
external driver the bare form is the contract.

---

## 1. What you can and can't do

You talk to agents via **commands** and read their output via **polling the
event backlog** (`GET /api/agent/{id}/events`).

- Agents are `primary` (a conversation root) or `subagent` (nested under a
  primary). Each has a stable `id` — the only handle you should reuse.
- **You steer primaries.** You give a primary a task, then steer / follow_up /
  abort it as it works.
- **Subagents are spawned BY the primary**, not by you. When a primary decides
  it needs help it calls its own `subagent_spawn` / `subagent_result` /
  `subagent_followup` / `subagent_abort` / `subagent_steer` / `subagent_glimpse`
  tools. You observe and manage the tree through `get_tree`, but you don't run
  the subagents yourself. `subagent_glimpse` is HTTP-only; you can use the same
  endpoint (`/hive/agent/glimpse`) to peek at ANY agent.
- **The hive does not do the "thinking."** It routes your commands into each
  agent and records every event (text, tool calls, settlement) into a bounded
  per-agent transcript log you read back over HTTP. It is memory-efficient:
  settled / idle agents are reclaimed and restarted on demand, so an idle `id`
  still works — it just may take a moment to come back.

### Ground rules
- Spawn a NEW primary with `POST /api/primary/spawn`; it returns the new `id`.
  To **continue** an existing one, target its `agent` id explicitly.
- **`cwd` is fixed at spawn time.** It is honored only when a new primary is
  created (`/api/primary/spawn`). A later `prompt`/`steer` targeting an existing
  `id` ignores `cwd` — an agent's `read`/`bash`/`edit` tools always run
  relative to the directory it was launched in. Omit `cwd` to use the hive's
  configured default working directory.

---

## 2. Commands (all HTTP, all bare `{ok,...}`)

Every command below is a **one-shot HTTP request**. There is no persistent
connection, no socket to keep open, and no response correlation problem — a
request↔response pair is naturally one-to-one, so no `reqId` is needed (the WS
channel's correlation ids never appear here).

All `POST` bodies are JSON. The `/api` command endpoints honor the
`Accept: application/vnd.hive.bare+json` header.

### 2.1 Spawn a new conversation

```http
POST /api/primary/spawn
Content-Type: application/json

{ "label": "optional", "model": "optional", "cwd": "/optional/abs/path", "agent": "optional-profile" }
```
- Returns the bare `{ok: true, id: "<new-primary-id>", model: ..., error: ""}`.
- `cwd` (optional absolute path) sets the new primary's working directory.
- **Spawn by profile:** an optional `agent: "<profile-name>"` makes the new
  primary run THAT agent profile instead of the configured `default_primary`.
  The profile must exist and be primary-eligible, otherwise it fails with a
  clear error and NO node is created.

### 2.2 Send / continue / steer / follow-up

```http
POST /api/prompt      { "agent": "<id>", "message": "..." }   # send / continue
POST /api/steer       { "agent": "<id>", "message": "..." }   # mid-stream nudge
POST /api/follow_up   { "agent": "<id>", "message": "..." }   # queued until finish
```
- `prompt` on an existing `id` = continue that conversation (lazily restored if idle).
- `steer` = mid-stream guidance to a **running** agent (it can't reach a
  settled target; prefer steering agents you know are running).
- `follow_up` = queued until the agent finishes; on an **idle** agent use `prompt`.
- The bare response is `{ok: true, error: ""}` on success.

### 2.3 Abort
```http
POST /api/abort   { "agent": "<id>", "reason": "time budget exceeded" }
```
- Aborting an already done/idle/aborted/failed agent is a no-op.
- Driver-level abort is **cooperative** (an RPC abort to that agent; it does
  not kill the process — only the primary's `subagent_abort` tool hard-stops a
  subagent). Once aborted, a node stays **`aborted` / terminal** — the settle
  that follows an aborted run does NOT flip it back to `done`.

### 2.4 Query
```http
GET /api/tree                          # bare {ok, tree: [...]} — all nodes
GET /api/agent/{id}                    # bare {ok, ...node} — one node
GET /api/agent/{id}/questions          # bare {ok, questions: [...]} — read-only Q&A (issue #9)
```
- `/api/agent/{id}` lazily materializes a persisted-but-unloaded session (the
  "select/click" signal), so a lookup may briefly take a moment on a cold id.
- `/api/agent/{id}/questions` is **read-only** visibility into the questions an
  agent ASKED (pending first, then recently answered, bounded by retention).
  The driver never answers here — it reads to decide whether to intervene.

### 2.5 Peek at an agent's live output (optional)
```http
POST /hive/agent/glimpse   { "id": "<agent_id>", "n": <int, clamped to [1,1024]> }
```
Works for ANY agent — subagents and the primary alike. The response carries
`status`, `phase`, `complete`, `truncated`, `totalChars`, `text`; treat
`complete:false` as a *live fragment*, never a final answer. Also note:
- `complete` is the authoritative "is this a final answer?" signal; rely on it.
  `status` is a reference label only — primaries settle to `idle` while
  subagents settle to `done`, and it can briefly disagree with the live state
  (e.g. at the moment of an abort), so do not use `status` alone to judge
  completeness.
- `totalChars` is a monotonic per-process counter of everything streamed since
  the process started; it is NOT the length of the returned `text` and is never
  reset between turns.
- `truncated:true` only means the 8KB tail window is longer than `n` — with a
  settled answer it is normal, NOT a sign the answer is cut off. The FULL final
  text is only available from the event backlog's `message_end` (see §3) or
  `subagent_result`'s `result.finalText`, never from a glimpse.

### 2.6 Wait for an agent to settle (long-poll — the ONLY sanctioned way)
Do **NOT** sleep-poll `GET /api/agent/{id}` to detect that an agent finished
its turn. Use the long-poll:
```http
POST /hive/agent/wait   { "id": "<agent_id>", "wait_time": <ms> }
```
- `wait_time: 0` returns the current state immediately.
- It works for primaries AND subagents: a settled or unloaded agent
  (idle/done/failed/aborted) returns its current node status + result payload
  at once (never waking or materializing it); a still-running agent blocks up
  to `wait_time` and returns the result the moment it settles, or
  `{ok:true, id, status:"running", progress:{...}}` with anti-stall signals
  (`recentlyActive`, `lastEventAgeMs`, `streaming`, `phase`, optional
  `liveOutputChars`/`usage`) if the bound elapses first. **Re-issue the call**
  on a `running` response to keep waiting. Unknown ids return `{ok:false, error}`.

---

## 3. Reading output (the event backlog)

There is no live event stream for external drivers; the hive keeps a bounded
in-memory transcript log per agent and you read it back:
```http
GET /api/agent/{id}/events?since=<lastSeq>
```
- Returns `{ok, agentId, events: [...], latest: <seq>}`. `since` is the last
  `seq` you applied (0 = all). `latest` is the newest seq in the batch — record
  it and use it as `since` on the next read to avoid re-fetching.
- Each `event` in the list is a `hive:event`-shaped record with an `event.type`:
  - `message_update` — streamed deltas (**not cumulative**).
  - `message_end` — the **authoritative** final text of a turn; use this, not deltas.
  - `tool_execution_*` — a tool invocation on that node.
  - `turn_end` / `agent_settled` — lifecycle boundaries / the agent stopped.
- Treat `message_end` + a completion signal (node status from `/hive/agent/wait`
  or `/api/agent/{id}`) as "the turn ended" before issuing the next command.

The reference client's `drive()` hides most of this: it spawns, sends the task,
long-polls `/hive/agent/wait` until settled, then pulls the `message_end`
texts from the event backlog.

---

## 4. Traps (read before driving)

1. **Spawn is explicit.** `POST /api/primary/spawn` always creates a NEW
   primary; it never continues an old one. To continue, target the existing
   `id` via `/api/prompt`.
2. **Read `message_end`, not `message_update` deltas**, for final text.
3. **Idle reaping is real.** A settled agent may be reclaimed and silently
   restarted on the next command to its `id`. `loaded=false` is not "lost" —
   it means a one-time restart latency.
4. **`follow_up` is a queue for running agents.** To continue an idle agent
   use `prompt` with the explicit `agent` id.
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
8. **Working ≠ stalled.** A subagent can spend a long time thinking or running
   tools; the `/hive/agent/wait` and `/hive/agent/glimpse` `progress` blocks are
   the honest signal only via their event-layer fields: `recentlyActive` (true
   while events are arriving), `lastEventAgeMs`, `streaming` and `phase`
   (`thinking` / `generating` / `toolcalling` / `tool_running`). The numeric
   fields are OPTIONAL and appear only once they carry information —
   `liveOutputChars` once the model has streamed output, `usage` once the
   provider reports a non-zero counter — so a healthy-but-quiet subagent
   (silent TTFT, long tool runs, local endpoints that report usage only at
   completion) legitimately shows neither. Absence is NOT a stall. Do not
   force-stop a primary just because a subagent it spawned has gone quiet — if
   you must look, `GET /api/agent/{id}/events` on that subagent rather than
   guessing from silence.
9. **You never maintain a socket.** Every command is a self-contained HTTP
   request. If a request times out, re-issue the long-poll wait — do not build
   any reconnect/lifecycle machinery.

---

## 5. A minimal safe loop

1. `GET /api/tree` to see what exists (or start fresh).
2. Create work: `POST /api/primary/spawn` (capture the new `id`), then
   `POST /api/prompt` with that `id` and the task.
3. Long-poll `POST /hive/agent/wait {id}` repeatedly until status ≠ `running`.
4. Read `GET /api/agent/{id}/events?since=...` and keep every `message_end`
   text for that `id`; watch `tool_execution_*` only if you need to know which
   tools it used.
5. To redirect: `POST /api/steer {agent, message}`.
6. When done or out of budget: `POST /api/abort {agent, reason}`.
7. For parallel work, hold each primary's `id` and target every command at it
   explicitly (independent HTTP requests, so no cross-contamination).

---

## 6. Reference client

The working directory here is this skill's directory (`.agents/skills/pi-hive-driver/`);
`scripts/...` paths below are relative to it — from the repo root that is
`.agents/skills/pi-hive-driver/scripts/...`.

### 6.1 Quickstart — verified to work (do NOT hand-roll a client)

From the repo root, against the configured hive (API port is `server.apiPort`
in `hive.config.json`, e.g. 4101):

```bash
# end-to-end drive (spawn + prompt + wait-for-settle + read the answer)
.venv/bin/python .agents/skills/pi-hive-driver/scripts/http_client.py \
    --host 127.0.0.1 --port 4101 \
    --prompt "Reply with exactly one short sentence about the Eiffel Tower." \
    --wall-timeout 120
```

This is the **verified-on-a-real-running-hive** path — it prints `agent_id`,
`settled: True`, `status`, and the agent's `final_text`. If you find yourself
writing a bespoke driver instead of calling this, you are duplicating work:
hand-rolled clients routinely miss the two failure modes this one already
handles (see 6.3). Use this script or the `HiveClient` methods it wraps.

### 6.2 `HiveClient`

`scripts/http_client.py` is the single, HTTP-only reference client (issue #12):
- `HiveClient(host="127.0.0.1", port=3001)` (or `api_base=...`, or the
  `PI_HIVE_API_HOST` / `PI_HIVE_API_PORT` / `PI_HIVE_API_BASE` env vars) — the
  API port is configurable (`hive.config.json` -> `server.apiPort`), so it is
  never hard-coded.
- Methods: `spawn`, `prompt`, `steer`, `follow_up`, `abort`, `wait`, `drive`,
  `get_tree`, `get_agent`, `questions`, `agent_glimpse`, `check_online`.
- `drive(prompt, agent_id=None, cwd=None, wall_timeout=1800)` is the blocking
  complete-turn driver: it spawns (if no `agent_id`), sends the task,
  long-polls `/hive/agent/wait` until the agent settles, then reads the
  `message_end` transcripts from the event backlog. It raises `HiveError` on
  transport/protocol failure instead of busy-spinning.
- Only dependency: Python's stdlib `urllib`. No third-party package.

### 6.3 Two failure modes the reference client already handles

These are real ops conditions found by driving a live hive; a hand-rolled
client must handle them or it will hang or return empty output:
1. **`/api/primary/spawn` + the node settles, but the answer is NOT in the
   `POST /hive/agent/wait` payload** — primaries settle to `idle` and their
   wait payload carries no `finalText`. Final text comes only from the event
   backlog (`message_end`).
2. **The settle signal can beat the event-record flush** — right after
   the agent settles, an immediate `events` read may still be empty. `drive()`
   polls the backlog briefly until `message_end` appears (bounded by the wall
   timeout) instead of giving up after one empty read.

### 6.4 Server-side note (correctness fix shipped with this issue)

`POST /hive/agent/wait` previously reported a **settled primary as `running`
forever** because the primary prompt path never marks the in-memory state
terminal. It now short-circuits on the authoritative graph-node status
(primaries settle to `idle`, subagents to `done`), so the long-poll returns
promptly for a finished primary. Do not reintroduce dependence on the
in-memory `status` for completion detection.

The old WS-based drivers (`python_client.py`, `hivedriver.py`, and the shared
`hive_protocol.py`) were removed — external callers no longer maintain a
WebSocket lifecycle; that is now reserved for the web GUI only.

Drive-loop invariants preserved from the removed WS drivers (so reasoning
patterns don't change):
- Completion is detected by the settle signal (a settled node status from the
  long-poll), not by mere liveliness — never treat a `running`/partial payload
  as "finished".
- Final text comes from `message_end` records, never from deltas or a glimpse.
- A task targets one `id`; output is filtered to that `id` so other agents'
  activity never leaks into the result.

---

This skill concerns **you, the driver**. You do not touch Python,
`hive.config.json`, session files, or how the daemon is launched — the hive is an
infrastructure detail that is already running for you.
