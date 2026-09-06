# Inter-agent Q&A: parent-child scope, relation/id addressing, exactly-once

We are adding an inter-agent question/answer channel. Q&A is restricted to a
direct **parent/child** relationship within the same conversation (no
sibling, no cross-level); the target is addressed **relationally for the
upward direction** (a subagent asks "its parent"; the hive resolves it from
the graph `parentId`) and **by agent id for the downward direction** (a
primary targets a child it spawned); the `agent_allowlist` does not
participate in Q&A addressing. Each question is permanently keyed by a
hive-unique opaque `questionId` and accepts **exactly one** answer
(once), delivered **asynchronously** (non-blocking ask) and routed back to
the asker via the existing steer channel when it is still running, or left
for the asker to retrieve on its own schedule.

Status: accepted

Considered options

- Sibling and cross-level Q&A: rejected for v1. The request scoped Q&A to
  "the same group of primary and subagent"; parent-child keeps the exchange
  unambiguous (exactly one parent; children disambiguated by id) and mirrors
  the existing `subagent_*` bridge. Broader scope can be a future ADR.
- Address-by-name / via `agent_allowlist`: rejected. The allowlist is a
  spawn-time, by-name gate and cannot disambiguate multiple subagents that
  share a profile name (e.g. two `coder`s). Addressing is therefore by
  relation (upward) or by hive agent id (downward), never by name.
- Synchronous blocking ask: rejected. The feature's defining property is an
  explicit question↔answer correlation (unlike `steer`/`follow_up`); a
  blocking ask would degrade it into another polling mechanism. Delivery is
  asynchronous; an asker that settles before the answer arrives retrieves it
  rather than being forcibly woken.
- Multi-answer questions: rejected. "One answer corresponds to one question"
  is an invariant; the first answer stands and later answers are rejected.

Consequences

- A subagent needs no knowledge of its parent's id to ask upward; the hive
  must track `from` per question so an answer routes back to the correct
  asker.
- The Q&A tools must be visible to every family member regardless of spawn
  allowlist, which diverges from how the existing `subagent_*` tools are
  gated.
- Idle askers deliberately are not woken to receive answers; they must poll
  (`question_status` / `pending_questions`), so delivery is best-effort for
  running askers and pull-based for idle ones.
- The Q&A store is in-memory only (v1): a hive restart drops open questions,
  so `pending_questions` after a restart under-reports what an agent is owed.
  Answered-record retention is bounded by the store itself; persistence can
  be layered into `HiveState` later if open questions must survive restarts.
