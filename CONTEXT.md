# pi-hive

pi-hive supervises a tree of named pi agents under a primary root. This
glossary pins the language for its agent-relations domain.

## Language

**Agent**:
A supervised pi process, either a `primary` (a conversation root) or a
`subagent` (nested under a parent within a conversation).
_Avoid_: worker, participant, node

**Conversation**:
A primary agent together with every agent it (transitively) spawned as
subagents. A conversation is the largest unit an agent-relation can span.
_Avoid_: group, family, session, tree

**Primary**:
The root agent of a conversation. Every agent belongs to exactly one
conversation, identified by its primary.
_Avoid_: main, root agent

**Subagent**:
An agent spawned by another agent inside the same conversation. A subagent's
parent is the agent that spawned it; a subagent may itself spawn further
subagents.
_Avoid_: child, worker

**Question**:
A request for information from one agent to another within the same
conversation.
_Avoid_: query, ask, prompt

**Answer**:
The single accepted response to a question. There is at most one answer per
question.
_Avoid_: reply, result

**Asker**:
The agent that posed a question.
_Avoid_: requester, requester-agent, questioner

**Answerer**:
The agent that produces the answer to a question.
_Avoid_: responder, replier

**Addressee**:
The agent a question is directed to and that must answer it. The addressee of
a question is fixed at ask time and never changes.
_Avoid_: target, recipient

**Question id**:
A hive-unique opaque identifier permanently attached to a question, used to
unambiguously pair an answer with the exact question it answers.
_Avoid_: question number, qid

**Inter-agent question-and-answer (Q&A)**:
An addressed, correlated, exactly-once information exchange between two agents
that share a direct parent/child relationship within the same conversation.
Unlike a steering or follow-up instruction, Q&A carries a strict one-to-one
link between a question and its answer, keyed by the question id; only the
asker and the answerer of that question participate in the exchange.
_Avoid_: chat, messaging, inter-agent dialogue

**Pending question**:
A question that has been asked of an agent but not yet answered. An agent may
list its own pending questions (the answers it is owed) to retrieve them on
its own schedule.
_Avoid_: outstanding question, open query
