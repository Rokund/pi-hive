"""In-memory question store for inter-agent Q&A (ADR-0001 / issue #4).

PURE storage: the store knows nothing about the agent graph or the process
manager.  Scope validation (direct parent/child, same conversation) and
delivery (steer injection / idle wake) live in the routes that own the graph
(``hive/server.py``).

Wire record shape (JSON keys, exactly as returned to clients):

    {
      "id":         opaque uuid4 hex — the hive-unique questionId,
      "from":       asker agent id,
      "to":         addressee agent id (fixed at ask time, never changes),
      "question":   question text,
      "answer":     answer text               (absent while pending),
      "status":     "pending" | "answered",
      "askedAt":    epoch ms,
      "answeredAt": epoch ms                  (absent while pending),
    }

Invariants (ADR-0001):

* Exactly-once — the first ``answer()`` for a questionId wins; later answers
  are rejected and handed back the existing answer.
* No TTL — open questions are never pruned; only ANSWERED records are
  retained boundedly (the most recent ``max_answered`` of them).
"""

from __future__ import annotations

import threading
import time
import uuid
from typing import Any, Callable, Dict, List, Optional, Tuple

#: Bounded retention cap: keep at most this many ANSWERED records.  Open
#: (pending) questions are never pruned — they have no TTL, so an asker can
#: always come back for the answer it is owed.
DEFAULT_MAX_ANSWERED = 500

#: Rejection error returned by :meth:`QuestionStore.answer` for a questionId
#: that already carries its (single, permanent) answer.
ALREADY_ANSWERED_ERROR = "already answered"


def _default_now_ms() -> int:
    return int(time.time() * 1000)


class QuestionStore:
    """Small in-memory question/answer registry keyed by opaque questionId.

    Thread-safe (an RLock guards every mutation — the same pattern as
    ``AgentGraph``), deterministic under tests via the injectable ``now_ms``
    clock.  Records are plain dicts with the JSON wire keys documented in the
    module docstring, returned as copies so callers cannot mutate the store.
    """

    def __init__(
        self,
        max_answered: int = DEFAULT_MAX_ANSWERED,
        now_ms: Optional[Callable[[], int]] = None,
    ) -> None:
        self._max_answered = max(1, int(max_answered))
        self._now_ms = now_ms or _default_now_ms
        self._questions: Dict[str, Dict[str, Any]] = {}  # id -> record
        self._lock = threading.RLock()

    # -- write --------------------------------------------------------------
    def create(self, *, frm: str, to: str, question: str) -> Dict[str, Any]:
        """Record a new pending question and return its stored record.

        The questionId is minted here (uuid4 hex): hive-unique, opaque, and
        permanently attached to this question.
        """
        record: Dict[str, Any] = {
            "id": uuid.uuid4().hex,
            "from": frm,
            "to": to,
            "question": question,
            "status": "pending",
            "askedAt": self._now_ms(),
        }
        with self._lock:
            self._questions[record["id"]] = record
        return dict(record)

    def answer(
        self, question_id: str, text: str
    ) -> Tuple[bool, Optional[Dict[str, Any]], Optional[str]]:
        """Attach THE answer to a question — exactly once.

        Returns ``(ok, record, error)``:

        * first answer   -> ``(True, answered_record, None)``; the record now
          carries ``answer``/``answeredAt``/``status: "answered"``, and
          bounded pruning of answered records runs immediately;
        * later answers  -> ``(False, existing_record, "already answered")`` —
          the existing answer is handed back so the caller can stop retrying;
        * unknown id     -> ``(False, None, "unknown question id: ...")``.
        """
        with self._lock:
            record = self._questions.get(question_id)
            if record is None:
                return False, None, f"unknown question id: {question_id}"
            if record["status"] == "answered":
                return False, dict(record), ALREADY_ANSWERED_ERROR
            record["answer"] = text
            record["status"] = "answered"
            record["answeredAt"] = self._now_ms()
            self._prune_locked()
            return True, dict(record), None

    # -- read ---------------------------------------------------------------
    def get(self, question_id: str) -> Optional[Dict[str, Any]]:
        """The record for a questionId (copy), or None when unknown/pruned."""
        with self._lock:
            record = self._questions.get(question_id)
            return dict(record) if record is not None else None

    def pending_asked_by(self, frm: str) -> List[Dict[str, Any]]:
        """Questions ASKED BY ``frm`` that are still pending.

        These are the answers the agent is owed — its pull-based retrieval
        list when it settled before an answer arrived (ADR-0001: idle askers
        are never woken).  Ordered oldest-asked-first.
        """
        with self._lock:
            pending = [
                dict(r) for r in self._questions.values()
                if r["from"] == frm and r["status"] == "pending"
            ]
        pending.sort(key=lambda r: r["askedAt"])
        return pending

    def __len__(self) -> int:
        with self._lock:
            return len(self._questions)

    # -- retention ----------------------------------------------------------
    def prune(self) -> int:
        """Drop answered records beyond the cap; open questions are untouchable.

        Keeps the most recent ``max_answered`` answered records (by
        ``answeredAt``) and deletes older ones.  Returns the number removed.
        """
        with self._lock:
            return self._prune_locked()

    def _prune_locked(self) -> int:
        answered = sorted(
            (r for r in self._questions.values() if r["status"] == "answered"),
            key=lambda r: (r["answeredAt"], r["askedAt"]),
        )
        excess = len(answered) - self._max_answered
        removed = 0
        for record in answered[:excess]:
            self._questions.pop(record["id"], None)
            removed += 1
        return removed
