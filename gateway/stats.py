"""Persistent ops statistics: reason counters and replay progress.

Backed by SQLite (same db file as the buffer is fine -- WAL mode allows
multiple connections), so counters survive restarts and crashes:

* Reason counters: one counter per ReasonCode, incremented for every
  rejected / deduplicated / flagged record. Callers read the ReasonCode
  enum directly; no detail-string parsing.
* Upload ledger: ``uploaded_total`` counts rows acked by the uplink.
  Together with ``buffer.pending_count()`` the books always balance:
  accepted = pending + uploaded (modulo duplicates, which are counted
  under ReasonCode.DUPLICATE instead).
* Last replay: the most recent flush round (trigger, attempted, acked,
  remaining) so ops can see what the latest resume/replay did.
"""
from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

from gateway.models import ReasonCode

_SCHEMA = """
CREATE TABLE IF NOT EXISTS counters (
    key TEXT PRIMARY KEY,
    value INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS last_replay (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    at_ms INTEGER NOT NULL,
    trigger TEXT NOT NULL,
    attempted INTEGER NOT NULL,
    acked INTEGER NOT NULL,
    remaining INTEGER NOT NULL
);
"""

_REASON_PREFIX = "reason:"
_UPLOADED_KEY = "uploaded_total"


class StatsStore:
    def __init__(self, db_path: str | Path) -> None:
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    def _incr(self, key: str, n: int) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO counters (key, value) VALUES (?, ?)"
                " ON CONFLICT(key) DO UPDATE SET value = value + ?",
                (key, n, n),
            )
            self._conn.commit()

    def incr_reason(self, code: ReasonCode, n: int = 1) -> None:
        """Count n records under the given unified reason code."""
        self._incr(_REASON_PREFIX + code.value, n)

    def add_uploaded(self, n: int) -> None:
        """Add n to the total of rows acked by the uplink."""
        if n > 0:
            self._incr(_UPLOADED_KEY, n)

    def record_replay(
        self, at_ms: int, trigger: str, attempted: int, acked: int, remaining: int
    ) -> None:
        """Record the outcome of one replay (flush) round."""
        with self._lock:
            self._conn.execute(
                "INSERT INTO last_replay (id, at_ms, trigger, attempted, acked, remaining)"
                " VALUES (1, ?, ?, ?, ?, ?)"
                " ON CONFLICT(id) DO UPDATE SET at_ms=excluded.at_ms,"
                " trigger=excluded.trigger, attempted=excluded.attempted,"
                " acked=excluded.acked, remaining=excluded.remaining",
                (at_ms, trigger, attempted, acked, remaining),
            )
            self._conn.commit()

    def _counter(self, key: str) -> int:
        row = self._conn.execute(
            "SELECT value FROM counters WHERE key = ?", (key,)
        ).fetchone()
        return int(row["value"]) if row else 0

    @property
    def uploaded_total(self) -> int:
        with self._lock:
            return self._counter(_UPLOADED_KEY)

    def reason_counts(self) -> dict[str, int]:
        """{reason_code_value: count} for every ReasonCode (zeros included)."""
        with self._lock:
            counts = {code.value: 0 for code in ReasonCode}
            rows = self._conn.execute(
                "SELECT key, value FROM counters WHERE key LIKE ?",
                (_REASON_PREFIX + "%",),
            ).fetchall()
        for r in rows:
            counts[r["key"][len(_REASON_PREFIX):]] = int(r["value"])
        return counts

    def last_replay(self) -> dict | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT at_ms, trigger, attempted, acked, remaining FROM last_replay WHERE id = 1"
            ).fetchone()
        if row is None:
            return None
        return {
            "at_ms": int(row["at_ms"]),
            "trigger": row["trigger"],
            "attempted": int(row["attempted"]),
            "acked": int(row["acked"]),
            "remaining": int(row["remaining"]),
        }

    def close(self) -> None:
        with self._lock:
            self._conn.close()
