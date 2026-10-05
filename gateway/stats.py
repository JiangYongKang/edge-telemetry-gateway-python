"""Persistent operations statistics (SQLite) -- survives process restarts.

Counters and last-flush progress are committed to the same database file as
the buffer, so the books (pending vs. uploaded vs. rejected) balance across
restarts, including a crash in the middle of a replay.

Counter naming:
* ``uploaded_total``        -- rows acked by the uplink (cumulative)
* ``reject:<reason_code>``  -- records refused, per ReasonCode
* ``dedup:<reason_code>``   -- records deduplicated, per ReasonCode
* ``flag:<reason_code>``    -- accepted but flagged (e.g. out-of-order)

Last-flush progress is stored as key/value pairs:
``last_flush_at_ms`` / ``last_flush_acked`` / ``last_flush_rounds``.
"""
from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

from gateway.models import ReasonCode

_SCHEMA = """
CREATE TABLE IF NOT EXISTS stats_counters (
    name TEXT PRIMARY KEY,
    value INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS stats_kv (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""

UPLOADED_TOTAL = "uploaded_total"


class StatsStore:
    def __init__(self, db_path: str | Path) -> None:
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    # -- counters ----------------------------------------------------------

    def incr(self, name: str, n: int = 1) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO stats_counters (name, value) VALUES (?, ?)"
                " ON CONFLICT(name) DO UPDATE SET value = value + ?",
                (name, n, n),
            )
            self._conn.commit()

    def record_reject(self, code: ReasonCode) -> None:
        self.incr(f"reject:{code.value}")

    def record_dedup(self, code: ReasonCode) -> None:
        self.incr(f"dedup:{code.value}")

    def record_flag(self, code: ReasonCode) -> None:
        self.incr(f"flag:{code.value}")

    def record_uploaded(self, n: int) -> None:
        if n > 0:
            self.incr(UPLOADED_TOTAL, n)

    def counters(self) -> dict[str, int]:
        with self._lock:
            rows = self._conn.execute("SELECT name, value FROM stats_counters").fetchall()
        return {r["name"]: int(r["value"]) for r in rows}

    # -- last flush progress -------------------------------------------------

    def record_flush(self, acked: int, rounds: int, at_ms: int) -> None:
        with self._lock:
            self._conn.executemany(
                "INSERT INTO stats_kv (key, value) VALUES (?, ?)"
                " ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                [
                    ("last_flush_at_ms", str(at_ms)),
                    ("last_flush_acked", str(acked)),
                    ("last_flush_rounds", str(rounds)),
                ],
            )
            self._conn.commit()

    def last_flush(self) -> dict[str, int] | None:
        with self._lock:
            rows = self._conn.execute(
                "SELECT key, value FROM stats_kv WHERE key LIKE 'last_flush_%'"
            ).fetchall()
        if not rows:
            return None
        kv = {r["key"]: int(r["value"]) for r in rows}
        return {
            "at_ms": kv.get("last_flush_at_ms", 0),
            "acked": kv.get("last_flush_acked", 0),
            "rounds": kv.get("last_flush_rounds", 0),
        }

    # -- aggregate view ------------------------------------------------------

    def reason_summary(self) -> dict[str, dict[str, int]]:
        """Rejected / deduplicated / flagged counts keyed by ReasonCode value."""
        summary: dict[str, dict[str, int]] = {"rejected": {}, "deduplicated": {}, "flagged": {}}
        for name, value in self.counters().items():
            if name.startswith("reject:"):
                summary["rejected"][name[len("reject:"):]] = value
            elif name.startswith("dedup:"):
                summary["deduplicated"][name[len("dedup:"):]] = value
            elif name.startswith("flag:"):
                summary["flagged"][name[len("flag:"):]] = value
        return summary

    def uploaded_total(self) -> int:
        return self.counters().get(UPLOADED_TOTAL, 0)

    def close(self) -> None:
        with self._lock:
            self._conn.close()
