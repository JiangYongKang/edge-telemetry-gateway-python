"""Persistent local buffer (SQLite) with capacity, backpressure and ordered replay.

Semantics:
* Durability: every accepted record is committed to SQLite before enqueue()
  returns, so pending data survives a process restart.
* Capacity / backpressure: when ``capacity`` pending rows are stored, enqueue()
  raises BufferFullError -- an explicit reject, never a silent drop.
* Idempotent dedup: the dedup key (device_id|metric|timestamp_ms) has a UNIQUE
  constraint; concurrent duplicate deliveries are resolved atomically by the
  database, so exactly one copy is stored.
  - same key + same value  -> "exact_duplicate"
  - same key + other value -> "conflicting_duplicate"
* Replay: fetch_batch() returns pending rows oldest-first (collection order);
  rows are removed only via ack() after the uplink confirms, so a crash
  mid-flush cannot lose data (at-least-once, deduped downstream by key).
"""
from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

from gateway.models import TelemetryRecord


class BufferFullError(Exception):
    """Raised when the buffer is at capacity (backpressure, explicit reject)."""

    def __init__(self, capacity: int) -> None:
        super().__init__(f"buffer full: capacity {capacity} reached")
        self.capacity = capacity


_SCHEMA = """
CREATE TABLE IF NOT EXISTS buffer (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dedup_key TEXT NOT NULL,
    device_id TEXT NOT NULL,
    metric TEXT NOT NULL,
    value REAL NOT NULL,
    unit TEXT NOT NULL,
    timestamp_ms INTEGER NOT NULL,
    acked INTEGER NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_buffer_dedup ON buffer(dedup_key);
"""


class PersistentBuffer:
    def __init__(self, db_path: str | Path, capacity: int = 10_000) -> None:
        self._capacity = capacity
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    def enqueue(self, record: TelemetryRecord) -> str:
        """Store record. Returns 'stored' | 'exact_duplicate' | 'conflicting_duplicate'."""
        with self._lock:
            try:
                self._conn.execute(
                    "INSERT INTO buffer (dedup_key, device_id, metric, value, unit, timestamp_ms)"
                    " VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        record.dedup_key,
                        record.device_id,
                        record.metric,
                        record.value,
                        record.unit,
                        record.timestamp_ms,
                    ),
                )
            except sqlite3.IntegrityError:
                # Unique constraint on dedup_key hit: atomic under concurrency.
                row = self._conn.execute(
                    "SELECT value FROM buffer WHERE dedup_key = ?", (record.dedup_key,)
                ).fetchone()
                self._conn.rollback()
                if row is not None and abs(row["value"] - record.value) < 1e-9:
                    return "exact_duplicate"
                return "conflicting_duplicate"
            # Capacity check counts only pending (unacked) rows; done after the
            # insert attempt so duplicates never consume capacity.
            pending = self._pending_count_locked()
            if pending > self._capacity:
                self._conn.rollback()
                raise BufferFullError(self._capacity)
            self._conn.commit()
            return "stored"

    def _pending_count_locked(self) -> int:
        row = self._conn.execute("SELECT COUNT(*) AS n FROM buffer WHERE acked = 0").fetchone()
        return int(row["n"])

    def pending_count(self) -> int:
        with self._lock:
            return self._pending_count_locked()

    def fetch_batch(self, limit: int) -> list[tuple[int, TelemetryRecord]]:
        """Oldest-first pending rows as (row_id, record)."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT id, device_id, metric, value, unit, timestamp_ms"
                " FROM buffer WHERE acked = 0 ORDER BY id ASC LIMIT ?",
                (limit,),
            ).fetchall()
        return [
            (
                int(r["id"]),
                TelemetryRecord(
                    device_id=r["device_id"],
                    metric=r["metric"],
                    value=float(r["value"]),
                    unit=r["unit"],
                    timestamp_ms=int(r["timestamp_ms"]),
                ),
            )
            for r in rows
        ]

    def ack(self, row_ids: list[int]) -> None:
        """Mark rows as uploaded (remove from pending)."""
        if not row_ids:
            return
        with self._lock:
            self._conn.executemany(
                "UPDATE buffer SET acked = 1 WHERE id = ?", [(i,) for i in row_ids]
            )
            self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()
