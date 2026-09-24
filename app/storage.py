"""SQLite 本地持久化。

包含：
- records:        已接收记录（idempotency key 唯一约束兜底并发去重）
- watermarks:     每 (设备,指标) 的采集时间水位，用于乱序判定
- spool:          待补传队列（FIFO，seq = 入库顺序）
- fault_states:   故障判定状态（含去抖候选计数，重启可恢复）
- fault_events:   故障翻转事件流水

并发：进程内 threading.RLock 串行化 + SQLite UNIQUE 约束硬保证。
"""
from __future__ import annotations

import logging
import os
import sqlite3
import threading
from typing import Iterable

from app.models import RejectReason, SourceProtocol, TelemetryRecord

logger = logging.getLogger("telemetry.storage")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS records (
    seq            INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id      TEXT NOT NULL,
    metric         TEXT NOT NULL,
    value          REAL NOT NULL,
    unit           TEXT NOT NULL,
    ts_ms          INTEGER NOT NULL,
    source         TEXT NOT NULL,
    idem_key       TEXT NOT NULL UNIQUE,
    received_ts_ms INTEGER NOT NULL,
    in_range       INTEGER NOT NULL,
    delivered      INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_records_stream
    ON records(device_id, metric, ts_ms);
CREATE INDEX IF NOT EXISTS idx_records_dev_ts ON records(device_id, ts_ms);

CREATE TABLE IF NOT EXISTS watermarks (
    device_id  TEXT NOT NULL,
    metric     TEXT NOT NULL,
    last_ts_ms INTEGER NOT NULL,
    last_seq   INTEGER NOT NULL,
    PRIMARY KEY (device_id, metric)
);

CREATE TABLE IF NOT EXISTS spool (
    seq            INTEGER PRIMARY KEY,
    enqueued_at_ms INTEGER NOT NULL,
    FOREIGN KEY (seq) REFERENCES records(seq)
);

CREATE TABLE IF NOT EXISTS fault_states (
    stream_key TEXT PRIMARY KEY,
    device_id  TEXT NOT NULL,
    metric     TEXT NOT NULL,
    fault_type TEXT NOT NULL,
    state      TEXT NOT NULL,
    cand_state TEXT NOT NULL,
    cand_count INTEGER NOT NULL,
    since_ms   INTEGER NOT NULL,
    detail     TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS fault_events (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id  TEXT NOT NULL,
    metric     TEXT NOT NULL,
    fault_type TEXT NOT NULL,
    state      TEXT NOT NULL,
    ts_ms      INTEGER NOT NULL,
    detail     TEXT NOT NULL DEFAULT ''
);
"""


class StoreError(Exception):
    def __init__(self, reason: RejectReason, detail: str = ""):
        super().__init__(f"{reason.value}: {detail}")
        self.reason = reason
        self.detail = detail


class DuplicateReport(StoreError):
    def __init__(self, key: str):
        super().__init__(RejectReason.DUPLICATE_REPORT, f"idempotency key={key}")


class OutOfOrder(StoreError):
    def __init__(self, last_ts_ms: int, ts_ms: int):
        super().__init__(RejectReason.TIMESTAMP_OUT_OF_ORDER,
                         f"ts_ms={ts_ms} < watermark={last_ts_ms}")


class BufferFull(StoreError):
    def __init__(self, capacity: int):
        super().__init__(RejectReason.BUFFER_FULL,
                         f"spool capacity={capacity} reached")


class StoreClosed(StoreError):
    def __init__(self):
        super().__init__(RejectReason.STORE_CLOSED, "store already closed")


class LocalStore:
    """单文件 SQLite 存储，进程重启后状态可恢复。"""

    def __init__(self, db_path: str = ":memory:"):
        if db_path != ":memory:":
            parent = os.path.dirname(os.path.abspath(db_path))
            os.makedirs(parent, exist_ok=True)
        self._db_path = db_path
        self._lock = threading.RLock()
        self._closed = False
        self._conn = sqlite3.connect(
            db_path, check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        with self._lock:
            self._conn.executescript(_SCHEMA)
        logger.info("store opened path=%s", db_path)

    # ------------------------------------------------------------------
    def _check_open(self) -> None:
        if self._closed:
            raise StoreClosed()

    # ------------------------------------------------------------------
    # 接收路径：幂等 -> 乱序水位 -> 容量背压 -> 落库 + 入队（单事务）
    # ------------------------------------------------------------------
    def accept_record(self, record: TelemetryRecord,
                      buffer_capacity: int) -> int:
        """返回全局递增 seq；失败抛 DuplicateReport / OutOfOrder / BufferFull。"""
        self._check_open()
        with self._lock:
            conn = self._conn
            conn.execute("BEGIN IMMEDIATE")
            try:
                # 1) 容量背压：写满明确拒绝（在插入前判定，回滚成本最小）
                pending = conn.execute(
                    "SELECT COUNT(*) AS c FROM spool").fetchone()["c"]
                if pending >= buffer_capacity:
                    conn.execute("ROLLBACK")
                    logger.info("reject key=%s reason=buffer_full pending=%d cap=%d",
                                record.raw_idempotency_key, pending,
                                buffer_capacity)
                    raise BufferFull(buffer_capacity)

                # 2) 乱序水位判定（同键并发由下面 UNIQUE 兜底）
                row = conn.execute(
                    "SELECT last_ts_ms FROM watermarks "
                    "WHERE device_id=? AND metric=?",
                    (record.device_id, record.metric)).fetchone()
                if row is not None and record.ts_ms < row["last_ts_ms"]:
                    conn.execute("ROLLBACK")
                    logger.info(
                        "reject key=%s reason=out_of_order ts=%d watermark=%d",
                        record.raw_idempotency_key, record.ts_ms,
                        row["last_ts_ms"])
                    raise OutOfOrder(row["last_ts_ms"], record.ts_ms)

                # 3) 插入记录（UNIQUE 兜底并发重复）
                try:
                    cur = conn.execute(
                        "INSERT INTO records(device_id, metric, value, unit, "
                        "ts_ms, source, idem_key, received_ts_ms, in_range) "
                        "VALUES (?,?,?,?,?,?,?,?,?)",
                        (record.device_id, record.metric, record.value,
                         record.unit, record.ts_ms, record.source.value,
                         record.raw_idempotency_key, record.received_ts_ms,
                         1 if record.in_range else 0))
                except sqlite3.IntegrityError as exc:
                    conn.execute("ROLLBACK")
                    if "UNIQUE" in str(exc).upper():
                        logger.info("reject key=%s reason=duplicate_report",
                                    record.raw_idempotency_key)
                        raise DuplicateReport(record.raw_idempotency_key) from exc
                    raise
                seq = cur.lastrowid

                # 4) 推进水位
                conn.execute(
                    "INSERT INTO watermarks(device_id, metric, last_ts_ms, "
                    "last_seq) VALUES(?,?,?,?) "
                    "ON CONFLICT(device_id, metric) DO UPDATE SET "
                    "last_ts_ms=excluded.last_ts_ms, last_seq=excluded.last_seq",
                    (record.device_id, record.metric, record.ts_ms, seq))

                # 5) 入待补传队列
                conn.execute(
                    "INSERT INTO spool(seq, enqueued_at_ms) VALUES(?,?)",
                    (seq, record.received_ts_ms))
                conn.execute("COMMIT")
            except Exception:
                # 任何异常路径都保证事务结束
                if conn.in_transaction:
                    conn.execute("ROLLBACK")
                raise
        logger.info("accepted seq=%d key=%s pending=%d",
                    seq, record.raw_idempotency_key, pending + 1)
        return seq

    # ------------------------------------------------------------------
    # 补传队列
    # ------------------------------------------------------------------
    def spool_peek_batch(self, max_count: int) -> list[int]:
        self._check_open()
        with self._lock:
            rows = self._conn.execute(
                "SELECT seq FROM spool ORDER BY seq ASC LIMIT ?",
                (max_count,)).fetchall()
        return [r["seq"] for r in rows]

    def spool_ack_batch(self, seqs: Iterable[int]) -> None:
        self._check_open()
        seqs = list(seqs)
        if not seqs:
            return
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._conn.executemany(
                    "UPDATE records SET delivered=1 WHERE seq=?",
                    [(s,) for s in seqs])
                self._conn.executemany(
                    "DELETE FROM spool WHERE seq=?", [(s,) for s in seqs])
                self._conn.execute("COMMIT")
            except Exception:
                if self._conn.in_transaction:
                    self._conn.execute("ROLLBACK")
                raise
        logger.info("acked seqs=%s", seqs)

    def spool_count(self) -> int:
        self._check_open()
        with self._lock:
            return self._conn.execute(
                "SELECT COUNT(*) AS c FROM spool").fetchone()["c"]

    def spool_all_seqs(self) -> list[int]:
        self._check_open()
        with self._lock:
            rows = self._conn.execute(
                "SELECT seq FROM spool ORDER BY seq ASC").fetchall()
        return [r["seq"] for r in rows]

    # ------------------------------------------------------------------
    # 记录查询
    # ------------------------------------------------------------------
    def get_record(self, seq: int) -> TelemetryRecord | None:
        self._check_open()
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM records WHERE seq=?", (seq,)).fetchone()
        return _row_to_record(row) if row else None

    def delivered_records(self) -> list[TelemetryRecord]:
        self._check_open()
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM records WHERE delivered=1 ORDER BY seq ASC"
            ).fetchall()
        return [_row_to_record(r) for r in rows]

    def all_records(self) -> list[TelemetryRecord]:
        self._check_open()
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM records ORDER BY seq ASC").fetchall()
        return [_row_to_record(r) for r in rows]

    def latest_per_device(self) -> dict[str, int]:
        """设备 -> 最近一条记录的采集时间 ts_ms（失联判定用）。"""
        self._check_open()
        with self._lock:
            rows = self._conn.execute(
                "SELECT device_id, MAX(ts_ms) AS t FROM records GROUP BY device_id"
            ).fetchall()
        return {r["device_id"]: r["t"] for r in rows}

    def last_two(self, device_id: str, metric: str):
        """返回 (最新, 次新) 两条 (value, ts_ms)，不足则元素为 None。"""
        self._check_open()
        with self._lock:
            rows = self._conn.execute(
                "SELECT value, ts_ms FROM records "
                "WHERE device_id=? AND metric=? ORDER BY ts_ms DESC LIMIT 2",
                (device_id, metric)).fetchall()
        latest = (rows[0]["value"], rows[0]["ts_ms"]) if len(rows) >= 1 else None
        prev = (rows[1]["value"], rows[1]["ts_ms"]) if len(rows) >= 2 else None
        return latest, prev

    def known_streams(self) -> list[tuple[str, str]]:
        self._check_open()
        with self._lock:
            rows = self._conn.execute(
                "SELECT DISTINCT device_id, metric FROM records"
            ).fetchall()
        return [(r["device_id"], r["metric"]) for r in rows]

    # ------------------------------------------------------------------
    # 故障状态 / 事件
    # ------------------------------------------------------------------
    def fault_get(self, key: str):
        self._check_open()
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM fault_states WHERE stream_key=?",
                (key,)).fetchone()

    def fault_put(self, key: str, device_id: str, metric: str, fault_type: str,
                  state: str, cand_state: str, cand_count: int, since_ms: int,
                  detail: str = "") -> None:
        self._check_open()
        with self._lock:
            self._conn.execute(
                "INSERT INTO fault_states(stream_key, device_id, metric, "
                "fault_type, state, cand_state, cand_count, since_ms, detail) "
                "VALUES(?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(stream_key) DO UPDATE SET state=excluded.state, "
                "cand_state=excluded.cand_state, cand_count=excluded.cand_count, "
                "since_ms=excluded.since_ms, detail=excluded.detail",
                (key, device_id, metric, fault_type, state, cand_state,
                 cand_count, since_ms, detail))

    def fault_all(self) -> list[sqlite3.Row]:
        self._check_open()
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM fault_states ORDER BY stream_key").fetchall()

    def add_event(self, device_id: str, metric: str, fault_type: str,
                  state: str, ts_ms: int, detail: str = "") -> None:
        self._check_open()
        with self._lock:
            self._conn.execute(
                "INSERT INTO fault_events(device_id, metric, fault_type, "
                "state, ts_ms, detail) VALUES(?,?,?,?,?,?)",
                (device_id, metric, fault_type, state, ts_ms, detail))
        logger.info("fault_event dev=%s metric=%s type=%s state=%s ts=%d %s",
                    device_id, metric, fault_type, state, ts_ms, detail)

    def list_events(self) -> list[dict]:
        self._check_open()
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM fault_events ORDER BY id ASC").fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    def close(self) -> None:
        with self._lock:
            if not self._closed:
                self._conn.close()
                self._closed = True
                logger.info("store closed path=%s", self._db_path)

    def __enter__(self):
        return self

    def __exit__(self, *exc) -> None:
        self.close()


def _row_to_record(row: sqlite3.Row) -> TelemetryRecord:
    return TelemetryRecord(
        device_id=row["device_id"],
        metric=row["metric"],
        value=row["value"],
        unit=row["unit"],
        ts_ms=row["ts_ms"],
        source=SourceProtocol(row["source"]),
        raw_idempotency_key=row["idem_key"],
        received_ts_ms=row["received_ts_ms"],
        in_range=bool(row["in_range"]),
    )
