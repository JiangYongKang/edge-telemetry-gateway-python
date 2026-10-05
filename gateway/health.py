"""Fault detection: offline / stall / out-of-range with debounce (hysteresis).

Faults:
* DEVICE_OFFLINE      -- no record from the device for ``offline_after_ms``.
* DATA_STALL          -- the device still reports, but a metric's sample
                         timestamp has not advanced for ``stall_after_ms``.
* VALUE_OUT_OF_RANGE  -- a value falls outside the physical plausible range
                         (canonical units, see models.VALUE_RANGES).

Debounce: each fault key keeps a bad/good streak. A fault only turns active
after ``debounce_count`` consecutive detections, and only clears after
``debounce_count`` consecutive clear observations. Short jitter therefore
produces neither false alarms nor duplicate notifications: FaultEvent is
emitted exclusively on state transitions.

Persistence (optional, ``state_db``): the full monitor state -- per-device
last-seen timestamps, per-metric sample timestamps, and every fault key's
active flag plus bad/good streaks -- is written to SQLite after each
observe()/tick() call and restored on construction. A restart therefore
continues exactly where the previous process left off: active faults are
not re-alarmed, and half-built debounce streaks are not reset.

Fault episodes: every activation opens an episode row (device, fault,
metric, start_ms); the matching recovery closes it (end_ms, duration_ms).
Episodes are queryable via episodes() for the full lifecycle history.

Determinism: results depend only on the sequence of observe()/tick() calls
and their timestamps -- the same input batch always yields the same events,
with or without a restart in the middle.
"""
from __future__ import annotations

import json
import sqlite3
import threading
from dataclasses import dataclass, field
from pathlib import Path

from gateway.models import VALUE_RANGES, FaultEvent, FaultType, TelemetryRecord


@dataclass
class _FaultState:
    active: bool = False
    bad_streak: int = 0
    good_streak: int = 0


@dataclass
class _DeviceState:
    last_seen_ms: int | None = None
    metric_last_ts: dict[str, int] = field(default_factory=dict)
    faults: dict[tuple, _FaultState] = field(default_factory=dict)


_SCHEMA = """
CREATE TABLE IF NOT EXISTS health_devices (
    device_id TEXT PRIMARY KEY,
    last_seen_ms INTEGER,
    metric_last_ts_json TEXT NOT NULL DEFAULT '{}'
);
CREATE TABLE IF NOT EXISTS health_faults (
    device_id TEXT NOT NULL,
    fault_type TEXT NOT NULL,
    metric TEXT NOT NULL DEFAULT '',
    active INTEGER NOT NULL,
    bad_streak INTEGER NOT NULL,
    good_streak INTEGER NOT NULL,
    PRIMARY KEY (device_id, fault_type, metric)
);
CREATE TABLE IF NOT EXISTS fault_episodes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id TEXT NOT NULL,
    fault_type TEXT NOT NULL,
    metric TEXT,
    start_ms INTEGER NOT NULL,
    end_ms INTEGER,
    duration_ms INTEGER
);
"""


class HealthMonitor:
    def __init__(
        self,
        offline_after_ms: int = 60_000,
        stall_after_ms: int = 30_000,
        debounce_count: int = 3,
        state_db: str | Path | None = None,
    ) -> None:
        if debounce_count < 1:
            raise ValueError("debounce_count must be >= 1")
        self._offline_after_ms = offline_after_ms
        self._stall_after_ms = stall_after_ms
        self._debounce = debounce_count
        self._devices: dict[str, _DeviceState] = {}
        self._episodes_mem: list[dict] = []  # used when state_db is None
        self._lock = threading.Lock()
        self._conn: sqlite3.Connection | None = None
        if state_db is not None:
            self._conn = sqlite3.connect(str(state_db), check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.executescript(_SCHEMA)
            self._conn.commit()
            self._load()

    # -- persistence ------------------------------------------------------

    def _load(self) -> None:
        assert self._conn is not None
        for row in self._conn.execute("SELECT * FROM health_devices").fetchall():
            dev = self._devices.setdefault(row["device_id"], _DeviceState())
            dev.last_seen_ms = row["last_seen_ms"]
            dev.metric_last_ts = {k: int(v) for k, v in json.loads(row["metric_last_ts_json"]).items()}
        for row in self._conn.execute("SELECT * FROM health_faults").fetchall():
            dev = self._devices.setdefault(row["device_id"], _DeviceState())
            fault = FaultType(row["fault_type"])
            key = (fault,) if row["metric"] == "" else (fault, row["metric"])
            dev.faults[key] = _FaultState(
                active=bool(row["active"]),
                bad_streak=int(row["bad_streak"]),
                good_streak=int(row["good_streak"]),
            )

    def _persist(self) -> None:
        """Snapshot the full state in one transaction (called after each mutation)."""
        if self._conn is None:
            return
        with self._lock:
            for device_id, dev in self._devices.items():
                self._conn.execute(
                    "INSERT INTO health_devices (device_id, last_seen_ms, metric_last_ts_json)"
                    " VALUES (?, ?, ?)"
                    " ON CONFLICT(device_id) DO UPDATE SET last_seen_ms=excluded.last_seen_ms,"
                    " metric_last_ts_json=excluded.metric_last_ts_json",
                    (device_id, dev.last_seen_ms, json.dumps(dev.metric_last_ts)),
                )
                for (fault, *metric_parts), st in dev.faults.items():
                    metric = metric_parts[0] if metric_parts else ""
                    self._conn.execute(
                        "INSERT INTO health_faults"
                        " (device_id, fault_type, metric, active, bad_streak, good_streak)"
                        " VALUES (?, ?, ?, ?, ?, ?)"
                        " ON CONFLICT(device_id, fault_type, metric) DO UPDATE SET"
                        " active=excluded.active, bad_streak=excluded.bad_streak,"
                        " good_streak=excluded.good_streak",
                        (device_id, fault.value, metric,
                         int(st.active), st.bad_streak, st.good_streak),
                    )
            self._conn.commit()

    def _episode_open(self, device_id: str, fault: FaultType, metric: str | None, start_ms: int) -> None:
        episode = {
            "device_id": device_id,
            "fault": fault.value,
            "metric": metric,
            "start_ms": start_ms,
            "end_ms": None,
            "duration_ms": None,
        }
        if self._conn is None:
            self._episodes_mem.append(episode)
            return
        with self._lock:
            self._conn.execute(
                "INSERT INTO fault_episodes (device_id, fault_type, metric, start_ms)"
                " VALUES (?, ?, ?, ?)",
                (device_id, fault.value, metric, start_ms),
            )
            self._conn.commit()

    def _episode_close(self, device_id: str, fault: FaultType, metric: str | None, end_ms: int) -> None:
        if self._conn is None:
            for ep in reversed(self._episodes_mem):
                if (ep["device_id"] == device_id and ep["fault"] == fault.value
                        and ep["metric"] == metric and ep["end_ms"] is None):
                    ep["end_ms"] = end_ms
                    ep["duration_ms"] = end_ms - ep["start_ms"]
                    return
            return
        with self._lock:
            if metric is None:
                row = self._conn.execute(
                    "SELECT id, start_ms FROM fault_episodes"
                    " WHERE device_id = ? AND fault_type = ? AND metric IS NULL"
                    " AND end_ms IS NULL ORDER BY id DESC LIMIT 1",
                    (device_id, fault.value),
                ).fetchone()
            else:
                row = self._conn.execute(
                    "SELECT id, start_ms FROM fault_episodes"
                    " WHERE device_id = ? AND fault_type = ? AND metric = ?"
                    " AND end_ms IS NULL ORDER BY id DESC LIMIT 1",
                    (device_id, fault.value, metric),
                ).fetchone()
            if row is not None:
                self._conn.execute(
                    "UPDATE fault_episodes SET end_ms = ?, duration_ms = ? WHERE id = ?",
                    (end_ms, end_ms - int(row["start_ms"]), int(row["id"])),
                )
                self._conn.commit()

    # -- internal ---------------------------------------------------------

    def _device(self, device_id: str) -> _DeviceState:
        return self._devices.setdefault(device_id, _DeviceState())

    def _update_fault(
        self,
        dev: _DeviceState,
        key: tuple,
        device_id: str,
        fault: FaultType,
        is_bad: bool,
        detail: str,
        now_ms: int,
        metric: str | None = None,
    ) -> FaultEvent | None:
        st = dev.faults.setdefault(key, _FaultState())
        if is_bad:
            st.bad_streak += 1
            st.good_streak = 0
        else:
            st.good_streak += 1
            st.bad_streak = 0
        if not st.active and st.bad_streak >= self._debounce:
            st.active = True
            self._episode_open(device_id, fault, metric, now_ms)
            return FaultEvent(device_id, fault, True, detail, metric, at_ms=now_ms)
        if st.active and st.good_streak >= self._debounce:
            st.active = False
            self._episode_close(device_id, fault, metric, now_ms)
            return FaultEvent(device_id, fault, False, detail, metric, at_ms=now_ms)
        return None

    # -- public API -------------------------------------------------------

    def observe(self, record: TelemetryRecord, now_ms: int) -> list[FaultEvent]:
        """Feed one accepted record; returns fault state transitions (debounced)."""
        dev = self._device(record.device_id)
        dev.last_seen_ms = max(now_ms, dev.last_seen_ms or now_ms)
        prev_ts = dev.metric_last_ts.get(record.metric)
        if prev_ts is None or record.timestamp_ms > prev_ts:
            dev.metric_last_ts[record.metric] = record.timestamp_ms

        events: list[FaultEvent] = []

        # Any fresh record is evidence against OFFLINE and (for this metric) STALL.
        ev = self._update_fault(
            dev, (FaultType.DEVICE_OFFLINE,), record.device_id,
            FaultType.DEVICE_OFFLINE, False, "device reported", now_ms,
        )
        if ev:
            events.append(ev)
        ev = self._update_fault(
            dev, (FaultType.DATA_STALL, record.metric), record.device_id,
            FaultType.DATA_STALL, False, "metric advanced", now_ms, record.metric,
        )
        if ev:
            events.append(ev)

        lo, hi = VALUE_RANGES[record.metric]
        in_range = lo <= record.value <= hi
        ev = self._update_fault(
            dev, (FaultType.VALUE_OUT_OF_RANGE, record.metric), record.device_id,
            FaultType.VALUE_OUT_OF_RANGE, not in_range,
            f"value {record.value} {record.unit} vs range [{lo}, {hi}]",
            now_ms, record.metric,
        )
        if ev:
            events.append(ev)
        self._persist()
        return events

    def tick(self, device_ids: list[str], now_ms: int) -> list[FaultEvent]:
        """Periodic check for silence-based faults (offline / stall)."""
        events: list[FaultEvent] = []
        for device_id in device_ids:
            dev = self._device(device_id)
            last_seen = dev.last_seen_ms
            offline = last_seen is None or (now_ms - last_seen) > self._offline_after_ms
            ev = self._update_fault(
                dev, (FaultType.DEVICE_OFFLINE,), device_id, FaultType.DEVICE_OFFLINE,
                offline,
                f"last_seen={last_seen} now={now_ms} threshold={self._offline_after_ms}ms",
                now_ms,
            )
            if ev:
                events.append(ev)
            if offline:
                continue  # offline dominates; stall is meaningless while offline
            for metric, last_ts in sorted(dev.metric_last_ts.items()):
                stalled = (now_ms - last_ts) > self._stall_after_ms
                ev = self._update_fault(
                    dev, (FaultType.DATA_STALL, metric), device_id, FaultType.DATA_STALL,
                    stalled,
                    f"metric last_ts={last_ts} now={now_ms} threshold={self._stall_after_ms}ms",
                    now_ms, metric,
                )
                if ev:
                    events.append(ev)
        self._persist()
        return events

    # -- ops queries ------------------------------------------------------

    def episodes(self) -> list[dict]:
        """Full fault lifecycle history: device, fault, metric, start, end, duration."""
        if self._conn is None:
            return [dict(ep) for ep in self._episodes_mem]
        with self._lock:
            rows = self._conn.execute(
                "SELECT device_id, fault_type, metric, start_ms, end_ms, duration_ms"
                " FROM fault_episodes ORDER BY id"
            ).fetchall()
        return [
            {
                "device_id": r["device_id"],
                "fault": r["fault_type"],
                "metric": r["metric"],
                "start_ms": int(r["start_ms"]),
                "end_ms": int(r["end_ms"]) if r["end_ms"] is not None else None,
                "duration_ms": int(r["duration_ms"]) if r["duration_ms"] is not None else None,
            }
            for r in rows
        ]

    def fault_status(self) -> list[dict]:
        """Current per-device fault snapshot (active flags + debounce progress)."""
        open_episodes: dict[tuple, int] = {}
        for ep in self.episodes():
            if ep["end_ms"] is None:
                open_episodes[(ep["device_id"], ep["fault"], ep["metric"])] = ep["start_ms"]
        out: list[dict] = []
        for device_id, dev in sorted(self._devices.items()):
            faults = []
            for (fault, *metric_parts), st in sorted(
                dev.faults.items(), key=lambda kv: (kv[0][0].value, kv[0][1] if len(kv[0]) > 1 else "")
            ):
                metric = metric_parts[0] if metric_parts else None
                faults.append({
                    "fault": fault.value,
                    "metric": metric,
                    "active": st.active,
                    "bad_streak": st.bad_streak,
                    "good_streak": st.good_streak,
                    "started_ms": open_episodes.get((device_id, fault.value, metric)),
                })
            out.append({
                "device_id": device_id,
                "last_seen_ms": dev.last_seen_ms,
                "faults": faults,
            })
        return out

    def close(self) -> None:
        if self._conn is not None:
            with self._lock:
                self._conn.close()
            self._conn = None
