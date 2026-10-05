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

Persistence: when constructed with a ``HealthStateStore``, the full monitor
state (last-seen timestamps, per-metric progress, debounce streaks, active
flags) is committed to SQLite on every mutation and reloaded on startup.
A restart therefore neither re-raises already-active faults nor resets
half-built debounce streaks. Every activation/recovery transition is also
recorded as a fault episode (device, fault, metric, start, recovery,
duration) queryable via ``HealthStateStore.episodes()``.

Determinism: results depend only on the sequence of observe()/tick() calls
and their timestamps -- the same input batch always yields the same events
and the same episodes, with or without a restart in between.
"""
from __future__ import annotations

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
CREATE TABLE IF NOT EXISTS health_device (
    device_id TEXT PRIMARY KEY,
    last_seen_ms INTEGER
);
CREATE TABLE IF NOT EXISTS health_metric (
    device_id TEXT NOT NULL,
    metric TEXT NOT NULL,
    last_ts_ms INTEGER NOT NULL,
    PRIMARY KEY (device_id, metric)
);
CREATE TABLE IF NOT EXISTS health_fault (
    device_id TEXT NOT NULL,
    fault TEXT NOT NULL,
    metric TEXT NOT NULL DEFAULT '',
    active INTEGER NOT NULL DEFAULT 0,
    bad_streak INTEGER NOT NULL DEFAULT 0,
    good_streak INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (device_id, fault, metric)
);
CREATE TABLE IF NOT EXISTS fault_episode (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id TEXT NOT NULL,
    fault TEXT NOT NULL,
    metric TEXT NOT NULL DEFAULT '',
    started_ms INTEGER NOT NULL,
    recovered_ms INTEGER,
    duration_ms INTEGER
);
"""


def _fault_key_parts(key: tuple) -> tuple[str, str]:
    """Serialize a fault key tuple to (fault_value, metric_or_empty)."""
    fault: FaultType = key[0]
    metric = key[1] if len(key) > 1 else ""
    return fault.value, metric or ""


class HealthStateStore:
    """SQLite-backed persistence for HealthMonitor state and fault episodes."""

    def __init__(self, db_path: str | Path) -> None:
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    # -- monitor state -------------------------------------------------------

    def load_devices(self) -> dict[str, _DeviceState]:
        with self._lock:
            devs: dict[str, _DeviceState] = {}
            for r in self._conn.execute("SELECT * FROM health_device").fetchall():
                devs[r["device_id"]] = _DeviceState(last_seen_ms=r["last_seen_ms"])
            for r in self._conn.execute("SELECT * FROM health_metric").fetchall():
                dev = devs.setdefault(r["device_id"], _DeviceState())
                dev.metric_last_ts[r["metric"]] = int(r["last_ts_ms"])
            for r in self._conn.execute("SELECT * FROM health_fault").fetchall():
                dev = devs.setdefault(r["device_id"], _DeviceState())
                fault = FaultType(r["fault"])
                key = (fault,) if not r["metric"] else (fault, r["metric"])
                dev.faults[key] = _FaultState(
                    active=bool(r["active"]),
                    bad_streak=int(r["bad_streak"]),
                    good_streak=int(r["good_streak"]),
                )
            return devs

    def save_device_meta(self, device_id: str, dev: _DeviceState) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO health_device (device_id, last_seen_ms) VALUES (?, ?)"
                " ON CONFLICT(device_id) DO UPDATE SET last_seen_ms = excluded.last_seen_ms",
                (device_id, dev.last_seen_ms),
            )
            self._conn.executemany(
                "INSERT INTO health_metric (device_id, metric, last_ts_ms) VALUES (?, ?, ?)"
                " ON CONFLICT(device_id, metric) DO UPDATE SET last_ts_ms = excluded.last_ts_ms",
                [(device_id, m, ts) for m, ts in dev.metric_last_ts.items()],
            )
            self._conn.commit()

    def save_fault(self, device_id: str, key: tuple, st: _FaultState) -> None:
        fault, metric = _fault_key_parts(key)
        with self._lock:
            self._conn.execute(
                "INSERT INTO health_fault (device_id, fault, metric, active, bad_streak, good_streak)"
                " VALUES (?, ?, ?, ?, ?, ?)"
                " ON CONFLICT(device_id, fault, metric) DO UPDATE SET"
                " active = excluded.active, bad_streak = excluded.bad_streak,"
                " good_streak = excluded.good_streak",
                (device_id, fault, metric, int(st.active), st.bad_streak, st.good_streak),
            )
            self._conn.commit()

    # -- fault episodes --------------------------------------------------------

    def open_episode(self, device_id: str, fault: FaultType, metric: str | None, started_ms: int) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO fault_episode (device_id, fault, metric, started_ms)"
                " VALUES (?, ?, ?, ?)",
                (device_id, fault.value, metric or "", started_ms),
            )
            self._conn.commit()

    def close_episode(self, device_id: str, fault: FaultType, metric: str | None, recovered_ms: int) -> None:
        with self._lock:
            row = self._conn.execute(
                "SELECT id, started_ms FROM fault_episode"
                " WHERE device_id = ? AND fault = ? AND metric = ? AND recovered_ms IS NULL"
                " ORDER BY id DESC LIMIT 1",
                (device_id, fault.value, metric or ""),
            ).fetchone()
            if row is None:
                return
            self._conn.execute(
                "UPDATE fault_episode SET recovered_ms = ?, duration_ms = ? WHERE id = ?",
                (recovered_ms, recovered_ms - int(row["started_ms"]), int(row["id"])),
            )
            self._conn.commit()

    def episodes(self, device_id: str | None = None) -> list[dict]:
        """Fault history: device, fault, metric, start, recovery, duration."""
        sql = (
            "SELECT device_id, fault, metric, started_ms, recovered_ms, duration_ms"
            " FROM fault_episode"
        )
        args: tuple = ()
        if device_id is not None:
            sql += " WHERE device_id = ?"
            args = (device_id,)
        sql += " ORDER BY id ASC"
        with self._lock:
            rows = self._conn.execute(sql, args).fetchall()
        return [
            {
                "device_id": r["device_id"],
                "fault": r["fault"],
                "metric": r["metric"] or None,
                "started_ms": int(r["started_ms"]),
                "recovered_ms": int(r["recovered_ms"]) if r["recovered_ms"] is not None else None,
                "duration_ms": int(r["duration_ms"]) if r["duration_ms"] is not None else None,
            }
            for r in rows
        ]

    def close(self) -> None:
        with self._lock:
            self._conn.close()


class HealthMonitor:
    def __init__(
        self,
        offline_after_ms: int = 60_000,
        stall_after_ms: int = 30_000,
        debounce_count: int = 3,
        store: HealthStateStore | None = None,
    ) -> None:
        if debounce_count < 1:
            raise ValueError("debounce_count must be >= 1")
        self._offline_after_ms = offline_after_ms
        self._stall_after_ms = stall_after_ms
        self._debounce = debounce_count
        self._store = store
        # Reload persisted state so faults and streaks survive restarts.
        self._devices: dict[str, _DeviceState] = store.load_devices() if store else {}

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
        event: FaultEvent | None = None
        if not st.active and st.bad_streak >= self._debounce:
            st.active = True
            event = FaultEvent(device_id, fault, True, detail, metric, at_ms=now_ms)
            if self._store:
                self._store.open_episode(device_id, fault, metric, now_ms)
        elif st.active and st.good_streak >= self._debounce:
            st.active = False
            event = FaultEvent(device_id, fault, False, detail, metric, at_ms=now_ms)
            if self._store:
                self._store.close_episode(device_id, fault, metric, now_ms)
        if self._store:
            self._store.save_fault(device_id, key, st)
        return event

    # -- public API -------------------------------------------------------

    def observe(self, record: TelemetryRecord, now_ms: int) -> list[FaultEvent]:
        """Feed one accepted record; returns fault state transitions (debounced)."""
        dev = self._device(record.device_id)
        dev.last_seen_ms = max(now_ms, dev.last_seen_ms or now_ms)
        prev_ts = dev.metric_last_ts.get(record.metric)
        if prev_ts is None or record.timestamp_ms > prev_ts:
            dev.metric_last_ts[record.metric] = record.timestamp_ms
        if self._store:
            self._store.save_device_meta(record.device_id, dev)

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
        return events

    def active_faults(self) -> dict[str, list[dict]]:
        """Current active faults per device (for the ops status view)."""
        view: dict[str, list[dict]] = {}
        for device_id, dev in sorted(self._devices.items()):
            entries = [
                {
                    "fault": key[0].value,
                    "metric": key[1] if len(key) > 1 else None,
                }
                for key, st in sorted(dev.faults.items(), key=lambda kv: str(kv[0]))
                if st.active
            ]
            if entries:
                view[device_id] = entries
        return view
