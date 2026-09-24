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

Determinism: results depend only on the sequence of observe()/tick() calls
and their timestamps -- the same input batch always yields the same events.
"""
from __future__ import annotations

from dataclasses import dataclass, field

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


class HealthMonitor:
    def __init__(
        self,
        offline_after_ms: int = 60_000,
        stall_after_ms: int = 30_000,
        debounce_count: int = 3,
    ) -> None:
        if debounce_count < 1:
            raise ValueError("debounce_count must be >= 1")
        self._offline_after_ms = offline_after_ms
        self._stall_after_ms = stall_after_ms
        self._debounce = debounce_count
        self._devices: dict[str, _DeviceState] = {}

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
            return FaultEvent(device_id, fault, True, detail, metric)
        if st.active and st.good_streak >= self._debounce:
            st.active = False
            return FaultEvent(device_id, fault, False, detail, metric)
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
            FaultType.DEVICE_OFFLINE, False, "device reported",
        )
        if ev:
            events.append(ev)
        ev = self._update_fault(
            dev, (FaultType.DATA_STALL, record.metric), record.device_id,
            FaultType.DATA_STALL, False, "metric advanced", record.metric,
        )
        if ev:
            events.append(ev)

        lo, hi = VALUE_RANGES[record.metric]
        in_range = lo <= record.value <= hi
        ev = self._update_fault(
            dev, (FaultType.VALUE_OUT_OF_RANGE, record.metric), record.device_id,
            FaultType.VALUE_OUT_OF_RANGE, not in_range,
            f"value {record.value} {record.unit} vs range [{lo}, {hi}]",
            record.metric,
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
                    metric,
                )
                if ev:
                    events.append(ev)
        return events
