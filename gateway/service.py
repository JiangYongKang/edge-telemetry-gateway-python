"""Gateway service: orchestrates normalize -> dedup/buffer -> uplink -> health.

Ingest pipeline per raw record:
1. normalize() maps protocol-specific fields to the canonical model and
   rejects malformed records with a distinguishable RejectReason.
2. Out-of-order device timestamps are flagged on the outcome (detail +
   ``out_of_order`` flag) but still stored -- replay order is collection
   order, not device time.
3. PersistentBuffer.enqueue() persists before returning; duplicates are
   reported as DedupReason, a full buffer as RejectReason.BUFFER_FULL.
4. Stored records feed the HealthMonitor for fault detection.
5. Every non-plain-accepted outcome is counted in the persistent StatsStore
   under a stable ReasonCode, so the ops summary survives restarts.

Auto-resume semantics:
* On startup, if the link is usable, any pending buffer rows are replayed
  immediately in collection order -- no manual trigger needed.
* When the link transitions from "down" to a usable state at runtime, a
  replay is triggered automatically as well.
* Replays are at-least-once with ack-after-confirm; uploaded counters are
  committed with each replay round, so a crash mid-replay followed by a
  restart neither loses data nor double-counts uploads.
"""
from __future__ import annotations

import threading
import time

from gateway.buffer import BufferFullError, PersistentBuffer
from gateway.health import HealthMonitor, HealthStateStore
from gateway.link import SimulatedUplink, UplinkManager
from gateway.models import (
    REJECT_TO_REASON,
    DedupReason,
    FaultEvent,
    IngestOutcome,
    ReasonCode,
    RejectReason,
)
from gateway.normalizer import normalize
from gateway.stats import StatsStore


class GatewayService:
    def __init__(
        self,
        db_path: str,
        buffer_capacity: int = 10_000,
        link: SimulatedUplink | None = None,
        health: HealthMonitor | None = None,
        clock=lambda: int(time.time() * 1000),
    ) -> None:
        self._buffer = PersistentBuffer(db_path, capacity=buffer_capacity)
        self._stats = StatsStore(db_path)
        self._health_store = HealthStateStore(db_path)
        self._link = link or SimulatedUplink()
        self._uplink = UplinkManager(self._buffer, self._link, stats=self._stats)
        self._health = health or HealthMonitor(store=self._health_store)
        self._clock = clock
        self._last_ts: dict[tuple[str, str], int] = {}
        self._lock = threading.Lock()
        # Auto-resume: replay whatever was pending when we (re)started.
        self._auto_flush()

    # -- internal ------------------------------------------------------------

    def _auto_flush(self) -> int:
        """Replay pending rows if the link is usable (anything but 'down')."""
        if self._link.state == "down":
            return 0
        return self._uplink.flush_all(now_ms=self._clock())

    def _record_outcome_stats(self, outcome: IngestOutcome) -> None:
        if outcome.status == "rejected" and outcome.reject_reason is not None:
            self._stats.record_reject(REJECT_TO_REASON[outcome.reject_reason])
        elif outcome.status == "duplicate":
            self._stats.record_dedup(ReasonCode.DUPLICATE)
        if outcome.out_of_order:
            self._stats.record_flag(ReasonCode.OUT_OF_ORDER_TIMESTAMP)

    # -- ingest ----------------------------------------------------------------

    def ingest(self, payload: dict, received_at_ms: int) -> list[IngestOutcome]:
        outcomes = normalize(payload, received_at_ms)
        for outcome in outcomes:
            if outcome.status != "accepted" or outcome.record is None:
                self._record_outcome_stats(outcome)
                continue
            record = outcome.record
            # Out-of-order detection (flagged, not dropped).
            key = (record.device_id, record.metric)
            last = self._last_ts.get(key)
            if last is not None and record.timestamp_ms < last:
                outcome.out_of_order = True
                outcome.detail = (
                    f"out_of_order_timestamp: ts={record.timestamp_ms} < last={last}"
                )
            else:
                self._last_ts[key] = record.timestamp_ms
            # Durable store with idempotent dedup and backpressure.
            try:
                result = self._buffer.enqueue(record)
            except BufferFullError:
                outcome.status = "rejected"
                outcome.reject_reason = RejectReason.BUFFER_FULL
                outcome.detail = "local buffer at capacity; record refused, not dropped"
                self._record_outcome_stats(outcome)
                continue
            if result == "stored":
                self._health.observe(record, received_at_ms)
            else:
                outcome.status = "duplicate"
                outcome.dedup_reason = (
                    DedupReason.EXACT_DUPLICATE
                    if result == "exact_duplicate"
                    else DedupReason.CONFLICTING_DUPLICATE
                )
                outcome.detail = f"dedup key {record.dedup_key} already stored"
            self._record_outcome_stats(outcome)
        return outcomes

    # -- uplink ------------------------------------------------------------------

    def flush(self) -> int:
        """Attempt to upload one batch of pending records in collection order."""
        return self._uplink.flush_once()

    def flush_all(self, max_rounds: int = 10_000) -> int:
        """Flush until the buffer is drained or the link stops accepting."""
        return self._uplink.flush_all(max_rounds=max_rounds, now_ms=self._clock())

    def set_link_state(self, state: str) -> None:
        self._link.set_state(state)
        # Link recovered -> start replaying automatically, no manual trigger.
        self._auto_flush()

    @property
    def link(self) -> SimulatedUplink:
        return self._link

    @property
    def buffer(self) -> PersistentBuffer:
        return self._buffer

    # -- health --------------------------------------------------------------------

    def health_tick(self, device_ids: list[str], now_ms: int) -> list[FaultEvent]:
        return self._health.tick(device_ids, now_ms)

    def fault_history(self, device_id: str | None = None) -> list[dict]:
        """Fault episodes: device, fault, metric, start, recovery, duration."""
        return self._health_store.episodes(device_id)

    # -- ops status ------------------------------------------------------------------

    def status(self) -> dict:
        """Snapshot for operations: backlog, upload books, link, faults, reasons."""
        return {
            "link": self._link.state,
            "pending": self._buffer.pending_count(),
            "uploaded_total": self._stats.uploaded_total(),
            "last_flush": self._stats.last_flush(),
            "active_faults": self._health.active_faults(),
            "reasons": self._stats.reason_summary(),
        }

    def close(self) -> None:
        self._buffer.close()
        self._stats.close()
        self._health_store.close()
