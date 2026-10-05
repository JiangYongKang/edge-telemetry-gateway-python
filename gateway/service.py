"""Gateway service: orchestrates normalize -> dedup/buffer -> uplink -> health.

Ingest pipeline per raw record:
1. normalize() maps protocol-specific fields to the canonical model and
   rejects malformed records with a distinguishable RejectReason.
2. Out-of-order device timestamps are flagged on the outcome (detail) but
   still stored -- replay order is collection order, not device time.
3. PersistentBuffer.enqueue() persists before returning; duplicates are
   reported as DedupReason, a full buffer as RejectReason.BUFFER_FULL.
4. Stored records feed the HealthMonitor for fault detection.
"""
from __future__ import annotations

import threading

from gateway.buffer import BufferFullError, PersistentBuffer
from gateway.health import HealthMonitor
from gateway.link import SimulatedUplink, UplinkManager
from gateway.models import DedupReason, FaultEvent, IngestOutcome, ReasonCode, RejectReason
from gateway.normalizer import normalize
from gateway.stats import StatsStore


class GatewayService:
    def __init__(
        self,
        db_path: str,
        buffer_capacity: int = 10_000,
        link: SimulatedUplink | None = None,
        health: HealthMonitor | None = None,
    ) -> None:
        self._buffer = PersistentBuffer(db_path, capacity=buffer_capacity)
        self._link = link or SimulatedUplink()
        self._uplink = UplinkManager(self._buffer, self._link)
        # Health state persists across restarts in the same db file unless a
        # custom (e.g. in-memory) monitor is injected.
        self._health = health or HealthMonitor(state_db=db_path)
        self._stats = StatsStore(db_path)
        self._last_ts: dict[tuple[str, str], int] = {}
        self._lock = threading.Lock()
        # Auto-resume: if the link is already usable at startup, any pending
        # backlog from before the restart replays immediately, in order.
        self._auto_resume(trigger="startup")

    def ingest(self, payload: dict, received_at_ms: int) -> list[IngestOutcome]:
        outcomes = normalize(payload, received_at_ms)
        for outcome in outcomes:
            if outcome.status != "accepted" or outcome.record is None:
                continue
            record = outcome.record
            # Out-of-order detection (flagged, not dropped).
            key = (record.device_id, record.metric)
            last = self._last_ts.get(key)
            if last is not None and record.timestamp_ms < last:
                outcome.detail = (
                    f"out_of_order_timestamp: ts={record.timestamp_ms} < last={last}"
                )
                outcome.flagged_code = ReasonCode.OUT_OF_ORDER_TIMESTAMP
            else:
                self._last_ts[key] = record.timestamp_ms
            # Durable store with idempotent dedup and backpressure.
            try:
                result = self._buffer.enqueue(record)
            except BufferFullError:
                outcome.status = "rejected"
                outcome.reject_reason = RejectReason.BUFFER_FULL
                outcome.detail = "local buffer at capacity; record refused, not dropped"
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
        # Ops statistics: count every distinguishable non-clean outcome.
        for outcome in outcomes:
            code = outcome.reason_code
            if code is not None:
                self._stats.incr_reason(code)
        return outcomes

    def _flush_round(self) -> int:
        acked = self._uplink.flush_once()
        self._stats.add_uploaded(acked)
        return acked

    def flush(self, trigger: str = "manual", at_ms: int = 0) -> int:
        """Attempt to upload one batch of pending records in collection order."""
        pending_before = self._buffer.pending_count()
        acked = self._flush_round()
        self._stats.record_replay(
            at_ms=at_ms,
            trigger=trigger,
            attempted=min(pending_before, self._uplink.batch_size),
            acked=acked,
            remaining=self._buffer.pending_count(),
        )
        return acked

    def flush_all(self, max_rounds: int = 10_000, trigger: str = "manual", at_ms: int = 0) -> int:
        """Flush until the buffer is drained or the link stops accepting.

        The whole drain is recorded as ONE replay entry so ops see the
        outcome of the latest resume/replay, not its final empty round.
        """
        pending_before = self._buffer.pending_count()
        total = 0
        for _ in range(max_rounds):
            n = self._flush_round()
            total += n
            if n == 0:
                break
        if pending_before or total:
            self._stats.record_replay(
                at_ms=at_ms,
                trigger=trigger,
                attempted=pending_before,
                acked=total,
                remaining=self._buffer.pending_count(),
            )
        return total

    def _auto_resume(self, trigger: str, at_ms: int = 0) -> int:
        """Replay pending backlog automatically whenever the link is usable."""
        if self._link.state == "down" or self._buffer.pending_count() == 0:
            return 0
        return self.flush_all(trigger=trigger, at_ms=at_ms)

    def set_link_state(self, state: str, now_ms: int = 0) -> None:
        self._link.set_state(state)
        # Link recovered -> start replaying without waiting for a manual flush.
        if state != "down":
            self._auto_resume(trigger="link_recovered", at_ms=now_ms)

    @property
    def link(self) -> SimulatedUplink:
        return self._link

    @property
    def buffer(self) -> PersistentBuffer:
        return self._buffer

    @property
    def stats(self) -> StatsStore:
        return self._stats

    def health_tick(self, device_ids: list[str], now_ms: int) -> list[FaultEvent]:
        return self._health.tick(device_ids, now_ms)

    def status(self) -> dict:
        """Ops snapshot: link, backlog ledger, replay progress, faults, reasons."""
        return {
            "link_state": self._link.state,
            "pending": self._buffer.pending_count(),
            "uploaded_total": self._stats.uploaded_total,
            "last_replay": self._stats.last_replay(),
            "reason_counts": self._stats.reason_counts(),
            "devices": self._health.fault_status(),
            "fault_episodes": self._health.episodes(),
        }

    def close(self) -> None:
        self._buffer.close()
        self._stats.close()
        self._health.close()
