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
from gateway.models import DedupReason, FaultEvent, IngestOutcome, RejectReason
from gateway.normalizer import normalize


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
        self._health = health or HealthMonitor()
        self._last_ts: dict[tuple[str, str], int] = {}
        self._lock = threading.Lock()

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
        return outcomes

    def flush(self) -> int:
        """Attempt to upload one batch of pending records in collection order."""
        return self._uplink.flush_once()

    def flush_all(self, max_rounds: int = 10_000) -> int:
        """Flush until the buffer is drained or the link stops accepting."""
        total = 0
        for _ in range(max_rounds):
            n = self.flush()
            total += n
            if n == 0:
                break
        return total

    def set_link_state(self, state: str) -> None:
        self._link.set_state(state)

    @property
    def link(self) -> SimulatedUplink:
        return self._link

    @property
    def buffer(self) -> PersistentBuffer:
        return self._buffer

    def health_tick(self, device_ids: list[str], now_ms: int) -> list[FaultEvent]:
        return self._health.tick(device_ids, now_ms)

    def close(self) -> None:
        self._buffer.close()
