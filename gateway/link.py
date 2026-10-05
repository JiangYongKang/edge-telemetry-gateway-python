"""Uplink simulation and replay (store-and-forward) logic.

SimulatedUplink states:
* "up"    -- every send succeeds.
* "down"  -- every send fails (data stays buffered).
* "flaky" -- deterministic jitter: every ``flaky_period``-th attempt fails,
             so the same input sequence always reproduces the same result.

UplinkManager.flush_once() uploads pending rows oldest-first and acks only
the rows the link confirmed; an unacked prefix is retried on the next flush,
giving at-least-once delivery without reordering.
"""
from __future__ import annotations

import json
import threading
from dataclasses import asdict

from gateway.buffer import PersistentBuffer
from gateway.stats import StatsStore

VALID_STATES = ("up", "down", "flaky")


class SimulatedUplink:
    """Scriptable uplink: 'up', 'down', or 'flaky' (drops some attempts)."""

    def __init__(self, flaky_period: int = 3) -> None:
        if flaky_period < 2:
            raise ValueError("flaky_period must be >= 2")
        self._state = "up"
        self._flaky_period = flaky_period
        self._attempts = 0
        self.sent: list[tuple[int, str]] = []  # everything the "cloud" accepted
        self._lock = threading.Lock()

    def set_state(self, state: str) -> None:
        if state not in VALID_STATES:
            raise ValueError(f"unknown link state {state!r}; valid: {VALID_STATES}")
        with self._lock:
            self._state = state

    @property
    def state(self) -> str:
        return self._state

    def try_send(self, row_id: int, payload: str) -> bool:
        """Attempt one send; returns True if the (simulated) cloud accepted it."""
        with self._lock:
            self._attempts += 1
            ok = self._state == "up" or (
                self._state == "flaky" and self._attempts % self._flaky_period != 0
            )
            if ok:
                self.sent.append((row_id, payload))
            return ok


class UplinkManager:
    """Drains the buffer in collection order when the link is available."""

    def __init__(
        self,
        buffer: PersistentBuffer,
        link: SimulatedUplink,
        batch_size: int = 100,
        stats: "StatsStore | None" = None,
    ) -> None:
        self._buffer = buffer
        self._link = link
        self._batch_size = batch_size
        self._stats = stats

    def flush_once(self) -> int:
        """Upload one batch in order; returns number of rows acked.

        Stops at the first failed send so order is preserved and nothing is
        acked out of turn.
        """
        batch = self._buffer.fetch_batch(self._batch_size)
        acked: list[int] = []
        for row_id, record in batch:
            payload = json.dumps(asdict(record), sort_keys=True)
            if not self._link.try_send(row_id, payload):
                break
            acked.append(row_id)
        self._buffer.ack(acked)
        return len(acked)

    def flush_all(self, max_rounds: int = 10_000, now_ms: int = 0) -> int:
        """Flush until the buffer is drained or the link stops accepting.

        When a StatsStore is attached, the acked count and the progress of
        this replay round are persisted, so the uploaded/pending books stay
        consistent even if the process crashes right afterwards.
        """
        total = 0
        rounds = 0
        for _ in range(max_rounds):
            n = self.flush_once()
            rounds += 1
            total += n
            if n == 0:
                break
        if self._stats is not None:
            self._stats.record_uploaded(total)
            self._stats.record_flush(total, rounds, now_ms)
        return total
