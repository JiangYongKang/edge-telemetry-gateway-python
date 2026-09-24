"""离线缓冲：容量上限、背压拒绝、FIFO 顺序续传。

真正的"容量判定 + 记录落库 + 入队"在 LocalStore.accept_record 单事务内完成，
本类是网关侧的容量策略与队列操作门面，异常类型复用 storage.BufferFull，
保证写满时返回 BUFFER_FULL 而非静默丢弃。
"""
from __future__ import annotations

import logging

from app.storage import BufferFull  # re-export  # noqa: F401

logger = logging.getLogger("telemetry.buffer")


class SpoolBuffer:
    def __init__(self, store, capacity: int):
        if capacity <= 0:
            raise ValueError("buffer capacity must be positive")
        self._store = store
        self._capacity = capacity

    @property
    def capacity(self) -> int:
        return self._capacity

    def append_record(self, record) -> int:
        """原子完成：容量背压判定 -> 落库 -> 入队，返回 seq。"""
        seq = self._store.accept_record(record, self._capacity)
        logger.info("buffered seq=%d pending=%d/%d",
                    seq, self.pending_count(), self._capacity)
        return seq

    def pop_batch(self, max_count: int) -> list[int]:
        """按采集（入库）顺序查看最早的一批 seq，不删除（ack 后才删除）。"""
        return self._store.spool_peek_batch(max_count)

    def ack(self, seqs) -> None:
        self._store.spool_ack_batch(seqs)

    def pending_count(self) -> int:
        return self._store.spool_count()

    def pending_seqs(self) -> list[int]:
        return self._store.spool_all_seqs()
