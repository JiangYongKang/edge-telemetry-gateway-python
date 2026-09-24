"""上行链路管理。

- DOWN 期间：新记录进入 spool 排队，不调用云端 sink。
- UP：每次 ingest 后立即 flush；DOWN->UP 切换时自动补传。
- flush 严格按 seq（= 采集入库顺序）FIFO 批量发送，发送成功后 ack 删除；
  spool 持久化在 SQLite，进程重启后仍可继续补传。
"""
from __future__ import annotations

import logging

from app.models import LinkStatus

logger = logging.getLogger("telemetry.uplink")


class CloudSink:
    """内存版"云端"：仅记录收到的 seq，用于观测与去重校验。"""

    def __init__(self):
        self._delivered: list[int] = []

    def send(self, seqs) -> None:
        self._delivered.extend(seqs)

    def delivered_seqs(self) -> list[int]:
        return list(self._delivered)

    def reset(self) -> None:
        self._delivered.clear()


class Uplink:
    def __init__(self, store, buffer, status: LinkStatus = LinkStatus.UP,
                 flush_batch_size: int = 100, sink: CloudSink | None = None):
        self._store = store
        self._buffer = buffer
        self._status = LinkStatus(status)
        self._batch = flush_batch_size
        self.sink = sink or CloudSink()

    @property
    def status(self) -> LinkStatus:
        return self._status

    def set_link(self, status: LinkStatus) -> dict:
        """切换链路；DOWN->UP 时立即按序补传。"""
        new_status = LinkStatus(status)
        old = self._status
        self._status = new_status
        logger.info("link %s -> %s", old.value, new_status.value)
        result = {"previous": old.value, "current": new_status.value,
                  "flushed": 0, "remaining": self._buffer.pending_count()}
        if old is LinkStatus.DOWN and new_status is LinkStatus.UP:
            result.update(self.flush())
        return result

    def flush(self) -> dict:
        """把 spool 中所有待补传记录按 seq 顺序发完，返回汇总。"""
        total_flushed = 0
        if self._status is LinkStatus.DOWN:
            return {"flushed": 0,
                    "remaining": self._buffer.pending_count(),
                    "skipped": "link_down"}
        while True:
            seqs = self._buffer.pop_batch(self._batch)
            if not seqs:
                break
            records = [self._store.get_record(s) for s in seqs]
            records = [r for r in records if r is not None]
            self.sink.send([s for s, r in zip(seqs, records)])
            self._buffer.ack(seqs)
            total_flushed += len(records)
            logger.info("flushed batch seqs=%s count=%d", seqs, len(records))
            if len(seqs) < self._batch:
                break
        return {"flushed": total_flushed,
                "remaining": self._buffer.pending_count()}

    def delivered_seqs(self) -> list[int]:
        return self.sink.delivered_seqs()

    def delivered(self) -> list:
        """已成功上行的记录（测试观测用），按 seq 升序。"""
        return self._store.delivered_records()
