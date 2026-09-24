"""网关门面：归一化 -> 幂等/乱序/背压 -> 存储缓冲 -> 上行 -> 故障判定。"""
from __future__ import annotations

import logging
import time
from dataclasses import asdict

from app.buffer import SpoolBuffer
from app.detector import FaultDetector
from app.normalizer import RejectError, normalize
from app.storage import (BufferFull, DuplicateReport, LocalStore,
                         OutOfOrder, StoreClosed)
from app.models import IngestResult, LinkStatus, SourceProtocol

logger = logging.getLogger("telemetry.gateway")


class EdgeGateway:
    def __init__(self, db_path: str = ":memory:", buffer_capacity: int = 1000,
                 link_status: LinkStatus = LinkStatus.UP,
                 offline_ms: int = 30_000, stall_ms: int = 20_000,
                 confirm_count: int = 2, flush_batch_size: int = 100):
        self.store = LocalStore(db_path)
        self.buffer = SpoolBuffer(self.store, buffer_capacity)
        from app.uplink import Uplink
        self.uplink = Uplink(self.store, self.buffer, link_status,
                             flush_batch_size)
        self.detector = FaultDetector(self.store, offline_ms, stall_ms,
                                      confirm_count)
        # 以 UP 启动且 spool 中存在上次进程遗留数据 -> 立即按序续传
        if link_status is LinkStatus.UP and self.buffer.pending_count() > 0:
            info = self.uplink.flush()
            logger.info("startup replay flushed=%s remaining=%s",
                        info.get("flushed"), info.get("remaining"))

    # ------------------------------------------------------------------
    def ingest(self, payload: dict,
               source: SourceProtocol = SourceProtocol.HTTP_JSON,
               received_ts_ms: int | None = None,
               now_ms: int | None = None) -> IngestResult:
        """处理单条上报。now_ms 仅用于故障时钟，默认取记录 ts_ms（确定性）。"""
        if received_ts_ms is None:
            received_ts_ms = time.time_ns() // 1_000_000
        source = SourceProtocol(source)

        # 1) 归一化（字段缺失/单位/时间戳/指标合法性）
        try:
            record = normalize(payload, source, received_ts_ms)
        except RejectError as exc:
            logger.info("ingest rejected source=%s reason=%s detail=%s input=%r",
                        source.value, exc.reason.value, exc.detail, payload)
            return IngestResult(False, exc.reason, exc.detail)

        # 2) 幂等 + 乱序 + 容量背压，单事务落库入队
        try:
            seq = self.buffer.append_record(record)
        except DuplicateReport as exc:
            logger.info("ingest rejected key=%s reason=duplicate_report",
                        record.raw_idempotency_key)
            return IngestResult(False, exc.reason, exc.detail, record)
        except OutOfOrder as exc:
            logger.info("ingest rejected key=%s reason=out_of_order %s",
                        record.raw_idempotency_key, exc.detail)
            return IngestResult(False, exc.reason, exc.detail, record)
        except BufferFull as exc:
            logger.info("ingest rejected key=%s reason=buffer_full cap=%d",
                        record.raw_idempotency_key, self.buffer.capacity)
            return IngestResult(False, exc.reason, exc.detail, record)
        except StoreClosed as exc:
            return IngestResult(False, exc.reason, exc.detail, record)

        logger.info("ingest accepted seq=%d key=%s link=%s pending=%d",
                    seq, record.raw_idempotency_key,
                    self.uplink.status.value, self.buffer.pending_count())

        # 3) 链路可用则立即上行（内部按 FIFO）
        flush_info = {}
        if self.uplink.status is LinkStatus.UP:
            flush_info = self.uplink.flush()

        # 4) 故障判定（越界/停顿）；失败原因与质量判定互不影响
        events = self.detector.on_record(
            record, now_ms=now_ms if now_ms is not None else record.ts_ms)
        for ev in events:
            logger.info("fault triggered by record: %s", asdict(ev))

        detail = ""
        if flush_info:
            detail = f"flushed={flush_info.get('flushed', 0)}"
        return IngestResult(True, None, detail, record, seq)

    # ------------------------------------------------------------------
    def set_link(self, status: LinkStatus) -> dict:
        return self.uplink.set_link(LinkStatus(status))

    def flush(self) -> dict:
        return self.uplink.flush()

    def evaluate_timeouts(self, now_ms: int) -> list[dict]:
        events = self.detector.evaluate_timeouts(now_ms)
        return [asdict(e) for e in events]

    def fault_events(self) -> list[dict]:
        return self.detector.events()

    def fault_states(self) -> list[dict]:
        return self.detector.current_states()

    def delivered(self) -> list:
        return self.uplink.delivered()

    def pending_seqs(self) -> list[int]:
        return self.buffer.pending_seqs()

    def close(self) -> None:
        self.store.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc) -> None:
        self.close()
