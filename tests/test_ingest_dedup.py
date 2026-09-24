"""重复上报、乱序、并发投递幂等。"""
import logging
import threading

from app.gateway import EdgeGateway
from app.models import LinkStatus, RejectReason, SourceProtocol

log = logging.getLogger("test.ingest")


def test_duplicate_report_idempotent():
    gw = EdgeGateway(":memory:")
    payload = {"device_id": "d1", "metric": "temperature",
               "value": 25.0, "unit": "C", "ts_ms": 10_000}
    log.info("输入: 同一载荷投递两次 %s", payload)
    r1 = gw.ingest(payload, SourceProtocol.HTTP_JSON)
    r2 = gw.ingest(payload, SourceProtocol.HTTP_JSON)
    log.info("判定: 第一次 accepted=%s seq=%s; 第二次 accepted=%s reason=%s",
             r1.accepted, r1.seq, r2.accepted,
             r2.reason.value if r2.reason else None)
    assert r1.accepted and r1.seq == 1
    assert not r2.accepted
    assert r2.reason is RejectReason.DUPLICATE_REPORT
    assert len(gw.store.all_records()) == 1  # 不重复入库
    gw.close()


def test_out_of_order_rejected_distinguishable():
    gw = EdgeGateway(":memory:")
    p1 = {"device_id": "d1", "metric": "temperature", "value": 25,
          "unit": "C", "ts_ms": 20_000}
    p2 = {"device_id": "d1", "metric": "temperature", "value": 24,
          "unit": "C", "ts_ms": 10_000}
    log.info("输入: 先 ts=20000 再 ts=10000 (乱序)")
    gw.ingest(p1)
    r = gw.ingest(p2)
    log.info("判定: accepted=%s reason=%s detail=%s",
             r.accepted, r.reason.value if r.reason else None, r.detail)
    assert not r.accepted
    assert r.reason is RejectReason.TIMESTAMP_OUT_OF_ORDER
    gw.close()


def test_concurrent_duplicate_only_one_persisted():
    """并发投递同一上报：UNIQUE 约束 + 串行化保证恰好一条入库。"""
    gw = EdgeGateway(":memory:")
    payload = {"device_id": "d1", "metric": "temperature",
               "value": 25.0, "unit": "C", "ts_ms": 5_000}
    results: list = []
    barrier = threading.Barrier(8)

    def worker():
        barrier.wait()
        results.append(gw.ingest(payload, SourceProtocol.HTTP_JSON))

    log.info("输入: 8 线程并发投递同一 key 的载荷 %s", payload)
    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    accepted = [r for r in results if r.accepted]
    rejected = [r for r in results if not r.accepted]
    log.info("判定: accepted=%d, rejected=%d (原因分布=%s), 实际入库=%d",
             len(accepted), len(rejected),
             {r.reason.value: 1 for r in rejected},
             len(gw.store.all_records()))
    assert len(accepted) == 1
    assert all(r.reason is RejectReason.DUPLICATE_REPORT for r in rejected)
    assert len(gw.store.all_records()) == 1
    assert gw.delivered() and len(gw.delivered()) == 1
    gw.close()


def test_two_protocols_normalize_same_model():
    gw = EdgeGateway(":memory:")
    http_p = {"device_id": "d1", "metric": "pressure", "value": 101.3,
              "unit": "kPa", "ts_ms": 1000}
    mqtt_p = {"dev": "d1", "k": "press", "v": 1013.0, "u": "hPa", "ts": 1.0}
    log.info("输入 HTTP=%s ; MQTT=%s", http_p, mqtt_p)
    r1 = gw.ingest(http_p, SourceProtocol.HTTP_JSON)
    r2 = gw.ingest(mqtt_p, SourceProtocol.MQTT_CBORISH)
    log.info("判定: HTTP -> %s ; MQTT -> %s", r1.record, r2.record)
    assert r1.accepted and r2.accepted
    assert r1.record.metric == r2.record.metric == "pressure"
    assert r1.record.unit == r2.record.unit == "kPa"
    assert abs(r1.record.value - r2.record.value) < 1e-9
    gw.close()
