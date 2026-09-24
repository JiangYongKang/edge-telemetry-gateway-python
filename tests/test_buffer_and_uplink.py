"""Buffer, dedup, backpressure, concurrency, link jitter and restart replay."""
import json
import logging
import threading

import pytest

from gateway.buffer import BufferFullError, PersistentBuffer
from gateway.link import SimulatedUplink, UplinkManager
from gateway.models import TelemetryRecord
from gateway.service import GatewayService
from tests.testutil import NOW, json_payload, log_decision, reading

log = logging.getLogger("gateway-test")


def rec(device, ts, value=20.0, metric="temperature"):
    return TelemetryRecord(device, metric, value, "celsius", ts)


def test_exact_and_conflicting_duplicates(tmp_path):
    buf = PersistentBuffer(tmp_path / "b.db")
    log.info("input: same dedup key (d1|temperature|1000) three times, values 20/20/99")
    r1 = buf.enqueue(rec("d1", 1000, 20.0))
    r2 = buf.enqueue(rec("d1", 1000, 20.0))
    r3 = buf.enqueue(rec("d1", 1000, 99.0))
    log.info("decisions: %s, %s, %s (unique constraint on dedup_key)", r1, r2, r3)
    assert (r1, r2, r3) == ("stored", "exact_duplicate", "conflicting_duplicate")
    assert buf.pending_count() == 1
    buf.close()


def test_buffer_full_rejects_explicitly(tmp_path):
    buf = PersistentBuffer(tmp_path / "b.db", capacity=2)
    buf.enqueue(rec("d1", 1))
    buf.enqueue(rec("d1", 2))
    log.info("input: 3rd record into capacity=2 buffer")
    with pytest.raises(BufferFullError):
        buf.enqueue(rec("d1", 3))
    log.info("decision: BufferFullError raised; pending=%d (nothing silently dropped)",
             buf.pending_count())
    assert buf.pending_count() == 2
    buf.close()


def test_concurrent_delivery_is_idempotent(tmp_path):
    buf = PersistentBuffer(tmp_path / "b.db", capacity=10_000)
    results = []
    results_lock = threading.Lock()

    def deliver():
        r = buf.enqueue(rec("d1", 1000, 20.0))
        with results_lock:
            results.append(r)

    threads = [threading.Thread(target=deliver) for _ in range(16)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    log.info("input: 16 threads concurrently enqueue the SAME record")
    log.info("decisions: %s (sqlite UNIQUE constraint resolves atomically)", results)
    assert results.count("stored") == 1
    assert results.count("exact_duplicate") == 15
    assert buf.pending_count() == 1
    buf.close()


def test_service_rejects_with_buffer_full_reason(tmp_path):
    svc = GatewayService(str(tmp_path / "gw.db"), buffer_capacity=1)
    svc.set_link_state("down")
    svc.ingest(json_payload("d1", [reading("temperature", 20, "celsius", NOW)]), NOW)
    out = svc.ingest(json_payload("d1", [reading("temperature", 21, "celsius", NOW + 1000)]), NOW)
    log_decision("buffer full at service level", "2nd record, capacity=1, link down", out)
    assert out[0].status == "rejected"
    assert out[0].reject_reason.value == "buffer_full"
    svc.close()


def test_out_of_order_flagged_but_stored(tmp_path):
    svc = GatewayService(str(tmp_path / "gw.db"))
    svc.set_link_state("down")
    svc.ingest(json_payload("d1", [reading("temperature", 20, "celsius", NOW)]), NOW)
    out = svc.ingest(json_payload("d1", [reading("temperature", 19, "celsius", NOW - 5000)]), NOW)
    log_decision("out-of-order timestamp", "ts goes backwards by 5s", out)
    assert out[0].status == "accepted"
    assert "out_of_order_timestamp" in out[0].detail
    assert svc.buffer.pending_count() == 2
    svc.close()


def test_offline_buffering_and_replay_in_order(tmp_path):
    svc = GatewayService(str(tmp_path / "gw.db"))
    svc.set_link_state("down")
    for i in range(5):
        svc.ingest(json_payload("d1", [reading("temperature", 20 + i, "celsius", NOW + i)]), NOW)
    assert svc.flush() == 0  # link down: nothing leaves
    log.info("input: 5 records while link down; then link up")
    svc.set_link_state("up")
    assert svc.flush_all() == 5
    sent_ts = [json.loads(p)["timestamp_ms"] for _, p in svc.link.sent]
    log.info("decision: replayed in collection order %s, pending=%d", sent_ts,
             svc.buffer.pending_count())
    assert sent_ts == [NOW + i for i in range(5)]
    assert svc.buffer.pending_count() == 0
    svc.close()


def test_flaky_link_replays_without_loss_or_reorder(tmp_path):
    buf = PersistentBuffer(tmp_path / "b.db")
    link = SimulatedUplink(flaky_period=2)  # every 2nd attempt fails
    mgr = UplinkManager(buf, link, batch_size=4)
    link.set_state("flaky")
    for i in range(6):
        buf.enqueue(rec("d1", NOW + i, float(i)))
    rounds = 0
    while buf.pending_count() and rounds < 50:
        mgr.flush_once()
        rounds += 1
    sent_ts = [json.loads(p)["timestamp_ms"] for _, p in link.sent]
    log.info("input: 6 records over flaky link (every 2nd attempt fails)")
    log.info("decision: sent=%s in %d flush rounds; order preserved, no loss", sent_ts, rounds)
    assert sent_ts == [NOW + i for i in range(6)]
    assert buf.pending_count() == 0
    buf.close()


def test_restart_resumes_pending_upload(tmp_path):
    db = str(tmp_path / "gw.db")
    svc = GatewayService(db)
    svc.set_link_state("down")
    for i in range(3):
        svc.ingest(json_payload("d1", [reading("temperature", 20 + i, "celsius", NOW + i)]), NOW)
    svc.close()  # simulate process crash/restart with link still down
    log.info("input: 3 records buffered, process restarted before any upload")

    svc2 = GatewayService(db)  # same db file: pending data survives
    assert svc2.buffer.pending_count() == 3
    svc2.set_link_state("up")
    assert svc2.flush_all() == 3
    sent_ts = [json.loads(p)["timestamp_ms"] for _, p in svc2.link.sent]
    log.info("decision: after restart replayed %s, pending=%d", sent_ts,
             svc2.buffer.pending_count())
    assert sent_ts == [NOW, NOW + 1, NOW + 2]
    svc2.close()


def test_flaky_replay_is_reproducible(tmp_path):
    """Same input batch executed twice yields identical conclusions."""
    def run(db):
        buf = PersistentBuffer(db)
        link = SimulatedUplink(flaky_period=3)
        mgr = UplinkManager(buf, link, batch_size=3)
        link.set_state("flaky")
        for i in range(8):
            buf.enqueue(rec("d1", NOW + i, float(i)))
        acked_per_round = []
        while buf.pending_count():
            acked_per_round.append(mgr.flush_once())
        sent = [json.loads(p)["timestamp_ms"] for _, p in link.sent]
        buf.close()
        return acked_per_round, sent

    run1 = run(tmp_path / "a.db")
    run2 = run(tmp_path / "b.db")
    log.info("input: identical 8-record batch over flaky link, executed twice")
    log.info("decision: run1=%s run2=%s", run1, run2)
    assert run1 == run2
