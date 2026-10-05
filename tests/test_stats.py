"""Reason taxonomy coverage, persistent counters, and the ops status view."""
import logging

from gateway.models import ReasonCode
from gateway.service import GatewayService
from tests.testutil import NOW, json_payload, log_decision, reading

log = logging.getLogger("gateway-test")


def test_every_reason_code_is_classified_and_counted(tmp_path):
    svc = GatewayService(str(tmp_path / "gw.db"))
    svc.set_link_state("down")  # keep everything local; no auto-upload

    cases = [
        # (payload, expected ReasonCode)
        (json_payload("d1", [reading("co2", 400, "ppm", NOW)]), ReasonCode.UNKNOWN_METRIC),
        (json_payload("d1", [{"metric": "temperature", "unit": "celsius", "ts": NOW}]),
         ReasonCode.MISSING_FIELD),
        (json_payload("d1", [reading("temperature", 20, "yard", NOW)]), ReasonCode.INVALID_UNIT),
        (json_payload("d1", [reading("temperature", "hot", "celsius", NOW)]),
         ReasonCode.INVALID_VALUE),
        (json_payload("d1", [reading("temperature", 20, "celsius", NOW + 10**10)]),
         ReasonCode.INVALID_TIMESTAMP),
        (json_payload("d1", [reading("temperature", 20, "celsius", "not-a-number")]),
         ReasonCode.INVALID_TIMESTAMP),
    ]
    for payload, want in cases:
        out = svc.ingest(payload, NOW)
        log_decision(f"reason {want.value}", payload, out)
        assert out[0].status == "rejected"
        assert out[0].reason_code is want  # caller reads the enum directly

    # Out-of-order: accepted but flagged and counted.
    svc.ingest(json_payload("d1", [reading("temperature", 20, "celsius", NOW)]), NOW)
    out = svc.ingest(json_payload("d1", [reading("temperature", 19, "celsius", NOW - 5000)]), NOW)
    log_decision("reason out_of_order", "ts goes backwards", out)
    assert out[0].status == "accepted"
    assert out[0].reason_code is ReasonCode.OUT_OF_ORDER_TIMESTAMP

    # Duplicate: same record again.
    out = svc.ingest(json_payload("d1", [reading("temperature", 20, "celsius", NOW)]), NOW)
    log_decision("reason duplicate", "same dedup key redelivered", out)
    assert out[0].status == "duplicate"
    assert out[0].reason_code is ReasonCode.DUPLICATE

    counts = svc.stats.reason_counts()
    log.info("reason_counts=%s", counts)
    assert counts["unknown_metric"] == 1
    assert counts["missing_field"] == 1
    assert counts["invalid_unit"] == 1
    assert counts["invalid_value"] == 1
    assert counts["invalid_timestamp"] == 2
    assert counts["out_of_order_timestamp"] == 1
    assert counts["duplicate"] == 1
    assert counts["buffer_full"] == 0
    # The taxonomy is enumerable and stable: every ReasonCode has a counter.
    assert set(counts) == {c.value for c in ReasonCode}
    svc.close()


def test_buffer_full_counted_and_reasons_persist_across_restart(tmp_path):
    db = str(tmp_path / "gw.db")
    svc = GatewayService(db, buffer_capacity=1)
    svc.set_link_state("down")
    svc.ingest(json_payload("d1", [reading("temperature", 20, "celsius", NOW)]), NOW)
    out = svc.ingest(json_payload("d1", [reading("temperature", 21, "celsius", NOW + 1000)]), NOW)
    log_decision("reason buffer_full", "2nd record into capacity=1 buffer", out)
    assert out[0].reason_code is ReasonCode.BUFFER_FULL
    assert svc.stats.reason_counts()["buffer_full"] == 1
    svc.close()

    svc2 = GatewayService(db, buffer_capacity=1)  # restart: counters survive
    log.info("after restart reason_counts=%s", svc2.stats.reason_counts())
    assert svc2.stats.reason_counts()["buffer_full"] == 1
    svc2.close()


def test_ops_status_view(tmp_path):
    db = str(tmp_path / "gw.db")
    svc = GatewayService(db)
    svc.set_link_state("down")
    svc.ingest(json_payload("d1", [reading("temperature", 20, "celsius", NOW)]), NOW)
    svc.ingest(json_payload("d1", [reading("co2", 400, "ppm", NOW + 1)]), NOW)  # rejected
    svc.close()

    svc2 = GatewayService(db)  # restart with link up: auto-resume kicks in
    status = svc2.status()
    log.info("status=%s", status)
    assert status["link_state"] == "up"
    assert status["pending"] == 0          # auto-resumed at startup
    assert status["uploaded_total"] == 1   # the one accepted record
    assert status["last_replay"]["trigger"] == "startup"
    assert status["last_replay"]["acked"] == 1
    assert status["reason_counts"]["unknown_metric"] == 1
    # Per-device fault view is present and structured.
    assert status["devices"][0]["device_id"] == "d1"
    assert isinstance(status["devices"][0]["faults"], list)
    assert isinstance(status["fault_episodes"], list)
    svc2.close()


def test_fault_episodes_visible_in_status(tmp_path):
    db = str(tmp_path / "gw.db")
    svc = GatewayService(db)
    svc.set_link_state("down")
    # 3 out-of-range values -> alarm (default debounce 3); then 3 good -> recovery.
    for i in range(3):
        svc.ingest(json_payload("d1", [reading("temperature", 500, "celsius", NOW + i * 1000)]), NOW + i * 1000)
    for i in range(3, 6):
        svc.ingest(json_payload("d1", [reading("temperature", 20, "celsius", NOW + i * 1000)]), NOW + i * 1000)
    svc.close()

    svc2 = GatewayService(db)  # restart: episode history still queryable
    episodes = svc2.status()["fault_episodes"]
    log.info("fault_episodes=%s", episodes)
    assert len(episodes) == 1
    ep = episodes[0]
    assert ep["device_id"] == "d1" and ep["metric"] == "temperature"
    assert ep["start_ms"] == NOW + 2000
    assert ep["end_ms"] == NOW + 5000
    assert ep["duration_ms"] == 3000
    svc2.close()
