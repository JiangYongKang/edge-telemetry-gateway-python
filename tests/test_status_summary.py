"""Ops status view and stable reason classification (rejected/dedup/flagged)."""
import logging

from gateway.models import ReasonCode
from gateway.service import GatewayService
from tests.testutil import NOW, json_payload, log_decision, reading

log = logging.getLogger("gateway-test")


def test_reason_codes_are_stable_and_enumerable():
    codes = {c.value for c in ReasonCode}
    log.info("ReasonCode enumeration: %s", sorted(codes))
    # The eight categories ops asked for, each independently switchable.
    for expected in (
        "unknown_metric", "missing_field", "invalid_unit", "invalid_value",
        "invalid_timestamp", "out_of_order_timestamp", "duplicate", "buffer_full",
    ):
        assert expected in codes


def test_reason_summary_counts_each_category(tmp_path):
    svc = GatewayService(str(tmp_path / "gw.db"), buffer_capacity=1)
    svc.set_link_state("down")  # keep accepted records pending

    out = svc.ingest(json_payload("d1", [reading("co2", 400, "ppm", NOW)]), NOW)
    log_decision("unknown metric", "metric=co2", out)
    out = svc.ingest(json_payload("d1", [{"metric": "temperature", "unit": "celsius", "ts": NOW}]), NOW)
    log_decision("missing field", "reading without value", out)
    out = svc.ingest(json_payload("d1", [reading("temperature", 20, "yard", NOW)]), NOW)
    log_decision("invalid unit", "temperature in yard", out)
    out = svc.ingest(json_payload("d1", [reading("temperature", "hot", "celsius", NOW)]), NOW)
    log_decision("invalid value", "value='hot'", out)
    out = svc.ingest(json_payload("d1", [reading("temperature", 20, "celsius", NOW + 10**10)]), NOW)
    log_decision("invalid timestamp", "ts 10^10 ms in the future", out)

    # One accepted record fills the capacity-1 buffer.
    out = svc.ingest(json_payload("d1", [reading("temperature", 20, "celsius", NOW)]), NOW)
    log_decision("accepted (fills buffer)", "capacity=1", out)
    # Next distinct record: buffer full.
    out = svc.ingest(json_payload("d1", [reading("temperature", 21, "celsius", NOW + 1000)]), NOW)
    log_decision("buffer full", "2nd distinct record, capacity=1", out)
    # Same record again: duplicate (dedup key already stored).
    out = svc.ingest(json_payload("d1", [reading("temperature", 20, "celsius", NOW)]), NOW)
    log_decision("duplicate", "same dedup key as the stored one", out)
    # Out-of-order device timestamp: accepted but flagged.
    svc2 = GatewayService(str(tmp_path / "gw2.db"))
    svc2.set_link_state("down")
    svc2.ingest(json_payload("d1", [reading("temperature", 20, "celsius", NOW)]), NOW)
    out = svc2.ingest(json_payload("d1", [reading("temperature", 19, "celsius", NOW - 5000)]), NOW)
    log_decision("out of order", "ts goes backwards by 5s", out)

    summary = svc.status()["reasons"]
    log.info("reason summary (svc): %s", summary)
    assert summary["rejected"] == {
        "unknown_metric": 1,
        "missing_field": 1,
        "invalid_unit": 1,
        "invalid_value": 1,
        "invalid_timestamp": 1,
        "buffer_full": 1,
    }
    assert summary["deduplicated"] == {"duplicate": 1}

    summary2 = svc2.status()["reasons"]
    log.info("reason summary (svc2): %s", summary2)
    assert summary2["flagged"] == {"out_of_order_timestamp": 1}
    svc.close()
    svc2.close()


def test_status_view_and_stats_survive_restart(tmp_path):
    db = str(tmp_path / "gw.db")
    svc = GatewayService(db)
    svc.set_link_state("down")
    svc.ingest(json_payload("d1", [reading("temperature", 20, "celsius", NOW)]), NOW)
    svc.ingest(json_payload("d1", [reading("temperature", 21, "celsius", NOW + 1000)]), NOW)
    svc.ingest(json_payload("d1", [reading("co2", 400, "ppm", NOW + 2000)]), NOW)  # rejected
    svc.close()
    log.info("input: 2 accepted + 1 rejected, then restart with link up")

    svc2 = GatewayService(db)  # link up at startup -> auto replay of the 2 pending
    status = svc2.status()
    log.info("status after restart: %s", status)
    assert status["link"] == "up"
    assert status["pending"] == 0
    assert status["uploaded_total"] == 2
    assert status["last_flush"]["acked"] == 2
    assert status["reasons"]["rejected"] == {"unknown_metric": 1}  # survived restart
    assert status["active_faults"] == {}  # no fault ever triggered
    svc2.close()


def test_active_faults_visible_in_status_and_history(tmp_path):
    db = str(tmp_path / "gw.db")
    svc = GatewayService(db)
    svc.set_link_state("down")
    for i in range(3):  # 3 consecutive out-of-range -> alarm (debounce=3)
        svc.ingest(
            json_payload("d1", [reading("temperature", 500, "celsius", NOW + i * 1000)]),
            NOW + i * 1000,
        )
    status = svc.status()
    log.info("status with active fault: %s", status["active_faults"])
    assert status["active_faults"] == {
        "d1": [{"fault": "value_out_of_range", "metric": "temperature"}]
    }
    svc.close()

    # Restart: fault still active (not re-raised, but visible), history intact.
    svc2 = GatewayService(db)
    svc2.set_link_state("down")
    status2 = svc2.status()
    log.info("active faults after restart: %s", status2["active_faults"])
    assert status2["active_faults"] == status["active_faults"]
    history = svc2.fault_history("d1")
    log.info("fault history: %s", history)
    assert len(history) == 1
    assert history[0]["started_ms"] == NOW + 2000
    assert history[0]["recovered_ms"] is None  # still open
    assert history[0]["duration_ms"] is None
    svc2.close()
