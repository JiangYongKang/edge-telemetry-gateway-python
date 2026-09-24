"""Fault detection: offline / stall / out-of-range with debounce."""
import logging

from gateway.health import HealthMonitor
from gateway.models import FaultType, TelemetryRecord

log = logging.getLogger("gateway-test")

T0 = 1_700_000_000_000


def rec(ts, value=20.0, device="d1", metric="temperature"):
    return TelemetryRecord(device, metric, value, "celsius", ts)


def log_events(context, detail, events):
    log.info("%s | %s", context, detail)
    for e in events:
        log.info("  -> fault=%s active=%s metric=%s detail=%s",
                 e.fault.value, e.active, e.metric, e.detail)
    if not events:
        log.info("  -> no transition (debounce streak not reached)")


def test_out_of_range_alarm_and_recovery_are_debounced():
    hm = HealthMonitor(debounce_count=3)
    # Two bad values: no alarm yet.
    assert hm.observe(rec(T0, 500.0), T0) == []
    assert hm.observe(rec(T0 + 1000, 500.0), T0 + 1000) == []
    ev = hm.observe(rec(T0 + 2000, 500.0), T0 + 2000)
    log_events("out-of-range", "3 consecutive 500C values (range [-100,200])", ev)
    assert [(e.fault, e.active) for e in ev] == [(FaultType.VALUE_OUT_OF_RANGE, True)]
    # One good value (jitter) must NOT clear the alarm.
    assert hm.observe(rec(T0 + 3000, 20.0), T0 + 3000) == []
    ev = hm.observe(rec(T0 + 4000, 500.0), T0 + 4000)
    log_events("jitter while alarmed", "good,bad alternating: no flapping", ev)
    assert ev == []  # no duplicate alarm either
    # Three consecutive good values clear it.
    hm.observe(rec(T0 + 5000, 20.0), T0 + 5000)
    hm.observe(rec(T0 + 6000, 20.0), T0 + 6000)
    ev = hm.observe(rec(T0 + 7000, 20.0), T0 + 7000)
    log_events("recovery", "3 consecutive in-range values", ev)
    assert [(e.fault, e.active) for e in ev] == [(FaultType.VALUE_OUT_OF_RANGE, False)]


def test_device_offline_after_silence_with_debounce():
    hm = HealthMonitor(offline_after_ms=60_000, debounce_count=2)
    hm.observe(rec(T0), T0)
    # Ticks below threshold: nothing.
    assert hm.tick(["d1"], T0 + 30_000) == []
    # First tick past threshold: streak 1, still no alarm (debounce).
    assert hm.tick(["d1"], T0 + 61_000) == []
    ev = hm.tick(["d1"], T0 + 62_000)
    log_events("offline", "silent for >60s across 2 ticks", ev)
    assert [(e.fault, e.active) for e in ev] == [(FaultType.DEVICE_OFFLINE, True)]
    # Further ticks while offline: no duplicate alarm.
    assert hm.tick(["d1"], T0 + 63_000) == []
    # Device returns: first record does not clear yet (debounce), second does.
    assert hm.observe(rec(T0 + 64_000), T0 + 64_000) == []
    ev = hm.observe(rec(T0 + 65_000), T0 + 65_000)
    log_events("back online", "2 consecutive reports after silence", ev)
    assert [(e.fault, e.active) for e in ev] == [(FaultType.DEVICE_OFFLINE, False)]


def test_data_stall_when_metric_timestamp_stops_advancing():
    hm = HealthMonitor(offline_after_ms=600_000, stall_after_ms=30_000, debounce_count=2)
    # Device keeps reporting (not offline) but temperature ts frozen at T0.
    hm.observe(rec(T0, device="d1", metric="temperature"), T0)
    hm.observe(rec(T0 + 40_000, device="d1", metric="humidity"), T0 + 40_000)
    ev = hm.tick(["d1"], T0 + 31_000)
    log_events("stall streak 1", "temperature ts frozen, device alive", ev)
    assert ev == []
    ev = hm.tick(["d1"], T0 + 32_000)
    log_events("stall streak 2", "temperature ts frozen >30s, 2 ticks", ev)
    assert [(e.fault, e.active, e.metric) for e in ev] == [
        (FaultType.DATA_STALL, True, "temperature")
    ]
    # Fresh temperature sample: streak toward recovery.
    hm.observe(rec(T0 + 33_000, device="d1", metric="temperature"), T0 + 33_000)
    ev = hm.observe(rec(T0 + 34_000, device="d1", metric="temperature"), T0 + 34_000)
    log_events("stall recovery", "temperature ts advancing again, 2 samples", ev)
    assert [(e.fault, e.active) for e in ev] == [(FaultType.DATA_STALL, False)]


def test_same_input_batch_reproduces_same_events():
    def run():
        hm = HealthMonitor(offline_after_ms=60_000, debounce_count=2)
        out = []
        out += hm.observe(rec(T0, 500.0), T0)
        out += hm.observe(rec(T0 + 1000, 500.0), T0 + 1000)
        out += hm.tick(["d1"], T0 + 62_000)
        out += hm.tick(["d1"], T0 + 63_000)
        return [(e.fault.value, e.active, e.metric) for e in out]

    r1, r2 = run(), run()
    log.info("input: identical record/tick sequence executed twice")
    log.info("decision: run1=%s run2=%s", r1, r2)
    assert r1 == r2
    assert (FaultType.VALUE_OUT_OF_RANGE.value, True, "temperature") in r1
    assert (FaultType.DEVICE_OFFLINE.value, True, None) in r1
