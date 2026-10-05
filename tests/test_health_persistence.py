"""Fault state survives restarts: no re-alarm, streak progress kept, episodes queryable."""
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
        log.info("  -> fault=%s active=%s metric=%s at_ms=%d detail=%s",
                 e.fault.value, e.active, e.metric, e.at_ms, e.detail)
    if not events:
        log.info("  -> no transition (state carried over, debounce streak unchanged)")


def test_active_fault_is_not_realarmed_after_restart(tmp_path):
    db = tmp_path / "h.db"
    hm = HealthMonitor(debounce_count=3, state_db=db)
    for i in range(3):
        ev = hm.observe(rec(T0 + i * 1000, 500.0), T0 + i * 1000)
    log_events("before restart", "3 consecutive 500C -> alarm active", ev)
    assert [(e.fault, e.active) for e in ev] == [(FaultType.VALUE_OUT_OF_RANGE, True)]
    hm.close()

    hm2 = HealthMonitor(debounce_count=3, state_db=db)  # restart
    ev = hm2.observe(rec(T0 + 3000, 500.0), T0 + 3000)  # still bad, already active
    log_events("after restart", "4th bad value while alarm active", ev)
    assert ev == []  # no duplicate alarm for the ongoing fault
    hm2.close()


def test_debounce_streak_progress_survives_restart(tmp_path):
    db = tmp_path / "h.db"
    hm = HealthMonitor(debounce_count=3, state_db=db)
    assert hm.observe(rec(T0, 500.0), T0) == []        # bad streak 1
    assert hm.observe(rec(T0 + 1000, 500.0), T0) == []  # bad streak 2
    hm.close()
    log.info("input: 2 of 3 bad observations before restart (streak must not reset)")

    hm2 = HealthMonitor(debounce_count=3, state_db=db)
    ev = hm2.observe(rec(T0 + 2000, 500.0), T0 + 2000)  # streak 3 -> alarm
    log_events("after restart", "3rd bad observation completes the streak", ev)
    assert [(e.fault, e.active) for e in ev] == [(FaultType.VALUE_OUT_OF_RANGE, True)]
    hm2.close()


def test_offline_tick_streak_survives_restart(tmp_path):
    db = tmp_path / "h.db"
    hm = HealthMonitor(offline_after_ms=60_000, debounce_count=2, state_db=db)
    hm.observe(rec(T0), T0)
    assert hm.tick(["d1"], T0 + 61_000) == []  # streak 1, below debounce
    hm.close()
    log.info("input: 1 offline tick before restart (streak 1 of 2)")

    hm2 = HealthMonitor(offline_after_ms=60_000, debounce_count=2, state_db=db)
    ev = hm2.tick(["d1"], T0 + 62_000)  # streak 2 -> alarm
    log_events("after restart", "2nd offline tick completes the streak", ev)
    assert [(e.fault, e.active) for e in ev] == [(FaultType.DEVICE_OFFLINE, True)]
    hm2.close()


def test_fault_episode_lifecycle_queryable_across_restart(tmp_path):
    db = tmp_path / "h.db"
    hm = HealthMonitor(debounce_count=3, state_db=db)
    for i in range(3):  # alarm at T0+2000
        hm.observe(rec(T0 + i * 1000, 500.0), T0 + i * 1000)
    hm.close()

    hm2 = HealthMonitor(debounce_count=3, state_db=db)  # restart while alarmed
    for i in range(3, 6):  # 3 good values -> recovery at T0+5000
        ev = hm2.observe(rec(T0 + i * 1000, 20.0), T0 + i * 1000)
    log_events("recovery after restart", "3 consecutive in-range values", ev)
    assert [(e.fault, e.active) for e in ev] == [(FaultType.VALUE_OUT_OF_RANGE, False)]

    episodes = hm2.episodes()
    log.info("episodes=%s", episodes)
    assert len(episodes) == 1
    ep = episodes[0]
    assert ep["device_id"] == "d1"
    assert ep["fault"] == "value_out_of_range"
    assert ep["metric"] == "temperature"
    assert ep["start_ms"] == T0 + 2000
    assert ep["end_ms"] == T0 + 5000
    assert ep["duration_ms"] == 3000

    # Episodes and fault status remain queryable after yet another restart.
    hm2.close()
    hm3 = HealthMonitor(debounce_count=3, state_db=db)
    assert hm3.episodes() == episodes
    status = hm3.fault_status()
    log.info("fault_status=%s", status)
    assert status[0]["device_id"] == "d1"
    oor = [f for f in status[0]["faults"] if f["fault"] == "value_out_of_range"]
    assert oor[0]["active"] is False
    hm3.close()


def test_same_conclusions_with_and_without_restart(tmp_path):
    """Identical input sequence; one run restarts mid-way. Events must match."""
    def script(hm):
        out = []
        out += hm.observe(rec(T0, 500.0), T0)
        out += hm.observe(rec(T0 + 1000, 500.0), T0 + 1000)
        out += hm.tick(["d1"], T0 + 62_000)
        out += hm.observe(rec(T0 + 62_100, 500.0), T0 + 62_100)
        out += hm.tick(["d1"], T0 + 63_000)
        out += hm.observe(rec(T0 + 63_100, 20.0), T0 + 63_100)
        return out

    hm = HealthMonitor(offline_after_ms=60_000, debounce_count=2)
    plain = script(hm)

    db = tmp_path / "h.db"
    # Run the same script step by step, restarting after the 2nd and 4th call.
    hm1 = HealthMonitor(offline_after_ms=60_000, debounce_count=2, state_db=db)
    events = []
    events += hm1.observe(rec(T0, 500.0), T0)
    events += hm1.observe(rec(T0 + 1000, 500.0), T0 + 1000)
    hm1.close()  # restart #1
    hm2 = HealthMonitor(offline_after_ms=60_000, debounce_count=2, state_db=db)
    events += hm2.tick(["d1"], T0 + 62_000)
    events += hm2.observe(rec(T0 + 62_100, 500.0), T0 + 62_100)
    hm2.close()  # restart #2
    hm3 = HealthMonitor(offline_after_ms=60_000, debounce_count=2, state_db=db)
    events += hm3.tick(["d1"], T0 + 63_000)
    events += hm3.observe(rec(T0 + 63_100, 20.0), T0 + 63_100)
    hm3.close()

    key = lambda evs: [(e.fault.value, e.active, e.metric, e.at_ms) for e in evs]
    log.info("input: identical observe/tick sequence; run1 no restart, run2 two restarts")
    log.info("decision: plain=%s restarted=%s", key(plain), key(events))
    assert key(events) == key(plain)
