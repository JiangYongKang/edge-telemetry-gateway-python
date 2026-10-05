"""Fault state survives restarts: no re-alarms, streaks kept, episodes queryable."""
import logging

from gateway.health import HealthMonitor, HealthStateStore
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
        log.info("  -> no transition")


def _monitor(db, **kw):
    store = HealthStateStore(db)
    return HealthMonitor(store=store, **kw), store


def test_active_fault_not_re_raised_after_restart(tmp_path):
    db = str(tmp_path / "h.db")
    hm, store = _monitor(db, debounce_count=3)
    for i in range(3):
        ev = hm.observe(rec(T0 + i * 1000, 500.0), T0 + i * 1000)
    log_events("before restart", "3 consecutive 500C values -> alarm active", ev)
    assert [(e.fault, e.active) for e in ev] == [(FaultType.VALUE_OUT_OF_RANGE, True)]
    assert ev and ev[-1].active is True
    store.close()

    # Restart: still-bad values must NOT re-raise the already-active fault.
    hm2, store2 = _monitor(db, debounce_count=3)
    ev = hm2.observe(rec(T0 + 3000, 500.0), T0 + 3000)
    log_events("after restart", "fault already active, another bad value", ev)
    assert ev == []  # no duplicate alarm
    # Recovery still works across the restart boundary.
    assert hm2.observe(rec(T0 + 4000, 20.0), T0 + 4000) == []
    assert hm2.observe(rec(T0 + 5000, 20.0), T0 + 5000) == []
    ev = hm2.observe(rec(T0 + 6000, 20.0), T0 + 6000)
    log_events("recovery after restart", "3 consecutive in-range values", ev)
    assert [(e.fault, e.active) for e in ev] == [(FaultType.VALUE_OUT_OF_RANGE, False)]

    episodes = store2.episodes()
    log.info("episodes: %s", episodes)
    assert len(episodes) == 1
    ep = episodes[0]
    assert ep["device_id"] == "d1" and ep["fault"] == "value_out_of_range"
    assert ep["metric"] == "temperature"
    assert ep["started_ms"] == T0 + 2000
    assert ep["recovered_ms"] == T0 + 6000
    assert ep["duration_ms"] == 4000
    store2.close()


def test_debounce_streak_survives_restart(tmp_path):
    db = str(tmp_path / "h.db")
    hm, store = _monitor(db, debounce_count=3)
    assert hm.observe(rec(T0, 500.0), T0) == []
    assert hm.observe(rec(T0 + 1000, 500.0), T0 + 1000) == []
    log.info("input: 2 bad observations (streak 2/3), then restart")
    store.close()

    hm2, store2 = _monitor(db, debounce_count=3)
    ev = hm2.observe(rec(T0 + 2000, 500.0), T0 + 2000)
    log_events("after restart", "3rd bad observation; streak kept, not reset", ev)
    # Streak was preserved: the 3rd bad value completes the debounce.
    assert [(e.fault, e.active) for e in ev] == [(FaultType.VALUE_OUT_OF_RANGE, True)]
    store2.close()


def test_offline_streak_and_last_seen_survive_restart(tmp_path):
    db = str(tmp_path / "h.db")
    hm, store = _monitor(db, offline_after_ms=60_000, debounce_count=2)
    hm.observe(rec(T0), T0)
    assert hm.tick(["d1"], T0 + 61_000) == []  # streak 1/2
    log.info("input: device silent >60s, 1 tick (streak 1/2), then restart")
    store.close()

    hm2, store2 = _monitor(db, offline_after_ms=60_000, debounce_count=2)
    ev = hm2.tick(["d1"], T0 + 62_000)
    log_events("after restart", "2nd silent tick; offline streak kept", ev)
    assert [(e.fault, e.active) for e in ev] == [(FaultType.DEVICE_OFFLINE, True)]
    # Further ticks after another restart: still no duplicate alarm.
    store2.close()
    hm3, store3 = _monitor(db, offline_after_ms=60_000, debounce_count=2)
    assert hm3.tick(["d1"], T0 + 63_000) == []
    log.info("decision: second restart, fault still active, no re-alarm")
    store3.close()


def test_same_conclusions_with_and_without_restart(tmp_path):
    """Identical input batch: one run continuous, one with a restart in the
    middle -- events and episodes must be identical."""

    def run(db, restart_after):
        events = []
        hm, store = _monitor(db, debounce_count=3)
        steps = [
            ("observe", rec(T0, 500.0), T0),
            ("observe", rec(T0 + 1000, 500.0), T0 + 1000),
            ("observe", rec(T0 + 2000, 500.0), T0 + 2000),
            ("observe", rec(T0 + 3000, 20.0), T0 + 3000),
            ("observe", rec(T0 + 4000, 20.0), T0 + 4000),
            ("observe", rec(T0 + 5000, 20.0), T0 + 5000),
        ]
        for i, (_, record, now) in enumerate(steps):
            if i == restart_after:
                store.close()
                hm, store = _monitor(db, debounce_count=3)
            events += hm.observe(record, now)
        episodes = store.episodes()
        store.close()
        return (
            [(e.fault.value, e.active, e.metric, e.at_ms) for e in events],
            episodes,
        )

    continuous = run(str(tmp_path / "a.db"), restart_after=-1)
    restarted = run(str(tmp_path / "b.db"), restart_after=3)
    log.info("input: identical 6-record batch, continuous vs restart mid-way")
    log.info("decision: continuous=%s restarted=%s", continuous, restarted)
    assert continuous == restarted
