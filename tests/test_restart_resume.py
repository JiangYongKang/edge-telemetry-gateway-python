"""Auto-resume after restart / link recovery, crash mid-replay, ledger consistency."""
import json
import logging

from gateway.link import SimulatedUplink
from gateway.service import GatewayService
from tests.testutil import NOW, json_payload, reading

log = logging.getLogger("gateway-test")


def ingest_n(svc, n, device="d1", base_ts=NOW):
    for i in range(n):
        svc.ingest(json_payload(device, [reading("temperature", 20 + i, "celsius", base_ts + i)]), NOW)


def sent_ts(link):
    return [json.loads(p)["timestamp_ms"] for _, p in link.sent]


def test_link_recovery_auto_replays_without_manual_trigger(tmp_path):
    svc = GatewayService(str(tmp_path / "gw.db"))
    svc.set_link_state("down")
    ingest_n(svc, 4)
    log.info("input: 4 records buffered while link down; link recovers by itself")
    svc.set_link_state("up", now_ms=NOW + 10)  # no flush() call anywhere
    log.info("decision: auto-replay sent=%s pending=%d", sent_ts(svc.link),
             svc.buffer.pending_count())
    assert sent_ts(svc.link) == [NOW + i for i in range(4)]
    assert svc.buffer.pending_count() == 0
    replay = svc.stats.last_replay()
    log.info("last_replay=%s", replay)
    assert replay["trigger"] == "link_recovered"
    assert replay["attempted"] == 4 and replay["acked"] == 4 and replay["remaining"] == 0
    svc.close()


def test_crash_mid_replay_restart_continues_without_loss_or_dup(tmp_path):
    db = str(tmp_path / "gw.db")
    svc = GatewayService(db, link=SimulatedUplink(flaky_period=2))
    svc.set_link_state("down")
    ingest_n(svc, 6)
    # Link comes back flaky (every 2nd attempt fails). Bypass the service
    # hook so we can "crash" (close) between replay rounds.
    svc.link.set_state("flaky")
    first_round = svc.flush()  # round 1: 1st attempt ok, 2nd fails -> acks 1
    log.info("input: 6 records, flaky link, crash after first replay round acked=%d", first_round)
    assert 0 < first_round < 6
    svc.close()  # crash mid-replay: unacked rows must survive

    svc2 = GatewayService(db)  # restarts with link up -> startup auto-resume
    combined = sent_ts(svc.link) + sent_ts(svc2.link)
    log.info("decision: after crash+restart combined sent=%s", combined)
    assert combined == [NOW + i for i in range(6)]  # order preserved, no loss
    assert len(combined) == len(set(combined))  # no duplicates uploaded
    assert svc2.buffer.pending_count() == 0
    assert svc2.stats.uploaded_total == 6  # ledger: each acked row counted once
    svc2.close()


def test_ledger_balances_across_restarts(tmp_path):
    db = str(tmp_path / "gw.db")
    svc = GatewayService(db)
    svc.set_link_state("down")
    ingest_n(svc, 5)
    svc.close()

    link = SimulatedUplink()
    link.set_state("down")
    svc2 = GatewayService(db, link=link)  # restart, link still down
    assert svc2.buffer.pending_count() == 5
    assert svc2.stats.uploaded_total == 0
    ingest_n(svc2, 3, base_ts=NOW + 100)  # 3 more while down
    svc2.close()

    svc3 = GatewayService(db)  # restart, link up -> auto-resume everything
    log.info("input: 5+3 records across two restarts, link up at 3rd boot")
    log.info("decision: uploaded_total=%d pending=%d",
             svc3.stats.uploaded_total, svc3.buffer.pending_count())
    assert svc3.stats.uploaded_total == 8
    assert svc3.buffer.pending_count() == 0
    assert sent_ts(svc3.link) == [NOW + i for i in range(5)] + [NOW + 100 + i for i in range(3)]
    svc3.close()


def test_replay_sequence_identical_with_and_without_restart(tmp_path):
    """Same input batch: crash+restart mid-replay must not change the outcome."""
    # Run 1: no crash, flaky link drains the backlog in one process.
    svc = GatewayService(str(tmp_path / "a.db"), link=SimulatedUplink(flaky_period=3))
    svc.set_link_state("down")
    ingest_n(svc, 8)
    svc.set_link_state("flaky")  # auto-resume over flaky link
    baseline = sent_ts(svc.link)
    svc.close()

    # Run 2: crash after 2 replay rounds, restart finishes the backlog.
    db = str(tmp_path / "b.db")
    svc = GatewayService(db, link=SimulatedUplink(flaky_period=3))
    svc.set_link_state("down")
    ingest_n(svc, 8)
    svc.link.set_state("flaky")  # bypass hook: drive rounds manually
    svc.flush()
    svc.flush()
    part1 = sent_ts(svc.link)
    svc.close()  # crash mid-replay
    svc2 = GatewayService(db)  # startup auto-resume, link up
    restarted = part1 + sent_ts(svc2.link)
    svc2.close()

    log.info("input: identical 8-record batch; run1 no crash, run2 crash after 2 rounds")
    log.info("decision: baseline=%s restarted=%s", baseline, restarted)
    assert restarted == baseline == [NOW + i for i in range(8)]
