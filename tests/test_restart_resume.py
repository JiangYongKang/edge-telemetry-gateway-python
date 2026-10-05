"""Auto-resume of the uplink across restarts and crash-mid-replay accounting."""
import json
import logging

from gateway.link import SimulatedUplink
from gateway.service import GatewayService
from tests.testutil import NOW, json_payload, log_decision, reading

log = logging.getLogger("gateway-test")


class LimitedUplink(SimulatedUplink):
    """Link that accepts only ``budget`` sends, then dies (simulates a box
    losing power in the middle of a replay)."""

    def __init__(self, budget: int) -> None:
        super().__init__()
        self._budget = budget

    def try_send(self, row_id: int, payload: str) -> bool:
        if self._budget <= 0:
            return False
        self._budget -= 1
        return super().try_send(row_id, payload)


def _ingest_n(svc, n, device="d1"):
    for i in range(n):
        svc.ingest(json_payload(device, [reading("temperature", 20 + i, "celsius", NOW + i)]), NOW)


def test_restart_with_link_down_then_recovery_auto_replays(tmp_path):
    db = str(tmp_path / "gw.db")
    svc = GatewayService(db)
    svc.set_link_state("down")
    _ingest_n(svc, 4)
    svc.close()
    log.info("input: 4 records buffered, restart with link still down")

    link2 = SimulatedUplink()
    link2.set_state("down")
    svc2 = GatewayService(db, link=link2)
    log.info("decision: link down at startup -> no replay, pending=%d",
             svc2.buffer.pending_count())
    assert svc2.buffer.pending_count() == 4
    assert link2.sent == []

    svc2.set_link_state("up")  # recovery must auto-start the replay
    sent_ts = [json.loads(p)["timestamp_ms"] for _, p in link2.sent]
    log.info("decision: link recovered -> auto-replayed %s, pending=%d",
             sent_ts, svc2.buffer.pending_count())
    assert sent_ts == [NOW + i for i in range(4)]
    assert svc2.buffer.pending_count() == 0
    svc2.close()


def test_crash_mid_replay_then_restart_completes_without_loss(tmp_path):
    db = str(tmp_path / "gw.db")
    svc = GatewayService(db)
    svc.set_link_state("down")
    _ingest_n(svc, 6)
    svc.close()

    # First restart: link dies after 2 sends -> replay interrupted mid-way.
    dying = LimitedUplink(budget=2)
    svc2 = GatewayService(db, link=dying)
    first_round = [json.loads(p)["timestamp_ms"] for _, p in dying.sent]
    log.info("input: restart #1, link dies after 2 sends (power loss mid-replay)")
    log.info("decision: sent=%s before crash, pending=%d", first_round,
             svc2.buffer.pending_count())
    assert first_round == [NOW, NOW + 1]
    assert svc2.buffer.pending_count() == 4
    svc2.close()  # hard kill; 2 rows acked, 4 still pending

    # Second restart: healthy link -> remaining rows replay in original order.
    svc3 = GatewayService(db)
    second_round = [json.loads(p)["timestamp_ms"] for _, p in svc3.link.sent]
    log.info("input: restart #2 with healthy link")
    log.info("decision: auto-replayed remaining %s, pending=%d", second_round,
             svc3.buffer.pending_count())
    assert second_round == [NOW + i for i in range(2, 6)]
    assert svc3.buffer.pending_count() == 0

    # The books balance: 6 uploaded in total, counted once each.
    status = svc3.status()
    log.info("decision: uploaded_total=%d pending=%d last_flush=%s",
             status["uploaded_total"], status["pending"], status["last_flush"])
    assert status["uploaded_total"] == 6
    assert status["pending"] == 0
    assert status["last_flush"]["acked"] == 4  # most recent replay round
    svc3.close()


def test_no_duplicate_upload_count_across_restarts(tmp_path):
    """A row sent but unacked before a crash is re-sent (at-least-once) but
    must be counted exactly once in uploaded_total."""
    db = str(tmp_path / "gw.db")
    svc = GatewayService(db)
    svc.set_link_state("down")
    _ingest_n(svc, 3)
    svc.close()

    svc2 = GatewayService(db)  # healthy link: all 3 acked
    assert svc2.status()["uploaded_total"] == 3
    svc2.close()

    svc3 = GatewayService(db)  # nothing pending: nothing re-sent, counts stable
    status = svc3.status()
    log.info("input: restart with empty buffer")
    log.info("decision: uploaded_total=%d pending=%d sent=%s",
             status["uploaded_total"], status["pending"], svc3.link.sent)
    assert status["uploaded_total"] == 3
    assert status["pending"] == 0
    assert svc3.link.sent == []
    svc3.close()


def test_restart_replay_is_deterministic(tmp_path):
    """Same buffered input, same crash point -> identical upload sequences."""

    def run(db):
        svc = GatewayService(db)
        svc.set_link_state("down")
        _ingest_n(svc, 5)
        svc.close()
        dying = LimitedUplink(budget=2)
        svc2 = GatewayService(db, link=dying)
        part1 = [p for _, p in dying.sent]
        svc2.close()
        svc3 = GatewayService(db)
        part2 = [p for _, p in svc3.link.sent]
        uploaded = svc3.status()["uploaded_total"]
        svc3.close()
        return part1, part2, uploaded

    r1 = run(str(tmp_path / "a.db"))
    r2 = run(str(tmp_path / "b.db"))
    log.info("input: identical 5-record batch, crash after 2 sends, executed twice")
    log.info("decision: run1=%s run2=%s", r1, r2)
    assert r1 == r2
