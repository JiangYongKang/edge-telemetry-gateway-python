"""缓冲写满背压、断网/抖动补传顺序、重启续传不重不漏。"""
import logging
import os

from app.gateway import EdgeGateway
from app.models import LinkStatus, RejectReason

log = logging.getLogger("test.buffer")


def _temp_db(tmp_path, name="gw.db"):
    return os.path.join(tmp_path, name)


def test_buffer_full_explicit_reject(tmp_path):
    db = _temp_db(tmp_path)
    gw = EdgeGateway(db, buffer_capacity=3, link_status=LinkStatus.DOWN)
    results = []
    for i in range(5):
        p = {"device_id": "d1", "metric": "temperature", "value": 20.0 + i,
             "unit": "C", "ts_ms": 1000 + i * 1000}
        r = gw.ingest(p)
        results.append(r)
    log.info("输入: 容量3、链路DOWN 下连续投递5条; 结果=%s",
             [(r.accepted, r.reason.value if r.reason else None) for r in results])
    accepted = [r for r in results if r.accepted]
    rejected = [r for r in results if not r.accepted]
    assert len(accepted) == 3
    assert len(rejected) == 2
    assert all(r.reason is RejectReason.BUFFER_FULL for r in rejected)
    assert gw.pending_seqs() == [1, 2, 3]
    gw.close()


def test_down_then_up_replays_in_order_once(tmp_path):
    db = _temp_db(tmp_path)
    gw = EdgeGateway(db, buffer_capacity=100, link_status=LinkStatus.DOWN)
    for i in range(5):
        gw.ingest({"device_id": "d1", "metric": "temperature",
                   "value": 20.0 + i, "unit": "C",
                   "ts_ms": 1000 + i * 1000})
    log.info("断网期入队5条 pending=%s", gw.pending_seqs())
    assert gw.uplink.delivered_seqs() == []

    info = gw.set_link(LinkStatus.UP)
    log.info("恢复链路 flush 汇总=%s, 已投递 seq=%s",
             info, gw.uplink.delivered_seqs())
    assert info["flushed"] == 5
    assert gw.uplink.delivered_seqs() == [1, 2, 3, 4, 5]  # 按采集顺序
    assert gw.pending_seqs() == []
    # 再次 flush 不重复发送
    gw.flush()
    assert gw.uplink.delivered_seqs() == [1, 2, 3, 4, 5]
    gw.close()


def test_link_jitter_no_loss_no_dup(tmp_path):
    db = _temp_db(tmp_path)
    gw = EdgeGateway(db, buffer_capacity=100, link_status=LinkStatus.UP)
    # up: 立刻上行；down: 排队；up: 补传；down/up 抖动
    timeline = [
        (1, LinkStatus.UP, 1),
        (2, LinkStatus.DOWN, None),
        (3, LinkStatus.DOWN, None),
        (4, LinkStatus.UP, 4),     # 恢复 -> 补 2,3
        (5, LinkStatus.DOWN, None),
        (6, LinkStatus.UP, 6),     # 抖动恢复 -> 补 5
    ]
    for idx, (i, link, _) in enumerate(timeline):
        gw.set_link(link)
        r = gw.ingest({"device_id": "d1", "metric": "temperature",
                       "value": 20.0 + i, "unit": "C",
                       "ts_ms": i * 1000})
        log.info("抖动序列[%d] link=%s ingest ts=%d -> accepted=%s pending=%s",
                 idx, link.value, i * 1000, r.accepted, gw.pending_seqs())
        assert r.accepted
    assert gw.uplink.delivered_seqs() == [1, 2, 3, 4, 5, 6]
    assert gw.pending_seqs() == []
    gw.close()


def test_restart_resumes_spool(tmp_path):
    db = _temp_db(tmp_path)
    gw = EdgeGateway(db, buffer_capacity=100, link_status=LinkStatus.DOWN)
    for i in range(4):
        gw.ingest({"device_id": "d1", "metric": "temperature",
                   "value": 20.0 + i, "unit": "C",
                   "ts_ms": 1000 + i * 1000})
    gw.set_link(LinkStatus.UP)
    gw.ingest({"device_id": "d1", "metric": "temperature",
               "value": 24.0, "unit": "C", "ts_ms": 9999})
    assert len(gw.uplink.delivered_seqs()) == 5
    gw.close()

    log.info("== 模拟进程重启，重新打开同一个 DB（仍 DOWN）==")
    gw2 = EdgeGateway(db, buffer_capacity=100, link_status=LinkStatus.DOWN)
    # 已投递的不重复；无未补传
    assert gw2.pending_seqs() == []
    gw2.ingest({"device_id": "d1", "metric": "temperature",
                "value": 30.0, "unit": "C", "ts_ms": 20_000})
    assert gw2.pending_seqs() == [6]
    # 重启前已投递的记录仍在库中且 delivered=1
    delivered_before = [r.raw_idempotency_key for r in gw2.delivered()]
    log.info("重启后已投递记录数=%d, 新待补传=%s",
             len(delivered_before), gw2.pending_seqs())
    assert len(delivered_before) == 5

    log.info("== 重启后再次崩溃：未补传 seq=6 保留；换一个进程恢复 ==")
    gw2.close()
    gw3 = EdgeGateway(db, buffer_capacity=100, link_status=LinkStatus.UP)
    assert gw3.uplink.delivered_seqs() == [6]  # 只有新补传的进 sink
    assert gw3.pending_seqs() == []
    assert len(gw3.delivered()) == 6          # 库里累计全部已送达
    gw3.close()
