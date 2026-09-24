"""设备失联、数据停顿、数值越界判定 + 去抖 + 确定性复现。"""
import copy
import logging
import os

from app.gateway import EdgeGateway
from app.models import FaultState, FaultType, LinkStatus

log = logging.getLogger("test.detector")


def _p(ts, value=25.0, metric="temperature", unit="C", dev="d1"):
    return {"device_id": dev, "metric": metric, "value": value,
            "unit": unit, "ts_ms": ts}


def test_device_offline_debounce_and_recover():
    # 阈值 offline=10000ms，连续 2 次超时才判定
    gw = EdgeGateway(":memory:", link_status=LinkStatus.DOWN,
                     offline_ms=10_000, confirm_count=2)
    gw.ingest(_p(1_000))
    log.info("输入: t=1000 一条数据；age==阈值不判定(>边界)")

    assert gw.evaluate_timeouts(11_000) == []   # age=10000 边界不含
    log.info("t=11000 age==阈值 -> 正常")

    assert gw.evaluate_timeouts(11_001) == []   # 候选 1：去抖中
    log.info("t=11001 首次超时 -> 无事件（去抖）")

    e2 = gw.evaluate_timeouts(12_000)           # 候选 2：翻转
    log.info("t=12000 连续第二次超时 -> 事件=%s", e2)
    assert len(e2) == 1
    assert e2[0]["fault_type"] == FaultType.DEVICE_OFFLINE.value
    assert e2[0]["state"] == FaultState.FAULT.value

    assert gw.evaluate_timeouts(13_000) == []   # 不重复告警
    log.info("t=13000 持续离线 -> 无重复事件")

    # 新数据到达后恢复（恢复同样需要连续 2 次在线评估）
    gw.ingest(_p(13_000))
    assert gw.evaluate_timeouts(13_500) == []   # 恢复候选 1
    e_rec = gw.evaluate_timeouts(14_000)        # 恢复候选 2
    log.info("新数据后连续两次在线 -> 恢复事件=%s", e_rec)
    assert len(e_rec) == 1
    assert e_rec[0]["state"] == FaultState.NORMAL.value
    gw.close()


def test_data_stall_detects_frozen_value():
    # 阈值 stall=5000ms：最新两条值相等且超窗才算停顿
    gw = EdgeGateway(":memory:", link_status=LinkStatus.DOWN,
                     stall_ms=5_000, confirm_count=2)
    gw.ingest(_p(1_000, value=20.0))
    gw.ingest(_p(2_000, value=20.0))
    log.info("输入: t=1000,2000 两条同值20")

    assert gw.evaluate_timeouts(2_000) == []    # age=0 未超窗
    assert gw.evaluate_timeouts(7_000) == []    # age=5000 边界不含
    assert gw.evaluate_timeouts(8_001) == []    # 超窗候选 1
    events = gw.evaluate_timeouts(9_000)        # 候选 2 翻转
    log.info("值冻结且 age>5000 连续2次 -> %s", events)
    assert len(events) == 1
    assert events[0]["fault_type"] == FaultType.DATA_STALL.value
    assert events[0]["state"] == FaultState.FAULT.value

    # 新值到达：on_record 即产生恢复候选 1，再评估一次翻转
    gw.ingest(_p(8_500, value=21.0))
    events2 = gw.evaluate_timeouts(8_600)
    log.info("新值21到达后恢复 -> %s", events2)
    assert len(events2) == 1
    assert events2[0]["state"] == FaultState.NORMAL.value
    gw.close()


def test_value_out_of_range_debounce():
    gw = EdgeGateway(":memory:", link_status=LinkStatus.DOWN,
                     confirm_count=2)
    log.info("temperature 合法范围 [-40,125]C")
    gw.ingest(_p(1_000, value=200.0))
    assert gw.fault_events() == []
    log.info("第1条 200C 越界 -> 仅候选，无事件")

    gw.ingest(_p(2_000, value=201.0))
    types = [(e["fault_type"], e["state"]) for e in gw.fault_events()]
    log.info("第2条越界 -> 事件=%s；总入库=%d", types,
             len(gw.store.all_records()))
    assert (FaultType.VALUE_OUT_OF_RANGE.value, FaultState.FAULT.value) in types
    assert len(gw.store.all_records()) == 2  # 质量问题不拒收

    gw.ingest(_p(3_000, value=20.0))
    gw.ingest(_p(4_000, value=20.0))
    states = {s["fault_type"]: s["state"] for s in gw.fault_states()}
    log.info("两条合法后状态=%s", states)
    assert states[FaultType.VALUE_OUT_OF_RANGE.value] == FaultState.NORMAL.value
    gw.close()


def test_reproducible_same_inputs_same_events(tmp_path):
    """同一输入序列（记录 + 单调评估时钟）跑两遍，事件结论逐条一致。"""
    db1 = os.path.join(tmp_path, "a.db")
    db2 = os.path.join(tmp_path, "b.db")

    def run(db: str):
        gw = EdgeGateway(db, link_status=LinkStatus.DOWN,
                         offline_ms=10_000, stall_ms=5_000, confirm_count=2)
        for ts, val in [(1_000, 20.0), (2_000, 20.0), (12_500, 20.0)]:
            gw.ingest(_p(ts, value=val))
        # 单调前进的评估时钟（注意：不读取系统时钟，结论确定）
        for now in (7_000, 8_001, 9_000, 12_001, 13_000, 23_000, 24_000):
            gw.evaluate_timeouts(now)
        events = [
            (e["device_id"], e["metric"], e["fault_type"], e["state"],
             e["ts_ms"], e["detail"])
            for e in gw.fault_events()
        ]
        states = {
            (s["stream_key"]): (s["state"], s["cand_state"], s["cand_count"])
            for s in gw.fault_states()
        }
        gw.close()
        return events, states

    e1, s1 = run(db1)
    e2, s2 = run(db2)
    log.info("两次运行各产生事件 %d 条，内容逐条一致=%s", len(e1), e1 == e2)
    for ev in e1:
        log.info("事件 %s", ev[:5])
    assert e1 == e2
    assert s1 == s2
    # 至少包含一次停顿、一次失联的故障与恢复，证明判定确实生效且可复现
    kinds = {(e[2], e[3]) for e in e1}
    assert ("data_stall", "fault") in kinds
    assert ("device_offline", "fault") in kinds
