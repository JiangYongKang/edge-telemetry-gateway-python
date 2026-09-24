"""故障判定：设备失联、数据停顿、数值越界。

所有时间判定都以显式传入的 now_ms 为基准（不读取系统时钟），
因此同一批输入重复执行得到完全一致的结论。

去抖（状态稳定）：某候选状态必须连续出现 confirm_count 次才会真正翻转，
且仅在状态翻转时写入一条 FaultEvent，避免短时抖动误报与重复告警。
全部状态持久化在 fault_states 表，进程重启后继续累计。
"""
from __future__ import annotations

import logging

from app.models import FaultEvent, FaultState, FaultType

logger = logging.getLogger("telemetry.detector")

_DEVICE_METRIC = "*"  # 设备级故障使用的 metric 占位


class _FaultSubject:
    """单个故障主体的去抖状态机，状态行持久化在 store.fault_states。"""

    def __init__(self, store, key: str, device_id: str, metric: str,
                 fault_type: FaultType, confirm_count: int, now_ms: int):
        self._store = store
        self.key = key
        self.device_id = device_id
        self.metric = metric
        self.fault_type = fault_type
        self.confirm = max(1, confirm_count)
        row = store.fault_get(key)
        if row is None:
            self.state = FaultState.NORMAL
            self.cand_state = FaultState.NORMAL
            self.cand_count = 0
            self._persist(now_ms)
        else:
            self.state = FaultState(row["state"])
            self.cand_state = FaultState(row["cand_state"])
            self.cand_count = row["cand_count"]

    def _persist(self, now_ms: int, detail: str = "") -> None:
        self._store.fault_put(
            self.key, self.device_id, self.metric, self.fault_type.value,
            self.state.value, self.cand_state.value, self.cand_count,
            now_ms, detail)

    def observe(self, candidate: FaultState, now_ms: int,
                detail: str = "") -> list[FaultEvent]:
        """输入本轮判定候选态，做去抖；翻转时产出事件。"""
        events: list[FaultEvent] = []

        if candidate == self.state:
            # 候选与当前态一致：保持稳定，清空候选计数，绝不重复告警
            if self.cand_state != self.state or self.cand_count != 0:
                self.cand_state = self.state
                self.cand_count = 0
                self._persist(now_ms, detail)
            return events

        # 候选与当前态不同：累计同一候选的连续出现次数
        if candidate == self.cand_state:
            self.cand_count += 1
        else:
            self.cand_state = candidate
            self.cand_count = 1

        logger.info(
            "debounce key=%s type=%s state=%s candidate=%s streak=%d/%d %s",
            self.key, self.fault_type.value, self.state.value,
            candidate.value, self.cand_count, self.confirm, detail)

        if self.cand_count >= self.confirm:
            # 连续 confirm 次：真正翻转，候选计数清零并以新态为稳定态
            self.state = candidate
            self.cand_state = candidate
            self.cand_count = 0
            event = FaultEvent(
                device_id=self.device_id, metric=self.metric,
                fault_type=self.fault_type, state=self.state, ts_ms=now_ms,
                detail=detail)
            self._store.add_event(
                self.device_id, self.metric, self.fault_type.value,
                self.state.value, now_ms, detail)
            self._persist(now_ms, detail)
            events.append(event)
        else:
            self._persist(now_ms, detail)
        return events


class FaultDetector:
    def __init__(self, store, offline_ms: int = 30_000,
                 stall_ms: int = 20_000, confirm_count: int = 2):
        self._store = store
        self.offline_ms = offline_ms
        self.stall_ms = stall_ms
        self.confirm = confirm_count

    def _subject(self, device_id: str, metric: str,
                 fault_type: FaultType, now_ms: int) -> _FaultSubject:
        key = f"{fault_type.value}:{device_id}:{metric}"
        return _FaultSubject(self._store, key, device_id, metric,
                             fault_type, self.confirm, now_ms)

    # ------------------------------------------------------------------
    def on_record(self, record, now_ms: int | None = None) -> list[FaultEvent]:
        """新记录到达：评估数值越界与数据停顿。"""
        now_ms = record.ts_ms if now_ms is None else now_ms
        events: list[FaultEvent] = []

        # 1) 数值越界：连续 confirm 条越界才告警，连续合法才恢复
        oor = self._subject(record.device_id, record.metric,
                            FaultType.VALUE_OUT_OF_RANGE, now_ms)
        cand = FaultState.FAULT if not record.in_range else FaultState.NORMAL
        events += oor.observe(
            cand, now_ms,
            detail=(f"value={record.value:.4f}{record.unit} "
                    f"in_range={record.in_range}"))

        # 2) 数据停顿：新记录先做一次"是否仍停摆"的评估
        events += self._evaluate_stall(record.device_id, record.metric, now_ms)
        return events

    def _evaluate_stall(self, device_id: str, metric: str,
                        now_ms: int) -> list[FaultEvent]:
        latest, prev = self._store.last_two(device_id, metric)
        subj = self._subject(device_id, metric, FaultType.DATA_STALL, now_ms)
        if latest is None:
            return subj.observe(FaultState.NORMAL, now_ms, "no data yet")
        latest_value, latest_ts = latest
        age = now_ms - latest_ts
        stalled = (prev is not None
                   and latest_value == prev[0]
                   and age > self.stall_ms)
        detail = (f"latest_ts={latest_ts} age_ms={age} "
                  f"same_as_prev={prev is not None and latest_value == (prev[0] if prev else None)}")
        return subj.observe(
            FaultState.FAULT if stalled else FaultState.NORMAL, now_ms, detail)

    # ------------------------------------------------------------------
    def evaluate_timeouts(self, now_ms: int) -> list[FaultEvent]:
        """基于当前时钟评估设备失联与所有已知流的数据停顿。"""
        events: list[FaultEvent] = []

        # 设备失联：每个已知设备
        for device_id, last_ts in self._store.latest_per_device().items():
            subj = self._subject(device_id, _DEVICE_METRIC,
                                 FaultType.DEVICE_OFFLINE, now_ms)
            age = now_ms - last_ts
            offline = age > self.offline_ms
            events += subj.observe(
                FaultState.FAULT if offline else FaultState.NORMAL, now_ms,
                detail=f"last_ts={last_ts} age_ms={age} "
                       f"threshold_ms={self.offline_ms}")

        # 数据停顿：每个已知 (设备,指标) 流
        for device_id, metric in self._store.known_streams():
            events += self._evaluate_stall(device_id, metric, now_ms)
        return events

    def current_states(self) -> list[dict]:
        return [dict(r) for r in self._store.fault_all()]

    def events(self) -> list[dict]:
        return self._store.list_events()
