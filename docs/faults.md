# 链路与数据质量故障判定

## 1. 三类故障

| 故障 | 主体 | 判定条件 |
| --- | --- | --- |
| `device_offline` | 设备（metric=`*`） | 当前时钟 `now_ms` 距该设备最近一条记录的 ts_ms 超过 `offline_ms` |
| `data_stall` | 设备+指标流 | 最新两条记录数值相等，且最新一条 ts_ms 距今超过 `stall_ms` |
| `value_out_of_range` | 设备+指标流 | 归一化后 `in_range=false`（见 protocol.md 范围表） |

默认阈值（构造 `EdgeGateway` 可覆盖）：

| 参数 | 默认 | 含义 |
| --- | --- | --- |
| `offline_ms` | 30000 | 设备失联阈值，严格大于（age == 阈值不算） |
| `stall_ms` | 20000 | 数据停顿阈值，严格大于 |
| `confirm_count` | 2 | 去抖确认次数：候选态连续出现 N 次才翻转 |

## 2. 状态稳定（去抖）机制

每个故障主体维护一个持久化状态机：`state`（稳定态 normal/fault）、
`cand_state` + `cand_count`（候选态与连续计数）。

- 每轮评估产出一个"候选态"。候选与当前态一致：计数清零，**绝不重复产生事件**。
- 候选与当前态不同且与上轮候选相同：计数 +1；候选切换：计数重置为 1。
- 计数达到 `confirm_count`：状态翻转，写一条 `FaultEvent`，随后进入新稳定态。
- 因此短时抖动（超时 1 次又恢复、单点越界、单条重复值）不会误报；
  恢复也需要连续 `confirm_count` 次正常，避免抖动来回告警。

## 3. 触发时机

- 新记录到达 `ingest`：评估该流的 `value_out_of_range` 与 `data_stall`。
- 显式时钟推进：`evaluate_timeouts(now_ms)` 评估所有设备失联与所有流停顿。
  生产中由定时调度以单调时钟周期调用。

## 4. 确定性 / 可复现

- 所有时间判断只用显式传入的 `now_ms`，不读系统时钟；相同输入序列产出相同事件序列。
- 状态机状态持久化在 `fault_states`，事件流水在 `fault_events`，进程重启后计数与结论延续。
- 事件仅在状态**翻转**时产生，同一故障持续期间不重复告警；可经
  `GET /faults/events` 或 `gateway.fault_events()` 查询。

## 5. 事件字段

`device_id, metric, fault_type, state(normal/fault), ts_ms, detail`，
`detail` 给出判定依据，例如 `last_ts=1000 age_ms=20001 threshold_ms=20000`、
`value=200.0000C in_range=False`、`latest_ts=2000 age_ms=6000 same_as_prev=True`。

## 6. 单测如何观测判定依据

测试以 INFO 日志打印输入载荷、每个评估时钟点与返回事件；代码侧在
`normalizer / storage / detector / uplink / gateway` 各 logger 输出判定依据。
运行 `uv run python -m pytest --log-cli-level=INFO` 可查看完整链路。
