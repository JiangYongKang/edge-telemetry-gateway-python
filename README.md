# Edge Telemetry Gateway

边缘网关遥测采集与上行链路管理服务。使用本地 SQLite 模拟边缘侧缓冲与链路状态，
使设备数据在正常、抖动和断网场景下得到可预期、可解释的结果。

## 架构

```
原始报文 ──▶ normalizer ──▶ service ──▶ buffer (SQLite) ──▶ link/uplink ──▶ 云端(模拟)
           协议归一/校验        编排      持久缓冲/去重/背压     补传管理
                                │
                                └──▶ health  失联/停顿/越界 + 去抖
```

| 模块 | 职责 |
|---|---|
| `gateway/models.py` | 统一遥测模型 `TelemetryRecord`、拒绝/去重原因、故障类型枚举 |
| `gateway/normalizer.py` | 协议归一：字段映射、单位换算、入站校验 |
| `gateway/buffer.py` | SQLite 持久缓冲：容量上限、背压拒绝、幂等去重、按序补传 |
| `gateway/link.py` | 上行链路模拟（up/down/flaky）与补传管理器 |
| `gateway/health.py` | 故障判定：失联 / 数据停顿 / 数值越界，滞回去抖 |
| `gateway/service.py` | 编排：归一 → 乱序标记 → 缓冲/去重 → 健康观测 |
| `main.py` | FastAPI HTTP 接口 |

## 协议归一规则

支持两种上报方式，由报文中的 `protocol` 字段自动识别：

**`json_v1`（HTTP 风格 JSON 批量）**

```json
{"protocol": "json_v1", "device_id": "d1",
 "readings": [{"metric": "temperature", "value": 21.5, "unit": "celsius", "ts": 1700000000000}]}
```

**`csv_v1`（串口/MQTT 风格 CSV 行）**

```json
{"protocol": "csv_v1", "lines": ["d1,temperature,21.5,celsius,1700000000000"]}
```

两种协议映射到同一模型 `TelemetryRecord(device_id, metric, value, unit, timestamp_ms)`，
数值统一换算为规范单位：

| metric | 允许的入站单位 | 规范单位 |
|---|---|---|
| temperature | celsius / fahrenheit / kelvin | celsius |
| humidity | percent | percent |
| pressure | hpa / kpa / pa | hpa |
| voltage | v / mv | v |
| current | a / ma | a |

**拒绝原因（可区分，绝不静默丢弃）**——见 `models.RejectReason`：

- `missing_field`：device_id / metric / value / unit / ts 缺失或类型非法，或 metric 未知
- `invalid_unit`：单位不在该 metric 的允许集合内
- `invalid_timestamp`：时间戳 ≤ 0 或超过当前时间 24h 以上
- `unknown_protocol`：protocol 字段无法识别
- `buffer_full`：本地缓冲写满（背压，见下节）

**乱序时间戳**：设备时间戳回退的记录会被**接受并标记**
（outcome `detail` 含 `out_of_order_timestamp`），因为补传顺序以采集（到达）顺序为准，
设备时钟乱序不影响链路语义。

**重复上报（幂等去重）**——见 `models.DedupReason`：
去重键为 `device_id|metric|timestamp_ms`，在 SQLite 上以 UNIQUE 约束实现，
并发投递下由数据库原子判定，同一条记录无论多少线程同时投递只会入库一次：

- `exact_duplicate`：键相同且值相同（安全的重试）
- `conflicting_duplicate`：键相同但值不同（同一时刻上报了矛盾数据，需人工关注）

## 缓冲与续传语义

- **持久化**：`enqueue()` 先提交 SQLite（WAL 模式）再返回，进程重启后未补传数据仍在，
  重新打开同一 db 文件即可继续（见测试 `test_restart_resumes_pending_upload`）。
- **容量与背压**：缓冲有 `capacity` 上限（默认 10000，HTTP 服务可用
  环境变量 `GATEWAY_BUFFER_CAPACITY` 配置）。写满时 `enqueue` 抛出 `BufferFullError`，
  服务层将其转为 `buffer_full` 拒绝——明确拒绝，绝不静默丢弃。
- **断网续传**：链路 down 时数据全部落缓冲；恢复后 `flush` 按采集顺序（rowid 升序）
  逐条上传，只有被链路确认的行才会 ack；中途失败即停止，保证**不重不漏、不乱序**
  （at-least-once，下游按去重键幂等）。
- **链路抖动**：`SimulatedUplink` 的 `flaky` 状态按固定周期（每第 N 次尝试失败）
  模拟抖动，完全确定性——同一批输入重复执行得到完全相同的上传序列。

## 故障判定阈值与去抖

`HealthMonitor` 三个可配阈值（默认值）：

| 参数 | 默认 | 含义 |
|---|---|---|
| `offline_after_ms` | 60000 | 设备超过此时长无任何上报 → `device_offline` |
| `stall_after_ms` | 30000 | 设备仍在线但某 metric 的样本时间戳超过此时长未推进 → `data_stall` |
| `debounce_count` | 3 | 连续 N 次判定异常才告警，连续 N 次恢复正常才清除 |

数值越界（`value_out_of_range`）按规范单位的物理合理区间判定
（`models.VALUE_RANGES`，如温度 [-100, 200]°C、湿度 [0, 100]%）。

**去抖（滞回）**：每个故障键维护坏/好连续计数，只有计数达到 `debounce_count`
才产生状态迁移，`FaultEvent` 仅在迁移时发出。短时抖动（如一次毛刺越界、
一次心跳丢失）既不会误报，恢复时也不会抖动式反复告警；告警期间不会重复通知。
判定只依赖输入序列与时间戳，同一批输入重复执行结论完全一致
（测试 `test_same_input_batch_reproduces_same_events`）。

## 本地验证

```bash
uv sync                 # 安装依赖
uv run pytest -q        # 全部测试（19 个）
uv run pytest -q -s     # 带日志：每个用例打印输入与判定依据
```

测试覆盖：协议归一与单位换算、字段缺失/非法单位/非法时间戳拒绝、
精确与冲突重复、并发投递幂等（16 线程同一条记录）、缓冲写满背压、
乱序标记、断网缓冲与按序补传、链路抖动重放、重启续传、
失联/停顿/越界去抖、结论可复现。

启动 HTTP 服务：

```bash
uv run uvicorn main:app --port 8000
```

接口：

- `POST /ingest` — 上报遥测（json_v1 / csv_v1 报文），返回每条记录的判定结果
- `POST /link/{up|down|flaky}` — 切换模拟链路状态
- `POST /flush` — 触发一轮补传，返回 ack 数与剩余缓冲量
- `GET /buffer` — 查看待补传条数
- `POST /health/tick` — 传入设备列表做周期健康检查，返回故障迁移事件

示例：

```bash
curl -X POST localhost:8000/link/down
curl -X POST localhost:8000/ingest -H 'Content-Type: application/json' -d \
  '{"protocol":"csv_v1","lines":["d1,temperature,68,fahrenheit,1700000000000"]}'
curl -X POST localhost:8000/link/up
curl -X POST localhost:8000/flush
```
