# 协议归一规则

边缘网关兼容两种设备上报方式，进入统一遥测模型 `TelemetryRecord`：

| 统一字段 | 含义 |
| --- | --- |
| `device_id` | 设备标识（去空白、非空） |
| `metric` | 规范指标名（temperature / pressure / humidity，可扩展） |
| `value` | 归一为规范单位后的数值（float） |
| `unit` | 规范单位（见下表） |
| `ts_ms` | 采集时间戳，统一为 epoch 毫秒 |
| `source` | 来源协议 `http_json` / `mqtt_flat` |
| `raw_idempotency_key` | 幂等键 |
| `received_ts_ms` | 网关接收时间（毫秒） |
| `in_range` | 归一后是否在指标合法范围内（越界仍入库，交故障判定） |

## 1. HTTP JSON（`http_json`）

```json
{"device_id": "d1", "metric": "temperature", "value": 25.0, "unit": "C", "ts_ms": 1700000000000}
```

字段同名直取；`ts_ms` 必须是正整数毫秒。

## 2. MQTT 扁平载荷（`mqtt_flat`）

```json
{"dev": "d1", "k": "temp", "v": 77.0, "u": "F", "ts": 1700000000.5}
```

字段映射：`dev→device_id`、`k→指标别名`、`v→value`、`u→单位别名`、`ts→秒级时间戳`（浮点，乘 1000 四舍五入为毫秒）。

指标别名：`temp→temperature`、`press→pressure`、`hum→humidity`。

## 3. 单位归一（线性换算 canonical = value × factor + offset）

| 指标 | 规范单位 | 允许单位 | 合法范围（规范单位） |
| --- | --- | --- | --- |
| temperature | `C` | C / celsius / °C / F / K | [-40, 125] |
| pressure | `kPa` | kPa / Pa / hPa / bar / mbar | [0, 200] |
| humidity | `%RH` | %RH / rh / % | [0, 100] |

- 华氏度：`C = F × 5/9 − 160/9`；开尔文：`C = K − 273.15`
- 压力：`Pa×0.001`、`hPa/mbar×0.1`、`bar×100` → kPa
- 缺省单位（未携带 `unit/u`）按规范单位处理
- 单位不在允许集合内：拒绝，原因 `invalid_unit`（不静默丢弃）

## 4. 拒绝原因（相互可区分）

| reason | 触发条件 |
| --- | --- |
| `missing_required_field` | 必填字段缺失、为空或类型不是字符串 |
| `invalid_value` | value 不能转为数值（含布尔值） |
| `invalid_timestamp` | HTTP ts_ms 非正整数；MQTT ts 非数值；时间戳 ≤ 0 |
| `unknown_metric` | 指标名 / MQTT 别名无法识别 |
| `invalid_unit` | 单位不在该指标允许集合 |
| `duplicate_report` | 幂等键已存在（见第 5 节） |
| `timestamp_out_of_order` | 同一 (device, metric) 的 ts_ms 小于已入库水位 |
| `buffer_full` | 离线缓冲达到容量上限 |

所有拒绝都返回 `IngestResult(accepted=False, reason, detail)`，`detail` 给出判定依据
（如水位值、容量值、允许单位集合），并记录 INFO 日志。

## 5. 幂等键与乱序

- 幂等键 = `source|device_id|metric|ts_ms`。同协议、同设备、同指标、同采集时刻的
  重复上报判定为重复，**只入库一次**。
- 幂等由 SQLite `records.idem_key` 的 UNIQUE 约束硬保证；并发投递时，抢到约束者成功，
  其余得到 `duplicate_report`。
- 乱序在幂等检查之后进行：每 (device, metric) 维护 `last_ts_ms` 水位，回退时间戳以
  `timestamp_out_of_order` 拒绝，且不推进水位。
- 跨协议即使归一后 ts_ms 相同，因 `source` 前缀不同视为不同来源上报，不互相去重。

## 6. 数值越界语义

越界是**数据质量问题而非拒收理由**：记录照常归一、入库、上行，`in_range=false`，
由故障判定模块按"连续越界确认次数"决定是否产生 `value_out_of_range` 故障
（见 docs/faults.md）。
