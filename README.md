# 边缘遥测网关（Python）

在边缘侧模拟设备遥测采集、协议归一、离线缓冲、断网续传与链路/数据质量故障判定。
本地 SQLite 模拟边缘存储，内存 sink 模拟云端上行，使正常 / 抖动 / 断网场景的结果
可预期、可解释、可复现。

## 架构

```
main.py            FastAPI 包装层（/ingest/http、/ingest/mqtt、/link、/flush、观测接口）
app/
  models.py        统一遥测模型、枚举、MetricSpec、IngestResult、FaultEvent
  normalizer.py    两种协议字段归一、单位换算、字段/时间戳/单位合法性
  storage.py       SQLite：records / watermarks / spool / fault_states / fault_events
  buffer.py        容量背压与 FIFO 队列门面（容量判定在单事务内）
  uplink.py        链路 up/down、批量按序补传、内存 CloudSink
  detector.py      失联/停顿/越界判定 + 去抖状态机（仅翻转产生事件）
  gateway.py       门面编排：归一 -> 幂等/乱序/背压 -> 落库 -> 上行 -> 故障判定
tests/             归一化、去重/乱序/并发、缓冲写满、抖动、重启续传、故障、API
docs/              protocol.md / buffer.md / faults.md
```

## 关键能力速览

- **多协议归一**：HTTP JSON 与 MQTT 扁平载荷 -> 统一模型；线性单位换算（F/K/Pa/bar…）。
- **可区分拒绝**：`missing_required_field / invalid_value / invalid_timestamp /
  unknown_metric / invalid_unit / duplicate_report / timestamp_out_of_order /
  buffer_full`，均带 detail 判定依据，不静默丢弃。
- **并发幂等**：`source|device|metric|ts_ms` 键 + SQLite UNIQUE 硬约束，并发只入库一次。
- **断网续传**：单事务落库入队、容量背压明确拒绝、seq FIFO 补传不重不漏、重启可续。
- **故障判定**：失联 / 数据停顿 / 越界，连续 `confirm_count` 次确认才翻转，
  只在翻转时告警；显式时钟驱动，结论确定可复现。

## 本地验证

环境：Python 3.14、uv。

```bash
uv sync                                   # 安装依赖
uv run python -m pytest                   # 全量单测（默认打印 INFO 日志：输入+判定依据）
uv run python -m pytest -k restart -vv    # 只看重启续传
uv run python -m pytest -k concurrent -vv # 只看并发幂等
```

启动 HTTP 服务手工验证：

```bash
uv run fastapi dev main.py
# 正常上报
curl -s -X POST localhost:8000/ingest/http -H 'Content-Type: application/json' \
  -d '{"device_id":"d1","metric":"temperature","value":25,"unit":"C","ts_ms":1700000000000}'
# 断网 -> 上报 -> 查看缓冲 -> 恢复补传
curl -s -X POST localhost:8000/link/down
curl -s -X POST localhost:8000/ingest/mqtt -H 'Content-Type: application/json' \
  -d '{"dev":"d1","k":"temp","v":77,"u":"F","ts":1700000001.0}'
curl -s localhost:8000/spool
curl -s -X POST localhost:8000/link/up
```

默认文件库为 `data/gateway.db`（已被 .gitignore 忽略时请自行确认；删除文件即可重置）。

## 文档

- [协议归一规则](docs/protocol.md)
- [缓冲与续传语义](docs/buffer.md)
- [故障判定阈值](docs/faults.md)
