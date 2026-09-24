# 离线缓冲与断网续传语义

## 1. 数据流

```
上报 -> 归一化 -> 幂等/乱序/容量判定 -> records 落库 + spool 入队（单事务）
                                   -> 链路 UP：立即按 FIFO flush 到云端 sink
                                   -> 链路 DOWN：留在 spool
```

- 每条被接受的记录获得全局递增 `seq`（SQLite AUTOINCREMENT），严格等于采集入库顺序，
  也是补传顺序。
- 记录与入队在**同一个 SQLite 事务**内完成：要么同时可见，要么同时回滚，不存在
  "入库但丢队"或"在队但无记录"。

## 2. 容量上限与背压

- 缓冲容量 `buffer_capacity`（默认 1000）。插入前统计 `spool` 行数，达到上限直接拒绝，
  返回 `reason=buffer_full`，detail 带容量值。**明确拒绝，不静默丢弃，也不淘汰旧数据。**
- 容量判定、乱序判定、唯一约束插入、水位推进、入队在一个 `BEGIN IMMEDIATE` 事务内，
  并发下容量也不会被突破。

## 3. 链路状态与补传

- `UP`：每次 ingest 成功后立即 flush；`DOWN`：不调用 sink，只排队。
- `DOWN -> UP`：切换时自动 flush；网关以 `UP` 启动且 spool 非空（上次进程残留）时
  也会立即 flush（启动续传）。
- flush 以 `flush_batch_size`（默认 100）分批，`ORDER BY seq ASC LIMIT n` 取出，
  发送成功后在一个事务内 `delivered=1` 并从 spool 删除。
- 顺序保证：seq FIFO → **不重**（ack 后删除，重复 flush 不再发送）、**不漏**
  （未 ack 的一直在 spool）。

## 4. 重启续传

- spool、records.delivered、水位、幂等键、故障状态全部在 SQLite 文件中持久化
  （启用 WAL）。
- 进程崩溃 / 重启后：
  - 已 ack 的记录 `delivered=1`，不在 spool，不会重发；
  - 未 ack 的记录仍在 spool，下次 `UP` 时按 seq 继续补传；
  - 已持久化的幂等键保证重启后重复投递仍然被识别为 `duplicate_report`。
- 崩溃边界语义：发送成功但 ack 事务未完成时，重开可能重发该批（at-least-once）；
  绝不会丢失。本实现中"发送"为本地内存 sink，ack 与发送在同一调用链，正常停止无重复。

## 5. 本地存储位置

- 默认文件库 `data/gateway.db`（WAL 模式，另有 `-wal`/`-shm` 临时文件）；
  测试用临时目录或 `:memory:`。
- 云端由 `CloudSink`（内存列表）模拟，`GET /spool` 可查看 pending / capacity / delivered。

## 6. 观测接口

| 接口 | 说明 |
| --- | --- |
| `POST /link/{up|down}` | 切换链路，DOWN->UP 返回当次 flush 汇总 |
| `POST /flush` | 手动触发补传（DOWN 时返回 skipped=link_down） |
| `GET /spool` | pending 的 seq 列表、容量、已投递 seq |
