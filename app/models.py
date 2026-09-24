"""统一遥测模型与公共类型契约。"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class SourceProtocol(str, Enum):
    """接入协议来源。"""
    HTTP_JSON = "http_json"
    MQTT_CBORISH = "mqtt_flat"  # 第二种上报方式：扁平 MQTT 载荷


class RejectReason(str, Enum):
    """归一化 / 入库拒绝原因（可区分、可解释）。"""
    MISSING_REQUIRED_FIELD = "missing_required_field"
    INVALID_TIMESTAMP = "invalid_timestamp"
    TIMESTAMP_OUT_OF_ORDER = "timestamp_out_of_order"
    INVALID_UNIT = "invalid_unit"
    INVALID_VALUE = "invalid_value"
    UNKNOWN_METRIC = "unknown_metric"
    DUPLICATE_REPORT = "duplicate_report"
    BUFFER_FULL = "buffer_full"
    STORE_CLOSED = "store_closed"
    UNKNOWN_SOURCE = "unknown_source"


class LinkStatus(str, Enum):
    UP = "up"
    DOWN = "down"


class FaultType(str, Enum):
    DEVICE_OFFLINE = "device_offline"
    DATA_STALL = "data_stall"
    VALUE_OUT_OF_RANGE = "value_out_of_range"


class FaultState(str, Enum):
    NORMAL = "normal"
    FAULT = "fault"


@dataclass(frozen=True)
class MetricSpec:
    """指标规格：合法单位集合（以规范单位为基准）、允许范围与单位换算。"""
    name: str
    canonical_unit: str
    allowed_units: frozenset[str]
    min_value: float
    max_value: float
    # 非规范单位 -> (换算函数语义占位，具体实现见 normalizer)
    unit_factors: dict[str, float] = field(default_factory=dict)


@dataclass(frozen=True)
class TelemetryRecord:
    """归一化后的统一遥测记录。"""
    device_id: str
    metric: str
    value: float
    unit: str                 # 规范单位
    ts_ms: int                # 采集时间戳（毫秒）
    source: SourceProtocol
    raw_idempotency_key: str  # 幂等键（设备+指标+原始时间戳）
    received_ts_ms: int       # 网关接收时间戳
    in_range: bool = True


@dataclass(frozen=True)
class IngestResult:
    """单条投递的判定结果。"""
    accepted: bool
    reason: RejectReason | None = None
    detail: str = ""
    record: TelemetryRecord | None = None
    seq: int | None = None    # 入库 / 入缓冲后的全局序号


@dataclass(frozen=True)
class FaultEvent:
    """故障状态迁移事件（仅状态翻转时产生，避免重复告警）。"""
    device_id: str
    metric: str               # 设备级故障使用 "*"
    fault_type: FaultType
    state: FaultState
    ts_ms: int
    detail: str = ""
