"""Unified telemetry model and shared enums."""
from __future__ import annotations

import enum
from dataclasses import dataclass, field


class RejectReason(enum.Enum):
    """Why a record was rejected (distinguishable, never silent)."""
    MISSING_FIELD = "missing_field"
    UNKNOWN_METRIC = "unknown_metric"
    INVALID_UNIT = "invalid_unit"
    INVALID_VALUE = "invalid_value"
    INVALID_TIMESTAMP = "invalid_timestamp"
    UNKNOWN_PROTOCOL = "unknown_protocol"
    BUFFER_FULL = "buffer_full"


class DedupReason(enum.Enum):
    """Why a record was deduplicated instead of stored."""
    EXACT_DUPLICATE = "exact_duplicate"
    CONFLICTING_DUPLICATE = "conflicting_duplicate"


class ReasonCode(str, enum.Enum):
    """Unified, stable, enumerable classification of non-plain-accepted outcomes.

    Callers switch on the code directly -- no need to parse human-readable
    detail text. Covers rejects, dedup and accepted-but-flagged outcomes so
    the operations summary can aggregate every record by exactly one code.
    """
    UNKNOWN_METRIC = "unknown_metric"              # 未知指标
    MISSING_FIELD = "missing_field"                # 字段缺失
    INVALID_UNIT = "invalid_unit"                  # 单位非法
    INVALID_VALUE = "invalid_value"                # 数值非法
    INVALID_TIMESTAMP = "invalid_timestamp"        # 时间戳非法
    OUT_OF_ORDER_TIMESTAMP = "out_of_order_timestamp"  # 时间乱序（接受但标记）
    DUPLICATE = "duplicate"                        # 重复上报（去重）
    BUFFER_FULL = "buffer_full"                    # 缓冲写满（背压拒绝）
    UNKNOWN_PROTOCOL = "unknown_protocol"          # 协议无法识别


# RejectReason -> ReasonCode (1:1 by value).
REJECT_TO_REASON: dict["RejectReason", ReasonCode] = {
    RejectReason.MISSING_FIELD: ReasonCode.MISSING_FIELD,
    RejectReason.UNKNOWN_METRIC: ReasonCode.UNKNOWN_METRIC,
    RejectReason.INVALID_UNIT: ReasonCode.INVALID_UNIT,
    RejectReason.INVALID_VALUE: ReasonCode.INVALID_VALUE,
    RejectReason.INVALID_TIMESTAMP: ReasonCode.INVALID_TIMESTAMP,
    RejectReason.UNKNOWN_PROTOCOL: ReasonCode.UNKNOWN_PROTOCOL,
    RejectReason.BUFFER_FULL: ReasonCode.BUFFER_FULL,
}


class FaultType(enum.Enum):
    DEVICE_OFFLINE = "device_offline"
    DATA_STALL = "data_stall"
    VALUE_OUT_OF_RANGE = "value_out_of_range"


# Allowed units per metric kind; anything else -> INVALID_UNIT.
ALLOWED_UNITS: dict[str, frozenset[str]] = {
    "temperature": frozenset({"celsius", "fahrenheit", "kelvin"}),
    "humidity": frozenset({"percent"}),
    "pressure": frozenset({"hpa", "kpa", "pa"}),
    "voltage": frozenset({"v", "mv"}),
    "current": frozenset({"a", "ma"}),
}

# Physical plausible ranges (in canonical unit) for VALUE_OUT_OF_RANGE checks.
VALUE_RANGES: dict[str, tuple[float, float]] = {
    "temperature": (-100.0, 200.0),   # celsius
    "humidity": (0.0, 100.0),         # percent
    "pressure": (0.0, 2000.0),        # hpa
    "voltage": (0.0, 1000.0),         # v
    "current": (0.0, 1000.0),         # a
}


@dataclass(frozen=True)
class TelemetryRecord:
    """Canonical telemetry record after protocol normalization."""
    device_id: str
    metric: str
    value: float
    unit: str
    timestamp_ms: int

    @property
    def dedup_key(self) -> str:
        return f"{self.device_id}|{self.metric}|{self.timestamp_ms}"


@dataclass
class IngestOutcome:
    """Per-record result of an ingest attempt."""
    status: str  # "accepted" | "rejected" | "duplicate"
    record: TelemetryRecord | None = None
    reject_reason: RejectReason | None = None
    dedup_reason: DedupReason | None = None
    detail: str = ""
    out_of_order: bool = False  # accepted but device timestamp went backwards

    @property
    def reason_codes(self) -> list["ReasonCode"]:
        """Stable classification of this outcome for the ops summary."""
        codes: list[ReasonCode] = []
        if self.reject_reason is not None:
            codes.append(REJECT_TO_REASON[self.reject_reason])
        if self.dedup_reason is not None:
            codes.append(ReasonCode.DUPLICATE)
        if self.out_of_order:
            codes.append(ReasonCode.OUT_OF_ORDER_TIMESTAMP)
        return codes


@dataclass
class FaultEvent:
    device_id: str
    fault: FaultType
    active: bool
    detail: str = ""
    metric: str | None = None
    at_ms: int = 0  # observation time that triggered the transition
