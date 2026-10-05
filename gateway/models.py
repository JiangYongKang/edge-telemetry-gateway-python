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


class ReasonCode(enum.Enum):
    """Unified, stable, enumerable reason taxonomy for ops statistics.

    One code per distinguishable cause, across rejected / deduplicated /
    flagged records. Callers switch on the enum directly -- no need to
    parse human-readable detail strings.
    """
    UNKNOWN_METRIC = "unknown_metric"                # 未知指标
    MISSING_FIELD = "missing_field"                  # 字段缺失
    INVALID_UNIT = "invalid_unit"                    # 单位非法
    INVALID_VALUE = "invalid_value"                  # 数值非法
    INVALID_TIMESTAMP = "invalid_timestamp"          # 时间戳非法
    OUT_OF_ORDER_TIMESTAMP = "out_of_order_timestamp"  # 时间乱序
    DUPLICATE = "duplicate"                          # 重复上报
    BUFFER_FULL = "buffer_full"                      # 缓冲写满
    UNKNOWN_PROTOCOL = "unknown_protocol"            # 协议无法识别


# RejectReason -> ReasonCode (names/values intentionally aligned).
_REJECT_TO_CODE: dict["RejectReason", ReasonCode] = {
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
    flagged_code: ReasonCode | None = None  # e.g. OUT_OF_ORDER_TIMESTAMP on accepted records

    @property
    def reason_code(self) -> ReasonCode | None:
        """Unified ReasonCode for stats; None for a clean accepted record."""
        if self.reject_reason is not None:
            return _REJECT_TO_CODE[self.reject_reason]
        if self.dedup_reason is not None:
            return ReasonCode.DUPLICATE
        return self.flagged_code


@dataclass
class FaultEvent:
    device_id: str
    fault: FaultType
    active: bool
    detail: str = ""
    metric: str | None = None
    at_ms: int = 0  # timestamp of the observation that caused the transition
