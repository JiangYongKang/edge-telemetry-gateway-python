"""Unified telemetry model and shared enums."""
from __future__ import annotations

import enum
from dataclasses import dataclass, field


class RejectReason(enum.Enum):
    """Why a record was rejected (distinguishable, never silent)."""
    MISSING_FIELD = "missing_field"
    INVALID_UNIT = "invalid_unit"
    INVALID_VALUE = "invalid_value"
    INVALID_TIMESTAMP = "invalid_timestamp"
    UNKNOWN_PROTOCOL = "unknown_protocol"
    BUFFER_FULL = "buffer_full"


class DedupReason(enum.Enum):
    """Why a record was deduplicated instead of stored."""
    EXACT_DUPLICATE = "exact_duplicate"
    CONFLICTING_DUPLICATE = "conflicting_duplicate"


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


@dataclass
class FaultEvent:
    device_id: str
    fault: FaultType
    active: bool
    detail: str = ""
    metric: str | None = None
