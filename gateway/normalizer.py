"""Protocol adapters: normalize raw payloads into TelemetryRecord.

Supported inbound protocols (auto-detected by the ``protocol`` key):

* ``json_v1`` -- HTTP-style JSON batch:
  ``{"protocol": "json_v1", "device_id": "d1",
     "readings": [{"metric": "temperature", "value": 21.5,
                   "unit": "celsius", "ts": 1700000000000}, ...]}``
* ``csv_v1`` -- serial/MQTT-style CSV lines:
  ``{"protocol": "csv_v1",
     "lines": ["d1,temperature,21.5,celsius,1700000000000", ...]}``

All values are converted to canonical units (celsius, percent, hpa, v, a).
Bad records are rejected with a distinguishable RejectReason, never dropped
silently and never raising.
"""
from __future__ import annotations

from gateway.models import (
    ALLOWED_UNITS,
    IngestOutcome,
    RejectReason,
    TelemetryRecord,
)

# Canonical unit per metric.
CANONICAL_UNIT: dict[str, str] = {
    "temperature": "celsius",
    "humidity": "percent",
    "pressure": "hpa",
    "voltage": "v",
    "current": "a",
}

# Conversion factors to the canonical unit: canonical = value * scale + offset.
_UNIT_CONVERSION: dict[str, dict[str, tuple[float, float]]] = {
    "temperature": {
        "celsius": (1.0, 0.0),
        "fahrenheit": (5.0 / 9.0, -32.0 * 5.0 / 9.0),
        "kelvin": (1.0, -273.15),
    },
    "humidity": {"percent": (1.0, 0.0)},
    "pressure": {
        "hpa": (1.0, 0.0),
        "kpa": (10.0, 0.0),
        "pa": (0.01, 0.0),
    },
    "voltage": {"v": (1.0, 0.0), "mv": (0.001, 0.0)},
    "current": {"a": (1.0, 0.0), "ma": (0.001, 0.0)},
}

# Timestamps must be positive and not more than this far in the future.
MAX_FUTURE_SKEW_MS = 24 * 3600 * 1000


def _reject(reason: RejectReason, detail: str) -> IngestOutcome:
    return IngestOutcome(status="rejected", reject_reason=reason, detail=detail)


def _build_record(
    device_id: object,
    metric: object,
    value: object,
    unit: object,
    ts: object,
    received_at_ms: int,
) -> IngestOutcome:
    """Validate one raw reading and convert to the canonical model."""
    if not isinstance(device_id, str) or not device_id.strip():
        return _reject(RejectReason.MISSING_FIELD, f"device_id missing/invalid: {device_id!r}")
    if not isinstance(metric, str) or not metric:
        return _reject(RejectReason.MISSING_FIELD, f"metric missing: {metric!r}")
    if metric not in ALLOWED_UNITS:
        return _reject(
            RejectReason.UNKNOWN_METRIC,
            f"unknown metric {metric!r}; known: {sorted(ALLOWED_UNITS)}",
        )
    if not isinstance(unit, str) or not unit:
        return _reject(RejectReason.MISSING_FIELD, f"unit missing: {unit!r}")
    if value is None:
        return _reject(RejectReason.MISSING_FIELD, "value missing")
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return _reject(RejectReason.INVALID_VALUE, f"value not numeric: {value!r}")
    if ts is None:
        return _reject(RejectReason.MISSING_FIELD, "timestamp missing")
    if isinstance(ts, bool) or not isinstance(ts, (int, float)):
        return _reject(RejectReason.INVALID_TIMESTAMP, f"timestamp not numeric: {ts!r}")

    unit_l = unit.lower()
    if unit_l not in ALLOWED_UNITS[metric]:
        return _reject(
            RejectReason.INVALID_UNIT,
            f"unit {unit!r} not allowed for metric {metric!r}; "
            f"allowed: {sorted(ALLOWED_UNITS[metric])}",
        )

    ts_ms = int(ts)
    if ts_ms <= 0 or ts_ms > received_at_ms + MAX_FUTURE_SKEW_MS:
        return _reject(
            RejectReason.INVALID_TIMESTAMP,
            f"timestamp {ts_ms} out of plausible range (now={received_at_ms})",
        )

    scale, offset = _UNIT_CONVERSION[metric][unit_l]
    canonical_value = float(value) * scale + offset

    return IngestOutcome(
        status="accepted",
        record=TelemetryRecord(
            device_id=device_id.strip(),
            metric=metric,
            value=round(canonical_value, 6),
            unit=CANONICAL_UNIT[metric],
            timestamp_ms=ts_ms,
        ),
    )


def _normalize_json_v1(payload: dict, received_at_ms: int) -> list[IngestOutcome]:
    device_id = payload.get("device_id")
    readings = payload.get("readings")
    if not isinstance(readings, list) or not readings:
        return [_reject(RejectReason.MISSING_FIELD, "json_v1 payload has no 'readings' list")]
    return [
        _build_record(
            device_id,
            r.get("metric") if isinstance(r, dict) else None,
            r.get("value") if isinstance(r, dict) else None,
            r.get("unit") if isinstance(r, dict) else None,
            r.get("ts") if isinstance(r, dict) else None,
            received_at_ms,
        )
        for r in readings
    ]


def _normalize_csv_v1(payload: dict, received_at_ms: int) -> list[IngestOutcome]:
    lines = payload.get("lines")
    if not isinstance(lines, list) or not lines:
        return [_reject(RejectReason.MISSING_FIELD, "csv_v1 payload has no 'lines' list")]
    outcomes: list[IngestOutcome] = []
    for line in lines:
        if not isinstance(line, str):
            outcomes.append(_reject(RejectReason.MISSING_FIELD, f"csv line not a string: {line!r}"))
            continue
        parts = [p.strip() for p in line.split(",")]
        if len(parts) != 5:
            outcomes.append(
                _reject(
                    RejectReason.MISSING_FIELD,
                    f"csv line needs 5 fields (device,metric,value,unit,ts): {line!r}",
                )
            )
            continue
        device, metric, raw_value, unit, raw_ts = parts
        try:
            value: object = float(raw_value)
        except ValueError:
            value = None
        try:
            ts: object = int(raw_ts)
        except ValueError:
            ts = None
        outcomes.append(_build_record(device, metric, value, unit, ts, received_at_ms))
    return outcomes


_ADAPTERS = {
    "json_v1": _normalize_json_v1,
    "csv_v1": _normalize_csv_v1,
}


def normalize(payload: dict, received_at_ms: int) -> list[IngestOutcome]:
    """Normalize one raw payload (protocol auto-detected) into outcomes."""
    if not isinstance(payload, dict):
        return [_reject(RejectReason.UNKNOWN_PROTOCOL, f"payload not an object: {type(payload).__name__}")]
    protocol = payload.get("protocol")
    adapter = _ADAPTERS.get(protocol)
    if adapter is None:
        return [
            _reject(
                RejectReason.UNKNOWN_PROTOCOL,
                f"unknown protocol {protocol!r}; supported: {sorted(_ADAPTERS)}",
            )
        ]
    return adapter(payload, received_at_ms)
