"""多协议接入与字段归一。

两种接入方式：
1. HTTP_JSON: {"device_id", "metric", "value", "unit", "ts_ms"}
2. MQTT_FLAT: {"dev", "k"(指标别名), "v", "u"(单位别名), "ts"(秒级浮点)}

归一规则详见 docs/protocol.md。本模块只做"单条载荷 -> 统一模型"的纯转换，
非法输入抛出携带 RejectReason 的 RejectError，不做静默丢弃。
"""
from __future__ import annotations

import logging
import time
from typing import Any

from app.models import MetricSpec, RejectReason, SourceProtocol, TelemetryRecord

logger = logging.getLogger("telemetry.normalizer")

# ---------------------------------------------------------------------------
# 指标注册表
# ---------------------------------------------------------------------------

def _spec(name: str, canonical_unit: str, units: dict[str, tuple[float, float]],
          min_value: float, max_value: float) -> MetricSpec:
    """units: 允许单位 -> (factor, offset)，规范值 = 原始值 * factor + offset。"""
    return MetricSpec(
        name=name,
        canonical_unit=canonical_unit,
        allowed_units=frozenset(units),
        min_value=min_value,
        max_value=max_value,
        unit_factors=dict(units),
    )


METRIC_SPECS: dict[str, MetricSpec] = {
    "temperature": _spec(
        "temperature", "C",
        {"C": (1.0, 0.0), "celsius": (1.0, 0.0), "°C": (1.0, 0.0),
         "F": (5.0 / 9.0, -160.0 / 9.0), "K": (1.0, -273.15)},
        min_value=-40.0, max_value=125.0,
    ),
    "pressure": _spec(
        "pressure", "kPa",
        {"kPa": (1.0, 0.0), "Pa": (0.001, 0.0), "hPa": (0.1, 0.0),
         "bar": (100.0, 0.0), "mbar": (0.1, 0.0)},
        min_value=0.0, max_value=200.0,
    ),
    "humidity": _spec(
        "humidity", "%RH",
        {"%RH": (1.0, 0.0), "rh": (1.0, 0.0), "%": (1.0, 0.0)},
        min_value=0.0, max_value=100.0,
    ),
}

# MQTT 载荷的指标短名 -> 规范指标名
MQTT_METRIC_ALIASES: dict[str, str] = {
    "temp": "temperature",
    "press": "pressure",
    "hum": "humidity",
}


def register_metric(spec: MetricSpec) -> None:
    METRIC_SPECS[spec.name] = spec


class RejectError(Exception):
    """归一化阶段拒绝（携带可区分原因）。"""

    def __init__(self, reason: RejectReason, detail: str = "",
                 payload: Any | None = None):
        super().__init__(f"{reason.value}: {detail}")
        self.reason = reason
        self.detail = detail
        self.payload = payload


# ---------------------------------------------------------------------------
# 字段提取（按协议）
# ---------------------------------------------------------------------------

def _require(payload: dict, key: str) -> Any:
    if not isinstance(payload, dict) or key not in payload or payload[key] is None:
        raise RejectError(RejectReason.MISSING_REQUIRED_FIELD,
                          f"missing field '{key}'", payload)
    return payload[key]


def _coerce_float(raw: Any, field_name: str, payload: dict) -> float:
    if isinstance(raw, bool):  # bool 是 int 子类，显式排除
        raise RejectError(RejectReason.INVALID_VALUE,
                          f"field '{field_name}' must be number, got bool", payload)
    try:
        return float(raw)
    except (TypeError, ValueError):
        raise RejectError(RejectReason.INVALID_VALUE,
                          f"field '{field_name}' must be number, got {raw!r}",
                          payload) from None


def _parse_ts_ms(raw: Any, source: SourceProtocol, payload: dict) -> int:
    if source is SourceProtocol.HTTP_JSON:
        # 期望整数毫秒
        if isinstance(raw, bool) or not isinstance(raw, int):
            raise RejectError(RejectReason.INVALID_TIMESTAMP,
                              f"ts_ms must be int milliseconds, got {raw!r}",
                              payload)
        ts_ms = raw
    else:
        # MQTT: 秒级，可为浮点
        seconds = _coerce_float(raw, "ts", payload)
        ts_ms = int(round(seconds * 1000.0))
    if ts_ms <= 0:
        raise RejectError(RejectReason.INVALID_TIMESTAMP,
                          f"timestamp must be positive epoch ms, got {ts_ms}",
                          payload)
    return ts_ms


def _extract_http(payload: dict) -> tuple[str, str, float, str | None, int]:
    device_id = _require(payload, "device_id")
    metric = _require(payload, "metric")
    value = _coerce_float(_require(payload, "value"), "value", payload)
    unit = payload.get("unit")
    ts_ms = _parse_ts_ms(_require(payload, "ts_ms"), SourceProtocol.HTTP_JSON, payload)
    if not isinstance(device_id, str) or not device_id.strip():
        raise RejectError(RejectReason.MISSING_REQUIRED_FIELD,
                          "device_id must be non-empty string", payload)
    if not isinstance(metric, str) or not metric.strip():
        raise RejectError(RejectReason.MISSING_REQUIRED_FIELD,
                          "metric must be non-empty string", payload)
    return device_id.strip(), metric.strip(), value, unit, ts_ms


def _extract_mqtt(payload: dict) -> tuple[str, str, float, str | None, int]:
    device_id = _require(payload, "dev")
    short_metric = _require(payload, "k")
    value = _coerce_float(_require(payload, "v"), "v", payload)
    unit = payload.get("u")
    ts_ms = _parse_ts_ms(_require(payload, "ts"), SourceProtocol.MQTT_CBORISH, payload)
    if not isinstance(device_id, str) or not device_id.strip():
        raise RejectError(RejectReason.MISSING_REQUIRED_FIELD,
                          "dev must be non-empty string", payload)
    if not isinstance(short_metric, str):
        raise RejectError(RejectReason.MISSING_REQUIRED_FIELD,
                          "k must be string", payload)
    metric = MQTT_METRIC_ALIASES.get(short_metric.strip())
    if metric is None:
        raise RejectError(RejectReason.UNKNOWN_METRIC,
                          f"unknown metric alias '{short_metric}'", payload)
    return device_id.strip(), metric, value, unit, ts_ms


# ---------------------------------------------------------------------------
# 归一主流程
# ---------------------------------------------------------------------------

def normalize(
    payload: dict,
    source: SourceProtocol,
    received_ts_ms: int | None = None,
) -> TelemetryRecord:
    """把单条原始载荷归一为 TelemetryRecord；非法时抛出 RejectError。"""
    if received_ts_ms is None:
        received_ts_ms = time.time_ns() // 1_000_000

    if source is SourceProtocol.HTTP_JSON:
        device_id, metric, raw_value, raw_unit, ts_ms = _extract_http(payload)
    elif source is SourceProtocol.MQTT_CBORISH:
        device_id, metric, raw_value, raw_unit, ts_ms = _extract_mqtt(payload)
    else:
        raise RejectError(RejectReason.UNKNOWN_SOURCE,
                          f"unsupported source {source!r}", payload)

    spec = METRIC_SPECS.get(metric)
    if spec is None:
        raise RejectError(RejectReason.UNKNOWN_METRIC,
                          f"unknown metric '{metric}'", payload)

    unit = (raw_unit or spec.canonical_unit).strip()
    if unit not in spec.allowed_units:
        logger.info("reject source=%s dev=%s reason=invalid_unit unit=%r allowed=%s",
                    source.value, device_id, unit, sorted(spec.allowed_units))
        raise RejectError(RejectReason.INVALID_UNIT,
                          f"unit '{unit}' not allowed for metric '{metric}', "
                          f"allowed={sorted(spec.allowed_units)}", payload)

    factor, offset = spec.unit_factors[unit]
    value = raw_value * factor + offset
    in_range = spec.min_value <= value <= spec.max_value

    # 幂等键含来源前缀与归一后的采集毫秒：
    # 同协议同采集时刻的重投碰撞去重；不同协议即使 ts_ms 相同也属不同来源。
    record = TelemetryRecord(
        device_id=device_id,
        metric=metric,
        value=value,
        unit=spec.canonical_unit,
        ts_ms=ts_ms,
        source=SourceProtocol(source),
        raw_idempotency_key=f"{source.value}|{device_id}|{metric}|{ts_ms}",
        received_ts_ms=received_ts_ms,
        in_range=in_range,
    )
    logger.info(
        "normalized source=%s dev=%s metric=%s raw=%s%s -> %.4f%s ts_ms=%d "
        "in_range=%s key=%s",
        source.value, device_id, metric, raw_value, unit, value,
        spec.canonical_unit, ts_ms, in_range, record.raw_idempotency_key,
    )
    return record
