"""协议归一单测：两种来源、字段缺失、单位非法、时间戳非法、指标未知、换算。"""
import logging

import pytest

from app.models import RejectReason, SourceProtocol
from app.normalizer import RejectError, normalize

log = logging.getLogger("test.normalizer")


def test_http_json_basic():
    payload = {"device_id": "d1", "metric": "temperature",
               "value": 25.0, "unit": "C", "ts_ms": 1_000_000}
    log.info("input=%s", payload)
    r = normalize(payload, SourceProtocol.HTTP_JSON, received_ts_ms=2_000_000)
    log.info("判定: %s", r)
    assert r.value == 25.0 and r.unit == "C"
    assert r.raw_idempotency_key == "http_json|d1|temperature|1000000"


def test_mqtt_flat_alias_and_seconds():
    payload = {"dev": "d1", "k": "press", "v": 1013.0, "u": "hPa", "ts": 1.5}
    log.info("input=%s", payload)
    r = normalize(payload, SourceProtocol.MQTT_CBORISH, received_ts_ms=2000)
    log.info("判定: %s hPa->kPa 换算后 %.3f", r.metric, r.value)
    assert r.metric == "pressure" and r.unit == "kPa"
    assert r.value == pytest.approx(101.3)
    assert r.ts_ms == 1500  # 秒级 -> 毫秒


def test_fahrenheit_conversion():
    r = normalize({"device_id": "d", "metric": "temperature",
                   "value": 32, "unit": "F", "ts_ms": 1},
                  SourceProtocol.HTTP_JSON)
    assert r.value == pytest.approx(0.0)


@pytest.mark.parametrize("payload,reason", [
    ({"metric": "temperature", "value": 1, "unit": "C", "ts_ms": 1},
     RejectReason.MISSING_REQUIRED_FIELD),
    ({"device_id": "d", "metric": "temperature", "value": 1, "unit": "C"},
     RejectReason.MISSING_REQUIRED_FIELD),
    ({"device_id": "d", "metric": "temperature", "value": "x", "unit": "C",
      "ts_ms": 1}, RejectReason.INVALID_VALUE),
    ({"device_id": "d", "metric": "temperature", "value": 1, "unit": "X",
      "ts_ms": 1}, RejectReason.INVALID_UNIT),
    ({"device_id": "d", "metric": "nope", "value": 1, "unit": "C", "ts_ms": 1},
     RejectReason.UNKNOWN_METRIC),
    ({"device_id": "d", "metric": "temperature", "value": 1, "unit": "C",
      "ts_ms": -1}, RejectReason.INVALID_TIMESTAMP),
    ({"device_id": "d", "metric": "temperature", "value": 1, "unit": "C",
      "ts_ms": 1.5}, RejectReason.INVALID_TIMESTAMP),
])
def test_reject_reasons_distinguishable(payload, reason):
    log.info("input=%s 期望拒绝原因=%s", payload, reason.value)
    with pytest.raises(RejectError) as ei:
        normalize(payload, SourceProtocol.HTTP_JSON)
    log.info("实际拒绝原因=%s detail=%s", ei.value.reason.value, ei.value.detail)
    assert ei.value.reason is reason
