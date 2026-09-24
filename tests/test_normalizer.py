"""Protocol normalization: field mapping, unit conversion, distinguishable rejects."""
from gateway.models import RejectReason
from gateway.normalizer import normalize
from tests.testutil import NOW, json_payload, log_decision, reading


def test_json_and_csv_map_to_same_model():
    j = normalize(json_payload("d1", [reading("temperature", 32, "fahrenheit", NOW)]), NOW)
    c = normalize({"protocol": "csv_v1", "lines": [f"d1,temperature,32,fahrenheit,{NOW}"]}, NOW)
    log_decision("json vs csv normalization", "32 fahrenheit via both protocols", j + c)
    assert j[0].status == c[0].status == "accepted"
    assert j[0].record == c[0].record
    assert j[0].record.unit == "celsius" and j[0].record.value == 0.0


def test_missing_field_rejected_with_reason():
    out = normalize(json_payload("d1", [{"metric": "temperature", "unit": "celsius", "ts": NOW}]), NOW)
    log_decision("missing value field", "reading without 'value'", out)
    assert out[0].status == "rejected"
    assert out[0].reject_reason == RejectReason.MISSING_FIELD


def test_invalid_unit_rejected_with_reason():
    out = normalize(json_payload("d1", [reading("temperature", 20, "yard", NOW)]), NOW)
    log_decision("invalid unit", "temperature in 'yard'", out)
    assert out[0].status == "rejected"
    assert out[0].reject_reason == RejectReason.INVALID_UNIT


def test_implausible_timestamp_rejected():
    out = normalize(json_payload("d1", [reading("humidity", 50, "percent", NOW + 10**10)]), NOW)
    log_decision("future timestamp", "ts 10^10 ms in the future", out)
    assert out[0].status == "rejected"
    assert out[0].reject_reason == RejectReason.INVALID_TIMESTAMP


def test_unknown_protocol_rejected():
    out = normalize({"protocol": "xml_v9", "data": "<x/>"}, NOW)
    log_decision("unknown protocol", "protocol=xml_v9", out)
    assert out[0].status == "rejected"
    assert out[0].reject_reason == RejectReason.UNKNOWN_PROTOCOL


def test_unit_conversion_to_canonical():
    cases = [
        ("temperature", 212, "fahrenheit", 100.0, "celsius"),
        ("temperature", 273.15, "kelvin", 0.0, "celsius"),
        ("pressure", 1013, "pa", 10.13, "hpa"),
        ("voltage", 3300, "mv", 3.3, "v"),
        ("current", 500, "ma", 0.5, "a"),
    ]
    for metric, value, unit, want_value, want_unit in cases:
        out = normalize(json_payload("d1", [reading(metric, value, unit, NOW)]), NOW)
        log_decision("unit conversion", f"{value} {unit}", out)
        assert out[0].status == "accepted"
        assert abs(out[0].record.value - want_value) < 1e-6
        assert out[0].record.unit == want_unit
