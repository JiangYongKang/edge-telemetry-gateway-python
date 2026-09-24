import logging

log = logging.getLogger("gateway-test")

NOW = 1_700_000_000_000


def json_payload(device, readings):
    return {"protocol": "json_v1", "device_id": device, "readings": readings}


def reading(metric, value, unit, ts):
    return {"metric": metric, "value": value, "unit": unit, "ts": ts}


def log_decision(context, inputs, outcomes):
    log.info("%s | input=%s", context, inputs)
    for o in outcomes:
        log.info(
            "  -> status=%s reject=%s dedup=%s detail=%s",
            o.status,
            o.reject_reason.value if o.reject_reason else None,
            o.dedup_reason.value if o.dedup_reason else None,
            o.detail,
        )
