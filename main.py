"""Edge telemetry gateway HTTP API."""
from __future__ import annotations

import os
import time

from fastapi import FastAPI

from gateway.service import GatewayService

DB_PATH = os.environ.get("GATEWAY_DB", "gateway.db")
BUFFER_CAPACITY = int(os.environ.get("GATEWAY_BUFFER_CAPACITY", "10000"))

app = FastAPI(title="edge-telemetry-gateway")
service = GatewayService(DB_PATH, buffer_capacity=BUFFER_CAPACITY)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _outcome_to_dict(o):
    return {
        "status": o.status,
        "reject_reason": o.reject_reason.value if o.reject_reason else None,
        "dedup_reason": o.dedup_reason.value if o.dedup_reason else None,
        "detail": o.detail,
        "record": o.record.__dict__ if o.record else None,
    }


@app.get("/")
def read_root():
    return {"service": "edge-telemetry-gateway", "link": service.link.state}


@app.post("/ingest")
def ingest(payload: dict):
    outcomes = service.ingest(payload, _now_ms())
    return {"outcomes": [_outcome_to_dict(o) for o in outcomes]}


@app.post("/link/{state}")
def set_link_state(state: str):
    service.set_link_state(state, _now_ms())
    return {"link": state}


@app.post("/flush")
def flush():
    return {"acked": service.flush_all(at_ms=_now_ms()), "pending": service.buffer.pending_count()}


@app.get("/buffer")
def buffer_status():
    return {"pending": service.buffer.pending_count()}


@app.get("/status")
def ops_status():
    """Ops view: link state, backlog ledger, replay progress, per-device
    faults with full episode history, and per-reason reject/dedup counts."""
    return service.status()


@app.post("/health/tick")
def health_tick(device_ids: list[str]):
    events = service.health_tick(device_ids, _now_ms())
    return {"events": [e.__dict__ | {"fault": e.fault.value} for e in events]}


@app.on_event("shutdown")
def shutdown():
    service.close()
