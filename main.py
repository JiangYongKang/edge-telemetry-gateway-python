"""边缘遥测网关 HTTP 入口（fastapi 包装层，核心逻辑在 app 包内）。"""
from __future__ import annotations

from dataclasses import asdict

from fastapi import FastAPI

from app.gateway import EdgeGateway
from app.models import LinkStatus, SourceProtocol

app = FastAPI(title="edge-telemetry-gateway")
_gateway: EdgeGateway | None = None


def get_gateway() -> EdgeGateway:
    global _gateway
    if _gateway is None:
        _gateway = EdgeGateway(db_path="data/gateway.db")
    return _gateway


@app.post("/ingest/http")
def ingest_http(payload: dict):
    r = get_gateway().ingest(payload, SourceProtocol.HTTP_JSON)
    return asdict(r) | {"record": asdict(r.record) if r.record else None}


@app.post("/ingest/mqtt")
def ingest_mqtt(payload: dict):
    r = get_gateway().ingest(payload, SourceProtocol.MQTT_CBORISH)
    return asdict(r) | {"record": asdict(r.record) if r.record else None}


@app.post("/link/{status}")
def set_link(status: str):
    return get_gateway().set_link(LinkStatus(status))


@app.post("/flush")
def flush():
    return get_gateway().flush()


@app.post("/evaluate/{now_ms}")
def evaluate(now_ms: int):
    return get_gateway().evaluate_timeouts(now_ms)


@app.get("/faults/events")
def fault_events():
    return get_gateway().fault_events()


@app.get("/spool")
def spool():
    gw = get_gateway()
    return {"pending": gw.pending_seqs(),
            "capacity": gw.buffer.capacity,
            "delivered": gw.uplink.delivered_seqs()}
