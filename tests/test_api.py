"""FastAPI 端到端冒烟（TestClient）。"""
import logging

import pytest

import main
from app.gateway import EdgeGateway

log = logging.getLogger("test.api")


@pytest.fixture()
def client(monkeypatch, tmp_path):
    gw = EdgeGateway(str(tmp_path / "api.db"))
    monkeypatch.setattr(main, "_gateway", gw)
    from fastapi.testclient import TestClient
    with TestClient(main.app) as c:
        yield c
    gw.close()


def test_http_ingest_and_duplicate(client):
    p = {"device_id": "d1", "metric": "temperature", "value": 25,
         "unit": "C", "ts_ms": 1000}
    log.info("POST /ingest/http %s", p)
    r1 = client.post("/ingest/http", json=p).json()
    r2 = client.post("/ingest/http", json=p).json()
    log.info("第一次=%s 第二次reason=%s", r1["accepted"], r2["reason"])
    assert r1["accepted"] is True
    assert r2["accepted"] is False and r2["reason"] == "duplicate_report"


def test_mqtt_ingest_and_link_flush(client):
    client.post("/link/down")
    r = client.post("/ingest/mqtt",
                    json={"dev": "d1", "k": "temp", "v": 32,
                          "u": "F", "ts": 1.0}).json()
    log.info("MQTT 归一结果=%s", r["record"])
    assert r["accepted"] and r["record"]["unit"] == "C"
    spool = client.get("/spool").json()
    log.info("断网 spool=%s", spool)
    assert spool["pending"] == [1]
    up = client.post("/link/up").json()
    log.info("恢复补传=%s", up)
    assert up["flushed"] == 1
    assert client.get("/spool").json()["pending"] == []
