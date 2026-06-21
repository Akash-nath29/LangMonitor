from __future__ import annotations

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def client(main_engine):
    from contextlib import asynccontextmanager

    from langmonitor.main import create_app

    @asynccontextmanager
    async def _noop_lifespan(app):
        yield

    app = create_app()
    # Replace the lifespan so the test fixture's MainEngine stays in charge.
    app.router.lifespan_context = _noop_lifespan
    with TestClient(app) as c:
        yield c


@pytest.mark.asyncio
async def test_run_list_and_get(main_engine, client):
    # Seed a run via the SDK event path.
    await main_engine.handle_sdk_event(
        {
            "type": "run_start",
            "run_id": "rapi-1",
            "graph_name": "demo",
            "thread_id": "trapi-1",
            "payload": {"input": {"q": "hi"}},
        }
    )

    r = client.get("/api/v1/runs")
    assert r.status_code == 200
    body = r.json()
    assert body["success"] is True
    assert any(item["id"] == "rapi-1" for item in body["data"])

    one = client.get("/api/v1/runs/rapi-1").json()
    assert one["success"] is True
    assert one["data"]["run"]["id"] == "rapi-1"


@pytest.mark.asyncio
async def test_envelope_on_404(main_engine, client):
    r = client.get("/api/v1/runs/does-not-exist").json()
    assert r["success"] is False
    assert "not found" in (r["error"] or "")
