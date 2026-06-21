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
    app.router.lifespan_context = _noop_lifespan
    with TestClient(app) as c:
        yield c


@pytest.mark.asyncio
async def test_guardrail_crud(main_engine, client):
    r = client.post(
        "/api/v1/guardrails",
        json={
            "name": "no spam",
            "rule_type": "max_tool_calls",
            "config": {"node_name": "tool", "threshold": 5},
            "action": "alert",
            "is_active": True,
        },
    )
    assert r.status_code == 200
    rule = r.json()["data"]
    assert rule["name"] == "no spam"

    listed = client.get("/api/v1/guardrails").json()
    assert any(x["id"] == rule["id"] for x in listed["data"])

    toggled = client.patch(
        f"/api/v1/guardrails/{rule['id']}/toggle", json={"active": False}
    ).json()
    assert toggled["data"]["is_active"] is False

    deleted = client.delete(f"/api/v1/guardrails/{rule['id']}").json()
    assert deleted["data"]["deleted"] == rule["id"]


@pytest.mark.asyncio
async def test_alert_listing(main_engine, client):
    res = client.get("/api/v1/guardrails/alerts").json()
    assert res["success"] is True
    assert isinstance(res["data"], list)
