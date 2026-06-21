from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from langmonitor.models.db import get_session
from langmonitor.models.schemas import Run, RunStatus


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


async def _seed_run(rid: str):
    async with get_session() as s:
        s.add(Run(id=rid, graph_name="g", status=RunStatus.running, thread_id="t"))


@pytest.mark.asyncio
async def test_kill_pause_resume(main_engine, client):
    await _seed_run("ctl-1")

    kill = client.post("/api/v1/runs/ctl-1/kill").json()
    assert kill["success"] is True
    assert main_engine.control.is_killed("ctl-1") is True


@pytest.mark.asyncio
async def test_inject_state(main_engine, client):
    await _seed_run("ctl-2")
    res = client.post(
        "/api/v1/runs/ctl-2/inject-state", json={"patch": {"k": 99}}
    ).json()
    assert res["success"] is True
    assert main_engine.control.pop_pending_patches("ctl-2") == [{"k": 99}]


@pytest.mark.asyncio
async def test_ab_test_endpoints(main_engine, client):
    create = client.post(
        "/api/v1/ab-tests",
        json={"node_name": "planner", "prompt_a": "A", "prompt_b": "B"},
    ).json()
    assert create["success"] is True
    test_id = create["data"]["id"]

    swap = client.post(f"/api/v1/ab-tests/{test_id}/swap").json()
    assert swap["success"] is True
    assert swap["data"]["active_variant"] == "b"
