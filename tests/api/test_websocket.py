from __future__ import annotations

import json

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
async def test_ws_run_receives_broadcast(main_engine, client):
    with client.websocket_connect("/ws/runs/wsr-1") as ws:
        # The bus pump runs as background tasks — for deterministic delivery
        # in tests we broadcast directly via the manager.
        from langmonitor.api.websocket import ws_manager

        await ws_manager.broadcast_to_run(
            "wsr-1",
            {
                "type": "node_start",
                "run_id": "wsr-1",
                "timestamp": "2026-06-20T00:00:00",
                "payload": {"node_name": "n", "sequence": 1},
            },
        )
        data = ws.receive_json()
        assert data["type"] == "node_start"
        assert data["payload"]["node_name"] == "n"


@pytest.mark.asyncio
async def test_ws_run_accepts_sdk_event(main_engine, client):
    with client.websocket_connect("/ws/runs/wsr-2") as ws:
        ws.send_text(
            json.dumps(
                {
                    "kind": "sdk_event",
                    "event": {
                        "type": "run_start",
                        "run_id": "wsr-2",
                        "thread_id": "tx",
                        "graph_name": "g",
                        "payload": {"input": {}},
                    },
                }
            )
        )
        ack = ws.receive_json()
        assert ack["type"] == "ack"
        assert ack["payload"]["ok"] is True
