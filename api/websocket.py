from __future__ import annotations

import asyncio
import json
import logging
from collections import defaultdict
from typing import Dict, Set

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from langmonitor.api.auth import websocket_authorized
from langmonitor.config import settings
from langmonitor.engine.core import get_main_engine
from langmonitor.utils import is_valid_identifier

log = logging.getLogger(__name__)

router = APIRouter()


class WSManager:
    """Tracks live WebSocket connections per run + a global channel.

    SDK clients also connect — they POST events to the server over the same
    socket using {"type": "sdk_event", "event": {...}}.
    """

    def __init__(self) -> None:
        self._run_connections: Dict[str, Set[WebSocket]] = defaultdict(set)
        self._all_connections: Set[WebSocket] = set()
        self._lock = asyncio.Lock()

    async def connect_run(self, run_id: str, ws: WebSocket) -> bool:
        await ws.accept()
        async with self._lock:
            conns = self._run_connections[run_id]
            if len(conns) >= settings.MAX_WS_CONNECTIONS_PER_RUN:
                await ws.close(code=1013, reason="too many connections")
                return False
            conns.add(ws)
        return True

    async def connect_all(self, ws: WebSocket) -> bool:
        await ws.accept()
        async with self._lock:
            if len(self._all_connections) >= settings.MAX_WS_CONNECTIONS_GLOBAL:
                await ws.close(code=1013, reason="too many connections")
                return False
            self._all_connections.add(ws)
        return True

    async def disconnect_run(self, run_id: str, ws: WebSocket) -> None:
        async with self._lock:
            self._run_connections.get(run_id, set()).discard(ws)

    async def disconnect_all(self, ws: WebSocket) -> None:
        async with self._lock:
            self._all_connections.discard(ws)

    async def broadcast_to_run(self, run_id: str, message: dict) -> None:
        dead: list[WebSocket] = []
        for ws in list(self._run_connections.get(run_id, set())):
            try:
                await ws.send_json(message)
            except Exception:
                dead.append(ws)
        for ws in dead:
            await self.disconnect_run(run_id, ws)

    async def broadcast_to_all(self, message: dict) -> None:
        dead: list[WebSocket] = []
        for ws in list(self._all_connections):
            try:
                await ws.send_json(message)
            except Exception:
                dead.append(ws)
        for ws in dead:
            await self.disconnect_all(ws)


ws_manager = WSManager()


async def _pump_bus_to_clients() -> None:
    """Forward bus events into the per-run and global WebSocket fan-out.

    Spawned during FastAPI startup. The bus uses queues so each subscription
    here lives independently and slow clients don't backlog publishers.
    """
    main = get_main_engine()
    # Subscribe to the global ws stream.
    global_q = await main.bus.subscribe("ws:all")

    async def pump_global() -> None:
        while True:
            msg = await global_q.get()
            await ws_manager.broadcast_to_all(msg)
            run_id = msg.get("run_id")
            if run_id:
                await ws_manager.broadcast_to_run(run_id, msg)

    asyncio.create_task(pump_global())


@router.websocket("/ws/runs/{run_id}")
async def ws_run(websocket: WebSocket, run_id: str):
    if not websocket_authorized(websocket):
        await websocket.close(code=1008, reason="unauthorized")
        return
    if not is_valid_identifier(run_id):
        await websocket.close(code=1008, reason="invalid run id")
        return
    accepted = await ws_manager.connect_run(run_id, websocket)
    if not accepted:
        return
    main = get_main_engine()
    try:
        while True:
            raw = await websocket.receive_text()
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                await websocket.send_json(
                    {"type": "error", "payload": {"error": "invalid json"}}
                )
                continue

            # SDK control gate: lets the SDK poll kill/pause state over the same
            # socket so operator controls work in the standard WS deployment.
            if data.get("kind") == "control_poll":
                poll_run = data.get("run_id") or run_id
                payload = {
                    "killed": await main.control.is_killed_async(poll_run),
                    "paused": await main.control.is_paused_async(poll_run),
                }
                await websocket.send_json({"type": "control", "payload": payload})
                continue

            # SDK pipe: events the SDK pushes to the server.
            if data.get("kind") == "sdk_event" or data.get("type") in {
                "run_start",
                "node_start",
                "node_end",
                "llm_call",
                "run_end",
            }:
                event = data.get("event") if data.get("kind") == "sdk_event" else data
                event = dict(event or {})
                event.setdefault("run_id", run_id)
                result = await main.handle_sdk_event(event)
                await websocket.send_json({"type": "ack", "payload": result})
                continue

            # Otherwise treat as a client ping/echo.
            await websocket.send_json({"type": "echo", "payload": data})

    except WebSocketDisconnect:
        pass
    except Exception:
        log.exception("ws_run loop error")
    finally:
        await ws_manager.disconnect_run(run_id, websocket)


@router.websocket("/ws/all")
async def ws_all(websocket: WebSocket):
    if not websocket_authorized(websocket):
        await websocket.close(code=1008, reason="unauthorized")
        return
    accepted = await ws_manager.connect_all(websocket)
    if not accepted:
        return
    try:
        while True:
            # Dashboard clients usually only consume. Keep the loop alive so
            # disconnects are detected promptly.
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    except Exception:
        log.exception("ws_all loop error")
    finally:
        await ws_manager.disconnect_all(websocket)
