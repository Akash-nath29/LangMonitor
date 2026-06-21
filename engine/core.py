from __future__ import annotations

import asyncio
import logging
import uuid
from typing import Any, Dict, Optional

from sqlalchemy import select

from langmonitor.engine.bus import EventBus
from langmonitor.engines.checkpoint_engine import CheckpointEngine
from langmonitor.engines.control_engine import ControlEngine
from langmonitor.engines.guardrail_engine import GuardrailEngine
from langmonitor.engines.state_engine import StateEngine
from langmonitor.engines.trace_engine import TraceEngine
from langmonitor.models import RunStatus
from langmonitor.models.db import get_session, init_db
from langmonitor.models.schemas import Run
from langmonitor.utils import ensure_aware, utcnow

log = logging.getLogger(__name__)


class MainEngine:
    """Orchestrator that owns all sub-engines and routes SDK events.

    Sub-engines never call each other directly — everything goes through here
    (or through the event bus). Each sub-engine holds a reference back to this
    instance so it can broadcast WebSocket events and trigger cross-cutting
    actions (e.g. GuardrailEngine asking ControlEngine to pause a run).
    """

    def __init__(self) -> None:
        self.bus = EventBus()
        self.trace = TraceEngine(self)
        self.state = StateEngine(self)
        self.guardrail = GuardrailEngine(self)
        self.checkpoint = CheckpointEngine(self)
        self.control = ControlEngine(self)
        self._started = False

    async def startup(self) -> None:
        if self._started:
            return
        await init_db()
        await self.checkpoint.startup()
        self._started = True
        log.info("MainEngine started")

    async def shutdown(self) -> None:
        await self.checkpoint.shutdown()
        self._started = False

    # -------- WebSocket fan-out --------

    async def broadcast(
        self,
        run_id: str,
        event_type: str,
        payload: Dict[str, Any],
    ) -> None:
        """Publish a typed event for WebSocket fan-out."""
        message = {
            "type": event_type,
            "run_id": run_id,
            "timestamp": utcnow().isoformat(),
            "payload": payload,
        }
        # Per-run channel and global channel.
        await self.bus.publish(f"ws:run:{run_id}", message)
        await self.bus.publish("ws:all", message)

    # -------- SDK event routing --------

    async def handle_sdk_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Route an event from the SDK to the correct sub-engines.

        Returns a small ack payload (e.g. node_event_id) when applicable so the
        SDK can correlate subsequent llm_call events with the right NodeEvent.
        """
        etype = event.get("type")
        try:
            if etype == "run_start":
                return await self._handle_run_start(event)
            if etype == "node_start":
                return await self._handle_node_start(event)
            if etype == "node_end":
                return await self._handle_node_end(event)
            if etype == "llm_call":
                return await self._handle_llm_call(event)
            if etype == "run_end":
                return await self._handle_run_end(event)
            log.warning("Unknown SDK event type: %s", etype)
            return {"ok": False, "error": f"unknown event type: {etype}"}
        except Exception as e:
            log.exception("handle_sdk_event failed for %s", etype)
            return {"ok": False, "error": str(e)}

    async def _handle_run_start(self, event: Dict[str, Any]) -> Dict[str, Any]:
        payload = event.get("payload", {})
        run_id = event.get("run_id") or str(uuid.uuid4())
        thread_id = event.get("thread_id") or run_id
        graph_name = event.get("graph_name") or payload.get("graph_name") or "unknown"

        async with get_session() as s:
            run = Run(
                id=run_id,
                graph_name=graph_name,
                status=RunStatus.running,
                input=payload.get("input"),
                thread_id=thread_id,
            )
            s.add(run)
            await s.flush()

        await self.broadcast(
            run_id,
            "run_started",
            {"graph_name": graph_name, "input": payload.get("input")},
        )
        return {"ok": True, "run_id": run_id, "thread_id": thread_id}

    async def _handle_node_start(self, event: Dict[str, Any]) -> Dict[str, Any]:
        run_id = event["run_id"]
        node_event = await self.trace.ingest_event(
            {
                "run_id": run_id,
                "node_name": event["payload"].get("node_name"),
                "event_type": "start",
                "input_state": event["payload"].get("input_state"),
                "sequence_order": event["payload"].get("sequence"),
            }
        )
        await self.broadcast(
            run_id,
            "node_start",
            {
                "node_name": node_event.node_name,
                "sequence": node_event.sequence_order,
                "input_state": node_event.input_state,
            },
        )
        return {"ok": True, "node_event_id": node_event.id}

    async def _handle_node_end(self, event: Dict[str, Any]) -> Dict[str, Any]:
        run_id = event["run_id"]
        payload = event["payload"]
        node_event = await self.trace.ingest_event(
            {
                "run_id": run_id,
                "node_name": payload.get("node_name"),
                "event_type": "end",
                "input_state": payload.get("input_state"),
                "output_state": payload.get("output_state"),
                "latency_ms": payload.get("latency_ms"),
                "tokens_used": payload.get("tokens_used"),
                "sequence_order": payload.get("sequence"),
            }
        )

        # Update run-level totals if tokens included.
        if payload.get("tokens_used") or payload.get("cost_usd"):
            await self._add_run_totals(
                run_id,
                tokens=payload.get("tokens_used") or 0,
                cost=payload.get("cost_usd") or 0.0,
            )

        # State snapshot + diff.
        snapshot = await self.state.snapshot(
            run_id=run_id,
            node_event_id=node_event.id,
            state=payload.get("output_state") or {},
        )

        await self.broadcast(
            run_id,
            "node_end",
            {
                "node_name": node_event.node_name,
                "sequence": node_event.sequence_order,
                "output_state": node_event.output_state,
                "latency_ms": node_event.latency_ms,
                "tokens": node_event.tokens_used,
            },
        )
        await self.broadcast(
            run_id,
            "state_updated",
            {
                "sequence": node_event.sequence_order,
                "state": snapshot.state,
                "diff": snapshot.state_diff,
            },
        )

        # Guardrails (may pause/kill via ControlEngine).
        alerts = await self.guardrail.evaluate(run_id, node_event)
        for alert in alerts:
            await self.broadcast(
                run_id,
                "guardrail_alert",
                {
                    "rule_name": alert["rule_name"],
                    "rule_type": alert["rule_type"],
                    "action": alert["action"],
                    "context": alert["context"],
                },
            )
            if alert["action"] == "pause":
                await self.control.pause_run(run_id, reason="guardrail")
            elif alert["action"] == "kill":
                await self.control.kill_run(run_id, reason="guardrail")

        # Auto-checkpoint.
        from langmonitor.config import settings as cfg
        if cfg.CHECKPOINT_AUTO_SAVE:
            try:
                cp = await self.checkpoint.auto_checkpoint(
                    run_id=run_id,
                    node_name=node_event.node_name,
                    sequence=node_event.sequence_order,
                    state=payload.get("output_state") or {},
                )
                if cp:
                    await self.broadcast(
                        run_id,
                        "checkpoint_saved",
                        {
                            "checkpoint_id": cp.checkpoint_id,
                            "label": cp.label,
                            "sequence": node_event.sequence_order,
                        },
                    )
            except Exception:
                log.exception("auto_checkpoint failed")

        return {"ok": True, "node_event_id": node_event.id}

    async def _handle_llm_call(self, event: Dict[str, Any]) -> Dict[str, Any]:
        run_id = event["run_id"]
        payload = event["payload"]
        node_event_id = payload.get("node_event_id")
        if node_event_id:
            await self.trace.attach_llm_call(
                node_event_id=node_event_id,
                prompt=payload.get("prompt"),
                response=payload.get("response"),
                model=payload.get("model"),
                tokens=payload.get("tokens"),
                latency_ms=payload.get("latency_ms"),
            )
        await self.broadcast(
            run_id,
            "llm_call",
            {
                "node_name": payload.get("node_name"),
                "prompt": payload.get("prompt"),
                "response": payload.get("response"),
                "model": payload.get("model"),
                "tokens": payload.get("tokens"),
                "latency_ms": payload.get("latency_ms"),
            },
        )
        return {"ok": True}

    async def _handle_run_end(self, event: Dict[str, Any]) -> Dict[str, Any]:
        run_id = event["run_id"]
        payload = event["payload"]
        status = payload.get("status", "completed")

        async with get_session() as s:
            res = await s.execute(select(Run).where(Run.id == run_id))
            run = res.scalar_one_or_none()
            if run is None:
                return {"ok": False, "error": "run not found"}
            try:
                run.status = RunStatus(status)
            except ValueError:
                run.status = RunStatus.completed
            run.ended_at = utcnow()
            if payload.get("output") is not None:
                run.output = payload["output"]

            duration_ms = int(
                (
                    ensure_aware(run.ended_at) - ensure_aware(run.started_at)
                ).total_seconds()
                * 1000
            )
            total_tokens = run.total_tokens
            total_cost = run.total_cost_usd

        await self.broadcast(
            run_id,
            "run_ended",
            {
                "status": status,
                "total_tokens": total_tokens,
                "total_cost_usd": total_cost,
                "duration_ms": duration_ms,
            },
        )
        return {"ok": True}

    async def _add_run_totals(
        self, run_id: str, tokens: int = 0, cost: float = 0.0
    ) -> None:
        async with get_session() as s:
            res = await s.execute(select(Run).where(Run.id == run_id))
            run = res.scalar_one_or_none()
            if run is None:
                return
            run.total_tokens = (run.total_tokens or 0) + int(tokens or 0)
            run.total_cost_usd = (run.total_cost_usd or 0.0) + float(cost or 0.0)


# -------- Module-level singleton accessor --------

_main_engine: Optional[MainEngine] = None
_engine_lock = asyncio.Lock()


def set_main_engine(engine: MainEngine) -> None:
    global _main_engine
    _main_engine = engine


def get_main_engine() -> MainEngine:
    if _main_engine is None:
        raise RuntimeError("MainEngine not initialized — call set_main_engine() first")
    return _main_engine
