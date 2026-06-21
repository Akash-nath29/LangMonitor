from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from sqlalchemy import func, select

from langmonitor.models.db import get_session
from langmonitor.models.schemas import NodeEvent, NodeEventType
from langmonitor.utils import utcnow

if TYPE_CHECKING:
    from langmonitor.engine.core import MainEngine

log = logging.getLogger(__name__)


class TraceEngine:
    """Owns persistence and querying of NodeEvents.

    Receives every node_start/node_end from the MainEngine, saves it, and
    serves trace + per-node stats endpoints.
    """

    def __init__(self, main: "MainEngine") -> None:
        self.main = main

    async def ingest_event(self, event: Dict[str, Any]) -> NodeEvent:
        run_id = event["run_id"]
        sequence_order = event.get("sequence_order")
        if sequence_order is None:
            sequence_order = await self._next_sequence(run_id)

        try:
            etype = NodeEventType(event.get("event_type", "start"))
        except ValueError:
            etype = NodeEventType.start

        ne = NodeEvent(
            run_id=run_id,
            node_name=event.get("node_name") or "unknown",
            event_type=etype,
            input_state=event.get("input_state"),
            output_state=event.get("output_state"),
            llm_prompt=event.get("llm_prompt"),
            llm_response=event.get("llm_response"),
            llm_model=event.get("llm_model"),
            tokens_used=event.get("tokens_used"),
            latency_ms=event.get("latency_ms"),
            sequence_order=int(sequence_order),
            timestamp=utcnow(),
        )
        async with get_session() as s:
            s.add(ne)
            await s.flush()
            await s.refresh(ne)
        return ne

    async def attach_llm_call(
        self,
        node_event_id: str,
        prompt: Optional[str] = None,
        response: Optional[str] = None,
        model: Optional[str] = None,
        tokens: Optional[int] = None,
        latency_ms: Optional[int] = None,
    ) -> Optional[NodeEvent]:
        async with get_session() as s:
            res = await s.execute(
                select(NodeEvent).where(NodeEvent.id == node_event_id)
            )
            ne = res.scalar_one_or_none()
            if ne is None:
                return None
            if prompt is not None:
                ne.llm_prompt = prompt
            if response is not None:
                ne.llm_response = response
            if model is not None:
                ne.llm_model = model
            if tokens is not None:
                ne.tokens_used = (ne.tokens_used or 0) + int(tokens)
            if latency_ms is not None and ne.latency_ms is None:
                ne.latency_ms = int(latency_ms)
            return ne

    async def get_run_trace(self, run_id: str) -> List[NodeEvent]:
        async with get_session() as s:
            res = await s.execute(
                select(NodeEvent)
                .where(NodeEvent.run_id == run_id)
                .order_by(NodeEvent.sequence_order.asc(), NodeEvent.timestamp.asc())
            )
            return list(res.scalars().all())

    async def get_node_stats(self, run_id: str) -> List[Dict[str, Any]]:
        async with get_session() as s:
            res = await s.execute(
                select(
                    NodeEvent.node_name,
                    func.count(NodeEvent.id).label("call_count"),
                    func.avg(NodeEvent.latency_ms).label("avg_latency_ms"),
                    func.sum(NodeEvent.tokens_used).label("total_tokens"),
                )
                .where(NodeEvent.run_id == run_id)
                .where(NodeEvent.event_type == NodeEventType.end)
                .group_by(NodeEvent.node_name)
            )
            rows = res.all()
        return [
            {
                "node_name": r.node_name,
                "call_count": int(r.call_count or 0),
                "avg_latency_ms": float(r.avg_latency_ms or 0.0),
                "total_tokens": int(r.total_tokens or 0),
            }
            for r in rows
        ]

    async def get_llm_calls(self, run_id: str) -> List[NodeEvent]:
        async with get_session() as s:
            res = await s.execute(
                select(NodeEvent)
                .where(NodeEvent.run_id == run_id)
                .where(NodeEvent.llm_prompt.isnot(None))
                .order_by(NodeEvent.sequence_order.asc())
            )
            return list(res.scalars().all())

    async def _next_sequence(self, run_id: str) -> int:
        async with get_session() as s:
            res = await s.execute(
                select(func.max(NodeEvent.sequence_order)).where(
                    NodeEvent.run_id == run_id
                )
            )
            current = res.scalar()
        return (current or 0) + 1
