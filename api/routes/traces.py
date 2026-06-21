from __future__ import annotations

from fastapi import APIRouter

from langmonitor.engine.core import get_main_engine
from langmonitor.schemas.api import LLMCallOut, NodeEventOut, NodeStat, ok

router = APIRouter(prefix="/runs", tags=["traces"])


@router.get("/{run_id}/trace")
async def get_trace(run_id: str):
    main = get_main_engine()
    events = await main.trace.get_run_trace(run_id)
    return ok([NodeEventOut.model_validate(e).model_dump(mode="json") for e in events])


@router.get("/{run_id}/nodes")
async def get_node_stats(run_id: str):
    main = get_main_engine()
    stats = await main.trace.get_node_stats(run_id)
    return ok([NodeStat(**s).model_dump(mode="json") for s in stats])


@router.get("/{run_id}/llm-calls")
async def get_llm_calls(run_id: str):
    main = get_main_engine()
    events = await main.trace.get_llm_calls(run_id)
    return ok(
        [
            LLMCallOut(
                node_name=e.node_name,
                prompt=e.llm_prompt,
                response=e.llm_response,
                model=e.llm_model,
                tokens=e.tokens_used,
                latency_ms=e.latency_ms,
                timestamp=e.timestamp,
                sequence_order=e.sequence_order,
            ).model_dump(mode="json")
            for e in events
        ]
    )
