from __future__ import annotations

import pytest

from langmonitor.models.db import get_session
from langmonitor.models.schemas import Run, RunStatus


@pytest.mark.asyncio
async def test_ingest_event_assigns_sequence(main_engine):
    async with get_session() as s:
        s.add(
            Run(id="r1", graph_name="g", status=RunStatus.running, thread_id="t1")
        )
    e1 = await main_engine.trace.ingest_event(
        {"run_id": "r1", "node_name": "a", "event_type": "end"}
    )
    e2 = await main_engine.trace.ingest_event(
        {"run_id": "r1", "node_name": "b", "event_type": "end"}
    )
    assert e1.sequence_order == 1
    assert e2.sequence_order == 2


@pytest.mark.asyncio
async def test_get_run_trace_orders_by_sequence(main_engine):
    async with get_session() as s:
        s.add(
            Run(id="r2", graph_name="g", status=RunStatus.running, thread_id="t2")
        )
    await main_engine.trace.ingest_event(
        {"run_id": "r2", "node_name": "n", "event_type": "end", "sequence_order": 3}
    )
    await main_engine.trace.ingest_event(
        {"run_id": "r2", "node_name": "n", "event_type": "end", "sequence_order": 1}
    )
    await main_engine.trace.ingest_event(
        {"run_id": "r2", "node_name": "n", "event_type": "end", "sequence_order": 2}
    )
    trace = await main_engine.trace.get_run_trace("r2")
    assert [e.sequence_order for e in trace] == [1, 2, 3]


@pytest.mark.asyncio
async def test_node_stats_aggregates(main_engine):
    async with get_session() as s:
        s.add(
            Run(id="r3", graph_name="g", status=RunStatus.running, thread_id="t3")
        )
    for i, (name, lat, tok) in enumerate(
        [("a", 100, 10), ("a", 200, 20), ("b", 50, 5)]
    ):
        await main_engine.trace.ingest_event(
            {
                "run_id": "r3",
                "node_name": name,
                "event_type": "end",
                "latency_ms": lat,
                "tokens_used": tok,
                "sequence_order": i + 1,
            }
        )
    stats = {s["node_name"]: s for s in await main_engine.trace.get_node_stats("r3")}
    assert stats["a"]["call_count"] == 2
    assert stats["a"]["avg_latency_ms"] == 150.0
    assert stats["a"]["total_tokens"] == 30
    assert stats["b"]["call_count"] == 1


@pytest.mark.asyncio
async def test_attach_llm_call(main_engine):
    async with get_session() as s:
        s.add(
            Run(id="r4", graph_name="g", status=RunStatus.running, thread_id="t4")
        )
    e = await main_engine.trace.ingest_event(
        {"run_id": "r4", "node_name": "n", "event_type": "end"}
    )
    updated = await main_engine.trace.attach_llm_call(
        e.id, prompt="hi", response="hello", model="gpt", tokens=42
    )
    assert updated is not None
    assert updated.llm_prompt == "hi"
    assert updated.llm_response == "hello"
    assert updated.tokens_used == 42
