from __future__ import annotations

import pytest


pytest.importorskip("langgraph")


@pytest.mark.asyncio
async def test_real_langgraph_run(main_engine):
    """End-to-end with a real LangGraph StateGraph. Confirms the SDK can
    monitor a non-mocked compiled graph and that the CheckpointEngine still
    stores rows even when the native saver isn't backing them."""
    from typing import TypedDict

    from langgraph.graph import StateGraph, END

    class S(TypedDict):
        n: int

    def inc(s: S) -> S:
        return {"n": s["n"] + 1}

    def double(s: S) -> S:
        return {"n": s["n"] * 2}

    g = StateGraph(S)
    g.add_node("inc", inc)
    g.add_node("double", double)
    g.set_entry_point("inc")
    g.add_edge("inc", "double")
    g.add_edge("double", END)
    compiled = g.compile()

    from langmonitor.sdk import monitor

    wrapped = monitor(compiled, server_url=None, in_process_engine=main_engine)
    result = await wrapped.ainvoke({"n": 3})
    assert result["n"] == 8  # (3+1) * 2

    # Trace should contain at least both nodes.
    from sqlalchemy import select, func
    from langmonitor.models.db import get_session
    from langmonitor.models.schemas import NodeEvent, Checkpoint

    async with get_session() as s:
        names = (
            await s.execute(
                select(NodeEvent.node_name).where(
                    NodeEvent.event_type == "end"
                )
            )
        ).all()
        cp_count = (
            await s.execute(select(func.count(Checkpoint.id)))
        ).scalar()
    flat = {row[0] for row in names}
    assert "inc" in flat
    assert "double" in flat
    # Auto-checkpoint after every node_end.
    assert cp_count >= 2
