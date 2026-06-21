from __future__ import annotations

import pytest

from langmonitor.models.db import get_session
from langmonitor.models.schemas import Run, RunStatus


async def _seed_node(main_engine, run_id: str, seq: int):
    return await main_engine.trace.ingest_event(
        {
            "run_id": run_id,
            "node_name": f"n{seq}",
            "event_type": "end",
            "sequence_order": seq,
        }
    )


@pytest.mark.asyncio
async def test_snapshot_computes_diff(main_engine):
    async with get_session() as s:
        s.add(Run(id="s1", graph_name="g", status=RunStatus.running, thread_id="t"))

    e1 = await _seed_node(main_engine, "s1", 1)
    snap1 = await main_engine.state.snapshot(
        run_id="s1", node_event_id=e1.id, state={"x": 1, "y": "a"}
    )
    assert snap1.state == {"x": 1, "y": "a"}
    assert snap1.state_diff is None  # nothing to diff against

    e2 = await _seed_node(main_engine, "s1", 2)
    snap2 = await main_engine.state.snapshot(
        run_id="s1", node_event_id=e2.id, state={"x": 2, "y": "a", "z": True}
    )
    assert snap2.state_diff is not None
    # DeepDiff json output should mention values_changed or added.
    diff_text = str(snap2.state_diff).lower()
    assert "values_changed" in diff_text or "dictionary_item_added" in diff_text


@pytest.mark.asyncio
async def test_get_state_at_and_diff(main_engine):
    async with get_session() as s:
        s.add(Run(id="s2", graph_name="g", status=RunStatus.running, thread_id="t"))

    e1 = await _seed_node(main_engine, "s2", 1)
    await main_engine.state.snapshot(run_id="s2", node_event_id=e1.id, state={"v": 1})
    e2 = await _seed_node(main_engine, "s2", 2)
    await main_engine.state.snapshot(run_id="s2", node_event_id=e2.id, state={"v": 5})

    snap = await main_engine.state.get_state_at("s2", 2)
    assert snap is not None and snap.state == {"v": 5}

    diff = await main_engine.state.get_diff("s2", 1, 2)
    assert diff is not None
