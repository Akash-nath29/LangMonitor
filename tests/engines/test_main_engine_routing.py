from __future__ import annotations

import pytest


@pytest.mark.asyncio
async def test_run_lifecycle_routes_correctly(main_engine):
    res = await main_engine.handle_sdk_event(
        {
            "type": "run_start",
            "run_id": "lc-1",
            "thread_id": "tlc-1",
            "graph_name": "demo",
            "payload": {"input": {"q": "hi"}},
        }
    )
    assert res["ok"] is True

    start = await main_engine.handle_sdk_event(
        {
            "type": "node_start",
            "run_id": "lc-1",
            "payload": {"node_name": "a", "sequence": 1, "input_state": {"q": "hi"}},
        }
    )
    assert start["ok"] is True

    end = await main_engine.handle_sdk_event(
        {
            "type": "node_end",
            "run_id": "lc-1",
            "payload": {
                "node_name": "a",
                "sequence": 1,
                "output_state": {"q": "hi", "a": 1},
                "latency_ms": 25,
                "tokens_used": 50,
            },
        }
    )
    assert end["ok"] is True
    ne_id = end["node_event_id"]

    llm = await main_engine.handle_sdk_event(
        {
            "type": "llm_call",
            "run_id": "lc-1",
            "payload": {
                "node_event_id": ne_id,
                "prompt": "say hi",
                "response": "hi!",
                "model": "test",
                "tokens": 10,
                "latency_ms": 100,
            },
        }
    )
    assert llm["ok"] is True

    finished = await main_engine.handle_sdk_event(
        {
            "type": "run_end",
            "run_id": "lc-1",
            "payload": {"status": "completed", "output": {"final": True}},
        }
    )
    assert finished["ok"] is True

    # Trace has one start + one end event per node visited.
    trace = await main_engine.trace.get_run_trace("lc-1")
    assert len(trace) == 2
    end_event = next(e for e in trace if e.event_type.value == "end")
    assert end_event.llm_prompt == "say hi"
    assert end_event.tokens_used == 60  # 50 from node_end + 10 from llm_call

    # State snapshots created.
    snaps = await main_engine.state.get_all("lc-1")
    assert len(snaps) == 1
    assert snaps[0].state == {"q": "hi", "a": 1}
