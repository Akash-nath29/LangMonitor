from __future__ import annotations

import asyncio

import pytest

from langmonitor.models.db import get_session
from langmonitor.models.schemas import Run, RunStatus


async def _seed_run(run_id: str):
    async with get_session() as s:
        s.add(Run(id=run_id, graph_name="g", status=RunStatus.running, thread_id="t"))


@pytest.mark.asyncio
async def test_kill_sets_state(main_engine):
    await _seed_run("k1")
    res = await main_engine.control.kill_run("k1")
    assert res["ok"] is True
    assert main_engine.control.is_killed("k1") is True


@pytest.mark.asyncio
async def test_pause_blocks_until_resume(main_engine):
    await _seed_run("p1")
    await main_engine.control.pause_run("p1")

    async def wait_then_resume():
        await asyncio.sleep(0.05)
        await main_engine.control.resume_run("p1", injected_state={"k": "v"})

    waiter = asyncio.create_task(main_engine.control.await_if_paused("p1"))
    resumer = asyncio.create_task(wait_then_resume())
    await asyncio.wait_for(waiter, timeout=1.0)
    await resumer

    patches = main_engine.control.pop_pending_patches("p1")
    assert patches == [{"k": "v"}]


@pytest.mark.asyncio
async def test_inject_state_queues_patch(main_engine):
    await _seed_run("i1")
    await main_engine.control.inject_state("i1", {"new": 1})
    patches = main_engine.control.pop_pending_patches("i1")
    assert patches == [{"new": 1}]


@pytest.mark.asyncio
async def test_ab_test_swap_and_active_prompt(main_engine):
    test = await main_engine.control.create_ab_test(
        node_name="planner",
        prompt_a="Be a planner.",
        prompt_b="Be a strict planner.",
    )
    active = await main_engine.control.get_active_prompt("planner")
    assert active == "Be a planner."
    swapped = await main_engine.control.swap_ab_variant(test.id)
    assert swapped is not None and swapped.active_variant.value == "b"
    assert await main_engine.control.get_active_prompt("planner") == "Be a strict planner."
