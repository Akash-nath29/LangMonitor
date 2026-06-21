from __future__ import annotations

import pytest

from langmonitor.models.db import get_session
from langmonitor.models.schemas import Run, RunStatus


async def _seed_run(run_id: str = "cp1"):
    async with get_session() as s:
        s.add(Run(id=run_id, graph_name="g", status=RunStatus.running, thread_id="t-1"))


@pytest.mark.asyncio
async def test_save_and_list_checkpoint(main_engine):
    await _seed_run()
    cp = await main_engine.checkpoint.save_checkpoint(
        run_id="cp1", thread_id="t-1", state={"a": 1}, label="step1"
    )
    assert cp.label == "step1"
    cps = await main_engine.checkpoint.list_checkpoints("cp1")
    assert len(cps) == 1
    assert cps[0].label == "step1"


@pytest.mark.asyncio
async def test_auto_checkpoint(main_engine):
    await _seed_run("cp2")
    async with get_session() as s:
        from sqlalchemy import select
        from langmonitor.models.schemas import Run as R

        run = (await s.execute(select(R).where(R.id == "cp2"))).scalar_one()
        run.thread_id = "t-cp2"

    cp = await main_engine.checkpoint.auto_checkpoint(
        run_id="cp2", node_name="solver", sequence=3, state={"v": True}
    )
    assert cp is not None
    assert cp.label == "auto:solver:3"


@pytest.mark.asyncio
async def test_rollback_pauses_run(main_engine):
    await _seed_run("cp3")
    cp = await main_engine.checkpoint.save_checkpoint(
        run_id="cp3", thread_id="t-1", state={"a": 1}, label="pre"
    )
    result = await main_engine.checkpoint.rollback("cp3", cp.id)
    assert result["ok"] is True

    async with get_session() as s:
        from sqlalchemy import select
        from langmonitor.models.schemas import Run as R

        run = (await s.execute(select(R).where(R.id == "cp3"))).scalar_one()
        assert run.status == RunStatus.paused
