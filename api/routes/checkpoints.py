from __future__ import annotations

from fastapi import APIRouter

from langmonitor.engine.core import get_main_engine
from langmonitor.models.db import get_session
from langmonitor.models.schemas import Run
from langmonitor.schemas.api import CheckpointOut, CheckpointSaveIn, err, ok
from sqlalchemy import select

router = APIRouter(prefix="/runs", tags=["checkpoints"])


@router.get("/{run_id}/checkpoints")
async def list_checkpoints(run_id: str):
    main = get_main_engine()
    cps = await main.checkpoint.list_checkpoints(run_id)
    return ok([CheckpointOut.model_validate(c).model_dump(mode="json") for c in cps])


@router.post("/{run_id}/checkpoints")
async def save_checkpoint(run_id: str, body: CheckpointSaveIn):
    main = get_main_engine()
    async with get_session() as s:
        res = await s.execute(select(Run).where(Run.id == run_id))
        run = res.scalar_one_or_none()
        if run is None:
            return err("run not found")
        thread_id = run.thread_id

    # Snapshot the most recent state we have for this run.
    latest = await main.state.get_all(run_id)
    state = latest[-1].state if latest else {}

    cp = await main.checkpoint.save_checkpoint(
        run_id=run_id, thread_id=thread_id, state=state, label=body.label
    )
    await main.broadcast(
        run_id,
        "checkpoint_saved",
        {"checkpoint_id": cp.checkpoint_id, "label": cp.label, "sequence": None},
    )
    return ok(CheckpointOut.model_validate(cp).model_dump(mode="json"))


@router.post("/{run_id}/checkpoints/{checkpoint_id}/rollback")
async def rollback_checkpoint(run_id: str, checkpoint_id: str):
    main = get_main_engine()
    result = await main.checkpoint.rollback(run_id, checkpoint_id)
    if not result.get("ok"):
        return err(result.get("error") or "rollback failed")
    return ok(result)
