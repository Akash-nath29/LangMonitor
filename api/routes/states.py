from __future__ import annotations

from fastapi import APIRouter, Query

from langmonitor.engine.core import get_main_engine
from langmonitor.schemas.api import StateDiffOut, StateSnapshotOut, err, ok

router = APIRouter(prefix="/runs", tags=["states"])


@router.get("/{run_id}/states")
async def get_states(run_id: str):
    main = get_main_engine()
    snapshots = await main.state.get_all(run_id)
    return ok(
        [StateSnapshotOut.model_validate(s).model_dump(mode="json") for s in snapshots]
    )


@router.get("/{run_id}/states/diff")
async def get_diff(
    run_id: str,
    from_: int = Query(..., alias="from", ge=1),
    to: int = Query(..., ge=1),
):
    main = get_main_engine()
    diff = await main.state.get_diff(run_id, from_, to)
    if diff is None:
        return err("snapshots not found for given sequence range")
    return ok(
        StateDiffOut(from_seq=from_, to_seq=to, diff=diff).model_dump(mode="json")
    )


@router.get("/{run_id}/states/{seq}")
async def get_state_at(run_id: str, seq: int):
    main = get_main_engine()
    snap = await main.state.get_state_at(run_id, seq)
    if snap is None:
        return err("snapshot not found")
    return ok(StateSnapshotOut.model_validate(snap).model_dump(mode="json"))
