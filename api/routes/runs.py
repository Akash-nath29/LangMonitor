from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Query
from sqlalchemy import func, select

from langmonitor.models.db import get_session
from langmonitor.models.schemas import (
    Checkpoint,
    GuardrailAlert,
    NodeEvent,
    Run,
    RunStatus,
)
from langmonitor.schemas.api import RunOut, RunSummary, err, ok

router = APIRouter(prefix="/runs", tags=["runs"])


@router.get("")
async def list_runs(
    status: Optional[RunStatus] = None,
    graph_name: Optional[str] = None,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    async with get_session() as s:
        q = select(Run).order_by(Run.started_at.desc())
        if status is not None:
            q = q.where(Run.status == status)
        if graph_name is not None:
            q = q.where(Run.graph_name == graph_name)
        q = q.limit(limit).offset(offset)
        res = await s.execute(q)
        runs = list(res.scalars().all())
    return ok([RunOut.model_validate(r).model_dump(mode="json") for r in runs])


@router.get("/{run_id}")
async def get_run(run_id: str):
    async with get_session() as s:
        res = await s.execute(select(Run).where(Run.id == run_id))
        run = res.scalar_one_or_none()
        if run is None:
            return err("run not found")
        node_count = (
            await s.execute(
                select(func.count(NodeEvent.id)).where(NodeEvent.run_id == run_id)
            )
        ).scalar() or 0
        alert_count = (
            await s.execute(
                select(func.count(GuardrailAlert.id)).where(
                    GuardrailAlert.run_id == run_id
                )
            )
        ).scalar() or 0
        cp_count = (
            await s.execute(
                select(func.count(Checkpoint.id)).where(Checkpoint.run_id == run_id)
            )
        ).scalar() or 0

        summary = RunSummary(
            run=RunOut.model_validate(run),
            node_count=int(node_count),
            alert_count=int(alert_count),
            checkpoint_count=int(cp_count),
        )
    return ok(summary.model_dump(mode="json"))


@router.delete("/{run_id}")
async def delete_run(run_id: str):
    async with get_session() as s:
        res = await s.execute(select(Run).where(Run.id == run_id))
        run = res.scalar_one_or_none()
        if run is None:
            return err("run not found")
        await s.delete(run)
    return ok({"deleted": run_id})
