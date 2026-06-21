from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from deepdiff import DeepDiff
from sqlalchemy import select

from langmonitor.models.db import get_session
from langmonitor.models.schemas import NodeEvent, StateSnapshot

if TYPE_CHECKING:
    from langmonitor.engine.core import MainEngine

log = logging.getLogger(__name__)


def _to_jsonable(diff_obj: Any) -> Any:
    """DeepDiff returns custom containers — round-trip through JSON to flatten."""
    try:
        return json.loads(diff_obj.to_json())
    except Exception:
        try:
            return diff_obj.to_dict()
        except Exception:
            return {"raw": str(diff_obj)}


class StateEngine:
    """Captures state after every node and serves diffs."""

    def __init__(self, main: "MainEngine") -> None:
        self.main = main

    async def snapshot(
        self,
        run_id: str,
        node_event_id: str,
        state: Dict[str, Any],
    ) -> StateSnapshot:
        prev_state = await self._latest_state(run_id)
        diff: Optional[Dict[str, Any]] = None
        if prev_state is not None:
            try:
                d = DeepDiff(prev_state, state, ignore_order=True, view="tree")
                diff = _to_jsonable(d)
            except Exception as e:
                log.warning("DeepDiff failed: %s", e)
                diff = None

        snap = StateSnapshot(
            run_id=run_id,
            node_event_id=node_event_id,
            state=state,
            state_diff=diff,
        )
        async with get_session() as s:
            s.add(snap)
            await s.flush()
            await s.refresh(snap)
        return snap

    async def get_all(self, run_id: str) -> List[StateSnapshot]:
        async with get_session() as s:
            res = await s.execute(
                select(StateSnapshot)
                .where(StateSnapshot.run_id == run_id)
                .order_by(StateSnapshot.snapshot_at.asc())
            )
            return list(res.scalars().all())

    async def get_state_at(
        self, run_id: str, sequence: int
    ) -> Optional[StateSnapshot]:
        async with get_session() as s:
            res = await s.execute(
                select(StateSnapshot)
                .join(NodeEvent, NodeEvent.id == StateSnapshot.node_event_id)
                .where(NodeEvent.run_id == run_id)
                .where(NodeEvent.sequence_order == sequence)
                .limit(1)
            )
            return res.scalar_one_or_none()

    async def get_diff(
        self, run_id: str, from_seq: int, to_seq: int
    ) -> Optional[Dict[str, Any]]:
        a = await self.get_state_at(run_id, from_seq)
        b = await self.get_state_at(run_id, to_seq)
        if a is None or b is None:
            return None
        try:
            d = DeepDiff(a.state, b.state, ignore_order=True, view="tree")
            return _to_jsonable(d)
        except Exception as e:
            log.warning("get_diff DeepDiff failed: %s", e)
            return None

    async def _latest_state(self, run_id: str) -> Optional[Dict[str, Any]]:
        async with get_session() as s:
            res = await s.execute(
                select(StateSnapshot)
                .where(StateSnapshot.run_id == run_id)
                .order_by(StateSnapshot.snapshot_at.desc())
                .limit(1)
            )
            snap = res.scalar_one_or_none()
            return snap.state if snap else None
