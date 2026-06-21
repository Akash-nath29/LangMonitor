from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set

from sqlalchemy import select

from langmonitor.models.db import get_session
from langmonitor.models.schemas import ABTest, ABVariant, Run, RunStatus
from langmonitor.utils import utcnow

if TYPE_CHECKING:
    from langmonitor.engine.core import MainEngine

log = logging.getLogger(__name__)


class AgentKilledException(Exception):
    """Raised inside SDK loops to halt the agent immediately."""


class ControlEngine:
    """All Layer-3 operational controls.

    State that must live in memory (not the DB) because the SDK pings it
    synchronously between nodes:
      _paused_runs: per-run asyncio.Event that the SDK awaits
      _killed_runs: set of run IDs the SDK refuses to step further
      _pending_patches: queued state patches the SDK merges into the next node
    """

    def __init__(self, main: "MainEngine") -> None:
        self.main = main
        self._paused_runs: Dict[str, asyncio.Event] = {}
        self._killed_runs: Set[str] = set()
        self._pending_patches: Dict[str, List[Dict[str, Any]]] = {}
        self._active_ab_by_node: Dict[str, str] = {}

    # -------- Kill / Pause / Resume --------

    async def kill_run(
        self, run_id: str, reason: str = "manual"
    ) -> Dict[str, Any]:
        self._killed_runs.add(run_id)
        # Unblock any pause so the SDK can wake up and raise the kill.
        ev = self._paused_runs.get(run_id)
        if ev is not None and not ev.is_set():
            ev.set()
        await self._set_run_status(run_id, RunStatus.killed, ended=True)
        await self.main.broadcast(run_id, "agent_killed", {"reason": reason})
        return {"ok": True, "run_id": run_id, "reason": reason}

    async def pause_run(
        self, run_id: str, reason: str = "manual", node_name: Optional[str] = None
    ) -> Dict[str, Any]:
        ev = self._paused_runs.get(run_id)
        if ev is None:
            ev = asyncio.Event()
            self._paused_runs[run_id] = ev
        ev.clear()
        await self._set_run_status(run_id, RunStatus.paused)
        await self.main.broadcast(
            run_id, "agent_paused", {"reason": reason, "node_name": node_name}
        )
        return {"ok": True, "run_id": run_id, "reason": reason}

    async def resume_run(
        self,
        run_id: str,
        injected_state: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        if injected_state:
            self._pending_patches.setdefault(run_id, []).append(injected_state)
        ev = self._paused_runs.get(run_id)
        if ev is not None:
            ev.set()
        await self._set_run_status(run_id, RunStatus.running)
        await self.main.broadcast(
            run_id, "agent_resumed", {"state_patched": bool(injected_state)}
        )
        return {"ok": True, "run_id": run_id}

    async def inject_state(
        self, run_id: str, patch: Dict[str, Any]
    ) -> Dict[str, Any]:
        self._pending_patches.setdefault(run_id, []).append(patch)
        await self.main.broadcast(
            run_id, "state_injected", {"patch_keys": list(patch.keys())}
        )
        return {"ok": True, "run_id": run_id, "patch_keys": list(patch.keys())}

    # -------- SDK polling surface --------

    def is_killed(self, run_id: str) -> bool:
        """In-memory check (synchronous). Prefer is_killed_async, which also
        survives a server restart by consulting the persisted run status."""
        return run_id in self._killed_runs

    async def is_killed_async(self, run_id: str) -> bool:
        if run_id in self._killed_runs:
            return True
        # In-memory state is lost on restart, so fall back to the DB so a run
        # marked killed before the restart stays enforced.
        if await self._db_status(run_id) == RunStatus.killed:
            self._killed_runs.add(run_id)
            return True
        return False

    async def is_paused_async(self, run_id: str) -> bool:
        ev = self._paused_runs.get(run_id)
        if ev is not None and not ev.is_set():
            return True
        return await self._db_status(run_id) == RunStatus.paused

    async def await_if_paused(self, run_id: str) -> None:
        ev = self._paused_runs.get(run_id)
        if ev is None:
            # No in-memory event (e.g. after a restart) — honour a persisted
            # paused status by creating one so the SDK still blocks.
            if await self._db_status(run_id) == RunStatus.paused:
                ev = asyncio.Event()
                self._paused_runs[run_id] = ev
            else:
                return
        if ev.is_set():
            return
        await ev.wait()

    def pop_pending_patches(self, run_id: str) -> List[Dict[str, Any]]:
        return self._pending_patches.pop(run_id, [])

    async def _db_status(self, run_id: str) -> Optional[RunStatus]:
        async with get_session() as s:
            res = await s.execute(select(Run.status).where(Run.id == run_id))
            return res.scalar_one_or_none()

    # -------- A/B testing --------

    async def create_ab_test(
        self,
        node_name: str,
        prompt_a: str,
        prompt_b: str,
        run_id: Optional[str] = None,
    ) -> ABTest:
        test = ABTest(
            node_name=node_name,
            variant_a_prompt=prompt_a,
            variant_b_prompt=prompt_b,
            active_variant=ABVariant.a,
            run_id=run_id,
        )
        async with get_session() as s:
            s.add(test)
            await s.flush()
            await s.refresh(test)
        self._active_ab_by_node[node_name] = test.id
        return test

    async def list_ab_tests(self) -> List[ABTest]:
        async with get_session() as s:
            res = await s.execute(
                select(ABTest).order_by(ABTest.created_at.desc())
            )
            return list(res.scalars().all())

    async def get_ab_test(self, test_id: str) -> Optional[ABTest]:
        async with get_session() as s:
            res = await s.execute(select(ABTest).where(ABTest.id == test_id))
            return res.scalar_one_or_none()

    async def swap_ab_variant(self, ab_test_id: str) -> Optional[ABTest]:
        async with get_session() as s:
            res = await s.execute(select(ABTest).where(ABTest.id == ab_test_id))
            test = res.scalar_one_or_none()
            if test is None:
                return None
            test.active_variant = (
                ABVariant.b if test.active_variant == ABVariant.a else ABVariant.a
            )
            test.swapped_at = utcnow()
            await s.flush()
            await s.refresh(test)
        await self.main.broadcast(
            test.run_id or "global",
            "ab_swap",
            {
                "node_name": test.node_name,
                "active_variant": test.active_variant.value,
                "ab_test_id": test.id,
            },
        )
        return test

    async def get_active_prompt(
        self, node_name: str, run_id: Optional[str] = None
    ) -> Optional[str]:
        async with get_session() as s:
            q = (
                select(ABTest)
                .where(ABTest.node_name == node_name)
                .order_by(ABTest.created_at.desc())
            )
            res = await s.execute(q)
            tests = list(res.scalars().all())
        if not tests:
            return None
        # Prefer a run-scoped test if one matches; otherwise the most recent
        # global (run_id is null) test.
        chosen = None
        if run_id is not None:
            chosen = next((t for t in tests if t.run_id == run_id), None)
        if chosen is None:
            chosen = next((t for t in tests if t.run_id is None), tests[0])
        return (
            chosen.variant_a_prompt
            if chosen.active_variant == ABVariant.a
            else chosen.variant_b_prompt
        )

    # -------- Internal --------

    async def _set_run_status(
        self,
        run_id: str,
        status: RunStatus,
        ended: bool = False,
    ) -> None:
        async with get_session() as s:
            res = await s.execute(select(Run).where(Run.id == run_id))
            run = res.scalar_one_or_none()
            if run is None:
                return
            run.status = status
            if ended:
                run.ended_at = utcnow()
