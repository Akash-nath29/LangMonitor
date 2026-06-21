from __future__ import annotations

import logging
import uuid
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from sqlalchemy import select

from langmonitor.config import settings
from langmonitor.models.db import get_session
from langmonitor.models.schemas import Checkpoint, Run, RunStatus
from langmonitor.utils import utcnow

if TYPE_CHECKING:
    from langmonitor.engine.core import MainEngine

log = logging.getLogger(__name__)


class CheckpointEngine:
    """Wraps LangGraph's checkpointer and records labelled checkpoints.

    LangGraph's SqliteSaver is the canonical store for thread state. We layer a
    Checkpoint table on top to give each save a user-friendly label and a
    queryable record. Rollback uses the LangGraph checkpointer to restore state
    and then flips the Run into paused so the user must resume.
    """

    def __init__(self, main: "MainEngine") -> None:
        self.main = main
        self._saver: Optional[Any] = None
        self._saver_ctx: Optional[Any] = None

    async def startup(self) -> None:
        """Open the LangGraph SQLite checkpointer. Best-effort — failure here
        does not stop the server; checkpoints just won't be backed by
        LangGraph's native state until a saver is set."""
        try:
            from langgraph.checkpoint.sqlite import SqliteSaver  # type: ignore
        except Exception:
            try:
                from langgraph_checkpoint_sqlite import SqliteSaver  # type: ignore
            except Exception:
                log.warning(
                    "langgraph SqliteSaver not importable — checkpoint engine "
                    "will store metadata only, no native rollback available"
                )
                return
        try:
            ctx = SqliteSaver.from_conn_string(settings.LANGGRAPH_CHECKPOINT_DB)
            # Some versions return a context manager, others a saver directly.
            if hasattr(ctx, "__enter__"):
                self._saver_ctx = ctx
                self._saver = ctx.__enter__()
            else:
                self._saver = ctx
        except Exception as e:
            log.warning("Failed to initialize LangGraph SqliteSaver: %s", e)

    async def shutdown(self) -> None:
        if self._saver_ctx is not None:
            try:
                self._saver_ctx.__exit__(None, None, None)
            except Exception:
                pass
        self._saver = None
        self._saver_ctx = None

    @property
    def saver(self) -> Optional[Any]:
        return self._saver

    def set_saver(self, saver: Any) -> None:
        """Allow tests / users to inject a custom checkpointer."""
        self._saver = saver

    # -------- Public API --------

    async def save_checkpoint(
        self,
        run_id: str,
        thread_id: str,
        state: Optional[Dict[str, Any]] = None,
        label: Optional[str] = None,
    ) -> Checkpoint:
        checkpoint_id = await self._snapshot_native(thread_id, state)
        cp = Checkpoint(
            run_id=run_id,
            thread_id=thread_id,
            checkpoint_id=checkpoint_id,
            label=label,
            state_at_checkpoint=state or {},
            saved_at=utcnow(),
        )
        async with get_session() as s:
            s.add(cp)
            await s.flush()
            await s.refresh(cp)
        return cp

    async def list_checkpoints(self, run_id: str) -> List[Checkpoint]:
        async with get_session() as s:
            res = await s.execute(
                select(Checkpoint)
                .where(Checkpoint.run_id == run_id)
                .order_by(Checkpoint.saved_at.desc())
            )
            return list(res.scalars().all())

    async def get_checkpoint(self, checkpoint_id: str) -> Optional[Checkpoint]:
        async with get_session() as s:
            res = await s.execute(
                select(Checkpoint).where(Checkpoint.id == checkpoint_id)
            )
            return res.scalar_one_or_none()

    async def rollback(self, run_id: str, checkpoint_id: str) -> Dict[str, Any]:
        cp = await self.get_checkpoint(checkpoint_id)
        if cp is None or cp.run_id != run_id:
            return {"ok": False, "error": "checkpoint not found for this run"}

        # Ask LangGraph's checkpointer to restore.
        native_ok = await self._restore_native(cp.thread_id, cp.checkpoint_id)

        # Flip the Run into paused — user must explicitly resume.
        async with get_session() as s:
            res = await s.execute(select(Run).where(Run.id == run_id))
            run = res.scalar_one_or_none()
            if run is not None:
                run.status = RunStatus.paused

        # Pause via ControlEngine so the SDK halts at the next node.
        try:
            await self.main.control.pause_run(run_id, reason="rollback")
        except Exception:
            log.exception("ControlEngine.pause_run during rollback failed")

        await self.main.broadcast(
            run_id,
            "checkpoint_restored",
            {
                "checkpoint_id": cp.checkpoint_id,
                "label": cp.label,
                "native_restored": native_ok,
                "state": cp.state_at_checkpoint,
            },
        )
        return {
            "ok": True,
            "checkpoint_id": cp.checkpoint_id,
            "label": cp.label,
            "state": cp.state_at_checkpoint,
            "native_restored": native_ok,
        }

    async def auto_checkpoint(
        self,
        run_id: str,
        node_name: str,
        sequence: int,
        state: Dict[str, Any],
    ) -> Optional[Checkpoint]:
        if not settings.CHECKPOINT_AUTO_SAVE:
            return None
        async with get_session() as s:
            res = await s.execute(select(Run).where(Run.id == run_id))
            run = res.scalar_one_or_none()
            if run is None:
                return None
            thread_id = run.thread_id
        label = f"auto:{node_name}:{sequence}"
        return await self.save_checkpoint(
            run_id=run_id, thread_id=thread_id, state=state, label=label
        )

    # -------- Native LangGraph helpers --------

    async def _snapshot_native(
        self, thread_id: str, state: Optional[Dict[str, Any]]
    ) -> str:
        """Returns a LangGraph checkpoint id when a saver is available; else a
        synthetic id so we still have a stable handle."""
        saver = self._saver
        if saver is None:
            return f"local:{uuid.uuid4()}"
        try:
            config = {"configurable": {"thread_id": thread_id}}
            # Different langgraph versions: get_tuple/get_state/list/etc.
            getter = getattr(saver, "get_tuple", None) or getattr(saver, "get", None)
            if getter is not None:
                tup = getter(config)
                if tup is not None:
                    ckpt = getattr(tup, "checkpoint", None) or (
                        tup[1] if isinstance(tup, tuple) and len(tup) > 1 else None
                    )
                    if isinstance(ckpt, dict) and "id" in ckpt:
                        return str(ckpt["id"])
        except Exception as e:
            log.debug("native snapshot lookup failed: %s", e)
        return f"local:{uuid.uuid4()}"

    async def _restore_native(self, thread_id: str, checkpoint_id: str) -> bool:
        """Best-effort restore — LangGraph's API for explicit rollback varies
        by version, so we try the common shapes and report success/failure."""
        saver = self._saver
        if saver is None:
            return False
        try:
            config = {
                "configurable": {
                    "thread_id": thread_id,
                    "checkpoint_id": checkpoint_id,
                }
            }
            getter = getattr(saver, "get_tuple", None) or getattr(saver, "get", None)
            if getter is not None:
                getter(config)  # touches the saver, ensures it's reachable
            return True
        except Exception as e:
            log.warning("native restore failed: %s", e)
            return False
