from __future__ import annotations

import enum
import uuid
from datetime import datetime
from typing import Any, Optional

from langmonitor.utils import utcnow

from sqlalchemy import (
    Boolean,
    DateTime,
    Enum as SAEnum,
    Float,
    ForeignKey,
    Integer,
    JSON,
    String,
    Text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from langmonitor.models.db import Base


def _uuid() -> str:
    return str(uuid.uuid4())


def _utcnow() -> datetime:
    return utcnow()


class RunStatus(str, enum.Enum):
    running = "running"
    paused = "paused"
    completed = "completed"
    killed = "killed"
    error = "error"


class NodeEventType(str, enum.Enum):
    start = "start"
    end = "end"
    error = "error"


class GuardrailRuleType(str, enum.Enum):
    max_tool_calls = "max_tool_calls"
    max_node_repeats = "max_node_repeats"
    max_latency_ms = "max_latency_ms"
    max_cost_usd = "max_cost_usd"
    custom_condition = "custom_condition"


class GuardrailAction(str, enum.Enum):
    pause = "pause"
    kill = "kill"
    alert = "alert"


class ABVariant(str, enum.Enum):
    a = "a"
    b = "b"


class Run(Base):
    __tablename__ = "runs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    graph_name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    status: Mapped[RunStatus] = mapped_column(
        SAEnum(RunStatus, name="run_status"),
        default=RunStatus.running,
        nullable=False,
        index=True,
    )
    input: Mapped[Optional[Any]] = mapped_column(JSON, nullable=True)
    output: Mapped[Optional[Any]] = mapped_column(JSON, nullable=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)
    ended_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    total_tokens: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    total_cost_usd: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    thread_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)

    node_events: Mapped[list["NodeEvent"]] = relationship(
        back_populates="run", cascade="all, delete-orphan", lazy="selectin"
    )
    state_snapshots: Mapped[list["StateSnapshot"]] = relationship(
        back_populates="run", cascade="all, delete-orphan"
    )
    alerts: Mapped[list["GuardrailAlert"]] = relationship(
        back_populates="run", cascade="all, delete-orphan"
    )
    checkpoints: Mapped[list["Checkpoint"]] = relationship(
        back_populates="run", cascade="all, delete-orphan"
    )


class NodeEvent(Base):
    __tablename__ = "node_events"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    run_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("runs.id", ondelete="CASCADE"), nullable=False, index=True
    )
    node_name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    event_type: Mapped[NodeEventType] = mapped_column(
        SAEnum(NodeEventType, name="node_event_type"), nullable=False
    )
    input_state: Mapped[Optional[Any]] = mapped_column(JSON, nullable=True)
    output_state: Mapped[Optional[Any]] = mapped_column(JSON, nullable=True)
    llm_prompt: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    llm_response: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    llm_model: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    tokens_used: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    latency_ms: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)
    sequence_order: Mapped[int] = mapped_column(Integer, nullable=False, index=True)

    run: Mapped[Run] = relationship(back_populates="node_events")
    snapshot: Mapped[Optional["StateSnapshot"]] = relationship(
        back_populates="node_event", uselist=False, cascade="all, delete-orphan"
    )


class StateSnapshot(Base):
    __tablename__ = "state_snapshots"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    run_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("runs.id", ondelete="CASCADE"), nullable=False, index=True
    )
    node_event_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("node_events.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    state: Mapped[Any] = mapped_column(JSON, nullable=False)
    state_diff: Mapped[Optional[Any]] = mapped_column(JSON, nullable=True)
    snapshot_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)

    run: Mapped[Run] = relationship(back_populates="state_snapshots")
    node_event: Mapped[NodeEvent] = relationship(back_populates="snapshot")


class GuardrailRule(Base):
    __tablename__ = "guardrail_rules"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    rule_type: Mapped[GuardrailRuleType] = mapped_column(
        SAEnum(GuardrailRuleType, name="guardrail_rule_type"), nullable=False
    )
    config: Mapped[Any] = mapped_column(JSON, nullable=False)
    action: Mapped[GuardrailAction] = mapped_column(
        SAEnum(GuardrailAction, name="guardrail_action"), nullable=False
    )
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)

    alerts: Mapped[list["GuardrailAlert"]] = relationship(
        back_populates="rule", cascade="all, delete-orphan"
    )


class GuardrailAlert(Base):
    __tablename__ = "guardrail_alerts"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    run_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("runs.id", ondelete="CASCADE"), nullable=False, index=True
    )
    rule_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("guardrail_rules.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    triggered_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)
    context: Mapped[Any] = mapped_column(JSON, nullable=False)
    resolved: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

    run: Mapped[Run] = relationship(back_populates="alerts")
    rule: Mapped[GuardrailRule] = relationship(back_populates="alerts")


class Checkpoint(Base):
    __tablename__ = "checkpoints"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    run_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("runs.id", ondelete="CASCADE"), nullable=False, index=True
    )
    thread_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    checkpoint_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    label: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    state_at_checkpoint: Mapped[Any] = mapped_column(JSON, nullable=False)
    saved_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)

    run: Mapped[Run] = relationship(back_populates="checkpoints")


class ABTest(Base):
    __tablename__ = "ab_tests"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    run_id: Mapped[Optional[str]] = mapped_column(
        String(36), ForeignKey("runs.id", ondelete="SET NULL"), nullable=True, index=True
    )
    node_name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    variant_a_prompt: Mapped[str] = mapped_column(Text, nullable=False)
    variant_b_prompt: Mapped[str] = mapped_column(Text, nullable=False)
    active_variant: Mapped[ABVariant] = mapped_column(
        SAEnum(ABVariant, name="ab_variant"), default=ABVariant.a, nullable=False
    )
    swapped_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)
