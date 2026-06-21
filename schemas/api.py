from __future__ import annotations

import math
from datetime import datetime
from typing import Any, Dict, Generic, List, Optional, TypeVar

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from langmonitor.config import settings
from langmonitor.models.schemas import (
    ABVariant,
    GuardrailAction,
    GuardrailRuleType,
    NodeEventType,
    RunStatus,
)

T = TypeVar("T")


class Envelope(BaseModel, Generic[T]):
    success: bool = True
    data: Optional[T] = None
    error: Optional[str] = None


def ok(data: Any = None) -> Dict[str, Any]:
    return {"success": True, "data": data, "error": None}


def err(message: str, data: Any = None) -> Dict[str, Any]:
    return {"success": False, "data": data, "error": message}


# -------- Run schemas --------

class RunOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: str
    graph_name: str
    status: RunStatus
    input: Optional[Any] = None
    output: Optional[Any] = None
    started_at: datetime
    ended_at: Optional[datetime] = None
    total_tokens: int
    total_cost_usd: float
    thread_id: str


class RunSummary(BaseModel):
    run: RunOut
    node_count: int
    alert_count: int
    checkpoint_count: int


# -------- NodeEvent schemas --------

class NodeEventOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: str
    run_id: str
    node_name: str
    event_type: NodeEventType
    input_state: Optional[Any] = None
    output_state: Optional[Any] = None
    llm_prompt: Optional[str] = None
    llm_response: Optional[str] = None
    llm_model: Optional[str] = None
    tokens_used: Optional[int] = None
    latency_ms: Optional[int] = None
    timestamp: datetime
    sequence_order: int


class NodeStat(BaseModel):
    node_name: str
    call_count: int
    avg_latency_ms: float
    total_tokens: int


class LLMCallOut(BaseModel):
    node_name: str
    prompt: Optional[str] = None
    response: Optional[str] = None
    model: Optional[str] = None
    tokens: Optional[int] = None
    latency_ms: Optional[int] = None
    timestamp: datetime
    sequence_order: int


# -------- State schemas --------

class StateSnapshotOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: str
    run_id: str
    node_event_id: str
    state: Any
    state_diff: Optional[Any] = None
    snapshot_at: datetime


class StateDiffOut(BaseModel):
    from_seq: int
    to_seq: int
    diff: Any


# -------- Guardrail schemas --------

_THRESHOLD_RULES = {
    GuardrailRuleType.max_tool_calls,
    GuardrailRuleType.max_node_repeats,
    GuardrailRuleType.max_latency_ms,
    GuardrailRuleType.max_cost_usd,
}
# Generous upper bound — guards against nonsense values, not legitimate tuning.
_MAX_THRESHOLD = 1e12


def _validate_guardrail_config(
    rule_type: GuardrailRuleType, config: Dict[str, Any]
) -> Dict[str, Any]:
    if rule_type in _THRESHOLD_RULES:
        if "threshold" not in config:
            raise ValueError(f"{rule_type.value} requires a 'threshold'")
        threshold = config["threshold"]
        if isinstance(threshold, bool) or not isinstance(threshold, (int, float)):
            raise ValueError("threshold must be a number")
        if not math.isfinite(threshold):
            raise ValueError("threshold must be finite")
        if threshold < 0:
            raise ValueError("threshold must be non-negative")
        if threshold > _MAX_THRESHOLD:
            raise ValueError("threshold is unreasonably large")
    if rule_type == GuardrailRuleType.custom_condition:
        expr = config.get("expression")
        if not isinstance(expr, str) or not expr.strip():
            raise ValueError("custom_condition requires a non-empty 'expression'")
        if len(expr) > 500:
            raise ValueError("expression too long")
    return config


class GuardrailRuleIn(BaseModel):
    name: str = Field(min_length=1, max_length=255)
    rule_type: GuardrailRuleType
    config: Dict[str, Any]
    action: GuardrailAction
    is_active: bool = True

    @model_validator(mode="after")
    def _check_config(self) -> "GuardrailRuleIn":
        _validate_guardrail_config(self.rule_type, self.config)
        return self


class GuardrailRulePatch(BaseModel):
    name: Optional[str] = None
    config: Optional[Dict[str, Any]] = None
    action: Optional[GuardrailAction] = None
    is_active: Optional[bool] = None


class GuardrailRuleOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: str
    name: str
    rule_type: GuardrailRuleType
    config: Dict[str, Any]
    action: GuardrailAction
    is_active: bool
    created_at: datetime


class GuardrailToggleIn(BaseModel):
    active: bool


class GuardrailAlertOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: str
    run_id: str
    rule_id: str
    triggered_at: datetime
    context: Any
    resolved: bool


# -------- Checkpoint schemas --------

class CheckpointSaveIn(BaseModel):
    label: Optional[str] = None


class CheckpointOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: str
    run_id: str
    thread_id: str
    checkpoint_id: str
    label: Optional[str] = None
    state_at_checkpoint: Any
    saved_at: datetime


# -------- Control schemas --------

def _validate_state_patch(patch: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Bound operator-supplied state patches.

    LangMonitor is deliberately schema-agnostic — it cannot know any given
    agent's state shape, so the *contents* of a patch are intentionally open.
    What we can bound is resource usage: serialized size and nesting depth, so a
    caller can't exhaust memory or stall the merge with a pathological payload.
    """
    if patch is None:
        return None
    import json

    try:
        size = len(json.dumps(patch).encode("utf-8"))
    except (TypeError, ValueError) as e:
        raise ValueError(f"patch is not JSON-serializable: {e}") from e
    if size > settings.MAX_STATE_PATCH_BYTES:
        raise ValueError("state patch too large")

    def _depth(obj: Any, level: int) -> int:
        if level > settings.MAX_STATE_PATCH_DEPTH:
            raise ValueError("state patch nested too deeply")
        if isinstance(obj, dict):
            return max((_depth(v, level + 1) for v in obj.values()), default=level)
        if isinstance(obj, (list, tuple)):
            return max((_depth(v, level + 1) for v in obj), default=level)
        return level

    _depth(patch, 0)
    return patch


class ResumeIn(BaseModel):
    state_patch: Optional[Dict[str, Any]] = None

    @field_validator("state_patch")
    @classmethod
    def _bound_patch(cls, v):
        return _validate_state_patch(v)


class InjectStateIn(BaseModel):
    patch: Dict[str, Any]

    @field_validator("patch")
    @classmethod
    def _bound_patch(cls, v):
        return _validate_state_patch(v)


class ABTestIn(BaseModel):
    node_name: str = Field(min_length=1, max_length=255)
    prompt_a: str = Field(min_length=1, max_length=settings.MAX_AB_PROMPT_CHARS)
    prompt_b: str = Field(min_length=1, max_length=settings.MAX_AB_PROMPT_CHARS)
    run_id: Optional[str] = None


class ABTestOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: str
    run_id: Optional[str] = None
    node_name: str
    variant_a_prompt: str
    variant_b_prompt: str
    active_variant: ABVariant
    swapped_at: Optional[datetime] = None
    created_at: datetime


# -------- SDK event schemas --------

class SDKEvent(BaseModel):
    """Schema for events coming from the SDK over WebSocket or HTTP."""

    type: str = Field(
        description="Event type: run_start, node_start, node_end, llm_call, run_end"
    )
    run_id: Optional[str] = None
    thread_id: Optional[str] = None
    graph_name: Optional[str] = None
    payload: Dict[str, Any] = Field(default_factory=dict)


class WSMessage(BaseModel):
    type: str
    run_id: str
    timestamp: datetime
    payload: Dict[str, Any] = Field(default_factory=dict)
