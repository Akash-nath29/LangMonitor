from __future__ import annotations

import ast
import logging
import operator
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from sqlalchemy import func, select

from langmonitor.config import settings
from langmonitor.models.db import get_session
from langmonitor.utils import utcnow
from langmonitor.models.schemas import (
    GuardrailAction,
    GuardrailAlert,
    GuardrailRule,
    GuardrailRuleType,
    NodeEvent,
    NodeEventType,
    Run,
)

if TYPE_CHECKING:
    from langmonitor.engine.core import MainEngine

log = logging.getLogger(__name__)


class GuardrailLimitError(Exception):
    """Raised when creating a rule would exceed the active-rule cap."""


class GuardrailEngine:
    """Evaluates active rules after every node_end event."""

    def __init__(self, main: "MainEngine") -> None:
        self.main = main

    # -------- Rule CRUD --------

    async def create_rule(
        self,
        name: str,
        rule_type: GuardrailRuleType,
        config: Dict[str, Any],
        action: GuardrailAction,
        is_active: bool = True,
    ) -> GuardrailRule:
        if is_active:
            active = await self._count_active_rules()
            if active >= settings.MAX_ACTIVE_GUARDRAIL_RULES:
                raise GuardrailLimitError(
                    f"active guardrail rule limit reached "
                    f"({settings.MAX_ACTIVE_GUARDRAIL_RULES})"
                )
        rule = GuardrailRule(
            name=name,
            rule_type=rule_type,
            config=config,
            action=action,
            is_active=is_active,
        )
        async with get_session() as s:
            s.add(rule)
            await s.flush()
            await s.refresh(rule)
        return rule

    async def list_rules(self, active_only: bool = False) -> List[GuardrailRule]:
        async with get_session() as s:
            q = select(GuardrailRule).order_by(GuardrailRule.created_at.desc())
            if active_only:
                q = q.where(GuardrailRule.is_active.is_(True))
            res = await s.execute(q)
            return list(res.scalars().all())

    async def get_rule(self, rule_id: str) -> Optional[GuardrailRule]:
        async with get_session() as s:
            res = await s.execute(
                select(GuardrailRule).where(GuardrailRule.id == rule_id)
            )
            return res.scalar_one_or_none()

    async def update_rule(
        self, rule_id: str, patch: Dict[str, Any]
    ) -> Optional[GuardrailRule]:
        async with get_session() as s:
            res = await s.execute(
                select(GuardrailRule).where(GuardrailRule.id == rule_id)
            )
            rule = res.scalar_one_or_none()
            if rule is None:
                return None
            # Re-validate config against the rule's type if it is being changed.
            if patch.get("config") is not None:
                from langmonitor.schemas.api import _validate_guardrail_config

                _validate_guardrail_config(rule.rule_type, patch["config"])
            # Enforce the active-rule cap when (re)activating a rule.
            if patch.get("is_active") and not rule.is_active:
                if await self._count_active_rules() >= settings.MAX_ACTIVE_GUARDRAIL_RULES:
                    raise GuardrailLimitError(
                        f"active guardrail rule limit reached "
                        f"({settings.MAX_ACTIVE_GUARDRAIL_RULES})"
                    )
            for k, v in patch.items():
                if v is None:
                    continue
                if hasattr(rule, k):
                    setattr(rule, k, v)
            await s.flush()
            await s.refresh(rule)
            return rule

    async def _count_active_rules(self) -> int:
        async with get_session() as s:
            res = await s.execute(
                select(func.count(GuardrailRule.id)).where(
                    GuardrailRule.is_active.is_(True)
                )
            )
            return int(res.scalar() or 0)

    async def toggle_rule(self, rule_id: str, active: bool) -> Optional[GuardrailRule]:
        return await self.update_rule(rule_id, {"is_active": active})

    async def delete_rule(self, rule_id: str) -> bool:
        async with get_session() as s:
            res = await s.execute(
                select(GuardrailRule).where(GuardrailRule.id == rule_id)
            )
            rule = res.scalar_one_or_none()
            if rule is None:
                return False
            await s.delete(rule)
        return True

    # -------- Alert queries --------

    async def list_alerts(
        self,
        run_id: Optional[str] = None,
        resolved: Optional[bool] = None,
    ) -> List[GuardrailAlert]:
        async with get_session() as s:
            q = select(GuardrailAlert).order_by(GuardrailAlert.triggered_at.desc())
            if run_id is not None:
                q = q.where(GuardrailAlert.run_id == run_id)
            if resolved is not None:
                q = q.where(GuardrailAlert.resolved.is_(resolved))
            res = await s.execute(q)
            return list(res.scalars().all())

    async def resolve_alert(self, alert_id: str) -> Optional[GuardrailAlert]:
        async with get_session() as s:
            res = await s.execute(
                select(GuardrailAlert).where(GuardrailAlert.id == alert_id)
            )
            alert = res.scalar_one_or_none()
            if alert is None:
                return None
            alert.resolved = True
            await s.flush()
            await s.refresh(alert)
            return alert

    # -------- Evaluation --------

    async def evaluate(
        self, run_id: str, node_event: NodeEvent
    ) -> List[Dict[str, Any]]:
        """Run every active rule against the latest node_event.

        Returns a list of dicts (one per triggered alert) so the MainEngine can
        broadcast WebSocket events and decide whether to pause/kill the run.
        """
        if not settings.GUARDRAIL_EVAL_ENABLED:
            return []

        rules = await self.list_rules(active_only=True)
        triggered: List[Dict[str, Any]] = []
        for rule in rules:
            context = await self._check_rule(rule, run_id, node_event)
            if context is None:
                continue
            alert = GuardrailAlert(
                run_id=run_id,
                rule_id=rule.id,
                triggered_at=utcnow(),
                context=context,
                resolved=False,
            )
            async with get_session() as s:
                s.add(alert)
                await s.flush()
            triggered.append(
                {
                    "alert_id": alert.id,
                    "rule_id": rule.id,
                    "rule_name": rule.name,
                    "rule_type": rule.rule_type.value,
                    "action": rule.action.value,
                    "context": context,
                }
            )
        return triggered

    async def _check_rule(
        self,
        rule: GuardrailRule,
        run_id: str,
        node_event: NodeEvent,
    ) -> Optional[Dict[str, Any]]:
        rt = rule.rule_type
        cfg = rule.config or {}

        if rt == GuardrailRuleType.max_tool_calls:
            target_node = cfg.get("node_name")
            threshold = int(cfg.get("threshold", 0))
            count = await self._count_node_calls(run_id, target_node)
            if count > threshold:
                return {
                    "metric": "tool_call_count",
                    "node_name": target_node,
                    "count": count,
                    "threshold": threshold,
                }
            return None

        if rt == GuardrailRuleType.max_node_repeats:
            threshold = int(cfg.get("threshold", 0))
            target = cfg.get("node_name") or node_event.node_name
            consecutive = await self._count_consecutive(run_id, target)
            if consecutive > threshold:
                return {
                    "metric": "consecutive_node_repeats",
                    "node_name": target,
                    "consecutive": consecutive,
                    "threshold": threshold,
                }
            return None

        if rt == GuardrailRuleType.max_latency_ms:
            threshold = int(cfg.get("threshold", 0))
            latency = node_event.latency_ms or 0
            if latency > threshold:
                return {
                    "metric": "node_latency_ms",
                    "node_name": node_event.node_name,
                    "latency_ms": latency,
                    "threshold": threshold,
                }
            return None

        if rt == GuardrailRuleType.max_cost_usd:
            threshold = float(cfg.get("threshold", 0))
            total = await self._run_total_cost(run_id)
            if total > threshold:
                return {
                    "metric": "total_cost_usd",
                    "cost_usd": total,
                    "threshold": threshold,
                }
            return None

        if rt == GuardrailRuleType.custom_condition:
            # Very small DSL: cfg["expression"] evaluated against node_event
            # attributes only. Refuses if expression looks dangerous.
            expr = cfg.get("expression")
            if not isinstance(expr, str):
                return None
            return _eval_custom(expr, node_event, cfg)

        return None

    # -------- Helpers --------

    async def _count_node_calls(
        self, run_id: str, node_name: Optional[str]
    ) -> int:
        if not node_name:
            return 0
        async with get_session() as s:
            res = await s.execute(
                select(func.count(NodeEvent.id))
                .where(NodeEvent.run_id == run_id)
                .where(NodeEvent.node_name == node_name)
                .where(NodeEvent.event_type == NodeEventType.end)
            )
            return int(res.scalar() or 0)

    async def _count_consecutive(self, run_id: str, node_name: str) -> int:
        async with get_session() as s:
            res = await s.execute(
                select(NodeEvent.node_name)
                .where(NodeEvent.run_id == run_id)
                .where(NodeEvent.event_type == NodeEventType.end)
                .order_by(NodeEvent.sequence_order.desc())
            )
            names = [row[0] for row in res.all()]
        count = 0
        for n in names:
            if n == node_name:
                count += 1
            else:
                break
        return count

    async def _run_total_cost(self, run_id: str) -> float:
        async with get_session() as s:
            res = await s.execute(select(Run).where(Run.id == run_id))
            run = res.scalar_one_or_none()
            return float(run.total_cost_usd) if run else 0.0


def _eval_custom(
    expression: str,
    node_event: NodeEvent,
    cfg: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """Evaluate a single boolean expression against a small whitelist.

    Allowed names: node_name, latency_ms, tokens_used, sequence_order.
    The expression is parsed to an AST and walked through ``_safe_eval`` — only
    literals, the whitelisted names, comparisons, boolean and basic arithmetic
    operators are permitted. There is no ``eval``: attribute access, function
    calls, comprehensions and every other node type are rejected, so the rule
    cannot reach the interpreter or the process. Returns the matching context
    dict if the expression is truthy, else None.
    """
    scope = {
        "node_name": node_event.node_name,
        "latency_ms": node_event.latency_ms or 0,
        "tokens_used": node_event.tokens_used or 0,
        "sequence_order": node_event.sequence_order,
    }
    try:
        result = bool(safe_eval(expression, scope))
    except _UnsafeExpression as e:
        log.warning("Refusing unsafe guardrail expression %r: %s", expression, e)
        return None
    except Exception as e:
        log.warning("custom guardrail eval failed: %s", e)
        return None
    if not result:
        return None
    return {"metric": "custom_condition", "expression": expression, "scope": scope}


class _UnsafeExpression(ValueError):
    """Raised when a custom_condition expression uses a disallowed construct."""


# Operators we are willing to execute. Notably absent: ** (cheap memory/CPU
# blow-ups like 9**9**9) and bitwise ops.
_BIN_OPS = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.FloorDiv: operator.floordiv,
    ast.Mod: operator.mod,
}
_CMP_OPS = {
    ast.Eq: operator.eq,
    ast.NotEq: operator.ne,
    ast.Lt: operator.lt,
    ast.LtE: operator.le,
    ast.Gt: operator.gt,
    ast.GtE: operator.ge,
    ast.In: lambda a, b: a in b,
    ast.NotIn: lambda a, b: a not in b,
}
_UNARY_OPS = {
    ast.Not: operator.not_,
    ast.USub: operator.neg,
    ast.UAdd: operator.pos,
}
_MAX_EXPR_LEN = 500


def safe_eval(expression: str, scope: Dict[str, Any]) -> Any:
    """Evaluate ``expression`` against ``scope`` with a hard AST whitelist."""
    if not isinstance(expression, str):
        raise _UnsafeExpression("expression must be a string")
    if len(expression) > _MAX_EXPR_LEN:
        raise _UnsafeExpression("expression too long")
    try:
        tree = ast.parse(expression, mode="eval")
    except SyntaxError as e:
        raise _UnsafeExpression(f"syntax error: {e}") from e
    return _eval_node(tree.body, scope)


def _eval_node(node: ast.AST, scope: Dict[str, Any]) -> Any:
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.Name):
        if node.id not in scope:
            raise _UnsafeExpression(f"unknown name: {node.id}")
        return scope[node.id]
    if isinstance(node, ast.BoolOp):
        values = [_eval_node(v, scope) for v in node.values]
        if isinstance(node.op, ast.And):
            result = True
            for v in values:
                result = result and v
            return result
        if isinstance(node.op, ast.Or):
            result = False
            for v in values:
                result = result or v
            return result
        raise _UnsafeExpression("unsupported boolean operator")
    if isinstance(node, ast.UnaryOp):
        op = _UNARY_OPS.get(type(node.op))
        if op is None:
            raise _UnsafeExpression("unsupported unary operator")
        return op(_eval_node(node.operand, scope))
    if isinstance(node, ast.BinOp):
        op = _BIN_OPS.get(type(node.op))
        if op is None:
            raise _UnsafeExpression("unsupported binary operator")
        return op(_eval_node(node.left, scope), _eval_node(node.right, scope))
    if isinstance(node, ast.Compare):
        left = _eval_node(node.left, scope)
        for op_node, comparator in zip(node.ops, node.comparators):
            op = _CMP_OPS.get(type(op_node))
            if op is None:
                raise _UnsafeExpression("unsupported comparison operator")
            right = _eval_node(comparator, scope)
            if not op(left, right):
                return False
            left = right
        return True
    if isinstance(node, (ast.List, ast.Tuple, ast.Set)):
        return [_eval_node(e, scope) for e in node.elts]
    raise _UnsafeExpression(f"disallowed expression element: {type(node).__name__}")
