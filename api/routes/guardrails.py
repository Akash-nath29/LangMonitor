from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from langmonitor.engine.core import get_main_engine
from langmonitor.engines.guardrail_engine import GuardrailLimitError
from langmonitor.schemas.api import (
    GuardrailAlertOut,
    GuardrailRuleIn,
    GuardrailRuleOut,
    GuardrailRulePatch,
    GuardrailToggleIn,
    err,
    ok,
)

router = APIRouter(prefix="/guardrails", tags=["guardrails"])


@router.get("")
async def list_rules():
    main = get_main_engine()
    rules = await main.guardrail.list_rules()
    return ok([GuardrailRuleOut.model_validate(r).model_dump(mode="json") for r in rules])


@router.post("")
async def create_rule(body: GuardrailRuleIn):
    main = get_main_engine()
    try:
        rule = await main.guardrail.create_rule(
            name=body.name,
            rule_type=body.rule_type,
            config=body.config,
            action=body.action,
            is_active=body.is_active,
        )
    except GuardrailLimitError as e:
        raise HTTPException(status_code=429, detail=str(e))
    return ok(GuardrailRuleOut.model_validate(rule).model_dump(mode="json"))


@router.patch("/{rule_id}")
async def update_rule(rule_id: str, body: GuardrailRulePatch):
    main = get_main_engine()
    try:
        rule = await main.guardrail.update_rule(
            rule_id, body.model_dump(exclude_none=True)
        )
    except GuardrailLimitError as e:
        raise HTTPException(status_code=429, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    if rule is None:
        return err("rule not found")
    return ok(GuardrailRuleOut.model_validate(rule).model_dump(mode="json"))


@router.delete("/{rule_id}")
async def delete_rule(rule_id: str):
    main = get_main_engine()
    deleted = await main.guardrail.delete_rule(rule_id)
    if not deleted:
        return err("rule not found")
    return ok({"deleted": rule_id})


@router.patch("/{rule_id}/toggle")
async def toggle_rule(rule_id: str, body: GuardrailToggleIn):
    main = get_main_engine()
    try:
        rule = await main.guardrail.toggle_rule(rule_id, body.active)
    except GuardrailLimitError as e:
        raise HTTPException(status_code=429, detail=str(e))
    if rule is None:
        return err("rule not found")
    return ok(GuardrailRuleOut.model_validate(rule).model_dump(mode="json"))


@router.get("/alerts")
async def list_alerts(
    run_id: Optional[str] = Query(None),
    resolved: Optional[bool] = Query(None),
):
    main = get_main_engine()
    alerts = await main.guardrail.list_alerts(run_id=run_id, resolved=resolved)
    return ok(
        [GuardrailAlertOut.model_validate(a).model_dump(mode="json") for a in alerts]
    )


@router.patch("/alerts/{alert_id}/resolve")
async def resolve_alert(alert_id: str):
    main = get_main_engine()
    alert = await main.guardrail.resolve_alert(alert_id)
    if alert is None:
        return err("alert not found")
    return ok(GuardrailAlertOut.model_validate(alert).model_dump(mode="json"))
