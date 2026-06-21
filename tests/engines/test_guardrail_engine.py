from __future__ import annotations

import pytest

from langmonitor.models.db import get_session
from langmonitor.models.schemas import (
    GuardrailAction,
    GuardrailRuleType,
    Run,
    RunStatus,
)


async def _seed_run(run_id: str = "g1"):
    async with get_session() as s:
        s.add(Run(id=run_id, graph_name="g", status=RunStatus.running, thread_id="t"))


@pytest.mark.asyncio
async def test_max_tool_calls_triggers(main_engine):
    await _seed_run("g1")
    await main_engine.guardrail.create_rule(
        name="too many tool calls",
        rule_type=GuardrailRuleType.max_tool_calls,
        config={"node_name": "tool", "threshold": 2},
        action=GuardrailAction.alert,
    )
    for i in range(3):
        ne = await main_engine.trace.ingest_event(
            {
                "run_id": "g1",
                "node_name": "tool",
                "event_type": "end",
                "sequence_order": i + 1,
            }
        )
    alerts = await main_engine.guardrail.evaluate("g1", ne)
    assert len(alerts) == 1
    assert alerts[0]["rule_type"] == "max_tool_calls"
    assert alerts[0]["context"]["count"] == 3


@pytest.mark.asyncio
async def test_max_latency_triggers(main_engine):
    await _seed_run("g2")
    await main_engine.guardrail.create_rule(
        name="slow",
        rule_type=GuardrailRuleType.max_latency_ms,
        config={"threshold": 500},
        action=GuardrailAction.alert,
    )
    ne = await main_engine.trace.ingest_event(
        {
            "run_id": "g2",
            "node_name": "x",
            "event_type": "end",
            "latency_ms": 1200,
            "sequence_order": 1,
        }
    )
    alerts = await main_engine.guardrail.evaluate("g2", ne)
    assert len(alerts) == 1
    assert alerts[0]["context"]["latency_ms"] == 1200


@pytest.mark.asyncio
async def test_max_node_repeats_triggers(main_engine):
    await _seed_run("g3")
    await main_engine.guardrail.create_rule(
        name="loop",
        rule_type=GuardrailRuleType.max_node_repeats,
        config={"threshold": 2, "node_name": "loop_node"},
        action=GuardrailAction.alert,
    )
    for i in range(3):
        ne = await main_engine.trace.ingest_event(
            {
                "run_id": "g3",
                "node_name": "loop_node",
                "event_type": "end",
                "sequence_order": i + 1,
            }
        )
    alerts = await main_engine.guardrail.evaluate("g3", ne)
    assert len(alerts) == 1
    assert alerts[0]["context"]["consecutive"] == 3


@pytest.mark.asyncio
async def test_max_cost_triggers(main_engine):
    await _seed_run("g4")
    # Bump run's total_cost_usd directly.
    async with get_session() as s:
        from sqlalchemy import select
        from langmonitor.models.schemas import Run as R

        run = (await s.execute(select(R).where(R.id == "g4"))).scalar_one()
        run.total_cost_usd = 12.50

    await main_engine.guardrail.create_rule(
        name="too pricey",
        rule_type=GuardrailRuleType.max_cost_usd,
        config={"threshold": 5.0},
        action=GuardrailAction.alert,
    )
    ne = await main_engine.trace.ingest_event(
        {"run_id": "g4", "node_name": "x", "event_type": "end", "sequence_order": 1}
    )
    alerts = await main_engine.guardrail.evaluate("g4", ne)
    assert len(alerts) == 1
    assert alerts[0]["context"]["cost_usd"] == 12.5


@pytest.mark.asyncio
async def test_rule_toggle(main_engine):
    rule = await main_engine.guardrail.create_rule(
        name="r",
        rule_type=GuardrailRuleType.max_latency_ms,
        config={"threshold": 1},
        action=GuardrailAction.alert,
    )
    toggled = await main_engine.guardrail.toggle_rule(rule.id, False)
    assert toggled is not None and toggled.is_active is False
