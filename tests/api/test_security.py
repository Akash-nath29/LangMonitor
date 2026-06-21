from __future__ import annotations

import json

import pytest
from fastapi.testclient import TestClient

import langmonitor.api.auth as auth_mod
from langmonitor.config import Settings
from langmonitor.engines.guardrail_engine import _eval_custom
from langmonitor.models.schemas import GuardrailAction, GuardrailRuleType, NodeEvent


@pytest.fixture
def app_factory(main_engine):
    from contextlib import asynccontextmanager

    from langmonitor.main import create_app

    @asynccontextmanager
    async def _noop_lifespan(app):
        yield

    def _make():
        app = create_app()
        app.router.lifespan_context = _noop_lifespan
        return app

    return _make


# -------- Authentication (findings #2, #3) --------


@pytest.mark.asyncio
async def test_rest_requires_api_key_when_configured(app_factory, monkeypatch):
    monkeypatch.setattr(auth_mod.settings, "API_KEY", "s3cret")
    with TestClient(app_factory()) as c:
        # No key -> 401.
        assert c.get("/api/v1/runs").status_code == 401
        # Wrong key -> 401.
        assert (
            c.get("/api/v1/runs", headers={"X-API-Key": "nope"}).status_code == 401
        )
        # Correct key -> 200.
        ok = c.get("/api/v1/runs", headers={"X-API-Key": "s3cret"})
        assert ok.status_code == 200
        # Health and root stay open.
        assert c.get("/healthz").status_code == 200


@pytest.mark.asyncio
async def test_open_mode_when_no_api_key(app_factory, monkeypatch):
    monkeypatch.setattr(auth_mod.settings, "API_KEY", "")
    with TestClient(app_factory()) as c:
        assert c.get("/api/v1/runs").status_code == 200


@pytest.mark.asyncio
async def test_ws_rejects_without_api_key(app_factory, monkeypatch):
    monkeypatch.setattr(auth_mod.settings, "API_KEY", "s3cret")
    with TestClient(app_factory()) as c:
        with pytest.raises(Exception):
            with c.websocket_connect("/ws/runs/wsr-auth") as ws:
                ws.receive_json()
        # With the key it connects.
        with c.websocket_connect("/ws/runs/wsr-auth?api_key=s3cret") as ws:
            ws.send_text(json.dumps({"ping": 1}))
            echo = ws.receive_json()
            assert echo["type"] == "echo"


# -------- CORS credentials + wildcard (finding #7) --------


def test_cors_credentials_disabled_with_wildcard():
    s = Settings(CORS_ORIGINS=["*"], CORS_ALLOW_CREDENTIALS=True)
    assert s.cors_allow_credentials_effective is False
    s2 = Settings(
        CORS_ORIGINS=["https://app.example.com"], CORS_ALLOW_CREDENTIALS=True
    )
    assert s2.cors_allow_credentials_effective is True


# -------- Guardrail threshold validation (finding #11) --------


@pytest.mark.asyncio
async def test_negative_threshold_rejected(app_factory, monkeypatch):
    monkeypatch.setattr(auth_mod.settings, "API_KEY", "")
    with TestClient(app_factory()) as c:
        r = c.post(
            "/api/v1/guardrails",
            json={
                "name": "bad",
                "rule_type": "max_cost_usd",
                "config": {"threshold": -1},
                "action": "kill",
            },
        )
        assert r.status_code == 422


@pytest.mark.asyncio
async def test_custom_condition_requires_expression(app_factory, monkeypatch):
    monkeypatch.setattr(auth_mod.settings, "API_KEY", "")
    with TestClient(app_factory()) as c:
        r = c.post(
            "/api/v1/guardrails",
            json={
                "name": "bad-custom",
                "rule_type": "custom_condition",
                "config": {},
                "action": "alert",
            },
        )
        assert r.status_code == 422


# -------- A/B prompt + state patch caps (findings #4, #5, #6) --------


@pytest.mark.asyncio
async def test_ab_prompt_length_capped(app_factory, monkeypatch):
    monkeypatch.setattr(auth_mod.settings, "API_KEY", "")
    with TestClient(app_factory()) as c:
        r = c.post(
            "/api/v1/ab-tests",
            json={
                "node_name": "planner",
                "prompt_a": "x" * 50_000,
                "prompt_b": "ok",
            },
        )
        assert r.status_code == 422


@pytest.mark.asyncio
async def test_inject_state_size_capped(app_factory, monkeypatch):
    monkeypatch.setattr(auth_mod.settings, "API_KEY", "")
    with TestClient(app_factory()) as c:
        r = c.post(
            "/api/v1/runs/ctl-sec/inject-state",
            json={"patch": {"blob": "x" * 500_000}},
        )
        assert r.status_code == 422


# -------- Custom guardrail RCE refusal end-to-end (finding #1) --------


def _node_event() -> NodeEvent:
    return NodeEvent(
        run_id="r",
        node_name="planner",
        event_type="end",
        latency_ms=1200,
        tokens_used=50,
        sequence_order=3,
    )


def test_custom_condition_evaluates_legit():
    ctx = _eval_custom("latency_ms > 1000", _node_event(), {})
    assert ctx is not None
    assert ctx["metric"] == "custom_condition"


@pytest.mark.parametrize(
    "expr",
    [
        "().__class__.__bases__[0].__subclasses__()",
        "__import__('os').system('id')",
        "node_name.__class__",
        "open('x').read()",
    ],
)
def test_custom_condition_refuses_rce(expr):
    assert _eval_custom(expr, _node_event(), {}) is None
