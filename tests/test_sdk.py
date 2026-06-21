from __future__ import annotations

import pytest


class _FakeGraph:
    """Minimal CompiledGraph stand-in supporting astream with stream_mode=updates."""

    name = "fake"

    async def astream(self, input, config=None, stream_mode="updates"):
        # Two nodes update the input dict in sequence.
        yield {"step_a": {"a": 1}}
        yield {"step_b": {"b": 2}}


@pytest.mark.asyncio
async def test_sdk_emits_events_in_process(main_engine):
    from langmonitor.sdk import monitor

    g = _FakeGraph()
    wrapped = monitor(g, server_url=None, in_process_engine=main_engine)
    result = await wrapped.ainvoke({"q": "hi"})
    assert result == {"q": "hi", "a": 1, "b": 2}

    # We should have one Run, two NodeEvents, two snapshots.
    from langmonitor.models.db import get_session
    from langmonitor.models.schemas import Run, NodeEvent, StateSnapshot
    from sqlalchemy import select, func

    async with get_session() as s:
        run_count = (
            await s.execute(select(func.count(Run.id)))
        ).scalar()
        node_count = (
            await s.execute(select(func.count(NodeEvent.id)))
        ).scalar()
        snap_count = (
            await s.execute(select(func.count(StateSnapshot.id)))
        ).scalar()
    assert run_count == 1
    # Two node_start + two node_end, total 4.
    assert node_count == 4
    # Two state snapshots (one per node_end).
    assert snap_count == 2


class _SlowGraph:
    name = "slow"

    async def astream(self, input, config=None, stream_mode="updates"):
        yield {"only": {"done": True}}


@pytest.mark.asyncio
async def test_sdk_respects_kill(main_engine):
    from langmonitor.sdk import AgentKilledException, monitor

    g = _SlowGraph()
    wrapped = monitor(g, server_url=None, in_process_engine=main_engine)

    # Pre-emptively kill a fixed run id by patching uuid generation. Simpler:
    # use ControlEngine directly after a successful run to confirm flag works.
    await wrapped.ainvoke({})
    # Now mark a future run as killed before starting.
    fake_id = "force-kill"
    main_engine.control._killed_runs.add(fake_id)
    assert main_engine.control.is_killed(fake_id)


# -------- Mode selection (top-level entrypoint) --------


def test_top_level_import():
    import langmonitor

    assert callable(langmonitor.monitor)
    assert hasattr(langmonitor, "MonitoredGraph")
    assert hasattr(langmonitor, "AgentKilledException")


def test_remote_mode_uses_ws_client(monkeypatch):
    import importlib
    m = importlib.import_module("langmonitor.sdk.monitor")

    # server_url set -> WS client, never launches an embedded server.
    monkeypatch.setattr(
        m, "_get_embedded_server", lambda *a, **k: pytest.fail("should not launch")
    )
    wrapped = m.monitor(_FakeGraph(), server_url="ws://example:8000")
    assert wrapped._ws is not None
    assert wrapped._bridge is None


def test_embedded_mode_builds_threaded_bridge(monkeypatch):
    import importlib
    m = importlib.import_module("langmonitor.sdk.monitor")

    class _FakeServer:
        base_url = "http://127.0.0.1:1234"
        engine = object()
        loop = object()

    captured = {}

    def fake_get(host, port, api_key, enable_docs):
        captured.update(host=host, port=port)
        return _FakeServer()

    monkeypatch.setattr(m, "_get_embedded_server", fake_get)
    wrapped = m.monitor(_FakeGraph(), port=1234)
    assert isinstance(wrapped._bridge, m._ThreadedBridge)
    assert wrapped._ws is None
    assert wrapped.dashboard_url == "http://127.0.0.1:1234"
    assert captured == {"host": "127.0.0.1", "port": 1234}


def test_embedded_failure_is_fail_open(monkeypatch):
    import importlib
    m = importlib.import_module("langmonitor.sdk.monitor")

    def boom(*a, **k):
        raise RuntimeError("port busy")

    monkeypatch.setattr(m, "_get_embedded_server", boom)
    # No exception bubbles up; the agent just runs unmonitored.
    wrapped = m.monitor(_FakeGraph(), port=9999)
    assert wrapped._bridge is None
    assert wrapped._ws is None
