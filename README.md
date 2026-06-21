<div align="center">
  <!-- Replace with your banner: <img src="your-banner.png" alt="LangMonitor" width="800" /> -->
  ![Langmonitor banner](langmonitor.png)

  <h1>LangMonitor</h1>

  <p><strong>Real-time observability and operator controls for LangGraph agents. One import. Full control.</strong></p>

  [![PyPI](https://img.shields.io/badge/pip-langmonitor-blue)](https://pypi.org/project/langmonitor/)
  [![Python](https://img.shields.io/badge/python-3.10+-blue)](https://python.org)
  [![FastAPI](https://img.shields.io/badge/fastapi-0.100+-green)](https://fastapi.tiangolo.com)
  [![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)
</div>

---

LangMonitor watches and controls your LangGraph agents from the outside. It does three things:

1. **Streams** every node start/end, LLM call, and state diff in real time over WebSocket.
2. **Controls** any live run — kill, pause, resume, inject state, swap prompts — over a REST API.
3. **Checkpoints** each run on top of LangGraph's native checkpointer, so you can roll back to any point.

Wrap your compiled graph in one line. LangMonitor spins up a dashboard with full Swagger docs on a port you choose — open it in a browser and operate your agent live.

## Install

```bash
pip install langmonitor
```

Python 3.10+.

## Quick Start

Wrap your compiled graph and pick a port. That's it:

```python
from langmonitor import monitor

monitored = monitor(compiled_graph, port=8000, open_browser=True)
result = monitored.invoke({"input": "hello"})
```

`monitor()` returns a drop-in stand-in for your graph — same `invoke` / `ainvoke`. The moment you call it, a dashboard goes live at **http://localhost:8000/docs**. From that Swagger page you can watch every node, then kill, pause, resume, inject state, roll back, or A/B-swap the running agent.

No separate server to run. The dashboard lives inside your process.

## Full example

A complete, copy-paste script — builds a tiny graph, monitors it, and opens the dashboard:

```python
from typing import TypedDict

from langgraph.graph import StateGraph, END
from langmonitor import monitor


class State(TypedDict):
    n: int


def inc(state: State) -> State:
    return {"n": state["n"] + 1}


def double(state: State) -> State:
    return {"n": state["n"] * 2}


# 1. Build a normal LangGraph graph
graph = StateGraph(State)
graph.add_node("inc", inc)
graph.add_node("double", double)
graph.set_entry_point("inc")
graph.add_edge("inc", "double")
graph.add_edge("double", END)
compiled = graph.compile()

# 2. Wrap it — launches the dashboard at http://localhost:8000/docs
monitored = monitor(compiled, port=8000, open_browser=True)

# 3. Run it like any compiled graph
result = monitored.invoke({"n": 3})
print("result:", result)                 # {'n': 8}  → (3 + 1) * 2
print("dashboard:", monitored.dashboard_url)

# 4. Keep the process alive so you can explore the run in Swagger.
#    (The dashboard lives in this process, so it stops when the script exits.)
input("Press Enter to quit…")
```

Run it:

```bash
pip install langmonitor langgraph
python example.py
```

Then open **http://localhost:8000/docs** to inspect the trace, replay checkpoints, add guardrails, or kill/pause the next run.

## How it works

`monitor()` has three modes — pick one by what you pass:

```python
# 1. Embedded (default) — launches a dashboard in this process on the given port
monitor(graph, port=8000)

# 2. Remote — connect to a LangMonitor server running elsewhere
monitor(graph, server_url="ws://monitor.internal:8000", api_key="…")

# 3. In-process engine — route events straight to a MainEngine (used in tests)
monitor(graph, in_process_engine=engine)
```

If the dashboard can't start (or a remote server is down), monitoring fails open — your agent keeps running, just unmonitored. Monitoring never breaks the thing it's monitoring.

## Usage

### Wrap a graph

```python
from langgraph.graph import StateGraph, END
from langmonitor import monitor

graph = StateGraph(MyState)
# ... add_node / add_edge / set_entry_point ...
compiled = graph.compile()

monitored = monitor(compiled, port=8000)

# Sync
result = monitored.invoke({"input": "hello"})

# Or async
result = await monitored.ainvoke({"input": "hello"})

print("Dashboard:", monitored.dashboard_url)   # http://127.0.0.1:8000
```

Each run gets a `run_id` (emitted as the `run_started` event and listed at `GET /api/v1/runs`). Use it for every control below.

### Operate from Swagger

Open **http://localhost:8000/docs** and call the endpoints directly — it's the fastest way to drive a run by hand. Everything there is also a plain REST call you can script.

From Python:

```python
import httpx

base = "http://localhost:8000/api/v1"

httpx.post(f"{base}/runs/{run_id}/pause")
httpx.post(f"{base}/runs/{run_id}/resume", json={"state_patch": {"context": "updated"}})
httpx.post(f"{base}/runs/{run_id}/kill")
```

Or from the shell:

```bash
curl -X POST localhost:8000/api/v1/runs/<run_id>/pause
curl -X POST localhost:8000/api/v1/runs/<run_id>/kill
```

Kill and pause take effect before the next node — the wrapper checks for them between steps.

### Roll back to a checkpoint

```bash
# Save a named checkpoint
curl -X POST localhost:8000/api/v1/runs/<run_id>/checkpoints \
  -d '{"label": "before-tool-call"}' -H 'content-type: application/json'

# Restore it — auto-pauses the run so you can inspect before resuming
curl -X POST localhost:8000/api/v1/runs/<run_id>/checkpoints/<checkpoint_id>/rollback
```

With `CHECKPOINT_AUTO_SAVE=true` (the default) a checkpoint is also taken after every node end.

### Add guardrails

Guardrails run after every node end. When one trips, it fires the configured action (`kill`, `pause`, or `alert`).

```bash
curl -X POST localhost:8000/api/v1/guardrails -H 'content-type: application/json' -d '{
  "name": "cost cap",
  "rule_type": "max_cost_usd",
  "config": { "threshold": 2.0 },
  "action": "kill"
}'
```

Built-in rule types: `max_tool_calls`, `max_node_repeats`, `max_latency_ms`, `max_cost_usd`, and `custom_condition`.

A `custom_condition` evaluates a small, **sandboxed** boolean expression against the current node — no `eval`, no attribute access or calls:

```json
{
  "name": "slow planner",
  "rule_type": "custom_condition",
  "config": { "expression": "node_name == 'planner' and latency_ms > 5000" },
  "action": "alert"
}
```

Available names: `node_name`, `latency_ms`, `tokens_used`, `sequence_order`.

### A/B test a node prompt

```bash
# Create the test
curl -X POST localhost:8000/api/v1/ab-tests -H 'content-type: application/json' -d '{
  "node_name": "planner",
  "prompt_a": "You are a careful planner.",
  "prompt_b": "You are an aggressive planner."
}'

# Swap the active variant mid-run
curl -X POST localhost:8000/api/v1/ab-tests/<id>/swap
```

The wrapper picks up the active variant automatically before each node — no code changes needed.

### Stream events

Consume the live event stream from any client. In Python:

```python
import asyncio, json, websockets

async def watch():
    # Add ?api_key=YOUR_KEY when an API_KEY is set
    async with websockets.connect("ws://localhost:8000/ws/all") as ws:
        async for raw in ws:
            event = json.loads(raw)
            print(event["type"], event["payload"])

asyncio.run(watch())
```

Two channels are available:

```
WS  /ws/runs/{run_id}   — events for one run
WS  /ws/all             — every event across all runs
```

Every message has the same shape:

```json
{
  "type": "node_end",
  "run_id": "<uuid>",
  "timestamp": "<iso8601>",
  "payload": { "node_name": "planner", "latency_ms": 312, "tokens": 148 }
}
```

| Event | Key payload fields |
|---|---|
| `run_started` | `graph_name`, `input` |
| `node_start` | `node_name`, `sequence`, `input_state` |
| `node_end` | `node_name`, `sequence`, `output_state`, `latency_ms`, `tokens` |
| `llm_call` | `node_name`, `prompt`, `response`, `model`, `tokens`, `latency_ms` |
| `state_updated` | `sequence`, `state`, `diff` |
| `guardrail_alert` | `rule_name`, `rule_type`, `action` |
| `agent_paused` | `reason`, `node_name` |
| `agent_killed` | `reason` |
| `checkpoint_saved` | `checkpoint_id`, `label`, `sequence` |
| `run_ended` | `status`, `total_tokens`, `total_cost_usd`, `duration_ms` |

### Run a shared server (teams)

Embedded is perfect for one developer and one process. To watch agents running across many processes or machines, run one standalone server and point the SDK at it:

```bash
langmonitor                 # or: python -m langmonitor.main  → http://0.0.0.0:8000
```

```python
monitor(graph, server_url="ws://monitor.internal:8000", api_key="YOUR_KEY")
```

## Configuration

The embedded dashboard picks up the same settings as the standalone server. Set them in the environment or a `.env` file (see `.env.example`).

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | `sqlite+aiosqlite:///./langmonitor.db` | SQLAlchemy async URL. Use `postgresql+asyncpg://...` for Postgres. |
| `SERVER_HOST` | `0.0.0.0` | Bind host for the standalone server. |
| `SERVER_PORT` | `8000` | Bind port for the standalone server. |
| `LOG_LEVEL` | `INFO` | Python log level. |
| `API_KEY` | `""` | Shared secret required on every REST/WS request. Empty = unauthenticated (dev only). |
| `CORS_ALLOW_CREDENTIALS` | `false` | Send `Access-Control-Allow-Credentials`. Force-disabled with a `*` origin. |
| `ENABLE_DOCS` | `true` | Expose `/docs`, `/redoc`, `/openapi.json`. Set `false` in production. |
| `CHECKPOINT_AUTO_SAVE` | `true` | Auto-save a checkpoint after every node end. |
| `GUARDRAIL_EVAL_ENABLED` | `true` | Set `false` to bypass all guardrail evaluation. |
| `MAX_WS_CONNECTIONS_PER_RUN` | `50` | Cap on WebSocket connections per run channel. |
| `MAX_WS_CONNECTIONS_GLOBAL` | `200` | Cap on connections to `/ws/all`. |
| `MAX_ACTIVE_GUARDRAIL_RULES` | `500` | Cap on active rules (each is evaluated on every node end). |
| `MAX_REQUEST_BYTES` | `1048576` | Max REST request body size. |
| `MAX_STATE_PATCH_BYTES` / `MAX_STATE_PATCH_DEPTH` | `262144` / `32` | Bounds on injected state patches. |
| `MAX_AB_PROMPT_CHARS` | `20000` | Max length of an A/B prompt variant. |
| `CORS_ORIGINS` | `["http://localhost:3000"]` | JSON list, comma-separated string, or single origin. |
| `LANGGRAPH_CHECKPOINT_DB` | `./langgraph_checkpoints.db` | LangGraph SqliteSaver path. |

## Security

LangMonitor is a control plane — anyone who can reach it can kill, pause, or inject state into your agents. The embedded dashboard binds to `127.0.0.1` by default, so it's local-only. Before exposing it on a network:

- **Set `API_KEY`.** Once set, every `/api/v1/*` route and WebSocket requires it (`X-API-Key` header for REST and the SDK; `?api_key=` query for browser clients). Pass the same value to the SDK via `monitor(graph, api_key="...")` or the `LANGMONITOR_API_KEY` env var. When empty the server runs open and warns at startup — fine for local dev only.
- **Lock down CORS** to origins you trust; the `*` + credentials combination is force-disabled.
- **Disable docs** with `ENABLE_DOCS=false` in production.
- Connection caps, rule-count limits, and payload-size bounds (see the table) blunt trivial DoS vectors, and `custom_condition` guardrails are AST-sandboxed.

## Tests

```bash
pip install -e ".[dev]"
pytest
```

Covers every sub-engine (trace, state, guardrails, checkpoints, control), the main engine event routing, every REST endpoint, the WebSocket broadcast, the SDK modes, the security hardening, and an end-to-end run with a real LangGraph `StateGraph`.

---

## License

[MIT](LICENSE). Simple and permissive — no surprises.
