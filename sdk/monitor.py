from __future__ import annotations

import asyncio
import json
import logging
import os
import threading
import time
import uuid
from typing import Any, Callable, Dict, List, Optional, Tuple

log = logging.getLogger(__name__)


class AgentKilledException(Exception):
    """Raised inside the wrapped graph to halt execution when the operator
    kills the run from the LangMonitor dashboard."""


# -------- LLM callback handler --------

try:
    from langchain_core.callbacks.base import BaseCallbackHandler

    class _LLMCaptureHandler(BaseCallbackHandler):
        """Captures prompt/response/tokens for every LLM call so the SDK can
        emit llm_call events. Stays decoupled from any specific provider."""

        def __init__(self, sink: Callable[[Dict[str, Any]], None]) -> None:
            self.sink = sink
            self._starts: Dict[str, Dict[str, Any]] = {}

        def on_llm_start(self, serialized, prompts, *, run_id, **kwargs):
            self._starts[str(run_id)] = {
                "prompt": "\n\n---\n\n".join(prompts) if prompts else None,
                "model": (serialized or {}).get("name")
                or (serialized or {}).get("id", [None])[-1],
                "started_at": time.time(),
            }

        def on_chat_model_start(self, serialized, messages, *, run_id, **kwargs):
            try:
                flat = []
                for batch in messages:
                    for m in batch:
                        content = getattr(m, "content", str(m))
                        flat.append(content)
                prompt = "\n".join(flat)
            except Exception:
                prompt = None
            self._starts[str(run_id)] = {
                "prompt": prompt,
                "model": (serialized or {}).get("name")
                or (serialized or {}).get("id", [None])[-1],
                "started_at": time.time(),
            }

        def on_llm_end(self, response, *, run_id, **kwargs):
            start = self._starts.pop(str(run_id), {})
            text = None
            try:
                generations = getattr(response, "generations", None)
                if generations and generations[0]:
                    g = generations[0][0]
                    text = getattr(g, "text", None) or getattr(
                        getattr(g, "message", None), "content", None
                    )
            except Exception:
                pass
            usage = {}
            try:
                usage = (
                    getattr(response, "llm_output", {}) or {}
                ).get("token_usage", {}) or {}
            except Exception:
                pass
            tokens = (
                usage.get("total_tokens")
                or usage.get("output_tokens")
                or None
            )
            latency_ms = (
                int((time.time() - start["started_at"]) * 1000)
                if "started_at" in start
                else None
            )
            self.sink(
                {
                    "prompt": start.get("prompt"),
                    "response": text,
                    "model": start.get("model"),
                    "tokens": tokens,
                    "latency_ms": latency_ms,
                }
            )

        def on_llm_error(self, error, *, run_id, **kwargs):
            self._starts.pop(str(run_id), None)

except Exception:  # pragma: no cover — langchain not installed in some test envs
    BaseCallbackHandler = None  # type: ignore
    _LLMCaptureHandler = None  # type: ignore


# -------- WebSocket client --------

class _WSClient:
    """Async WebSocket client with exponential backoff. Failures never raise
    — the agent must keep running even if the monitor server is down."""

    def __init__(self, url: str, headers: Optional[Dict[str, str]] = None) -> None:
        self.url = url
        self._headers = headers or {}
        self._ws = None
        self._connected = False
        self._lock = asyncio.Lock()
        self._backoff = 1.0
        self._stop = False

    async def connect(self) -> bool:
        if self._connected:
            return True
        try:
            import websockets  # type: ignore
        except ImportError:
            log.warning("websockets not installed — monitoring disabled")
            return False
        try:
            if self._headers:
                # websockets renamed extra_headers -> additional_headers in v14.
                try:
                    self._ws = await websockets.connect(
                        self.url, ping_interval=20, additional_headers=self._headers
                    )
                except TypeError:
                    self._ws = await websockets.connect(
                        self.url, ping_interval=20, extra_headers=self._headers
                    )
            else:
                self._ws = await websockets.connect(self.url, ping_interval=20)
            self._connected = True
            self._backoff = 1.0
            return True
        except Exception as e:
            log.debug("WS connect failed (%s); will retry", e)
            return False

    async def ensure(self) -> bool:
        if self._connected:
            return True
        async with self._lock:
            if self._connected:
                return True
            ok = await self.connect()
            if not ok:
                await asyncio.sleep(min(self._backoff, 30.0))
                self._backoff = min(self._backoff * 2, 30.0)
            return ok

    async def send(self, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not await self.ensure():
            return None
        try:
            await self._ws.send(json.dumps(payload))
            raw = await asyncio.wait_for(self._ws.recv(), timeout=10.0)
            return json.loads(raw)
        except Exception as e:
            log.debug("WS send failed (%s) — marking disconnected", e)
            self._connected = False
            try:
                if self._ws is not None:
                    await self._ws.close()
            except Exception:
                pass
            return None

    async def close(self) -> None:
        self._stop = True
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                pass
        self._connected = False


# -------- Local backend bridges --------

class _InProcessBridge:
    """Talks directly to a MainEngine running on the *same* event loop.

    Used by tests (and any same-loop embedding) where the engine and the agent
    share one asyncio loop, so coroutines can be awaited directly.
    """

    def __init__(self, main: Any) -> None:
        self.main = main

    async def send_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        return await self.main.handle_sdk_event(event)

    async def is_killed(self, run_id: str) -> bool:
        return await self.main.control.is_killed_async(run_id)

    async def await_if_paused(self, run_id: str) -> None:
        await self.main.control.await_if_paused(run_id)


class _ThreadedBridge:
    """Talks to a MainEngine running on *another* thread's event loop.

    The embedded dashboard server runs in a background thread with its own loop.
    All engine coroutines are marshalled onto that loop with
    ``run_coroutine_threadsafe`` so every piece of engine state — including the
    asyncio.Events that back pause/resume — lives on a single loop, while the
    result is awaited on the caller's loop via ``wrap_future``.
    """

    def __init__(self, main: Any, loop: asyncio.AbstractEventLoop) -> None:
        self.main = main
        self._loop = loop

    def _run(self, coro):
        return asyncio.wrap_future(
            asyncio.run_coroutine_threadsafe(coro, self._loop)
        )

    async def send_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        return await self._run(self.main.handle_sdk_event(event))

    async def is_killed(self, run_id: str) -> bool:
        return await self._run(self.main.control.is_killed_async(run_id))

    async def await_if_paused(self, run_id: str) -> None:
        await self._run(self.main.control.await_if_paused(run_id))


# -------- Embedded dashboard server --------

class _EmbeddedServer:
    """Runs the full LangMonitor server (REST + Swagger + WebSocket) inside the
    current process on a background thread, bound to a chosen port."""

    def __init__(
        self,
        host: str,
        port: int,
        api_key: Optional[str] = None,
        enable_docs: bool = True,
        log_level: str = "warning",
    ) -> None:
        self.host = host
        self.port = port
        self.api_key = api_key
        self.enable_docs = enable_docs
        self.log_level = log_level
        self._thread: Optional[threading.Thread] = None
        self._server: Optional[Any] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._engine: Optional[Any] = None

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"

    @property
    def engine(self) -> Any:
        return self._engine

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        return self._loop

    def start(self, timeout: float = 20.0) -> None:
        self._thread = threading.Thread(
            target=self._serve, name=f"langmonitor:{self.port}", daemon=True
        )
        self._thread.start()
        deadline = time.time() + timeout
        while time.time() < deadline:
            if (
                self._server is not None
                and getattr(self._server, "started", False)
                and self._loop is not None
            ):
                from langmonitor.engine.core import get_main_engine

                try:
                    self._engine = get_main_engine()
                except RuntimeError:
                    self._engine = None
                if self._engine is not None:
                    return
            if self._thread is not None and not self._thread.is_alive():
                raise RuntimeError(
                    "LangMonitor embedded server exited during startup "
                    f"(is port {self.port} already in use?)"
                )
            time.sleep(0.05)
        raise RuntimeError(
            f"LangMonitor embedded server failed to start on "
            f"{self.host}:{self.port} within {timeout}s"
        )

    def _serve(self) -> None:
        try:
            import uvicorn

            # Configure settings before the app + engine are built.
            from langmonitor.config import settings as _settings

            if self.api_key is not None:
                _settings.API_KEY = self.api_key
            _settings.ENABLE_DOCS = self.enable_docs
            # Keep startup logs/warnings accurate for the embedded bind.
            _settings.SERVER_HOST = self.host
            _settings.SERVER_PORT = self.port

            from langmonitor.main import create_app

            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            self._loop = loop

            app = create_app()
            config = uvicorn.Config(
                app,
                host=self.host,
                port=self.port,
                log_level=self.log_level,
                lifespan="on",
            )
            self._server = uvicorn.Server(config)
            loop.run_until_complete(self._server.serve())
        except Exception:
            log.exception("LangMonitor embedded server crashed")
        finally:
            if self._loop is not None:
                try:
                    self._loop.run_until_complete(self._loop.shutdown_asyncgens())
                except Exception:
                    pass
                self._loop.close()


_embedded_servers: Dict[Tuple[str, int], _EmbeddedServer] = {}
_embedded_lock = threading.Lock()


def _get_embedded_server(
    host: str, port: int, api_key: Optional[str], enable_docs: bool
) -> _EmbeddedServer:
    """Start (or reuse) one embedded server per host:port in this process."""
    key = (host, port)
    with _embedded_lock:
        srv = _embedded_servers.get(key)
        if srv is None:
            srv = _EmbeddedServer(
                host, port, api_key=api_key, enable_docs=enable_docs
            )
            srv.start()
            _embedded_servers[key] = srv
            log.info("LangMonitor dashboard live at %s/docs", srv.base_url)
        return srv


# -------- Monitored graph --------

class MonitoredGraph:
    """Wraps a LangGraph CompiledGraph (or any object with invoke()/stream()).

    Pre-node:
      1. await control_engine.await_if_paused(run_id) — via server
      2. if is_killed(run_id): raise AgentKilledException
      3. Apply any queued state patches
      4. Inject the active A/B prompt if one exists
      5. Emit node_start

    Post-node:
      6. Emit node_end with output + latency
      7. Emit llm_call events accumulated by the callback handler
    """

    def __init__(
        self,
        graph: Any,
        project: str = "default",
        server_url: Optional[str] = None,
        in_process_engine: Optional[Any] = None,
        api_key: Optional[str] = None,
        port: Optional[int] = None,
        host: str = "127.0.0.1",
        open_browser: bool = False,
    ) -> None:
        self.graph = graph
        self.project = project
        self.server_url = server_url
        self.dashboard_url: Optional[str] = None
        self._sequence = 0
        self._llm_buffer: List[Dict[str, Any]] = []
        self._bridge: Optional[Any] = None
        self._ws: Optional[_WSClient] = None
        # API key for authenticating to the server. Falls back to the
        # LANGMONITOR_API_KEY environment variable so it can be set without
        # touching agent code.
        api_key = api_key or os.environ.get("LANGMONITOR_API_KEY")
        if in_process_engine is not None:
            self._bridge = _InProcessBridge(in_process_engine)
        elif server_url is not None:
            # Connect to a LangMonitor server running elsewhere.
            ws_url = server_url.rstrip("/")
            if ws_url.startswith("http"):
                ws_url = ws_url.replace("http", "ws", 1)
            headers = {"X-API-Key": api_key} if api_key else None
            self._ws = _WSClient(f"{ws_url}/ws/runs/__sdk__", headers=headers)
        else:
            # Embedded: launch (or reuse) a dashboard in this process. Monitoring
            # is best-effort — if the server can't start we warn and keep the
            # agent running unmonitored rather than crash it.
            resolved_port = int(port) if port is not None else 8000
            try:
                server = _get_embedded_server(
                    host, resolved_port, api_key, enable_docs=True
                )
                self._bridge = _ThreadedBridge(server.engine, server.loop)
                self.dashboard_url = server.base_url
                if open_browser:
                    try:
                        import webbrowser

                        webbrowser.open(f"{server.base_url}/docs")
                    except Exception:
                        pass
            except Exception as e:
                log.warning(
                    "LangMonitor dashboard could not start on %s:%s (%s) — "
                    "agent will run unmonitored",
                    host,
                    resolved_port,
                    e,
                )

        # Wire up LLM callback if langchain is available.
        if _LLMCaptureHandler is not None:
            self._callback = _LLMCaptureHandler(self._record_llm_call)
        else:
            self._callback = None

    # -------- Public API mirrors CompiledGraph --------

    def invoke(self, input: Any, config: Optional[Dict[str, Any]] = None) -> Any:
        return asyncio.run(self.ainvoke(input, config))

    async def ainvoke(
        self, input: Any, config: Optional[Dict[str, Any]] = None
    ) -> Any:
        run_id = str(uuid.uuid4())
        thread_id = (
            (config or {}).get("configurable", {}).get("thread_id") or run_id
        )
        await self._emit(
            {
                "type": "run_start",
                "run_id": run_id,
                "thread_id": thread_id,
                "graph_name": self._graph_name(),
                "payload": {"input": _safe_jsonable(input)},
            }
        )

        merged_config = dict(config or {})
        configurable = dict(merged_config.get("configurable", {}))
        configurable.setdefault("thread_id", thread_id)
        merged_config["configurable"] = configurable
        callbacks = list(merged_config.get("callbacks", []))
        if self._callback is not None:
            callbacks.append(self._callback)
        merged_config["callbacks"] = callbacks

        try:
            output = await self._run_streamed(run_id, input, merged_config)
            await self._emit(
                {
                    "type": "run_end",
                    "run_id": run_id,
                    "payload": {
                        "status": "completed",
                        "output": _safe_jsonable(output),
                    },
                }
            )
            return output
        except AgentKilledException:
            await self._emit(
                {
                    "type": "run_end",
                    "run_id": run_id,
                    "payload": {"status": "killed"},
                }
            )
            raise
        except Exception:
            await self._emit(
                {
                    "type": "run_end",
                    "run_id": run_id,
                    "payload": {"status": "error"},
                }
            )
            raise

    async def _run_streamed(
        self, run_id: str, input: Any, config: Dict[str, Any]
    ) -> Any:
        """Use LangGraph's stream() to observe per-node state, emitting
        node_start/node_end around each step."""
        astream = getattr(self.graph, "astream", None)
        stream = getattr(self.graph, "stream", None)

        last_state: Any = input
        try:
            if astream is not None:
                async for chunk in astream(input, config, stream_mode="updates"):
                    last_state = await self._handle_chunk(
                        run_id, chunk, last_state
                    )
            elif stream is not None:
                for chunk in stream(input, config, stream_mode="updates"):
                    last_state = await self._handle_chunk(
                        run_id, chunk, last_state
                    )
            else:
                # No stream support — fall back to single invoke without
                # per-node visibility.
                invoke = getattr(self.graph, "ainvoke", None) or getattr(
                    self.graph, "invoke"
                )
                if asyncio.iscoroutinefunction(invoke):
                    last_state = await invoke(input, config)
                else:
                    last_state = invoke(input, config)
        except AgentKilledException:
            raise
        return last_state

    async def _handle_chunk(
        self, run_id: str, chunk: Dict[str, Any], prev_state: Any
    ) -> Any:
        # stream_mode="updates" yields {node_name: state_delta}
        if not isinstance(chunk, dict):
            return prev_state
        for node_name, delta in chunk.items():
            # Layer-3 gates before each node.
            await self._pre_node(run_id, node_name)

            self._sequence += 1
            seq = self._sequence
            t0 = time.time()
            await self._emit(
                {
                    "type": "node_start",
                    "run_id": run_id,
                    "payload": {
                        "node_name": node_name,
                        "sequence": seq,
                        "input_state": _safe_jsonable(prev_state),
                    },
                }
            )

            merged_state = _merge_state(prev_state, delta)
            latency_ms = int((time.time() - t0) * 1000)

            ack = await self._emit(
                {
                    "type": "node_end",
                    "run_id": run_id,
                    "payload": {
                        "node_name": node_name,
                        "sequence": seq,
                        "output_state": _safe_jsonable(merged_state),
                        "latency_ms": latency_ms,
                    },
                }
            )

            # Flush any LLM calls captured during this node.
            node_event_id = (
                (ack or {}).get("data", {}).get("node_event_id")
                if isinstance(ack, dict)
                else None
            ) or (ack or {}).get("node_event_id") if isinstance(ack, dict) else None
            await self._flush_llm_buffer(run_id, node_name, node_event_id)

            prev_state = merged_state
        return prev_state

    async def _pre_node(self, run_id: str, node_name: str) -> None:
        # In-process / embedded: check kill + pause directly on the engine.
        # is_killed_async / await_if_paused also consult the DB so controls set
        # before a server restart stay enforced.
        if self._bridge is not None:
            if await self._bridge.is_killed(run_id):
                raise AgentKilledException(f"run {run_id} killed")
            await self._bridge.await_if_paused(run_id)
            return
        # Remote server: poll kill/pause over the same WebSocket. If the server
        # is unreachable we fail open and keep going so a monitoring outage
        # never blocks or breaks the user's agent.
        if self._ws is not None:
            await self._ws_control_gate(run_id)

    async def _ws_control_gate(self, run_id: str) -> None:
        while True:
            resp = await self._ws.send(
                {"kind": "control_poll", "run_id": run_id}
            )
            if not isinstance(resp, dict):
                return  # unreachable / no response — fail open
            payload = resp.get("payload") or {}
            if payload.get("killed"):
                raise AgentKilledException(f"run {run_id} killed")
            if not payload.get("paused"):
                return
            # Paused: wait and re-poll until resumed or killed. A dropped
            # connection ends the wait (resp is not a dict) so we never hang.
            await asyncio.sleep(0.5)

    async def _flush_llm_buffer(
        self,
        run_id: str,
        node_name: str,
        node_event_id: Optional[str],
    ) -> None:
        if not self._llm_buffer:
            return
        for call in self._llm_buffer:
            await self._emit(
                {
                    "type": "llm_call",
                    "run_id": run_id,
                    "payload": {
                        "node_name": node_name,
                        "node_event_id": node_event_id,
                        "prompt": call.get("prompt"),
                        "response": call.get("response"),
                        "model": call.get("model"),
                        "tokens": call.get("tokens"),
                        "latency_ms": call.get("latency_ms"),
                    },
                }
            )
        self._llm_buffer.clear()

    def _record_llm_call(self, call: Dict[str, Any]) -> None:
        self._llm_buffer.append(call)

    async def _emit(self, event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        # In-process: synchronous routing through MainEngine.
        if self._bridge is not None:
            try:
                return await self._bridge.send_event(event)
            except Exception:
                log.exception("local bridge send failed")
                return None
        # Remote: best-effort WebSocket. Never raise to the user's agent.
        if self._ws is not None:
            try:
                return await self._ws.send(
                    {"kind": "sdk_event", "event": event}
                )
            except Exception:
                log.debug("ws emit failed — agent continues without monitoring")
                return None
        return None

    def _graph_name(self) -> str:
        return getattr(self.graph, "name", None) or self.graph.__class__.__name__


# -------- Public entrypoint --------

def monitor(
    graph: Any,
    project: str = "default",
    server_url: Optional[str] = None,
    in_process_engine: Optional[Any] = None,
    api_key: Optional[str] = None,
    port: Optional[int] = None,
    host: str = "127.0.0.1",
    open_browser: bool = False,
) -> MonitoredGraph:
    """Wrap a LangGraph compiled graph with LangMonitor instrumentation.

    Three modes, chosen by what you pass:

    - **Embedded (default).** ``monitor(graph)`` or ``monitor(graph, port=8000)``
      launches a dashboard in this process — open ``http://<host>:<port>/docs``
      (Swagger) to watch and control the run (kill, pause, resume, inject state,
      checkpoints, guardrails, A/B). Pass ``open_browser=True`` to pop it open.
    - **Remote.** ``monitor(graph, server_url="ws://host:8000")`` connects to a
      LangMonitor server running elsewhere. Pass ``api_key`` (or set
      ``LANGMONITOR_API_KEY``) if that server requires one.
    - **In-process engine.** ``monitor(graph, in_process_engine=<MainEngine>)``
      routes events straight to a MainEngine on the current loop (used in tests).
    """
    return MonitoredGraph(
        graph=graph,
        project=project,
        server_url=server_url,
        in_process_engine=in_process_engine,
        api_key=api_key,
        port=port,
        host=host,
        open_browser=open_browser,
    )


# -------- Helpers --------

def _merge_state(prev: Any, delta: Any) -> Any:
    if isinstance(prev, dict) and isinstance(delta, dict):
        merged = dict(prev)
        merged.update(delta)
        return merged
    return delta if delta is not None else prev


def _safe_jsonable(obj: Any) -> Any:
    """Best-effort coerce arbitrary objects into JSON-serializable form."""
    try:
        json.dumps(obj)
        return obj
    except Exception:
        pass
    if hasattr(obj, "model_dump"):
        try:
            return obj.model_dump()
        except Exception:
            pass
    if hasattr(obj, "dict"):
        try:
            return obj.dict()
        except Exception:
            pass
    if isinstance(obj, dict):
        return {str(k): _safe_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_safe_jsonable(v) for v in obj]
    return str(obj)
