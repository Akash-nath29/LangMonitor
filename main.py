from __future__ import annotations

import logging
import time
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from langmonitor import __version__
from langmonitor.api.auth import require_api_key
from langmonitor.api.routes import (
    checkpoints_router,
    control_router,
    guardrails_router,
    runs_router,
    states_router,
    traces_router,
)
from langmonitor.api.websocket import _pump_bus_to_clients, router as ws_router
from langmonitor.config import settings
from langmonitor.engine.core import MainEngine, set_main_engine
from langmonitor.utils import sanitize_for_log

log = logging.getLogger(__name__)


def _warn_on_insecure_config() -> None:
    if not settings.API_KEY:
        log.warning(
            "API_KEY is not set — LangMonitor is running UNAUTHENTICATED. "
            "Anyone who can reach %s:%s can kill/pause runs, inject state, and "
            "read all traces. Set API_KEY before exposing this server beyond "
            "localhost.",
            settings.SERVER_HOST,
            settings.SERVER_PORT,
        )
    if "*" in settings.CORS_ORIGINS and settings.CORS_ALLOW_CREDENTIALS:
        log.warning(
            "CORS_ORIGINS contains '*' — credentialed CORS has been disabled to "
            "avoid the forbidden wildcard-origin + credentials combination."
        )


def _configure_logging() -> None:
    logging.basicConfig(
        level=getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    _configure_logging()
    _warn_on_insecure_config()
    engine = MainEngine()
    await engine.startup()
    set_main_engine(engine)
    await _pump_bus_to_clients()

    log.info("LangMonitor %s ready on %s:%s", __version__, settings.SERVER_HOST, settings.SERVER_PORT)
    log.info("REST:  /api/v1/runs, /api/v1/guardrails, /api/v1/ab-tests, ...")
    log.info("WS:    /ws/runs/{run_id}, /ws/all")

    try:
        yield
    finally:
        await engine.shutdown()


def create_app() -> FastAPI:
    docs_kwargs = (
        {}
        if settings.ENABLE_DOCS
        else {"docs_url": None, "redoc_url": None, "openapi_url": None}
    )
    app = FastAPI(
        title="LangMonitor",
        version=__version__,
        description="Real-time monitoring and control plane for LangGraph agents.",
        lifespan=lifespan,
        **docs_kwargs,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.CORS_ORIGINS,
        allow_credentials=settings.cors_allow_credentials_effective,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.middleware("http")
    async def limit_body_size(request: Request, call_next):
        cl = request.headers.get("content-length")
        if cl is not None:
            try:
                if int(cl) > settings.MAX_REQUEST_BYTES:
                    return JSONResponse(
                        status_code=413,
                        content={
                            "success": False,
                            "data": None,
                            "error": "request body too large",
                        },
                    )
            except ValueError:
                return JSONResponse(
                    status_code=400,
                    content={
                        "success": False,
                        "data": None,
                        "error": "invalid content-length",
                    },
                )
        return await call_next(request)

    @app.middleware("http")
    async def log_requests(request: Request, call_next):
        t0 = time.time()
        response = await call_next(request)
        duration_ms = int((time.time() - t0) * 1000)
        log.info(
            "%s %s -> %d in %dms",
            sanitize_for_log(request.method, max_len=16),
            sanitize_for_log(request.url.path),
            response.status_code,
            duration_ms,
        )
        return response

    @app.get("/")
    async def root():
        return {
            "success": True,
            "data": {
                "name": "LangMonitor",
                "version": __version__,
                "docs": "/docs",
                "endpoints": [
                    "/api/v1/runs",
                    "/api/v1/guardrails",
                    "/api/v1/guardrails/alerts",
                    "/api/v1/ab-tests",
                    "/ws/runs/{run_id}",
                    "/ws/all",
                ],
            },
            "error": None,
        }

    @app.get("/healthz")
    async def healthz():
        return {"success": True, "data": {"status": "ok"}, "error": None}

    prefix = "/api/v1"
    auth = [Depends(require_api_key)]
    app.include_router(runs_router, prefix=prefix, dependencies=auth)
    app.include_router(traces_router, prefix=prefix, dependencies=auth)
    app.include_router(states_router, prefix=prefix, dependencies=auth)
    app.include_router(guardrails_router, prefix=prefix, dependencies=auth)
    app.include_router(checkpoints_router, prefix=prefix, dependencies=auth)
    app.include_router(control_router, prefix=prefix, dependencies=auth)
    app.include_router(ws_router)

    return app


app = create_app()


def main() -> None:
    import uvicorn

    uvicorn.run(
        "langmonitor.main:app",
        host=settings.SERVER_HOST,
        port=settings.SERVER_PORT,
        reload=False,
    )


if __name__ == "__main__":
    main()
