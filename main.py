from __future__ import annotations

import logging
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import Depends, FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

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

# The interactive dashboard is a statically-exported single-page app bundled
# inside the package (langmonitor/static/dashboard). When present it is served
# at the server root, replacing Swagger as the default landing page.
DASHBOARD_DIR = Path(__file__).resolve().parent / "static" / "dashboard"


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
    if DASHBOARD_DIR.is_dir():
        log.info("UI:    interactive dashboard at /")
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

    dashboard_available = DASHBOARD_DIR.is_dir()

    @app.get("/api")
    async def api_info():
        return {
            "success": True,
            "data": {
                "name": "LangMonitor",
                "version": __version__,
                "dashboard": "/" if dashboard_available else None,
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

    # Serve the interactive dashboard SPA at the root. Mounted last so the API
    # (/api/v1/*), WebSocket (/ws/*), and meta routes (/healthz, /api, /docs)
    # always take precedence; the static mount only handles everything else.
    # html=True makes it serve <dir>/index.html for directory paths, so the
    # exported pages (/runs/, /run/, /guardrails/, /ab-tests/) resolve.
    if dashboard_available:
        app.mount(
            "/",
            StaticFiles(directory=str(DASHBOARD_DIR), html=True),
            name="dashboard",
        )
        log.info("Dashboard: serving interactive UI at /")
    else:
        log.warning(
            "Dashboard bundle not found at %s — serving API only. "
            "Build it with `npm run build` in langmonitor-ui and copy out/ "
            "to langmonitor/static/dashboard, or reinstall the package.",
            DASHBOARD_DIR,
        )

        @app.get("/")
        async def _root_redirect():
            return {
                "success": True,
                "data": {"name": "LangMonitor", "docs": "/docs", "api": "/api"},
                "error": None,
            }

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
