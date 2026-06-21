from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

import pytest
import pytest_asyncio


# Ensure the project root is on sys.path so `import langmonitor` works when
# pytest is invoked from the langmonitor/ directory.
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture(scope="session", autouse=True)
def _isolated_db(tmp_path_factory):
    """Point every test at an isolated SQLite file so we never touch the
    developer's dev database."""
    tmp_dir = tmp_path_factory.mktemp("langmonitor-tests")
    db_path = tmp_dir / "test.db"
    os.environ["DATABASE_URL"] = f"sqlite+aiosqlite:///{db_path}"
    os.environ["LANGGRAPH_CHECKPOINT_DB"] = str(tmp_dir / "lg.db")
    os.environ["CHECKPOINT_AUTO_SAVE"] = "true"
    os.environ["GUARDRAIL_EVAL_ENABLED"] = "true"
    # Force a fresh Settings instance.
    from langmonitor import config as _cfg

    _cfg.get_settings.cache_clear()
    _cfg.settings = _cfg.get_settings()

    # Rebuild the SQLAlchemy engine bound to the new URL.
    from langmonitor.models import db as _db
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    _db.engine = create_async_engine(_cfg.settings.DATABASE_URL, future=True)
    _db.async_session_factory = async_sessionmaker(
        bind=_db.engine, expire_on_commit=False, autoflush=False
    )
    yield


@pytest_asyncio.fixture
async def main_engine():
    """Fresh MainEngine + tables per test."""
    from langmonitor.engine.core import MainEngine, set_main_engine
    from langmonitor.models.db import Base, engine

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

    eng = MainEngine()
    await eng.startup()
    set_main_engine(eng)
    try:
        yield eng
    finally:
        await eng.shutdown()
