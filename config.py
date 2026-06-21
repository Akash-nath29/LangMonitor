from __future__ import annotations

import json
from functools import lru_cache
from typing import List

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    DATABASE_URL: str = "sqlite+aiosqlite:///./langmonitor.db"
    SERVER_HOST: str = "0.0.0.0"
    SERVER_PORT: int = 8000
    LOG_LEVEL: str = "INFO"
    CHECKPOINT_AUTO_SAVE: bool = True
    GUARDRAIL_EVAL_ENABLED: bool = True
    CORS_ORIGINS: List[str] = Field(default_factory=lambda: ["http://localhost:3000"])
    LANGGRAPH_CHECKPOINT_DB: str = "./langgraph_checkpoints.db"

    # -------- Security / auth --------
    # Shared secret required in the X-API-Key header (REST) or as the api_key
    # query/header on the WebSocket. When empty the server runs UNAUTHENTICATED
    # — only acceptable for local development behind a trusted boundary. A loud
    # warning is logged at startup in that case.
    API_KEY: str = ""
    # Send `Access-Control-Allow-Credentials: true`. Auth here is header-based
    # (X-API-Key), so cookies are not needed; keep this False unless a browser
    # client genuinely relies on credentialed requests. It is force-disabled
    # whenever CORS_ORIGINS contains the "*" wildcard to avoid the forbidden
    # wildcard-origin + credentials combination.
    CORS_ALLOW_CREDENTIALS: bool = False
    # Expose the interactive OpenAPI docs (/docs, /redoc, /openapi.json). Turn
    # this off in production so the schema isn't a free attack map.
    ENABLE_DOCS: bool = True

    # -------- Resource / DoS limits --------
    MAX_WS_CONNECTIONS_PER_RUN: int = 50
    MAX_WS_CONNECTIONS_GLOBAL: int = 200
    # Cap on active guardrail rules — every node_end evaluates all of them, so an
    # unbounded count is an algorithmic DoS.
    MAX_ACTIVE_GUARDRAIL_RULES: int = 500
    # Maximum JSON request body accepted on any REST endpoint (bytes).
    MAX_REQUEST_BYTES: int = 1_048_576  # 1 MiB
    # Bounds on operator-supplied state patches (inject-state / resume).
    MAX_STATE_PATCH_BYTES: int = 262_144  # 256 KiB
    MAX_STATE_PATCH_DEPTH: int = 32
    # Maximum length of an A/B test prompt variant.
    MAX_AB_PROMPT_CHARS: int = 20_000

    @field_validator("CORS_ORIGINS", mode="before")
    @classmethod
    def _parse_cors(cls, v):
        if v is None or v == "":
            return ["http://localhost:3000"]
        if isinstance(v, list):
            return v
        if isinstance(v, str):
            s = v.strip()
            if s.startswith("["):
                try:
                    return json.loads(s)
                except json.JSONDecodeError:
                    pass
            return [item.strip() for item in s.split(",") if item.strip()]
        return v

    @property
    def cors_allow_credentials_effective(self) -> bool:
        """Credentials are never sent alongside a wildcard origin — that combo
        is rejected by browsers and is a classic CORS footgun."""
        if "*" in self.CORS_ORIGINS:
            return False
        return self.CORS_ALLOW_CREDENTIALS


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
