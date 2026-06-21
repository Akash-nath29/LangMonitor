# Contributing to LangMonitor

Thanks for taking the time to contribute! This guide covers how to set up the
project, the conventions the codebase follows, and how to get a change merged.

## Development setup

LangMonitor targets **Python 3.10+**.

```bash
git clone <your-repo-url> langmonitor
cd langmonitor

python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate

pip install -e ".[dev]"          # editable install + dev/test extras
cp .env.example .env
```

Run the standalone server locally:

```bash
langmonitor                      # or: python -m langmonitor.main → http://localhost:8000
```

Run the test suite:

```bash
pytest
```

Please make sure the full suite passes before opening a pull request.

## Project layout

```
api/        FastAPI routes (api/routes/*) and the WebSocket endpoints (api/websocket.py)
api/auth.py API-key authentication for REST + WebSocket
engine/     MainEngine orchestrator (engine/core.py) and the in-process event bus
engines/    Sub-engines: trace, state, guardrail, checkpoint, control
models/     SQLAlchemy models (models/schemas.py) and the async session (models/db.py)
schemas/    Pydantic request/response models + envelope helpers (schemas/api.py)
sdk/        The client wrapper users import: monitor() / MonitoredGraph
config.py   Settings (env-driven, via pydantic-settings)
utils.py    Shared helpers (utcnow, identifier/log sanitization)
tests/      Pytest suite mirroring the package layout
```

## Architecture conventions

Keep changes consistent with how the code is already organized:

- **Routes stay thin.** A route validates input (via a Pydantic schema in
  `schemas/api.py`), calls the relevant engine, and wraps the result with the
  `ok()` / `err()` envelope helpers. No business logic or DB queries in routes.
- **Engines own the logic and the database.** Sub-engines don't call each other
  directly — they go through `MainEngine` (or the event bus). Each engine holds a
  back-reference to `MainEngine` so it can `broadcast(...)` WebSocket events.
- **Everything is async.** Use `async`/`await` end to end. Database access goes
  through `async with get_session() as s:`.
- **Responses use the envelope.** REST handlers return
  `{"success": bool, "data": ..., "error": ...}` via `ok()` / `err()`.
- **Timestamps are timezone-aware UTC.** Use `utils.utcnow()` — never
  `datetime.utcnow()` (it's deprecated and returns a naive value).

## Common changes

**Add a guardrail rule type**
1. Add the value to `GuardrailRuleType` in `models/schemas.py`.
2. Handle it in `GuardrailEngine._check_rule` (`engines/guardrail_engine.py`).
3. Validate its `config` in `_validate_guardrail_config` (`schemas/api.py`).
4. Add a test under `tests/engines/test_guardrail_engine.py`.

**Add a REST endpoint**
1. Add the route to the appropriate router in `api/routes/`.
2. Define request/response models in `schemas/api.py`.
3. The router is auth-gated automatically — don't add new public routes without a
   reason. Add a test under `tests/api/`.

**Add an SDK event**
Route it through `MainEngine.handle_sdk_event` and emit it from
`sdk/monitor.py`. Document any new WebSocket event in the README's events table.

## Security

LangMonitor is a control plane, so security regressions matter:

- **Never** introduce `eval`, `exec`, `pickle.loads` on untrusted input, or
  string-built SQL. Custom guardrail expressions go through the AST-sandboxed
  evaluator in `engines/guardrail_engine.py` — extend that, don't bypass it.
- Validate and bound any new user-supplied input (size, type, range). Mutating or
  sensitive endpoints must stay behind the `require_api_key` dependency.
- If you find a security issue, please report it privately to the maintainer
  rather than opening a public issue.

## Tests

- Add or update tests for every behavior change. The suite mirrors the package
  layout under `tests/`.
- Engine logic uses the `main_engine` fixture (`tests/conftest.py`); API tests
  build the app with a no-op lifespan and a `TestClient`.
- Tests run against an isolated temporary SQLite database — they never touch your
  dev `langmonitor.db`.

## Pull requests

1. Branch off `main` (or `master`).
2. Keep the change focused; one logical change per PR.
3. Match the surrounding style — no separate formatter is enforced, just keep it
   consistent (4-space indent, type hints, `from __future__ import annotations`).
4. Make sure `PYTHONPATH=. pytest` is green and update the README/`.env.example`
   if you add config or endpoints.
5. Write a clear PR description: what changed, why, and how you tested it.

## License

By contributing, you agree that your contributions are licensed under the
project's [MIT License](LICENSE).
