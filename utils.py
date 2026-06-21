from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any


def utcnow() -> datetime:
    """Timezone-aware current UTC time.

    Replaces the deprecated ``datetime.utcnow()`` (which returns a naive value).
    """
    return datetime.now(timezone.utc)


def ensure_aware(dt: datetime) -> datetime:
    """Return ``dt`` as a timezone-aware UTC datetime.

    Values read back from SQLite come out naive even when stored aware, so any
    arithmetic on persisted datetimes normalises through here first to avoid
    "can't subtract offset-naive and offset-aware" errors.
    """
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


# Identifiers (run_id, thread_id, checkpoint_id, alert/rule ids, …) are usually
# UUIDs but the SDK also uses caller-supplied thread ids and the literal
# "__sdk__" sentinel, so we validate shape/length rather than enforcing UUIDs.
_ID_RE = re.compile(r"^[A-Za-z0-9_:.\-]{1,128}$")


def is_valid_identifier(value: str) -> bool:
    return bool(_ID_RE.fullmatch(value or ""))


def sanitize_for_log(value: Any, max_len: int = 256) -> str:
    """Strip control characters so attacker-controlled values can't forge or
    corrupt log lines (newlines, carriage returns, ANSI escapes, …)."""
    s = str(value)
    s = re.sub(r"[\x00-\x1f\x7f]", "", s)
    if len(s) > max_len:
        s = s[:max_len] + "…"
    return s
