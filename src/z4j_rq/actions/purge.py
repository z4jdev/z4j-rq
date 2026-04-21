"""``purge_queue`` action - empty an RQ queue.

Mirror of the Celery purge action's safety properties (audit H13):

- The brain attaches a ``confirm_token = SHA-256(queue_name + depth)``
  to every purge command. The adapter recomputes that locally
  against the *current* depth and rejects the action on mismatch.
- ``force=True`` bypasses both the token check and the depth-
  threshold guard. Reserved for emergency tooling.
- The ``Z4J_PURGE_THRESHOLD`` env var caps the depth above which
  the adapter refuses without ``force=True`` even if the token
  matches. Default 10 000.

RQ's purge primitive is ``Queue.empty()`` which atomically deletes
all queued jobs from Redis. There is no equivalent for jobs already
fetched into a worker - those continue to run.
"""

from __future__ import annotations

import hashlib
import logging
import os
from typing import Any

from z4j_core.models import CommandResult

logger = logging.getLogger("z4j.agent.rq.actions.purge")

_DEFAULT_THRESHOLD = 10_000


async def purge_queue_action(
    rq_app: Any,
    *,
    queue_name: str,
    confirm_token: str | None = None,
    force: bool = False,
) -> CommandResult:
    """Empty ``queue_name`` after token + threshold checks."""
    queue = _resolve_queue(rq_app, queue_name)
    if queue is None:
        return CommandResult(
            status="failed",
            error=f"queue {queue_name!r} not resolvable",
        )

    try:
        depth = int(getattr(queue, "count", 0) or 0)
    except Exception:  # noqa: BLE001
        depth = 0

    if not force:
        threshold = _threshold()
        if depth > threshold:
            return CommandResult(
                status="failed",
                error=(
                    f"refusing to purge {queue_name!r}: depth {depth} "
                    f"exceeds Z4J_PURGE_THRESHOLD={threshold}. Re-issue "
                    f"with force=true if this is intentional."
                ),
            )
        expected_token = _derive_token(queue_name, depth)
        if not confirm_token or confirm_token != expected_token:
            return CommandResult(
                status="failed",
                error=(
                    "purge confirm_token missing or stale (queue depth "
                    "may have changed); re-issue from the dashboard"
                ),
            )

    try:
        queue.empty()
    except Exception as exc:  # noqa: BLE001
        return CommandResult(status="failed", error=f"purge failed: {exc}")
    return CommandResult(
        status="success",
        result={"queue": queue_name, "purged": depth},
    )


def _resolve_queue(rq_app: Any, queue_name: str) -> Any | None:
    """Get a Queue handle without instantiating duplicates if rq_app exposes one."""
    factory = getattr(rq_app, "queue_for_name", None)
    if callable(factory):
        try:
            return factory(queue_name)
        except Exception:  # noqa: BLE001
            return None
    try:
        from rq import Queue  # type: ignore[import-not-found]
    except ImportError:
        return None
    connection = getattr(rq_app, "connection", None)
    if connection is None and hasattr(rq_app, "ping"):
        connection = rq_app
    if connection is None:
        return None
    try:
        return Queue(name=queue_name, connection=connection)
    except Exception:  # noqa: BLE001
        return None


def _derive_token(queue_name: str, depth: int) -> str:
    """SHA-256 over ``queue_name||depth`` - same shape as the brain emits."""
    payload = f"{queue_name}|{depth}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _threshold() -> int:
    raw = os.environ.get("Z4J_PURGE_THRESHOLD")
    if not raw:
        return _DEFAULT_THRESHOLD
    try:
        return max(0, int(raw))
    except ValueError:
        return _DEFAULT_THRESHOLD


__all__ = ["purge_queue_action"]
