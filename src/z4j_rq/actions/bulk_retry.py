"""``bulk_retry`` action - re-enqueue many RQ jobs in a single command.

Two modes of input:

1. **Explicit ids** - ``filter["task_ids"]`` is a list of job ids.
   We fetch each, re-enqueue on its origin queue, stop at ``max``.
2. **FailedJobRegistry sweep** - no explicit ids. We walk the
   ``FailedJobRegistry`` for each queue the rq_app knows about and
   re-enqueue up to ``max`` failed jobs. This is the "retry
   everything that blew up last hour" shape.

Both modes are batched: we yield every 100 jobs so we don't hold
the asyncio event loop hostage. The ``max`` ceiling matches the
brain-side hard cap (10 000) per audit H12.

Return shape mirrors the Celery adapter's bulk_retry:

    {"retried": N, "skipped": M, "capped": True|False,
     "new_task_ids": [...], "errors": {original_id: "..."}}
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from z4j_core.models import CommandResult

from z4j_rq.actions.retry import retry_task_action

logger = logging.getLogger("z4j.adapter.rq.actions.bulk_retry")

# Hard ceiling matching the brain-side cap. If the caller passes a
# ``max`` larger than this we clamp and mark ``capped: True``.
_MAX_ABSOLUTE = 10_000

# Yield back to the event loop every N processed ids so a bulk
# retry of 10 k jobs never blocks the runtime's other coroutines
# (heartbeat, transport, receive loop).
_YIELD_EVERY = 100


async def bulk_retry_action(
    rq_app: Any,
    *,
    filter: dict[str, Any] | None = None,
    max: int = 1000,
    task_ids: list[str] | None = None,
    task_priorities: dict[str, object] | None = None,  # noqa: ARG001  (RQ has none)
) -> CommandResult:
    """Re-enqueue up to ``max`` jobs; returns a summary dict."""
    filter = filter or {}
    effective_max = min(max, _MAX_ABSOLUTE)
    capped = max > _MAX_ABSOLUTE

    # Mode selection: explicit ids beat registry sweep.
    explicit_ids: list[str] = []
    if task_ids is not None:
        explicit_ids = [str(t) for t in task_ids]
    else:
        raw = filter.get("task_ids")
        if isinstance(raw, list):
            explicit_ids = [str(t) for t in raw]

    if explicit_ids:
        ids = explicit_ids[:effective_max]
        source = "explicit_ids"
    else:
        # Registry sweep - walk FailedJobRegistry per queue.
        ids = _collect_failed_ids(rq_app, limit=effective_max)
        source = "failed_registry"
        if len(ids) > effective_max:
            ids = ids[:effective_max]

    retried = 0
    skipped = 0
    new_ids: list[str] = []
    errors: dict[str, str] = {}

    for i, job_id in enumerate(ids, start=1):
        result = await retry_task_action(rq_app, task_id=job_id)
        if result.status == "success":
            if (result.result or {}).get("noop"):
                skipped += 1
            else:
                retried += 1
                new_id = (result.result or {}).get("task_id")
                if new_id:
                    new_ids.append(str(new_id))
        else:
            errors[job_id] = result.error or "unknown"

        # Be polite to the event loop on long batches.
        if i % _YIELD_EVERY == 0:
            await asyncio.sleep(0)

    # Enforce the capped flag even when we ran under the limit -
    # the caller needs to know the requested ``max`` was too high,
    # even if the actual batch happened to fit.
    return CommandResult(
        status="success",
        result={
            "retried": retried,
            "skipped": skipped,
            "capped": capped or (len(ids) >= effective_max and len(explicit_ids) == 0),
            "source": source,
            "new_task_ids": new_ids,
            "errors": errors,
        },
    )


def _collect_failed_ids(rq_app: Any, *, limit: int) -> list[str]:
    """Walk every queue's FailedJobRegistry and collect failed job ids."""
    try:
        from rq.registry import (  # type: ignore[import-not-found]
            FailedJobRegistry,
        )
    except ImportError:
        # Test shims may stub ``rq_app.failed_job_ids`` to short-
        # circuit the registry walk without an actual RQ install.
        stub = getattr(rq_app, "failed_job_ids", None)
        if callable(stub):
            try:
                return list(stub(limit=limit))
            except Exception:  # noqa: BLE001
                return []
        return []

    queues = _iter_queues(rq_app)
    out: list[str] = []
    for queue in queues:
        if len(out) >= limit:
            break
        try:
            registry = FailedJobRegistry(queue=queue)
            # RQ's ``get_job_ids(start, end)`` is 0-indexed INCLUSIVE.
            chunk = registry.get_job_ids(0, limit - len(out) - 1)
            out.extend(str(jid) for jid in chunk)
        except Exception:  # noqa: BLE001
            continue
    return out


def _iter_queues(rq_app: Any) -> list[Any]:
    candidate = getattr(rq_app, "queues", None)
    if candidate is not None:
        try:
            return list(candidate)
        except Exception:  # noqa: BLE001
            pass
    try:
        from rq import Queue  # type: ignore[import-not-found]
    except ImportError:
        return []
    connection = getattr(rq_app, "connection", None)
    if connection is None and hasattr(rq_app, "ping"):
        connection = rq_app
    if connection is None:
        return []
    try:
        return list(Queue.all(connection=connection))
    except Exception:  # noqa: BLE001
        return []


__all__ = ["bulk_retry_action"]
