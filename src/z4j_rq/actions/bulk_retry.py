"""``bulk_retry`` action - re-enqueue many RQ jobs in a single command.

Two modes of input:

1. **Explicit ids** - ``filter["task_ids"]`` is a list of job ids.
   We fetch each, re-enqueue on its origin queue, stop at ``max``.
2. **FailedJobRegistry sweep** - no explicit ids. We walk the
   ``FailedJobRegistry`` for each queue the rq_app knows about and
   re-enqueue up to ``max`` failed jobs. This is the "retry
   everything that blew up last hour" shape.

Security (R7 H-2 + R8 H-1): the underlying :func:`retry_task_action`
refuses to read ``job.args`` / ``job.kwargs`` / ``job.func_name`` /
``job.instance`` (RQ packs all four in a single pickle blob and
lazy-loads on attribute access). Bulk retry therefore requires the
brain to supply per-job overrides AND per-job task_name:

    filter["overrides"]   = {task_id: {"args": [...], "kwargs": {...}}}
    filter["task_names"]  = {task_id: "myapp.tasks.send_email", ...}

If any single targeted job lacks either entry the whole batch is
refused so the operator gets a precise "these ids would have been
pickle-unsafe" list rather than a half-retried mess.

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
    """Re-enqueue up to ``max`` jobs; returns a summary dict.

    The brain MUST supply ``filter["overrides"]`` mapping each
    targeted ``task_id`` to ``{"args": [...], "kwargs": {...}}``.
    Jobs without a matching override entry are pickle-unsafe to
    retry (see R7 H-2); the whole batch is refused with a
    ``missing_overrides`` error listing every affected id so the
    operator can decide whether to skip them or fix the call site.
    Registry-sweep mode (no explicit ids) has the same requirement -
    the brain must look up the discovered ids and supply overrides
    before invoking the action again.
    """
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

    # R7 H-2 + R8 H-1: refuse the entire batch up-front if any
    # single job would have triggered the pickle path. Half-retrying
    # is worse than not retrying because the operator has no easy
    # way to tell which ids ran and which didn't from the result
    # envelope.
    overrides_raw = filter.get("overrides") or {}
    if not isinstance(overrides_raw, dict):
        overrides_raw = {}
    task_names_raw = filter.get("task_names") or {}
    if not isinstance(task_names_raw, dict):
        task_names_raw = {}

    missing_overrides = [
        jid for jid in ids if not _has_safe_override(overrides_raw.get(jid))
    ]
    missing_task_names = [
        jid for jid in ids if not _has_safe_task_name(task_names_raw.get(jid))
    ]
    if missing_overrides or missing_task_names:
        return CommandResult(
            status="failed",
            error=(
                "refusing bulk_retry: missing brain-supplied retry inputs - "
                f"{len(missing_overrides)} job id(s) missing overrides, "
                f"{len(missing_task_names)} missing task_name. RQ packs "
                "args/kwargs/func_name in a single pickle blob; the agent "
                "will not deserialize that blob. Supply "
                "filter['overrides'][task_id] = {'args': [...], 'kwargs': {...}} "
                "AND filter['task_names'][task_id] = '<dotted import path>' "
                "per affected id. See R7 audit H-2 and R8 audit H-1."
            ),
            result={
                "missing_overrides": missing_overrides,
                "missing_task_names": missing_task_names,
                "source": source,
            },
        )

    retried = 0
    skipped = 0
    new_ids: list[str] = []
    errors: dict[str, str] = {}

    for i, job_id in enumerate(ids, start=1):
        override = overrides_raw.get(job_id) or {}
        result = await retry_task_action(
            rq_app,
            task_id=job_id,
            task_name=str(task_names_raw[job_id]),
            override_args=tuple(override.get("args", ())),
            override_kwargs=dict(override.get("kwargs", {})),
        )
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


def _has_safe_override(entry: Any) -> bool:
    """A brain-supplied override entry is safe iff it explicitly
    carries ``args`` and ``kwargs`` keys (empty values are fine -
    the operator chose to retry with nothing). Anything else is
    treated as "no override" and rejected by the caller."""
    if not isinstance(entry, dict):
        return False
    return "args" in entry and "kwargs" in entry


def _has_safe_task_name(entry: Any) -> bool:
    """A brain-supplied task_name is safe iff it's a non-empty string.

    Empty strings and non-strings would otherwise reach
    :func:`retry_task_action` which fails closed for them (R8 H-1),
    but rejecting up-front in the bulk path lets the caller see the
    full ``missing_task_names`` list in one response.
    """
    return isinstance(entry, str) and bool(entry.strip())


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
