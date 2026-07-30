"""``bulk_retry`` action - re-enqueue many RQ jobs in a single command.

Two modes of input:

1. **Explicit ids** - ``filter["task_ids"]`` is a list of job ids.
   We fetch each, re-enqueue on its origin queue, stop at ``max``.
2. **FailedJobRegistry sweep** - no explicit ids. We walk the
   ``FailedJobRegistry`` for each queue the rq_app knows about and
   re-enqueue up to ``max`` failed jobs. This is the "retry
   everything that blew up last hour" shape.

Security: the underlying:func:`retry_task_action`
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
     "new_task_ids": [...], "errors": {original_id: "..."}}"""

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


async def bulk_retry_action(  # noqa: PLR0912, PLR0915  mode + override validation + offload
    rq_app: Any,
    *,
    filter: dict[str, Any] | None = None,  # noqa: A002  public bulk_retry signature
    max: int = 1000,  # noqa: A002  public bulk_retry signature
    task_ids: list[str] | None = None,
    task_priorities: dict[str, object] | None = None,
) -> CommandResult:
    """Re-enqueue up to ``max`` jobs; returns a summary dict.

    The brain MUST supply ``filter["overrides"]`` mapping each
    targeted ``task_id`` to ``{"args": [...], "kwargs": {...}}``.
    Jobs without a matching override entry are pickle-unsafe to
    retry (see); the whole batch is refused with a
    ``missing_overrides`` error listing every affected id so the
    operator can decide whether to skip them or fix the call site.
    Registry-sweep mode (no explicit ids) has the same requirement -
    the brain must look up the discovered ids and supply overrides
    before invoking the action again.
    """
    filter = filter or {}  # noqa: A001  public bulk_retry signature
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
        # RH4: NO broker-wide FailedJobRegistry sweep. Walking the registry
        # re-enqueued EVERY failed job on the (typically shared) Redis broker,
        # including jobs belonging to other projects / workloads that the brain
        # never ownership-verified -- a cross-tenant retry. The brain now
        # resolves the matching PROJECT-OWNED ids from its own Task rows and
        # supplies them explicitly, so a bulk_retry that arrives with no ids has
        # nothing to do and returns a clean no-op instead of sweeping the broker.
        return CommandResult(
            status="success",
            result={
                "retried": 0,
                "requested": 0,
                "skipped": 0,
                "source": "no_explicit_ids",
                "note": (
                    "bulk_retry received no explicit task_ids; z4j resolves "
                    "project-owned ids on the brain and performs no broker-wide "
                    "failed-registry sweep."
                ),
            },
        )

    # 1.7.1 (CX-M17): operator-supplied overrides (per-id args/kwargs)
    # are OPTIONAL. The brain strips them from the client filter and
    # never generates them (it stores task args redacted), so the common
    # path is requeue-by-reference with NO overrides -- which is what
    # makes bulk retry work at all (the old code required overrides for
    # every id and failed the whole batch when they were absent). An
    # override entry, when explicitly present, is the "retry with
    # different inputs" path and still needs a brain-supplied task_name
    # alongside it (the agent must not deserialize RQ's
    # pickle blob to recover the callable).
    overrides_raw = filter.get("overrides") or {}
    if not isinstance(overrides_raw, dict):
        overrides_raw = {}
    task_names_raw = filter.get("task_names") or {}
    if not isinstance(task_names_raw, dict):
        task_names_raw = {}

    retried = 0
    skipped = 0
    new_ids: list[str] = []
    errors: dict[str, str] = {}
    # M10: circuit breaker. Each retry against a hung Redis burns the full
    # per-job offload timeout (~10s) inline on the receive loop; a large batch
    # would freeze command handling and stall event acks for minutes while the
    # agent still looked healthy (heartbeats keep flowing). Abort after a short
    # run of CONSECUTIVE broker timeouts rather than grinding through every id.
    circuit_break_after = 3
    consecutive_timeouts = 0
    broker_unhealthy = False

    for i, job_id in enumerate(ids, start=1):
        override = overrides_raw.get(job_id)
        if _has_safe_override(override):
            # Explicit operator inputs -> reconstruct. task_name is
            # required for this path (retry_task_action).
            task_name = task_names_raw.get(job_id)
            if not _has_safe_task_name(task_name):
                errors[job_id] = (
                    "override supplied without a brain-known task_name; "
                    "refusing (the agent will not deserialize RQ's pickle blob)"
                )
                continue
            result = await retry_task_action(
                rq_app,
                task_id=job_id,
                task_name=str(task_name),
                override_args=tuple(override.get("args", ())),  # type: ignore[union-attr]
                override_kwargs=dict(override.get("kwargs", {})),  # type: ignore[union-attr]
            )
        else:
            # No operator override -> requeue the ORIGINAL failed job by
            # reference (FailedJobRegistry.requeue): pickle-safe and it
            # preserves the original arguments. retry_task_action takes
            # that path when both override_* are None.
            result = await retry_task_action(rq_app, task_id=job_id)
        # An offload timeout tags result["indeterminate"] -- the broker-hang
        # signal the breaker counts. M2: only a genuine SUCCESS resets the
        # counter; a determinate failure is neutral (neither trips nor
        # resets), so an alternating timeout/failure pattern cannot starve
        # the breaker into grinding the whole batch against a hung broker.
        if result.result and result.result.get("indeterminate"):
            consecutive_timeouts += 1
        elif result.status == "success":
            consecutive_timeouts = 0
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

        if consecutive_timeouts >= circuit_break_after:
            broker_unhealthy = True
            # Everything after this id is skipped: the broker is clearly
            # hung and grinding on would freeze the receive loop.
            skipped += len(ids) - i
            break

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
            "circuit_broken": broker_unhealthy,
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
    func:`retry_task_action` which fails closed for them,
    but rejecting up-front in the bulk path lets the caller see the
    full ``missing_task_names`` list in one response.
    """
    return isinstance(entry, str) and bool(entry.strip())


__all__ = ["bulk_retry_action"]
