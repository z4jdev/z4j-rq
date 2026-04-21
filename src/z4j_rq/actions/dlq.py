"""``requeue_dead_letter`` action - resurrect a job from the FailedJobRegistry.

RQ's ``FailedJobRegistry`` IS the dead-letter concept for RQ -
every failed job lands there with its full payload preserved.
``registry.requeue(job_id)`` is the blessed way to put it back on
its original queue.

Fallback when the registry API is unreachable (unusual RQ version,
test stub): we delegate to the generic ``retry_task_action`` which
re-enqueues by reading ``job.func_name`` + ``args`` + ``kwargs``.
Both paths preserve the original task identity so the audit row
records "requeued from DLQ" cleanly.
"""

from __future__ import annotations

import logging
from typing import Any

from z4j_core.models import CommandResult

from z4j_rq.actions.retry import retry_task_action

logger = logging.getLogger("z4j.agent.rq.actions.dlq")


async def requeue_dead_letter_action(
    rq_app: Any,
    *,
    task_id: str,
) -> CommandResult:
    """Requeue a failed RQ job from its FailedJobRegistry."""
    via_registry = _requeue_via_registry(rq_app, task_id)
    if via_registry is not None:
        return via_registry

    # Fallback: the generic retry path works regardless of whether
    # the job is currently in the FailedJobRegistry - RQ lets you
    # fetch any job by id and re-enqueue its (func, args, kwargs).
    result = await retry_task_action(rq_app, task_id=task_id)
    if result.status == "success" and result.result:
        enriched = dict(result.result)
        enriched["source"] = "dlq_fallback"
        return CommandResult(status="success", result=enriched)
    return result


def _requeue_via_registry(rq_app: Any, task_id: str) -> CommandResult | None:
    """Try ``FailedJobRegistry.requeue(task_id)`` and report the outcome.

    Returns ``None`` when the registry is unreachable (caller should
    fall back to generic retry). Returns a ``CommandResult`` on any
    definitive outcome - success, not-found, or explicit failure.
    """
    try:
        from rq.registry import (  # type: ignore[import-not-found]
            FailedJobRegistry,
        )
    except ImportError:
        return None

    # Find the FailedJobRegistry that owns this job id by walking
    # every queue. RQ jobs live on exactly one registry at a time.
    queues = _iter_queues(rq_app)
    for queue in queues:
        try:
            registry = FailedJobRegistry(queue=queue)
        except Exception:  # noqa: BLE001
            continue
        try:
            ids = registry.get_job_ids()
        except Exception:  # noqa: BLE001
            continue
        if task_id not in ids:
            continue
        # Found - requeue and report.
        try:
            registry.requeue(task_id)
        except Exception as exc:  # noqa: BLE001
            return CommandResult(
                status="failed",
                error=f"FailedJobRegistry.requeue failed: {exc}",
            )
        return CommandResult(
            status="success",
            result={
                "task_id": task_id,
                "queue": getattr(queue, "name", "default"),
                "source": "dlq",
            },
        )

    # Not in any FailedJobRegistry - fall through to caller's fallback.
    return None


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


__all__ = ["requeue_dead_letter_action"]
