"""``requeue_dead_letter`` action - resurrect a job from the FailedJobRegistry.

RQ's ``FailedJobRegistry`` IS the dead-letter concept for RQ -
every failed job lands there with its full payload preserved.
``registry.requeue(job_id)`` is the blessed way to put it back on
its original queue. The registry path never touches Python-level
``job.args`` / ``job.kwargs`` - it operates entirely on the RQ-
internal serialized blob and lets the worker do the deserialization
in its normal task context (which is the only place the pickle
load is expected by design).

Fallback when the registry API is unreachable (unusual RQ version,
test stub): we delegate to the generic ``retry_task_action``. That
delegation MUST carry brain-supplied ``task_name`` AND
``override_args`` / ``override_kwargs`` because the
fallback runs in the agent process where pickle deserialization
would be RCE. If the caller omits any of the three the fallback
fails closed with a clear error."""

from __future__ import annotations

import logging
from typing import Any

from z4j_core.models import CommandResult

from z4j_rq._offload import OffloadTimeoutError, indeterminate_timeout_result, offload
from z4j_rq.actions.retry import retry_task_action

logger = logging.getLogger("z4j.adapter.rq.actions.dlq")

#: Cap on the synchronous registry walk (Queue.all / registry.get_job_ids /
#: registry.requeue are all pure-sync redis-py). Bounds how long a broker
#: slowdown / failover can stall the offloaded call before we give up.
_OFFLOAD_TIMEOUT = 10.0


async def requeue_dead_letter_action(
    rq_app: Any,
    *,
    task_id: str,
    task_name: str | None = None,
    override_args: tuple[Any, ...] | None = None,
    override_kwargs: dict[str, Any] | None = None,
) -> CommandResult:
    """Requeue a failed RQ job from its FailedJobRegistry.

    ``task_name`` / ``override_args`` / ``override_kwargs`` are only
    consulted on the fallback path (generic retry). The native
    registry path operates on the RQ-serialized blob without
    surfacing it to the agent process, so brain-supplied inputs are
    not needed there. See and.
    """
    # ``_requeue_via_registry`` walks every queue and issues synchronous
    # redis-py calls (Queue.all, FailedJobRegistry.get_job_ids,
    # registry.requeue). redis-py is pure-sync, so running the walk inline
    # would freeze the agent's single event loop (heartbeat, send loop, ack
    # watchdog, WS ping/pong) for the duration of any broker slowdown /
    # failover -- exactly when an operator reaches for Requeue. Offload the
    # whole walk to a thread under a timeout. Mirrors the celery cancel /
    # rq worker actions.
    try:
        via_registry = await offload(
            _requeue_via_registry, rq_app, task_id, timeout=_OFFLOAD_TIMEOUT
        )
    except OffloadTimeoutError:
        return indeterminate_timeout_result(
            "requeue_dead_letter",
            _OFFLOAD_TIMEOUT,
            hint="the job may still be re-enqueued",
        )
    except Exception as exc:
        return CommandResult(
            status="failed",
            error=f"requeue_dead_letter failed: {exc}",
        )
    if via_registry is not None:
        return via_registry

    # Fallback: the generic retry path works regardless of whether
    # the job is currently in the FailedJobRegistry. It will fail
    # closed if task_name / override_args / override_kwargs are
    # missing, which is the correct behavior - the agent must not
    # load pickle.
    result = await retry_task_action(
        rq_app,
        task_id=task_id,
        task_name=task_name,
        override_args=override_args,
        override_kwargs=override_kwargs,
    )
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
        except Exception:  # noqa: S112  best-effort registry probe
            continue
        try:
            ids = registry.get_job_ids()
        except Exception:  # noqa: S112  best-effort registry ids
            continue
        if task_id not in ids:
            continue
        # Found - requeue and report.
        try:
            registry.requeue(task_id)
        except Exception as exc:
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
        except Exception:  # noqa: S110  best-effort queues coercion
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
    except Exception:
        return []


__all__ = ["requeue_dead_letter_action"]
