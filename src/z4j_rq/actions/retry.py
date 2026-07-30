"""``retry`` action - re-enqueue an RQ Job.

RQ's retry surface is conceptually simple: fetch the Job by id,
take a brain-supplied function reference, and ``Queue.enqueue`` a
fresh job with caller-supplied args + kwargs on the original
queue (or a caller-overridden queue).

Security: we **never** read ``job.args``,
``job.kwargs``, ``job.func_name``, or ``job.instance`` from the
stored Job. RQ stores all four inside a single pickle blob (see
``rq.job.Job._deserialize_data``) and lazy-loads them on first
attribute access. Inside the agent process - which holds the HMAC
signing key for the brain transport - that deserialization is
arbitrary code execution from anyone who can write to the Redis
backing store.

The retry path therefore *requires* brain-supplied ``task_name``
(replacing ``job.func_name``) AND ``override_args`` /
``override_kwargs`` (replacing ``job.args`` / ``job.kwargs``).
If any of the three is missing we fail closed with
:class:`RetryUnsafeError` rather than silently triggering the
pickle load.

Closed args/kwargs. closed func_name/instance after
the audit found ``queue.enqueue_call(func=job.func_name...)``
still triggered the lazy-pickle path even with args/kwargs gated.

Edge cases handled:

- **Job is gone.** RQ garbage-collects finished jobs after a TTL.
  We treat "not found" as a successful no-op for caller ergonomics
  (idempotent-retry is what dashboard users expect when they double-
  click the button) - but record the absence in ``result`` so the
  audit log is unambiguous.
- **Job is currently running.** RQ has no "wait then retry" primitive.
  We refuse the retry rather than enqueue a duplicate.
- **eta override.** A validated (in-bounds) ``eta`` schedules the
  retry for that time via RQ's native scheduled-job path
  (``create_job(status=SCHEDULED)`` + ``schedule_job``), not an
  immediate enqueue. Out-of-bounds etas (>60 s in the past, >1 y in
  the future) are refused with a clear error. RQ's built-in scheduler
  (or a scheduler-enabled worker) picks the job up at its time."""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import Any

from z4j_core.models import CommandResult

from z4j_rq._offload import OffloadTimeoutError, indeterminate_timeout_result, offload

logger = logging.getLogger("z4j.adapter.rq.actions.retry")

#: Cap on each synchronous Redis / RQ broker call. redis-py is pure-sync,
#: so every fetch / status / queue-resolve / enqueue below is offloaded to a
#: thread under this timeout: a broker slowdown / failover must never freeze
#: the agent's single event loop (heartbeat, send loop, ack watchdog, WS
#: ping/pong) -- exactly when an operator reaches for Retry.
_OFFLOAD_TIMEOUT = 10.0


class RetryUnsafeError(Exception):
    """Raised when a retry is attempted without brain-supplied safe inputs.

    The retry surface refuses to read ``job.args``, ``job.kwargs``,
    ``job.func_name``, or ``job.instance`` because RQ packs all four
    inside a single pickle blob; loading any of them would
    deserialize attacker-controlled data inside the agent process.
    The brain MUST supply ``task_name`` (replacing ``func_name``)
    AND ``override_args`` AND ``override_kwargs`` so the retry runs
    with operator-vetted inputs only. See audit finding H-2 (args /
    kwargs closure) and audit finding H-1 (func_name / instance
    closure).
    """


# Same bounds as Celery: refuse ETAs that fall outside a
# sane window before they reach the broker. Negative ETAs are
# allowed up to -60 s for clock-skew tolerance; positive ETAs are
# capped at one year.
_ETA_MIN = timedelta(seconds=-60)
_ETA_MAX = timedelta(days=365)


async def retry_task_action(  # noqa: PLR0911, PLR0912  broker-fallback + requeue-by-ref branches
    rq_app: Any,
    *,
    task_id: str,
    task_name: str | None = None,
    override_args: tuple[Any, ...] | None = None,
    override_kwargs: dict[str, Any] | None = None,
    eta: float | None = None,
    priority: object = None,
) -> CommandResult:
    """Re-enqueue an RQ job by id.

    ``rq_app`` is the duck-typed dispatcher object the engine adapter
    received in its constructor. It must expose ``.queue_for(job)``
    OR ``.queues`` so we can resolve the destination queue. Tests
    pass a minimal fake; production passes the redis ``Connection``
    or the user-supplied ``rq.Queue`` instance.

    ``task_name`` is the dotted import path of the callable to
    enqueue (e.g. ``"myapp.tasks.send_email"``), supplied by the brain
    from the original task observation captured at ``task.received``.
    It replaces ``job.func_name`` which RQ lazy-deserializes from
    pickle - see module docstring.
    """
    # ``_fetch_job`` performs a synchronous ``Job.fetch`` (redis-py) round
    # trip. Offload it so a stalled broker cannot freeze the event loop.
    try:
        job = await offload(_fetch_job, rq_app, task_id, timeout=_OFFLOAD_TIMEOUT)
    except OffloadTimeoutError:
        return indeterminate_timeout_result(
            "retry",
            _OFFLOAD_TIMEOUT,
            hint="the job may still be re-enqueued",
        )
    if job is None:
        return CommandResult(
            status="success",
            result={"task_id": task_id, "noop": True, "reason": "job_not_found"},
        )

    # ``job.get_status()`` refreshes from Redis by default -- another sync
    # broker read, so it is offloaded too.
    try:
        running = await offload(_is_running, job, timeout=_OFFLOAD_TIMEOUT)
    except OffloadTimeoutError:
        return indeterminate_timeout_result(
            "retry",
            _OFFLOAD_TIMEOUT,
            hint="the job may still be re-enqueued",
        )
    if running:
        return CommandResult(
            status="failed",
            error=f"refusing to retry job {task_id!r}: still running",
        )

    # 1.7.1 (CX-M17): when the operator supplied NO explicit overrides
    # (and no reschedule eta), re-run the ORIGINAL failed job BY
    # REFERENCE via FailedJobRegistry.requeue. That path operates on
    # RQ's serialized blob and lets the WORKER deserialize it in its own
    # task context, so it never surfaces args/kwargs/func to the agent
    # (pickle-safe like the DLQ action) AND preserves the original
    # arguments -- which the brain cannot supply, because it stores task
    # args/kwargs redacted. This is what makes an ordinary one-click
    # Retry work at all; the explicit-override branch below stays for
    # "retry with different inputs". Requeue-by-reference cannot honor an
    # eta (it enqueues immediately), so an eta falls through to the
    # override path, which fails closed without operator-supplied inputs.
    if override_args is None and override_kwargs is None and eta is None:
        try:
            from z4j_rq.actions.dlq import _requeue_via_registry

            by_ref = await offload(_requeue_via_registry, rq_app, task_id, timeout=_OFFLOAD_TIMEOUT)
        except OffloadTimeoutError:
            return indeterminate_timeout_result(
                "retry",
                _OFFLOAD_TIMEOUT,
                hint="the job may still be re-enqueued",
            )
        if by_ref is not None:
            return by_ref
        return CommandResult(
            status="failed",
            error=(
                f"refusing to retry job {task_id!r}: it is not in a "
                "FailedJobRegistry (already succeeded, expired, or "
                "garbage-collected), so there is no original job to requeue "
                "by reference, and no operator override_args/override_kwargs "
                "were supplied. The brain stores task arguments redacted and "
                "cannot reconstruct them; use 'retry with different inputs' "
                "to supply arguments explicitly."
            ),
        )

    if eta is not None:
        validation = _validate_eta(eta)
        if validation is not None:
            return validation

    # Queue resolution may hit Redis (dispatcher ``queue_for`` hook, or a
    # ``rq.Queue`` construction that touches the connection). Offload it.
    try:
        queue = await offload(_resolve_queue, rq_app, job, timeout=_OFFLOAD_TIMEOUT)
    except OffloadTimeoutError:
        return indeterminate_timeout_result(
            "retry",
            _OFFLOAD_TIMEOUT,
            hint="the job may still be re-enqueued",
        )
    if queue is None:
        return CommandResult(
            status="failed",
            error=f"could not resolve queue for job {task_id!r}",
        )

    # Fail-closed: every value the retry needs MUST
    # come from the brain. Reading any of job.func_name, job.args,
    # job.kwargs, job.instance triggers RQ's lazy pickle deserialization
    # inside the agent process - the RCE vector. An empty tuple / empty
    # dict for override_* counts as "supplied" (operator chose to retry
    # with no arguments) but task_name must be a non-empty string.
    if not task_name:
        return CommandResult(
            status="failed",
            error=(
                f"refusing to retry job {task_id!r}: missing brain-supplied "
                "task_name. RQ stores job.func_name inside the same pickle "
                "blob as args/kwargs; the agent will not deserialize that "
                "blob to recover the function reference. The brain must "
                "forward task_name from its observed task record."
            ),
        )
    if override_args is None or override_kwargs is None:
        return CommandResult(
            status="failed",
            error=(
                f"refusing to retry job {task_id!r}: missing brain-supplied "
                "override_args / override_kwargs. RQ stores job args as "
                "pickle by default; the agent will not deserialize them."
            ),
        )

    # The actual enqueue / schedule are synchronous RQ writes to Redis. Run
    # them in one executor hop under the same timeout.
    try:
        new_job = await offload(
            _enqueue_retry,
            queue,
            task_name,
            override_args,
            override_kwargs,
            eta,
            timeout=_OFFLOAD_TIMEOUT,
        )
    except OffloadTimeoutError:
        return indeterminate_timeout_result(
            "retry",
            _OFFLOAD_TIMEOUT,
            hint="the job may still be re-enqueued",
        )
    except Exception as exc:
        return CommandResult(status="failed", error=f"retry failed: {exc}")

    return CommandResult(
        status="success",
        result={
            "task_id": getattr(new_job, "id", ""),
            "queue": getattr(queue, "name", ""),
            "previous_task_id": task_id,
            "scheduled_for": eta,
        },
    )


def _enqueue_retry(
    queue: Any,
    task_name: str,
    override_args: tuple[Any, ...],
    override_kwargs: dict[str, Any],
    eta: float | None,
) -> Any:
    """Synchronous enqueue / schedule of the retried job.

    Extracted so the single blocking sequence of RQ Redis writes runs in one
    executor hop. Behavior is identical to the prior inline block:

    - B18: a validated (in-bounds) eta MUST actually schedule the job. The
      pre-B18 code validated eta then called ``enqueue_call`` (immediate),
      silently DROPPING the schedule. We use RQ's native scheduled path with
      the SAME explicit ``args=``/``kwargs=`` form (pickle-safe)
      rather than the splatting ``enqueue_at``.
    """
    if eta is not None:
        from rq.job import JobStatus

        target = datetime.fromtimestamp(eta, tz=UTC)
        new_job = queue.create_job(
            task_name,
            args=tuple(override_args),
            kwargs=dict(override_kwargs),
            status=JobStatus.SCHEDULED,
        )
        queue.schedule_job(new_job, target)
        return new_job
    return queue.enqueue_call(
        func=task_name,
        args=tuple(override_args),
        kwargs=dict(override_kwargs),
    )


def _fetch_job(rq_app: Any, task_id: str) -> Any | None:
    """Resolve a Job by id without raising on missing.

    RQ's ``Job.fetch(task_id, connection=...)`` raises
    ``NoSuchJobError`` on miss; we want the boolean instead.

    Resolution order:
    1. ``rq_app.fetch_job(task_id)`` if the dispatcher exposes it
       (covers test fakes, custom dispatchers with job caches, and
       any user wrapper that already resolves jobs itself).
    2. ``rq.Job.fetch`` against a resolved Redis connection (what
       bare ``redis.Redis`` users get).
    """
    fetch = getattr(rq_app, "fetch_job", None)
    if callable(fetch):
        try:
            return fetch(task_id)
        except Exception:
            return None

    try:
        from rq.job import Job  # type: ignore[import-not-found]
    except ImportError:
        return None

    connection = _resolve_connection(rq_app)
    if connection is None:
        return None

    try:
        return Job.fetch(task_id, connection=connection)
    except Exception:
        return None


def _is_running(job: Any) -> bool:
    """True when the job is in a state that forbids re-enqueue."""
    status = getattr(job, "get_status", None)
    if callable(status):
        try:
            return str(status()).lower() == "started"
        except Exception:
            return False
    return False


def _validate_eta(eta: float) -> CommandResult | None:
    """Return a failed CommandResult if ``eta`` is out of bounds, else None."""
    now = datetime.now(UTC)
    target = datetime.fromtimestamp(eta, tz=UTC)
    delta = target - now
    if delta < _ETA_MIN:
        return CommandResult(
            status="failed",
            error=(
                f"eta {eta} is more than 60 s in the past; "
                "refusing to retry into a window the user almost "
                "certainly didn't intend"
            ),
        )
    if delta > _ETA_MAX:
        return CommandResult(
            status="failed",
            error=f"eta {eta} is more than one year in the future; refusing",
        )
    return None


def _resolve_connection(rq_app: Any) -> Any | None:
    """Best-effort connection resolution from any rq_app shape."""
    candidate = getattr(rq_app, "connection", None)
    if candidate is not None:
        return candidate
    if hasattr(rq_app, "ping"):  # bare redis.Redis instance
        return rq_app
    return None


def _resolve_queue(rq_app: Any, job: Any) -> Any | None:
    """Resolve the destination queue for a retried job.

    Order of preference:
    1. ``rq_app.queue_for(job)`` if present (test-friendly hook).
    2. A pre-existing ``rq.Queue(job.origin)`` constructed against
       the same connection - this is what real RQ users have.
    """
    factory = getattr(rq_app, "queue_for", None)
    if callable(factory):
        try:
            return factory(job)
        except Exception:
            return None
    try:
        from rq import Queue  # type: ignore[import-not-found]
    except ImportError:
        return None
    connection = _resolve_connection(rq_app)
    if connection is None:
        return None
    try:
        return Queue(name=getattr(job, "origin", "default"), connection=connection)
    except Exception:
        return None


__all__ = ["RetryUnsafeError", "retry_task_action"]
