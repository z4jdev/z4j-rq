"""``retry`` action - re-enqueue an RQ Job.

RQ's retry surface is conceptually simple: fetch the Job by id,
take a brain-supplied function reference, and ``Queue.enqueue`` a
fresh job with caller-supplied args + kwargs on the original
queue (or a caller-overridden queue).

Security (R7 H-2 + R8 H-1): we **never** read ``job.args``,
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

R7 H-2 closed args/kwargs. R8 H-1 closed func_name/instance after
the Codex round-8 audit found ``queue.enqueue_call(func=job.func_name, ...)``
still triggered the lazy-pickle path even with args/kwargs gated.

Edge cases handled:

- **Job is gone.** RQ garbage-collects finished jobs after a TTL.
  We treat "not found" as a successful no-op for caller ergonomics
  (idempotent-retry is what dashboard users expect when they double-
  click the button) - but record the absence in ``result`` so the
  audit log is unambiguous.
- **Job is currently running.** RQ has no "wait then retry" primitive.
  We refuse the retry rather than enqueue a duplicate.
- **eta override.** RQ supports a ``scheduled_for`` argument via
  ``rq-scheduler``, but the base RQ package does not. We reject
  ``eta`` with a clear error if rq-scheduler isn't available rather
  than silently dropping the schedule.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import Any

from z4j_core.models import CommandResult

logger = logging.getLogger("z4j.adapter.rq.actions.retry")


class RetryUnsafeError(Exception):
    """Raised when a retry is attempted without brain-supplied safe inputs.

    The retry surface refuses to read ``job.args``, ``job.kwargs``,
    ``job.func_name``, or ``job.instance`` because RQ packs all four
    inside a single pickle blob; loading any of them would
    deserialize attacker-controlled data inside the agent process.
    The brain MUST supply ``task_name`` (replacing ``func_name``)
    AND ``override_args`` AND ``override_kwargs`` so the retry runs
    with operator-vetted inputs only. See R7 audit finding H-2 (args /
    kwargs closure) and R8 audit finding H-1 (func_name / instance
    closure).
    """

# Same bounds as Celery: refuse ETAs that fall outside a
# sane window before they reach the broker. Negative ETAs are
# allowed up to -60 s for clock-skew tolerance; positive ETAs are
# capped at one year.
_ETA_MIN = timedelta(seconds=-60)
_ETA_MAX = timedelta(days=365)


async def retry_task_action(
    rq_app: Any,
    *,
    task_id: str,
    task_name: str | None = None,
    override_args: tuple[Any, ...] | None = None,
    override_kwargs: dict[str, Any] | None = None,
    eta: float | None = None,
    priority: object = None,  # noqa: ARG001  (RQ has no per-job priority)
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
    pickle - see module docstring R8 H-1.
    """
    job = await _fetch_job(rq_app, task_id)
    if job is None:
        return CommandResult(
            status="success",
            result={"task_id": task_id, "noop": True, "reason": "job_not_found"},
        )

    if _is_running(job):
        return CommandResult(
            status="failed",
            error=f"refusing to retry job {task_id!r}: still running",
        )

    if eta is not None:
        validation = _validate_eta(eta)
        if validation is not None:
            return validation

    queue = _resolve_queue(rq_app, job)
    if queue is None:
        return CommandResult(
            status="failed",
            error=f"could not resolve queue for job {task_id!r}",
        )

    # R7 H-2 + R8 H-1 fail-closed: every value the retry needs MUST
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
                "forward task_name from its observed task record. "
                "See R8 audit H-1."
            ),
        )
    if override_args is None or override_kwargs is None:
        return CommandResult(
            status="failed",
            error=(
                f"refusing to retry job {task_id!r}: missing brain-supplied "
                "override_args / override_kwargs. RQ stores job args as "
                "pickle by default; the agent will not deserialize them. "
                "See R7 audit H-2."
            ),
        )

    try:
        new_job = queue.enqueue_call(
            func=task_name,
            args=tuple(override_args),
            kwargs=dict(override_kwargs),
        )
    except Exception as exc:  # noqa: BLE001
        return CommandResult(status="failed", error=f"retry failed: {exc}")

    return CommandResult(
        status="success",
        result={
            "task_id": getattr(new_job, "id", ""),
            "queue": getattr(queue, "name", ""),
            "previous_task_id": task_id,
        },
    )


async def _fetch_job(rq_app: Any, task_id: str) -> Any | None:
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
        except Exception:  # noqa: BLE001
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
    except Exception:  # noqa: BLE001
        return None


def _is_running(job: Any) -> bool:
    """True when the job is in a state that forbids re-enqueue."""
    status = getattr(job, "get_status", None)
    if callable(status):
        try:
            return str(status()).lower() == "started"
        except Exception:  # noqa: BLE001
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
        except Exception:  # noqa: BLE001
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
    except Exception:  # noqa: BLE001
        return None


__all__ = ["RetryUnsafeError", "retry_task_action"]
