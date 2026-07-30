"""``cancel`` action - best-effort cancel an RQ Job.

RQ does not support a hard cancel of an already-running job (no
remote SIGTERM-equivalent). The capability set advertised by
:mod:`z4j_rq.capabilities` already declares this honestly: dashboard
users running RQ see a Cancel button that *only* removes queued
jobs, and the engine adapter rejects cancel attempts on running
jobs with a clear error rather than silently succeeding.

The implementation:

- **Queued** jobs are removed via ``Job.cancel()`` (RQ 2.x API).
- **Started** jobs are tagged via ``send_stop_job_command`` if the
  caller's RQ version supports it (RQ 1.13+); the worker checks
  the tag at the next iteration. We return ``status="success"``
  with ``soft=True`` so the audit log captures the soft-cancel.
- **Already-finished** jobs return success with ``noop=True``.
- **Missing** jobs return success with ``noop=True``. Same rationale
  as in :mod:`z4j_rq.actions.retry` - idempotent for caller ergonomics.

Every Redis-touching call (``fetch_job`` / ``Job.fetch``, ``get_status``,
``Job.cancel``, ``send_stop_job_command``) is synchronous redis-py I/O.
This handler is ``async def`` and runs on the agent's single event loop,
so each such call is offloaded to a worker thread under a hard timeout:
a Redis slowdown / failover must not freeze the heartbeat, send loop,
ack watchdog, or WS ping/pong (which would let the brain close the
socket 1011 and age the agent offline exactly when the operator is
reaching for Cancel).
"""

from __future__ import annotations

import logging
from typing import Any

from z4j_core.models import CommandResult

from z4j_rq._offload import OffloadTimeoutError, indeterminate_timeout_result, offload

logger = logging.getLogger("z4j.adapter.rq.actions.cancel")

#: Cap on each offloaded synchronous Redis/RQ call. Bounds how long a
#: broker slowdown / failover can stall the offloaded call before we give
#: up and report failure, keeping the event loop responsive.
_OFFLOAD_TIMEOUT = 10.0


async def cancel_task_action(rq_app: Any, *, task_id: str) -> CommandResult:  # noqa: PLR0911  guard + per-call timeout branches
    """Cancel an RQ job by id (best-effort)."""
    # Fetch (rq_app.fetch_job / Job.fetch) is synchronous Redis I/O.
    try:
        job = await offload(_fetch_job, rq_app, task_id, timeout=_OFFLOAD_TIMEOUT)
    except OffloadTimeoutError:
        return indeterminate_timeout_result(
            "cancel",
            _OFFLOAD_TIMEOUT,
            hint="the job may still be canceled",
        )
    except Exception as exc:
        return CommandResult(status="failed", error=f"cancel failed: {exc}")

    if job is None:
        return CommandResult(
            status="success",
            result={"task_id": task_id, "noop": True, "reason": "job_not_found"},
        )

    # ``get_status`` may refresh from Redis - offload it too.
    try:
        status = await offload(_job_status, job, timeout=_OFFLOAD_TIMEOUT)
    except OffloadTimeoutError:
        return indeterminate_timeout_result(
            "cancel",
            _OFFLOAD_TIMEOUT,
            hint="the job may still be canceled",
        )
    except Exception as exc:
        return CommandResult(status="failed", error=f"cancel failed: {exc}")

    if status in ("finished", "failed", "canceled", "stopped"):
        return CommandResult(
            status="success",
            result={"task_id": task_id, "noop": True, "reason": f"already_{status}"},
        )

    if status == "started":
        # send_stop_job_command is a synchronous Redis publish.
        try:
            return await offload(
                _soft_cancel_started, rq_app, job, task_id, timeout=_OFFLOAD_TIMEOUT
            )
        except OffloadTimeoutError:
            return indeterminate_timeout_result(
                "cancel",
                _OFFLOAD_TIMEOUT,
                hint="the stop command may still reach the worker",
            )
        except Exception as exc:
            return CommandResult(status="failed", error=f"soft-cancel failed: {exc}")

    # Queued / deferred / scheduled - Job.cancel handles all three.
    # Job.cancel is synchronous Redis I/O.
    try:
        await offload(job.cancel, timeout=_OFFLOAD_TIMEOUT)
    except OffloadTimeoutError:
        return indeterminate_timeout_result(
            "cancel",
            _OFFLOAD_TIMEOUT,
            hint="the job may still be canceled",
        )
    except Exception as exc:
        return CommandResult(status="failed", error=f"cancel failed: {exc}")
    return CommandResult(
        status="success",
        result={"task_id": task_id, "soft": False},
    )


def _soft_cancel_started(
    rq_app: Any,
    job: Any,
    task_id: str,
) -> CommandResult:
    """Send a stop-job command to the worker that owns ``job``.

    RQ 1.13+ exposes ``send_stop_job_command(connection, job_id)``.
    If unavailable (older RQ or import failure) we fall back to a
    failure result - better an honest failure than a silent no-op.

    Runs entirely in a worker thread (offloaded by the caller): the
    ``send_stop_job_command`` Redis publish must not block the loop.
    """
    try:
        from rq.command import (  # type: ignore[import-not-found]
            send_stop_job_command,
        )
    except ImportError:
        return CommandResult(
            status="failed",
            error=(
                "RQ version does not expose send_stop_job_command; "
                "cannot cancel a running job. Upgrade rq to >=1.13."
            ),
        )
    connection = _resolve_connection(rq_app) or getattr(job, "connection", None)
    if connection is None:
        return CommandResult(
            status="failed",
            error="no Redis connection available to send stop_job command",
        )
    try:
        send_stop_job_command(connection, task_id)
    except Exception as exc:
        return CommandResult(status="failed", error=f"soft-cancel failed: {exc}")
    return CommandResult(
        status="success",
        result={
            "task_id": task_id,
            "soft": True,
            "note": "stop command sent; worker will honor at next iteration",
        },
    )


def _fetch_job(rq_app: Any, task_id: str) -> Any | None:
    """Fetch a Job by id. Synchronous Redis I/O - offload before calling."""
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


def _job_status(job: Any) -> str:
    fn = getattr(job, "get_status", None)
    if callable(fn):
        try:
            return str(fn()).lower()
        except Exception:
            return ""
    return str(getattr(job, "status", "")).lower()


def _resolve_connection(rq_app: Any) -> Any | None:
    candidate = getattr(rq_app, "connection", None)
    if candidate is not None:
        return candidate
    if hasattr(rq_app, "ping"):
        return rq_app
    return None


__all__ = ["cancel_task_action"]
