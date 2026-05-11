"""RQ Job-callback hooks for z4j event capture.

RQ exposes per-job callbacks (``success_callback``,
``failure_callback``, ``stopped_callback``) that fire inside the
worker process at the end of each job's lifecycle. When a user opts
into z4j they can attach these callbacks at job-creation time::

    from z4j_rq.events import capture_success, capture_failure

    queue.enqueue(
        my_task,
        on_success=capture_success,
        on_failure=capture_failure,
    )

But forcing every user to remember the callbacks is bad UX and
loses jobs that were enqueued before z4j was installed. The
canonical capture path is :class:`z4j_rq.events.worker_wrap.RqWorkerHook`,
which wraps ``rq.Worker.perform_job`` and emits the same events for
*every* job - no opt-in. The standalone callbacks here are kept for:

1. Users who want explicit, per-job control.
2. The (eventual) integration test suite, which uses them to assert
   the mapper produces the right shape without needing a worker.
3. Defense in depth - if a future RQ release changes
   ``perform_job``'s signature, the per-job callbacks still fire.

Each callback is **wrapped in a top-level try/except** so a bug in
z4j cannot crash the user's job. See CLAUDE.md §2.2.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

from z4j_core.models import Event, EventKind
from z4j_core.redaction.engine import RedactionEngine

from z4j_rq.events.mapper import build_event

logger = logging.getLogger("z4j.adapter.rq.callbacks")

# Sink + redaction engine the engine adapter installs. Callbacks
# are RQ-supplied - they don't get adapter context - so we stash
# them at module level when ``RqEngineAdapter.connect_signals``
# runs. Tests overwrite both directly.
_SINK: Callable[[Event], None] | None = None
_REDACTION: RedactionEngine | None = None


def install(*, sink: Callable[[Event], None], redaction: RedactionEngine) -> None:
    """Install the module-level sink + redaction engine.

    Called by :meth:`RqEngineAdapter.connect_signals`. Idempotent -
    a second call replaces the previous installation, which matches
    test cleanup semantics (each test installs its own sink).
    """
    global _SINK, _REDACTION
    _SINK = sink
    _REDACTION = redaction


def uninstall() -> None:
    """Drop the installed sink + redaction engine."""
    global _SINK, _REDACTION
    _SINK = None
    _REDACTION = None


def capture_started(job: Any, *_args: Any, **_kwargs: Any) -> None:
    """Emit ``task.started`` for a job that has begun execution."""
    _emit(EventKind.TASK_STARTED, job)


def capture_success(
    job: Any, _connection: Any = None, _result: Any = None,
) -> None:
    """RQ ``success_callback`` shape: emit ``task.succeeded``.

    RQ passes ``(job, connection, result)`` to success callbacks. We
    accept the first positional ``job`` and ignore the rest - the
    result body is intentionally dropped (see CLAUDE.md §2.3 - never
    forward result values without redaction; the user opts in via
    ``@z4j_meta(redact_result=False)`` if they want them).
    """
    _emit(EventKind.TASK_SUCCEEDED, job)


def capture_failure(
    job: Any, _connection: Any = None, _exc_type: Any = None,
    _exc_value: Any = None, _traceback: Any = None,
) -> None:
    """RQ ``failure_callback`` shape: emit ``task.failed``.

    RQ passes ``(job, connection, exc_type, exc_value, traceback)``
    to failure callbacks. The mapper reads ``job.exc_info`` (RQ's
    pre-formatted traceback string) so we only need ``job``.
    """
    _emit(EventKind.TASK_FAILED, job)


def capture_stopped(job: Any, *_args: Any, **_kwargs: Any) -> None:
    """RQ ``stopped_callback`` shape: emit ``task.revoked``."""
    _emit(EventKind.TASK_REVOKED, job)


def _emit(kind: EventKind, job: Any) -> None:
    """Build the event and hand it to the installed sink.

    Wraps everything in a top-level try/except - a callback raising
    inside a worker process would terminate the worker. CLAUDE.md
    §2.2 forbids that.
    """
    sink = _SINK
    redaction = _REDACTION
    if sink is None or redaction is None:
        return
    try:
        event = build_event(kind=kind, job=job, redaction=redaction)
        sink(event)
    except Exception:  # noqa: BLE001
        logger.exception(
            "z4j rq: capture callback raised - dropping event "
            "(this is a bug in z4j, NOT in your task code)",
        )


__all__ = [
    "capture_failure",
    "capture_started",
    "capture_stopped",
    "capture_success",
    "install",
    "uninstall",
]
