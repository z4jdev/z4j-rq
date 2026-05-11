"""Wrap ``rq.Worker.execute_job`` to capture every job's lifecycle.

This is the **canonical** RQ event-capture path. By monkey-patching
``Worker.execute_job`` at ``connect_signals`` time we observe every
job the worker handles - even jobs that were enqueued before z4j
was installed and have no per-job callback attached.

**Why ``execute_job`` and not ``perform_job``?** RQ 2.x's default
``Worker.execute_job`` forks a work-horse process for every job;
the actual work (and ``perform_job``) runs in the CHILD pid. The
agent runtime, the WebSocket transport, and the event sink all live
in the PARENT pid. Patching ``perform_job`` (the historical 1.4 path)
caused events to be buffered into a per-fork ``asyncio.Queue`` that
nobody drained, and every event was lost when the fork exited.

``execute_job`` is the parent-side boundary. Patching it lets us:

1. Emit ``task.started`` BEFORE ``fork_work_horse`` is called (or
   before ``perform_job`` is called in SimpleWorker mode). The event
   originates in the parent process and reaches the parent's sink.
2. Wait through the original ``execute_job`` (which forks + monitors,
   or executes in-process for ``SimpleWorker``).
3. Read the job's final status from Redis after the original
   returns, then emit ``task.succeeded`` or ``task.failed`` from
   the parent process.

Same pattern Sentry-sdk uses for RQ when running in fork mode.

Why monkey-patch and not a subclass?

- RQ does not have a middleware/plugin system. The only blessed
  extension point is the per-job callbacks (see ``callbacks.py``)
  which require user opt-in.
- Subclassing ``Worker`` requires the user to construct
  ``z4jWorker`` instead of ``Worker`` - a friction we explicitly
  rejected in the multi-engine plan §1 ("no host-app changes
  required to install the agent").
- Monkey-patching is the same approach Sentry's RQ integration
  uses (`sentry_sdk/integrations/rq.py`).

Safety properties:

- The patch is *additive*: we wrap, we do not replace. The original
  ``execute_job`` still runs with its original semantics.
- The patch is *idempotent*: calling :meth:`install` twice has the
  same effect as calling it once. Calling :meth:`uninstall` restores
  the original method.
- The patch is *boundary-safe*: every callback runs through
  ``safe_call``-equivalent handling. A bug in the wrapper cannot
  crash the worker - at worst we drop the event.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

from z4j_core.models import Event, EventKind
from z4j_core.redaction.engine import RedactionEngine

from z4j_rq.events.mapper import build_event

logger = logging.getLogger("z4j.adapter.rq.worker_wrap")

# We stash the original method on the Worker class so uninstall can
# restore it. ``None`` means "not installed".
_ORIGINAL_EXECUTE_JOB: Callable[..., Any] | None = None


class RqWorkerHook:
    """Encapsulates the install/uninstall lifecycle of the worker patch.

    One instance per :class:`RqEngineAdapter`. The instance owns
    the sink and the redaction engine; the patched method captures
    them via closure so multiple adapters in the same process do
    not stomp on each other.
    """

    def __init__(
        self,
        *,
        sink: Callable[[Event], None],
        redaction: RedactionEngine,
    ) -> None:
        self._sink = sink
        self._redaction = redaction
        self._installed = False

    def install(self) -> None:
        """Patch ``execute_job`` on the Worker class to fire z4j capture hooks.

        RQ 2.x split the worker hierarchy - ``execute_job`` is defined
        on :class:`rq.worker.base.BaseWorker`; ``rq.Worker`` inherits
        it via MRO. We resolve the class that actually defines the
        method and patch THERE so the patch covers both fork-mode
        ``Worker`` and in-process ``SimpleWorker``.

        Idempotent: a second call is a no-op.

        If the ``rq`` package is not importable (no RQ in the user's
        environment) we silently skip - the engine adapter handles
        the empty-capture case gracefully.
        """
        global _ORIGINAL_EXECUTE_JOB
        if self._installed:
            return
        target_cls = _resolve_execute_job_owner()
        if target_cls is None:
            logger.info(
                "z4j rq: rq.worker.BaseWorker / rq.Worker not importable "
                "- skipping worker patch",
            )
            return

        if _ORIGINAL_EXECUTE_JOB is None:
            _ORIGINAL_EXECUTE_JOB = target_cls.execute_job  # type: ignore[assignment]
        self._patched_cls = target_cls

        sink = self._sink
        redaction = self._redaction

        def _patched_execute_job(self_w: Any, job: Any, queue: Any) -> Any:
            """Drop-in replacement that fires started/succeeded/failed.

            Runs in the PARENT process. This is the boundary that
            ``Worker.execute_job`` represents in both fork mode (where
            it then calls ``fork_work_horse`` + ``monitor_work_horse``)
            and in-process mode (where it calls ``perform_job``
            directly via ``SimpleWorker.execute_job``).

            ``task.started`` fires synchronously before the original
            method is called. After the original returns, we re-read
            the job's status from Redis to determine outcome and emit
            ``task.succeeded`` or ``task.failed`` accordingly. If the
            original raises (rare - usually means a corrupt job or a
            Redis disconnect mid-execute), we emit ``task.failed`` and
            re-raise so RQ can handle it.

            Worker identity: we stamp ``self_w.name`` onto the job
            BEFORE emit so the brain's Workers page sees the right
            attribution even before ``prepare_job_execution`` runs in
            the work-horse. The mapper uses ``job.worker_name``.
            """
            try:
                if not getattr(job, "worker_name", None):
                    job.worker_name = getattr(self_w, "name", "") or ""
            except Exception:  # noqa: BLE001
                pass
            _safe_emit_started(job, sink, redaction)

            try:
                result = _ORIGINAL_EXECUTE_JOB(self_w, job, queue)  # type: ignore[misc]
            except BaseException:
                _safe_emit_failed(job, sink, redaction)
                raise

            # In fork mode, execute_job blocks until the work-horse
            # exits and the child's perform_job result is reflected
            # in the job's Redis state. In SimpleWorker mode, the
            # original execute_job runs perform_job directly in this
            # process and the job's state is updated there. Either
            # way, by the time we return from the original we can
            # inspect job.get_status() to determine outcome.
            outcome_kind = _read_job_outcome(job)
            if outcome_kind is EventKind.TASK_SUCCEEDED:
                _safe_emit_succeeded(job, sink, redaction)
            elif outcome_kind is EventKind.TASK_FAILED:
                _safe_emit_failed(job, sink, redaction)
            # Other statuses (deferred, canceled) do not get a
            # terminal lifecycle event; they're not "complete" yet.

            return result

        target_cls.execute_job = _patched_execute_job  # type: ignore[assignment]
        self._installed = True
        logger.info(
            "z4j rq: worker capture hook installed on %s.execute_job",
            target_cls.__qualname__,
        )

    def uninstall(self) -> None:
        """Restore the patched class's ``execute_job``."""
        global _ORIGINAL_EXECUTE_JOB
        if not self._installed:
            return
        target_cls = getattr(self, "_patched_cls", None)
        if target_cls is not None and _ORIGINAL_EXECUTE_JOB is not None:
            target_cls.execute_job = _ORIGINAL_EXECUTE_JOB  # type: ignore[assignment]
            _ORIGINAL_EXECUTE_JOB = None
        self._installed = False
        logger.info("z4j rq: worker capture hook removed")


def _resolve_execute_job_owner() -> type | None:
    """Return the class that defines ``execute_job`` (BaseWorker on RQ 2.x).

    We walk the MRO from ``rq.Worker`` upward and return the first
    class whose ``__dict__`` actually carries ``execute_job`` - that's
    where the real implementation lives and where the patch must land.
    SimpleWorker inherits via the same MRO, so a single patch covers
    both. Falls back to ``rq.Worker`` itself if nothing in the chain
    owns the method (highly unlikely - means the RQ API shape has
    shifted enough that we should investigate rather than silently
    guess).
    """
    try:
        from rq.worker import Worker  # type: ignore[import-not-found]
    except ImportError:
        return None
    for cls in Worker.__mro__:
        if "execute_job" in cls.__dict__:
            return cls
    return Worker


def _read_job_outcome(job: Any) -> EventKind | None:
    """Inspect a finished RQ Job and decide which terminal event to emit.

    Best-effort: we re-read the job state from Redis (``refresh=True``)
    so we see the final status even if the work-horse just wrote it.
    Returns:
        EventKind.TASK_SUCCEEDED if the job finished successfully.
        EventKind.TASK_FAILED if the job failed (any reason including
            timeout, exception, work-horse crash).
        None for non-terminal statuses (deferred, canceled, queued)
            so the caller knows not to emit a terminal event.
    """
    try:
        # Avoid hard import of rq.job.JobStatus so the patch keeps
        # working across RQ versions where the enum names shift.
        get_status = getattr(job, "get_status", None)
        if callable(get_status):
            try:
                status = get_status(refresh=True)
            except TypeError:
                # Older RQ signatures don't accept refresh kwarg.
                status = get_status()
        else:
            status = getattr(job, "_status", None)
    except Exception:  # noqa: BLE001
        return EventKind.TASK_FAILED
    if status is None:
        return None
    raw = str(status).lower()
    if "finished" in raw or "success" in raw:
        return EventKind.TASK_SUCCEEDED
    if "failed" in raw or "stopped" in raw:
        return EventKind.TASK_FAILED
    return None


def _safe_emit_started(
    job: Any,
    sink: Callable[[Event], None],
    redaction: RedactionEngine,
) -> None:
    _safe_emit(EventKind.TASK_STARTED, job, sink, redaction)


def _safe_emit_succeeded(
    job: Any,
    sink: Callable[[Event], None],
    redaction: RedactionEngine,
) -> None:
    _safe_emit(EventKind.TASK_SUCCEEDED, job, sink, redaction)


def _safe_emit_failed(
    job: Any,
    sink: Callable[[Event], None],
    redaction: RedactionEngine,
) -> None:
    _safe_emit(EventKind.TASK_FAILED, job, sink, redaction)


def _safe_emit(
    kind: EventKind,
    job: Any,
    sink: Callable[[Event], None],
    redaction: RedactionEngine,
) -> None:
    """Top-level boundary: build + sink without ever raising into RQ."""
    try:
        event = build_event(kind=kind, job=job, redaction=redaction)
        sink(event)
    except Exception:  # noqa: BLE001
        logger.exception(
            "z4j rq: worker-wrap event emit raised - dropping event "
            "(this is a bug in z4j, NOT in your task code)",
        )


__all__ = ["RqWorkerHook"]
