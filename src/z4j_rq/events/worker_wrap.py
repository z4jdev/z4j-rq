"""Wrap ``rq.Worker.perform_job`` to capture every job's lifecycle.

This is the **canonical** RQ event-capture path. By monkey-patching
``Worker.perform_job`` at ``connect_signals`` time we observe every
job the worker handles - even jobs that were enqueued before z4j
was installed and have no per-job callback attached.

Why monkey-patch and not a subclass?

- RQ does not have a middleware/plugin system. The only blessed
  extension point is the per-job callbacks (see ``callbacks.py``)
  which require user opt-in.
- Subclassing ``Worker`` requires the user to construct
  ``z4jWorker`` instead of ``Worker`` - a friction we explicitly
  rejected in the multi-engine plan §1 ("no host-app changes
  required to install the agent").
- Monkey-patching is the same approach Sentry's RQ integration
  uses (`sentry_sdk/integrations/rq.py`). It's the path the RQ
  community has implicitly blessed.

Safety properties:

- The patch is *additive*: we wrap, we do not replace. The original
  ``perform_job`` still runs with its original semantics.
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

logger = logging.getLogger("z4j.agent.rq.worker_wrap")

# We stash the original method on the Worker class so uninstall can
# restore it. ``None`` means "not installed".
_ORIGINAL_PERFORM_JOB: Callable[..., Any] | None = None


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
        """Patch ``perform_job`` on every Worker class to fire z4j capture hooks.

        RQ 2.x split the worker hierarchy - ``perform_job`` is defined
        on :class:`rq.worker.base.BaseWorker`; ``rq.Worker`` inherits
        it via MRO and does NOT override it. Patching only
        ``rq.Worker`` leaves the real method on ``BaseWorker``
        unaffected, so events never fire. We resolve the class that
        actually defines the method and patch THERE.

        Idempotent: a second call is a no-op. Stores the (class,
        method) pair so :meth:`uninstall` can restore the original.

        If the ``rq`` package is not importable (no RQ in the user's
        environment) we silently skip - the engine adapter handles
        the empty-capture case gracefully.
        """
        global _ORIGINAL_PERFORM_JOB
        if self._installed:
            return
        target_cls = _resolve_perform_job_owner()
        if target_cls is None:
            logger.info(
                "z4j rq: rq.worker.BaseWorker / rq.Worker not importable "
                "- skipping worker patch",
            )
            return

        if _ORIGINAL_PERFORM_JOB is None:
            _ORIGINAL_PERFORM_JOB = target_cls.perform_job  # type: ignore[assignment]
            self._patched_cls = target_cls
        else:
            self._patched_cls = target_cls

        sink = self._sink
        redaction = self._redaction

        def _patched_perform_job(self_w: Any, job: Any, queue: Any) -> bool:
            """Drop-in replacement that fires started/success/failed.

            ``self_w`` is the RQ Worker instance. We always emit
            ``task.started`` *before* calling through, then either
            ``task.succeeded`` or ``task.failed`` based on the
            returned boolean (RQ's contract: ``perform_job`` returns
            True on success, False on failure).

            Worker identity: ``Worker.prepare_job_execution()`` sets
            ``job.worker_name`` *inside* ``perform_job``, so at the
            ``task.started`` emission point the field is still empty.
            We stamp ``self_w.name`` onto the job ourselves so the
            mapper sees a worker name on every lifecycle event - the
            brain's Workers page depends on this.
            """
            try:
                if not getattr(job, "worker_name", None):
                    job.worker_name = getattr(self_w, "name", "") or ""
            except Exception:  # noqa: BLE001
                pass
            _safe_emit_started(job, sink, redaction)
            try:
                ok = _ORIGINAL_PERFORM_JOB(self_w, job, queue)  # type: ignore[misc]
            except BaseException:
                _safe_emit_failed(job, sink, redaction)
                raise
            if ok:
                _safe_emit_succeeded(job, sink, redaction)
            else:
                _safe_emit_failed(job, sink, redaction)
            return ok

        target_cls.perform_job = _patched_perform_job  # type: ignore[assignment]
        self._installed = True
        logger.info(
            "z4j rq: worker capture hook installed on %s",
            target_cls.__qualname__,
        )

    def uninstall(self) -> None:
        """Restore the patched class's ``perform_job``."""
        global _ORIGINAL_PERFORM_JOB
        if not self._installed:
            return
        target_cls = getattr(self, "_patched_cls", None)
        if target_cls is not None and _ORIGINAL_PERFORM_JOB is not None:
            target_cls.perform_job = _ORIGINAL_PERFORM_JOB  # type: ignore[assignment]
            _ORIGINAL_PERFORM_JOB = None
        self._installed = False
        logger.info("z4j rq: worker capture hook removed")


def _resolve_perform_job_owner() -> type | None:
    """Return the class that defines ``perform_job`` (BaseWorker on RQ 2.x).

    We walk the MRO from ``rq.Worker`` upward and return the first
    class whose ``__dict__`` actually carries ``perform_job`` - that's
    where the real implementation lives and where the patch must land.
    Falls back to ``rq.Worker`` itself if nothing in the chain owns
    the method (highly unlikely - means the RQ API shape has shifted
    enough that we should investigate rather than silently guess).
    """
    try:
        from rq.worker import Worker  # type: ignore[import-not-found]
    except ImportError:
        return None
    for cls in Worker.__mro__:
        if "perform_job" in cls.__dict__:
            return cls
    return Worker


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
