"""The :class:`RqEngineAdapter` - z4j's RQ queue engine adapter.

Implements :class:`z4j_core.protocols.QueueEngineAdapter` on top of
the user's RQ install. Wires up:

- The worker-wrap event-capture path
  (:class:`z4j_rq.events.worker_wrap.RqWorkerHook`) plus the optional
  per-job callback path (:mod:`z4j_rq.events.callbacks`).
- Discovery via :func:`z4j_rq.discovery.discover_runtime`.
- Action implementations (retry, cancel, purge_queue) - bulk_retry,
  DLQ, restart_worker, rate_limit are intentionally NOT implemented
  on day 1; see :mod:`z4j_rq.capabilities`.

Constructor arg ``rq_app`` is duck-typed:

- Most users will pass an ``rq.Queue`` instance - the queue knows
  its connection and the adapter can derive everything else.
- Users running multiple queues can pass a ``redis.Redis`` instance
  directly; the adapter will discover queues via ``Queue.all(...)``.
- Tests pass a minimal fake exposing the few methods the adapter
  reads.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator
from typing import Any

from z4j_core.errors import NotFoundError
from z4j_core.models import (
    CommandResult,
    DiscoveryHints,
    Event,
    Queue,
    Task,
    TaskDefinition,
    TaskRegistryDelta,
    Worker,
)
from z4j_core.redaction.engine import RedactionEngine
from z4j_core.version import PROTOCOL_VERSION

from z4j_rq.actions import (
    bulk_retry_action,
    cancel_task_action,
    purge_queue_action,
    requeue_dead_letter_action,
    retry_task_action,
)
from z4j_rq.capabilities import DEFAULT_CAPABILITIES
from z4j_rq.discovery import discover_runtime
from z4j_rq.events.callbacks import install as install_callbacks
from z4j_rq.events.callbacks import uninstall as uninstall_callbacks
from z4j_rq.events.mapper import RQ_ENGINE_NAME
from z4j_rq.events.worker_wrap import RqWorkerHook

logger = logging.getLogger("z4j.adapter.rq.engine")


class RqEngineAdapter:
    """Queue-engine adapter for RQ.

    Args:
        rq_app: An ``rq.Queue`` instance, an ``rq.Connection`` /
                ``redis.Redis`` instance, or any object exposing
                ``connection``, ``queues``, ``queue_for(job)``,
                ``queue_for_name(name)``, ``fetch_job(id)`` (tests).
        redaction: Optional shared :class:`RedactionEngine`. The
                   agent runtime's own engine is passed in production
                   so per-project config propagates.
    """

    name: str = RQ_ENGINE_NAME
    protocol_version: str = PROTOCOL_VERSION

    def __init__(
        self,
        *,
        rq_app: Any,
        redaction: RedactionEngine | None = None,
    ) -> None:
        self.rq_app = rq_app
        self.redaction = redaction or RedactionEngine()
        self._event_queue: asyncio.Queue[Event] = asyncio.Queue(maxsize=10_000)
        self._worker_hook: RqWorkerHook | None = None
        self._loop: asyncio.AbstractEventLoop | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def connect_signals(self, loop: asyncio.AbstractEventLoop | None = None) -> None:
        """Install the worker-wrap + callback capture paths.

        Both paths route into the same internal asyncio queue via
        :meth:`_enqueue_event`. We capture the runtime's loop here
        so RQ callbacks (which run on worker threads) can hop back
        via ``call_soon_threadsafe``.
        """
        target_loop = loop
        if target_loop is None:
            try:
                target_loop = asyncio.get_running_loop()
            except RuntimeError:
                target_loop = None
        self._loop = target_loop

        def sink(event: Event) -> None:
            current_loop = self._loop
            if current_loop is None:
                try:
                    current_loop = asyncio.get_running_loop()
                except RuntimeError:
                    logger.debug("z4j rq: no running loop; dropping event")
                    return
            current_loop.call_soon_threadsafe(self._enqueue_event, event)

        # Install both paths. The worker-wrap path covers every job
        # the worker handles; the per-job callback path is a back-
        # stop for users who attach callbacks explicitly. They
        # produce the same Event shape - duplicates are deduped on
        # the brain via the (event_id, occurred_at) UNIQUE index.
        install_callbacks(sink=sink, redaction=self.redaction)
        self._worker_hook = RqWorkerHook(sink=sink, redaction=self.redaction)
        self._worker_hook.install()
        logger.info(
            "z4j rq: capture installed (worker-wrap + per-job callbacks)",
        )

    def disconnect_signals(self) -> None:
        """Tear down both capture paths. Idempotent."""
        if self._worker_hook is not None:
            self._worker_hook.uninstall()
            self._worker_hook = None
        uninstall_callbacks()
        self._loop = None

    def _enqueue_event(self, event: Event) -> None:
        """Push an Event onto the internal queue, dropping oldest when full."""
        for _attempt in range(3):
            try:
                self._event_queue.put_nowait(event)
                return
            except asyncio.QueueFull:
                try:
                    dropped = self._event_queue.get_nowait()
                    logger.warning(
                        "z4j rq: event queue full, dropped event kind=%s",
                        getattr(dropped, "kind", "?"),
                    )
                except asyncio.QueueEmpty:
                    pass
        logger.error(
            "z4j rq: failed to enqueue event after retries kind=%s",
            getattr(event, "kind", "?"),
        )

    # ------------------------------------------------------------------
    # QueueEngineAdapter - discovery
    # ------------------------------------------------------------------

    async def discover_tasks(
        self,
        hints: DiscoveryHints | None = None,
    ) -> list[TaskDefinition]:
        """Return distinct ``TaskDefinition``s observed in the rq_app."""
        return discover_runtime(self.rq_app)

    async def subscribe_registry_changes(
        self,
    ) -> AsyncIterator[TaskRegistryDelta]:
        """Yield registry deltas as the task surface changes.

        v1: there is no signal-driven registry watcher in RQ - RQ
        does not fire any event when a new function gets enqueued
        for the first time. We block forever so the agent's task-
        group cancels us on shutdown without producing spurious
        deltas.

        Phase-1.1: hook the optional dev-mode filesystem watcher
        (already implemented in `z4j-bare`) to rescan ``app_paths``
        on save.
        """
        # Asyncio idiom for "park until cancelled". The agent's
        # task-group cancellation propagates here cleanly.
        return
        yield  # pragma: no cover  (makes this a generator)

    # ------------------------------------------------------------------
    # QueueEngineAdapter - observation
    # ------------------------------------------------------------------

    async def subscribe_events(self) -> AsyncIterator[Event]:
        """Drain the internal event queue."""
        while True:
            event = await self._event_queue.get()
            yield event

    async def list_queues(self) -> list[Queue]:
        # v1: queue listing is synthesized on the brain from
        # task.received events. RQ exposes no separate queue
        # metadata stream beyond ``Queue.all()``.
        return []

    async def list_workers(self) -> list[Worker]:
        # v1: worker listing surfaces via the heartbeat health dict
        # below (worker hostnames + last-heartbeat ts from the
        # Redis ``rq:workers`` set). The brain renders the worker
        # list from event aggregation.
        return []

    async def reconcile_task(self, task_id: str) -> CommandResult:
        """Query the RQ Job store for authoritative state.

        Called by the brain's ReconciliationWorker. RQ has a
        first-class result backend (every Job is a Redis hash with
        ``status`` / ``ended_at`` / ``exc_info``).
        """
        state_map = {
            "queued": "pending",
            "deferred": "pending",
            "scheduled": "pending",
            "started": "started",
            "finished": "success",
            "failed": "failure",
            "stopped": "failure",
            "canceled": "failure",
        }
        try:
            from rq.job import Job  # type: ignore[import-not-found]
        except ImportError:
            return CommandResult(
                status="success",
                result={
                    "task_id": task_id,
                    "engine_state": "unknown",
                    "finished_at": None,
                    "exception": "rq package not importable",
                },
            )
        connection = getattr(self.rq_app, "connection", None)
        if connection is None and hasattr(self.rq_app, "ping"):
            connection = self.rq_app
        if connection is None:
            return CommandResult(
                status="success",
                result={
                    "task_id": task_id,
                    "engine_state": "unknown",
                    "finished_at": None,
                    "exception": "no connection available",
                },
            )
        try:
            job = Job.fetch(task_id, connection=connection)
        except Exception:
            # RQ prunes finished jobs after a TTL; a missing Job is
            # canonical "unknown" - brain leaves its own state alone.
            return CommandResult(
                status="success",
                result={
                    "task_id": task_id,
                    "engine_state": "unknown",
                    "finished_at": None,
                    "exception": None,
                },
            )
        raw_status = ""
        with contextlib.suppress(Exception):
            raw_status = str(job.get_status()).lower()
        engine_state = state_map.get(raw_status, "unknown")
        finished_at: str | None = None
        try:
            if job.ended_at is not None:
                finished_at = job.ended_at.isoformat()
        except Exception:  # noqa: S110  best-effort ended_at
            pass
        exc_info: str | None = None
        try:
            if engine_state == "failure" and job.exc_info:
                exc_info = str(job.exc_info)[:2000]
        except Exception:  # noqa: S110  best-effort exc_info
            pass
        return CommandResult(
            status="success",
            result={
                "task_id": task_id,
                "engine_state": engine_state,
                "finished_at": finished_at,
                "exception": exc_info,
            },
        )

    async def get_task(self, task_id: str) -> Task | None:
        try:
            from rq.job import Job  # type: ignore[import-not-found]
        except ImportError:
            return None
        connection = getattr(self.rq_app, "connection", None)
        if connection is None and hasattr(self.rq_app, "ping"):
            connection = self.rq_app
        if connection is None:
            return None
        try:
            Job.fetch(task_id, connection=connection)
        except Exception as exc:
            raise NotFoundError(f"task {task_id!r} not found") from exc
        # The brain owns the authoritative Task state - adapter
        # returns None to indicate "task exists, brain has the data".
        return None

    def get_health(self) -> dict[str, Any]:
        """Return broker connectivity + queue depths for the heartbeat.

        Best-effort and synchronous (called from the heartbeat loop).
        Failures degrade to ``broker_connected=False`` rather than
        raising - heartbeats must never crash the agent.
        """
        health: dict[str, Any] = {
            "broker_type": "redis",  # RQ is Redis-only by engine design
            "broker_connected": False,
            "queue_depths": {},
        }
        connection = getattr(self.rq_app, "connection", None)
        if connection is None and hasattr(self.rq_app, "ping"):
            connection = self.rq_app
        if connection is None:
            return health

        try:
            connection.ping()
            health["broker_connected"] = True
        except Exception as exc:
            health["broker_error"] = str(exc)[:200]
            return health

        try:
            from rq import Queue  # type: ignore[import-not-found]
        except ImportError:
            return health

        try:
            for q in Queue.all(connection=connection):
                with contextlib.suppress(Exception):
                    health["queue_depths"][q.name] = int(q.count)
        except Exception as exc:
            health["queue_enum_error"] = str(exc)[:200]

        return health

    # ------------------------------------------------------------------
    # QueueEngineAdapter - data-plane actions
    #
    # Shipped in v2026.5: retry, cancel, purge, bulk_retry,
    # requeue_dead_letter. Honest absences below: rate_limit /
    # restart_worker / pool ops (engine constraints - never).
    # ------------------------------------------------------------------

    async def submit_task(
        self,
        name: str,
        *,
        args: tuple[Any, ...] = (),
        kwargs: dict[str, Any] | None = None,
        queue: str | None = None,
        eta: float | None = None,
        priority: int | None = None,
    ) -> CommandResult:
        """Universal enqueue via ``Queue.enqueue(name, ...)``.

        ``name`` is RQ's import path string ("module.path.func").
        """
        try:
            queue_name = queue or "default"
            q = None
            queue_for_name = getattr(self.rq_app, "queue_for_name", None)
            if callable(queue_for_name):
                q = queue_for_name(queue_name)
            else:
                from rq import Queue

                connection = getattr(self.rq_app, "connection", self.rq_app)
                q = Queue(name=queue_name, connection=connection)
            job = q.enqueue(name, *args, **(kwargs or {}))
            new_id = getattr(job, "id", None)
        except Exception as exc:
            return CommandResult(status="failed", error=str(exc))
        return CommandResult(
            status="success",
            result={"task_id": new_id, "engine": self.name},
        )

    async def retry_task(
        self,
        task_id: str,
        *,
        task_name: str | None = None,
        override_args: tuple[Any, ...] | None = None,
        override_kwargs: dict[str, Any] | None = None,
        eta: float | None = None,
        priority: object = None,
    ) -> CommandResult:
        # R8 H-1: brain-supplied task_name replaces job.func_name
        # (which would lazy-load pickle from the broker). The action
        # itself fails closed if task_name is empty - the dispatcher
        # already populates it from the original task observation,
        # so an empty value here means the brain never saw the task.
        return await retry_task_action(
            self.rq_app,
            task_id=task_id,
            task_name=task_name,
            override_args=override_args,
            override_kwargs=override_kwargs,
            eta=eta,
            priority=priority,
        )

    async def cancel_task(self, task_id: str) -> CommandResult:
        return await cancel_task_action(self.rq_app, task_id=task_id)

    async def purge_queue(
        self,
        queue_name: str,
        *,
        confirm_token: str | None = None,
        force: bool = False,
    ) -> CommandResult:
        return await purge_queue_action(
            self.rq_app,
            queue_name=queue_name,
            confirm_token=confirm_token,
            force=force,
        )

    async def bulk_retry(
        self,
        filter: dict[str, Any],  # noqa: A002
        *,
        max: int = 1000,  # noqa: A002
    ) -> CommandResult:
        return await bulk_retry_action(
            self.rq_app,
            filter=filter,
            max=max,
        )

    async def requeue_dead_letter(
        self,
        task_id: str,
        *,
        task_name: str | None = None,
        override_args: tuple[Any, ...] | None = None,
        override_kwargs: dict[str, Any] | None = None,
    ) -> CommandResult:
        return await requeue_dead_letter_action(
            self.rq_app,
            task_id=task_id,
            task_name=task_name,
            override_args=override_args,
            override_kwargs=override_kwargs,
        )

    # Honest absences below - these stay as loud failures because
    # the engine literally cannot perform them. capabilities() omits
    # them so the dashboard hides the buttons, but if anything ever
    # bypasses the capability gate the failure message is the
    # engine-constraint explanation.

    async def rate_limit(
        self,
        task_name: str,
        rate: str,
        *,
        worker_name: str | None = None,
    ) -> CommandResult:
        return CommandResult(
            status="failed",
            error=(
                "rate_limit is not supported by the RQ engine - RQ has no "
                "per-task rate-limit primitive. This is an honest engine "
                "constraint, not a missing feature."
            ),
        )

    async def restart_worker(self, worker_id: str) -> CommandResult:
        return CommandResult(
            status="failed",
            error=(
                "restart_worker is not supported by the RQ engine - RQ "
                "workers expose no remote-control channel. Restart the "
                "worker process out-of-band (systemd, k8s, etc.)."
            ),
        )

    # ------------------------------------------------------------------
    # QueueEngineAdapter - capabilities
    # ------------------------------------------------------------------

    def capabilities(self) -> set[str]:
        """Return the honest day-1 capability set.

        This drives dashboard button-gating; lying here corrupts the
        UX and gets us dunked on (multi-engine plan §3 N5).
        """
        return set(DEFAULT_CAPABILITIES)


__all__ = ["RqEngineAdapter"]
