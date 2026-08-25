"""RQ event capture surface.

Two cooperating capture paths:

- The synchronous functions in :mod:`z4j_rq.events.callbacks`:
  ``success_callback`` / ``failure_callback`` / ``stopped_callback``
  hooks that RQ runs in the worker process when a job lifecycle
  transition occurs. Best-effort but blessed by RQ's API.

- :class:`z4j_rq.events.worker_wrap.RqWorkerHook` - a thin wrapper
  around the class that owns ``rq.Worker.execute_job`` that fires lifecycle
  events even for jobs the user never attached a callback to. This
  is the same monkey-patch pattern Sentry's RQ integration uses.

Both paths feed the same sink callback supplied by
:class:`z4j_rq.engine.RqEngineAdapter`. The mapper in
:mod:`z4j_rq.events.mapper` is the single place that translates
``rq.job.Job`` state into a :class:`z4j_core.models.Event`.
"""

from __future__ import annotations

from z4j_rq.events.callbacks import (
    capture_failure,
    capture_started,
    capture_stopped,
    capture_success,
)
from z4j_rq.events.mapper import build_event
from z4j_rq.events.worker_wrap import RqWorkerHook

__all__ = [
    "RqWorkerHook",
    "build_event",
    "capture_failure",
    "capture_started",
    "capture_stopped",
    "capture_success",
]
