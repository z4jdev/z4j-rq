"""Discover the user's RQ task surface.

RQ does not maintain a canonical "task registry" the way Celery
does - there's no ``celery_app.tasks`` dict equivalent. A function
becomes an RQ task only when something enqueues it, and the RQ
worker accepts arbitrary import paths via the ``Job.func_name``
field.

We therefore discover tasks from two sources, both honest about
their limits:

1. **Recently-seen jobs.** Walk every queue the rq_app knows about
   plus the ``StartedJobRegistry`` and ``FinishedJobRegistry``;
   every distinct ``func_name`` becomes a ``TaskDefinition``. This
   is the "you'll see what you've actually run" view - matches the
   user's mental model.

2. **Decorated functions.** If the user used ``@job`` or
   ``@z4j_meta`` decorators we can find decorated symbols by
   walking the framework adapter's ``app_paths``. (Phase-1.1
   follow-up - for now we only return the recently-seen set.)

Either path produces the same :class:`TaskDefinition` shape; the
brain's discovery merger handles dedupe by ``name``.
"""

from __future__ import annotations

import logging
from typing import Any

from z4j_core.models import TaskDefinition

from z4j_rq.events.mapper import RQ_ENGINE_NAME

logger = logging.getLogger("z4j.agent.rq.discovery")

# How many jobs to walk per registry. RQ registries can hold
# millions of finished-job ids; we don't need a complete history,
# just a representative sample of the *task names* in use.
_MAX_JOBS_PER_REGISTRY = 500


def discover_runtime(rq_app: Any) -> list[TaskDefinition]:
    """Return distinct ``TaskDefinition``s observed in recent RQ activity."""
    seen_names: dict[str, TaskDefinition] = {}

    for queue in _iter_queues(rq_app):
        queue_name = getattr(queue, "name", "default")
        for job in _iter_queue_jobs(queue):
            _accumulate(seen_names, job, queue_name)

    for registry in _iter_registries(rq_app):
        registry_queue = _registry_queue_name(registry)
        for job in _iter_registry_jobs(registry):
            _accumulate(seen_names, job, registry_queue)

    return list(seen_names.values())


def _accumulate(
    bucket: dict[str, TaskDefinition],
    job: Any,
    queue_name: str,
) -> None:
    """Add a TaskDefinition to ``bucket`` keyed by func_name (dedupe)."""
    func_name = _safe_str(getattr(job, "func_name", None))
    if not func_name:
        return
    if func_name in bucket:
        return
    bucket[func_name] = TaskDefinition(
        name=func_name,
        engine=RQ_ENGINE_NAME,
        queue=queue_name or "default",
        module=_module_from_func_name(func_name),
    )


def _iter_queues(rq_app: Any) -> list[Any]:
    """Best-effort enumeration of queues the rq_app knows about."""
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


def _iter_queue_jobs(queue: Any) -> list[Any]:
    """Walk up to _MAX_JOBS_PER_REGISTRY job ids from a queue."""
    fn = getattr(queue, "get_jobs", None)
    if not callable(fn):
        return []
    try:
        return list(fn(0, _MAX_JOBS_PER_REGISTRY - 1))
    except Exception:  # noqa: BLE001
        return []


def _iter_registries(rq_app: Any) -> list[Any]:
    """Collect Started + Finished + Failed registries for every queue."""
    try:
        from rq.registry import (  # type: ignore[import-not-found]
            FailedJobRegistry,
            FinishedJobRegistry,
            StartedJobRegistry,
        )
    except ImportError:
        return []
    out: list[Any] = []
    for queue in _iter_queues(rq_app):
        for cls in (StartedJobRegistry, FinishedJobRegistry, FailedJobRegistry):
            try:
                out.append(cls(queue=queue))
            except Exception:  # noqa: BLE001
                pass
    return out


def _iter_registry_jobs(registry: Any) -> list[Any]:
    """Resolve job ids from a registry into Job objects."""
    fetch_ids = getattr(registry, "get_job_ids", None)
    if not callable(fetch_ids):
        return []
    try:
        ids = list(fetch_ids(0, _MAX_JOBS_PER_REGISTRY - 1))
    except Exception:  # noqa: BLE001
        return []
    try:
        from rq.job import Job  # type: ignore[import-not-found]
    except ImportError:
        return []
    connection = getattr(registry, "connection", None)
    if connection is None:
        return []
    out: list[Any] = []
    for job_id in ids:
        try:
            out.append(Job.fetch(job_id, connection=connection))
        except Exception:  # noqa: BLE001
            continue
    return out


def _registry_queue_name(registry: Any) -> str:
    queue = getattr(registry, "queue", None)
    if queue is not None:
        return _safe_str(getattr(queue, "name", "default")) or "default"
    return _safe_str(getattr(registry, "name", "default")) or "default"


def _module_from_func_name(func_name: str) -> str:
    """Drop the trailing ``.callable`` for ``module.callable`` strings."""
    if "." not in func_name:
        return ""
    return func_name.rsplit(".", 1)[0]


def _safe_str(value: Any) -> str:
    if value is None:
        return ""
    try:
        return str(value)
    except Exception:  # noqa: BLE001
        return ""


__all__ = ["discover_runtime"]
