"""RQ adapter actions exposed via :class:`RqEngineAdapter`.

Day-1 surface (per `docs/MULTI_ENGINE_PLAN.md` §5):

- :func:`retry_task_action` - re-enqueue a job by id.
- :func:`cancel_task_action` - best-effort cancel; queued jobs are
  removed, started jobs are tagged for stop. RQ has no hard kill
  outside the user opting into the ``StoppedJobRegistry`` flow.
- :func:`purge_queue_action` - empty a queue with a confirm-token
  guard mirroring the Celery adapter.

Deferred to v1.1 (also in §5):

- bulk_retry, requeue_dead_letter, restart_worker, rate_limit,
  pool ops, consumer ops.

Each module here is independent and unit-tested with a fake RQ
``Queue`` + ``Job`` from ``tests/unit/conftest.py``.
"""

from __future__ import annotations

from z4j_rq.actions.bulk_retry import bulk_retry_action
from z4j_rq.actions.cancel import cancel_task_action
from z4j_rq.actions.dlq import requeue_dead_letter_action
from z4j_rq.actions.purge import purge_queue_action
from z4j_rq.actions.retry import retry_task_action

__all__ = [
    "bulk_retry_action",
    "cancel_task_action",
    "purge_queue_action",
    "requeue_dead_letter_action",
    "retry_task_action",
]
