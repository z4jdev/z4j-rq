"""RQ adapter actions exposed via :class:`RqEngineAdapter`.

Implemented data-plane surface:

- :func:`retry_task_action` - re-enqueue a job by id.
- :func:`cancel_task_action` - best-effort cancel; queued jobs are removed.
  On RQ 1.13+, a started job receives RQ's ``stop-job`` command and the
  worker terminates its matching work horse. A successful publish does not
  verify that the worker received the command or that termination completed.
- :func:`purge_queue_action` - empty a queue with a confirm-token
  guard mirroring the Celery adapter.
- :func:`bulk_retry_action` - retry a bounded set of failed jobs.
- :func:`requeue_dead_letter_action` - move jobs from RQ's failed registry
  back to their origin queue.

RQ does not expose remote worker restart, rate-limit, pool-size, or consumer
operations, so those capabilities are not advertised.

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
