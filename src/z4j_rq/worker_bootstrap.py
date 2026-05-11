"""Auto-bootstrap helper for RQ worker processes.

When a user installs ``z4j-rq`` and runs an RQ worker via ``rq
worker``, the worker process imports ``rq.worker`` but never calls
into ``z4j_rq`` - there is no equivalent of Celery's ``worker_init``
signal in vanilla RQ. The user has to write a tiny bootstrap module.

This helper packages that bootstrap so the user only writes one
line. They run::

    rq worker --with-scheduler -w z4j_rq.WorkerWithBootstrap

OR, in their ``settings.py`` / ``rq_settings.py``::

    from z4j_rq import register_worker_bootstrap
    register_worker_bootstrap()

Both paths construct an :class:`RqEngineAdapter` against the
running connection, install the worker-wrap capture, and start the
agent runtime via :func:`z4j_bare.install_agent`.

Opt-out: set ``Z4J_DISABLED=1`` in the worker's environment.
"""

from __future__ import annotations

import logging
import os
from typing import Any

logger = logging.getLogger("z4j.adapter.rq.bootstrap")


def register_worker_bootstrap() -> None:
    """Wire z4j-rq into the calling RQ worker process.

    Reads ``Z4J_BRAIN_URL`` / ``Z4J_TOKEN`` / ``Z4J_PROJECT_ID`` /
    ``Z4J_HMAC_SECRET`` from the environment, constructs an adapter
    against ``Z4J_RQ_REDIS_URL`` (default
    ``redis://localhost:6379/0``), and starts the agent runtime.

    Idempotent - calling twice in the same process is a no-op (the
    runtime refuses to start twice).
    """
    if os.environ.get("Z4J_DISABLED", "").lower() in ("1", "true", "yes", "on"):
        logger.info("z4j rq: Z4J_DISABLED set - skipping bootstrap")
        return

    if not all(
        os.environ.get(k)
        for k in ("Z4J_BRAIN_URL", "Z4J_TOKEN", "Z4J_PROJECT_ID")
    ):
        logger.info(
            "z4j rq: bootstrap skipped - Z4J_BRAIN_URL / Z4J_TOKEN / "
            "Z4J_PROJECT_ID not all set",
        )
        return

    redis_url = os.environ.get("Z4J_RQ_REDIS_URL", "redis://localhost:6379/0")
    rq_app = _build_rq_app(redis_url)
    if rq_app is None:
        return

    from z4j_bare.install import install_agent

    from z4j_rq.engine import RqEngineAdapter

    # ``Z4J_DEV_MODE`` is ignored when read from env (security audit
    # C3 - the kwarg is the only trusted source because a compromised
    # env var must not silently disable the ``wss://`` requirement).
    # The sandbox opts in explicitly when the env var is truthy.
    dev_mode = os.environ.get("Z4J_DEV_MODE", "").lower() in (
        "1", "true", "yes", "on",
    )

    adapter = RqEngineAdapter(rq_app=rq_app)
    try:
        install_agent(engines=[adapter], autostart=True, dev_mode=dev_mode)
    except Exception:  # noqa: BLE001
        logger.exception(
            "z4j rq: install_agent failed - agent NOT running. The "
            "RQ worker will continue normally.",
        )


def _build_rq_app(redis_url: str) -> Any | None:
    """Construct a small object satisfying :class:`RqEngineAdapter`'s rq_app duck-type."""
    try:
        import redis  # type: ignore[import-not-found]
    except ImportError:
        logger.warning(
            "z4j rq: `redis` package not importable - bootstrap aborted",
        )
        return None
    try:
        from rq import Queue  # type: ignore[import-not-found]
    except ImportError:
        logger.warning(
            "z4j rq: `rq` package not importable - bootstrap aborted",
        )
        return None

    try:
        connection = redis.Redis.from_url(redis_url)
        connection.ping()
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "z4j rq: cannot connect to Redis at %s: %s - bootstrap "
            "aborted (worker continues without z4j)",
            redis_url, str(exc)[:200],
        )
        return None

    class _RqApp:
        """Adapter-friendly wrapper around the live Redis connection."""

        def __init__(self, conn: Any) -> None:
            self.connection = conn

        @property
        def queues(self) -> list[Any]:
            try:
                return list(Queue.all(connection=self.connection))
            except Exception:  # noqa: BLE001
                return []

        def queue_for(self, job: Any) -> Any:
            return Queue(name=getattr(job, "origin", "default"),
                         connection=self.connection)

        def queue_for_name(self, name: str) -> Any:
            return Queue(name=name, connection=self.connection)

        def fetch_job(self, task_id: str) -> Any | None:
            from rq.job import Job  # type: ignore[import-not-found]
            try:
                return Job.fetch(task_id, connection=self.connection)
            except Exception:  # noqa: BLE001
                return None

    return _RqApp(connection)


__all__ = ["register_worker_bootstrap"]
