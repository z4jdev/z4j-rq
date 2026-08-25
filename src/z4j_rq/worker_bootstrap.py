"""Auto-bootstrap helper for RQ worker processes.

When a user installs ``z4j-rq`` and runs an RQ worker via ``rq
worker``, the worker process imports ``rq.worker`` but never calls
into ``z4j_rq`` - there is no equivalent of Celery's ``worker_init``
signal in vanilla RQ. The user has to write a tiny bootstrap module.

This helper packages that bootstrap so the user only writes one line in a
module imported by the worker, such as ``settings.py`` or
``rq_settings.py``::

    from z4j_rq import register_worker_bootstrap

    register_worker_bootstrap()

The call constructs an :class:`RqEngineAdapter`, installs the worker-wrap
capture, and starts the agent runtime via :func:`z4j_bare.install_agent`.

Opt-out: set ``Z4J_DISABLED=1`` in the worker's environment.
"""

from __future__ import annotations

import logging
import os
from typing import Any

from z4j_core.redaction import redact_url_password

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

    if not all(os.environ.get(k) for k in ("Z4J_BRAIN_URL", "Z4J_TOKEN", "Z4J_PROJECT_ID")):
        logger.info(
            "z4j rq: bootstrap skipped - Z4J_BRAIN_URL / Z4J_TOKEN / Z4J_PROJECT_ID not all set",
        )
        return

    redis_url = os.environ.get("Z4J_RQ_REDIS_URL", "redis://localhost:6379/0")
    rq_app = _build_rq_app(redis_url)
    if rq_app is None:
        return

    from z4j_bare.install import install_agent

    from z4j_rq.engine import RqEngineAdapter

    # Auto-bootstrap treats a truthy ``Z4J_DEV_MODE`` environment value as
    # an explicit opt-in and forwards it to install_agent. This relaxes the
    # ``wss://`` requirement, so use it only on a trusted local network.
    dev_mode = os.environ.get("Z4J_DEV_MODE", "").lower() in (
        "1",
        "true",
        "yes",
        "on",
    )

    adapter = RqEngineAdapter(rq_app=rq_app)
    try:
        install_agent(engines=[adapter], autostart=True, dev_mode=dev_mode)
    except Exception:
        logger.exception(
            "z4j rq: install_agent failed - agent NOT running. The "
            "RQ worker will continue normally.",
        )


def _redis_protocol_hint(exc: BaseException) -> str:
    """Name the cause when a modern client meets a pre-6.0 Redis server.

    ``redis-py`` 6.0+ negotiates RESP3 with a ``HELLO`` command that does not
    exist before Redis 6, so the connection fails with an opaque
    "unknown command `HELLO`". Operators read logs, not release notes, and this
    error gives them nothing to act on.

    Deliberately NOT worked around by retrying with ``protocol=2``. ``rq``
    itself fails identically on the same pairing (verified against a real Redis
    5.0.14 server with redis-py 8.0.1), so a fallback here would let z4j report
    a healthy connection while the queue the operator actually cares about is
    dead. Failing with a usable message beats succeeding in isolation.
    """
    if "HELLO" not in str(exc):
        return ""
    return (
        ". Your Redis server predates 6.0 but redis-py is 6.0+, which "
        "negotiates RESP3; rq cannot talk to this server either. "
        'Pin the client: pip install "redis<6"'
    )


def _build_rq_app(redis_url: str) -> Any | None:
    """Construct a small object satisfying :class:`RqEngineAdapter`'s rq_app duck-type."""
    try:
        import redis
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
    except Exception as exc:
        # Both the URL and the driver's message carry the password: this runs
        # inside the user's own worker, so an unredacted line goes straight
        # into THEIR application logs, where it is retained and shipped on.
        logger.warning(
            "z4j rq: cannot connect to Redis at %s: %s - bootstrap "
            "aborted (worker continues without z4j)%s",
            redact_url_password(redis_url),
            redact_url_password(str(exc))[:200],
            _redis_protocol_hint(exc),
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
            except Exception:
                return []

        def queue_for(self, job: Any) -> Any:
            return Queue(name=getattr(job, "origin", "default"), connection=self.connection)

        def queue_for_name(self, name: str) -> Any:
            return Queue(name=name, connection=self.connection)

        def fetch_job(self, task_id: str) -> Any | None:
            from rq.job import Job  # type: ignore[import-not-found]

            try:
                return Job.fetch(task_id, connection=self.connection)
            except Exception:
                return None

    return _RqApp(connection)


__all__ = ["register_worker_bootstrap"]
