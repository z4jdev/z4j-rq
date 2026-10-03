"""``requeue_dead_letter`` action - resurrect a job from the FailedJobRegistry.

RQ's ``FailedJobRegistry`` IS the dead-letter concept for RQ -
every failed job lands there with its full payload preserved.
``registry.requeue(job_id)`` is the blessed way to put it back on
its original queue. The registry path never touches Python-level
``job.args`` / ``job.kwargs`` - it operates entirely on the RQ-
internal serialized blob and lets the worker do the deserialization
in its normal task context (which is the only place the pickle
load is expected by design).

Fallback when the registry API is unreachable (unusual RQ version,
test stub): we delegate to the generic ``retry_task_action``. That
delegation MUST carry brain-supplied ``task_name`` AND
``override_args`` / ``override_kwargs`` because the
fallback runs in the agent process where pickle deserialization
would be RCE. If the caller omits any of the three the fallback
fails closed with a clear error."""

from __future__ import annotations

import logging
import zlib
from datetime import UTC, datetime
from typing import Any

from z4j_core.errors import AdapterError, ValidationError, Z4JError
from z4j_core.models import (
    DLQ_LIST_MAX_LIMIT,
    CommandResult,
    DeadLetterEntry,
    DeadLetterPage,
    decode_offset_cursor,
    encode_offset_cursor,
    redact_error_excerpt,
)
from z4j_core.redaction.engine import RedactionEngine

from z4j_rq._offload import OffloadTimeoutError, indeterminate_timeout_result, offload
from z4j_rq.actions.retry import retry_task_action

logger = logging.getLogger("z4j.adapter.rq.actions.dlq")

#: Cap on the synchronous registry walk (Queue.all / registry.get_job_ids /
#: registry.requeue are all pure-sync redis-py). Bounds how long a broker
#: slowdown / failover can stall the offloaded call before we give up.
_OFFLOAD_TIMEOUT = 10.0

#: Plain-text fields of the ``rq:job:<id>`` hash the listing reads. ``data``
#: (the pickled callable + arguments) and ``meta`` (pickled) are deliberately
#: NOT in this list: the listing never deserialises a broker payload.
_JOB_HASH_FIELDS: tuple[str, ...] = ("description", "origin", "ended_at", "exc_info")

#: RQ's ``utcformat`` layouts, newest first.
_RQ_TIMESTAMP_FORMATS: tuple[str, ...] = ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ")


async def requeue_dead_letter_action(
    rq_app: Any,
    *,
    task_id: str,
    task_name: str | None = None,
    override_args: tuple[Any, ...] | None = None,
    override_kwargs: dict[str, Any] | None = None,
) -> CommandResult:
    """Requeue a failed RQ job from its FailedJobRegistry.

    ``task_name`` / ``override_args`` / ``override_kwargs`` are only
    consulted on the fallback path (generic retry). The native
    registry path operates on the RQ-serialized blob without
    surfacing it to the agent process, so brain-supplied inputs are
    not needed there. See and.
    """
    # ``_requeue_via_registry`` walks every queue and issues synchronous
    # redis-py calls (Queue.all, FailedJobRegistry.get_job_ids,
    # registry.requeue). redis-py is pure-sync, so running the walk inline
    # would freeze the agent's single event loop (heartbeat, send loop, ack
    # watchdog, WS ping/pong) for the duration of any broker slowdown /
    # failover -- exactly when an operator reaches for Requeue. Offload the
    # whole walk to a thread under a timeout. Mirrors the celery cancel /
    # rq worker actions.
    try:
        via_registry = await offload(
            _requeue_via_registry, rq_app, task_id, timeout=_OFFLOAD_TIMEOUT
        )
    except OffloadTimeoutError:
        return indeterminate_timeout_result(
            "requeue_dead_letter",
            _OFFLOAD_TIMEOUT,
            hint="the job may still be re-enqueued",
        )
    except Exception as exc:
        return CommandResult(
            status="failed",
            error=f"requeue_dead_letter failed: {exc}",
        )
    if via_registry is not None:
        return via_registry

    # Fallback: the generic retry path works regardless of whether
    # the job is currently in the FailedJobRegistry. It will fail
    # closed if task_name / override_args / override_kwargs are
    # missing, which is the correct behavior - the agent must not
    # load pickle.
    result = await retry_task_action(
        rq_app,
        task_id=task_id,
        task_name=task_name,
        override_args=override_args,
        override_kwargs=override_kwargs,
    )
    if result.status == "success" and result.result:
        enriched = dict(result.result)
        enriched["source"] = "dlq_fallback"
        return CommandResult(status="success", result=enriched)
    return result


def _requeue_via_registry(rq_app: Any, task_id: str) -> CommandResult | None:
    """Try ``FailedJobRegistry.requeue(task_id)`` and report the outcome.

    Returns ``None`` when the registry is unreachable (caller should
    fall back to generic retry). Returns a ``CommandResult`` on any
    definitive outcome - success, not-found, or explicit failure.
    """
    try:
        from rq.registry import (  # type: ignore[import-not-found]
            FailedJobRegistry,
        )
    except ImportError:
        return None

    # Find the FailedJobRegistry that owns this job id by walking
    # every queue. RQ jobs live on exactly one registry at a time.
    queues = _iter_queues(rq_app)
    for queue in queues:
        try:
            registry = FailedJobRegistry(queue=queue)
        except Exception:  # noqa: S112  best-effort registry probe
            continue
        try:
            ids = registry.get_job_ids()
        except Exception:  # noqa: S112  best-effort registry ids
            continue
        if task_id not in ids:
            continue
        # Found - requeue and report.
        try:
            registry.requeue(task_id)
        except Exception as exc:
            return CommandResult(
                status="failed",
                error=f"FailedJobRegistry.requeue failed: {exc}",
            )
        return CommandResult(
            status="success",
            result={
                "task_id": task_id,
                "queue": getattr(queue, "name", "default"),
                "source": "dlq",
            },
        )

    # Not in any FailedJobRegistry - fall through to caller's fallback.
    return None


def _iter_queues(rq_app: Any) -> list[Any]:
    candidate = getattr(rq_app, "queues", None)
    if candidate is not None:
        try:
            return list(candidate)
        except Exception:  # noqa: S110  best-effort queues coercion
            pass
    try:
        from rq import Queue  # type: ignore[import-not-found]
    except ImportError:
        return []
    connection = _connection_for(rq_app)
    if connection is None:
        return []
    try:
        return list(Queue.all(connection=connection))
    except Exception:
        return []


def _connection_for(rq_app: Any) -> Any | None:
    """The redis-py connection behind ``rq_app`` (Queue, Redis, or fake)."""
    connection = getattr(rq_app, "connection", None)
    if connection is None and hasattr(rq_app, "ping"):
        connection = rq_app
    return connection


# ---------------------------------------------------------------------------
# list_dead_letters
# ---------------------------------------------------------------------------


async def list_dead_letters_action(
    rq_app: Any,
    *,
    queue: str | None = None,
    limit: int = 100,
    cursor: str | None = None,
    redaction: RedactionEngine | None = None,
    engine_name: str = "rq",
) -> DeadLetterPage:
    """Page the ``FailedJobRegistry`` of one queue (or every queue), newest first.

    The registry is a sorted set keyed ``rq:failed:<queue>`` whose score is
    the entry's expiry (failure time plus ``failure_ttl``), so under the
    uniform default TTL "highest score first" is "most recently failed
    first". Jobs registered with ``failure_ttl=-1`` (never expire) carry a
    negative score and sort last.

    Per entry we read only plain-text fields of the ``rq:job:<id>`` hash
    (``description``, ``origin``, ``ended_at``, ``exc_info``). The pickled
    ``data`` and ``meta`` fields are never touched (the agent holds
    the HMAC key, so unpickling broker bytes would be RCE). ``task_name`` is
    the callable part of RQ's ``description`` (``"pkg.func(args...)"``
    minus the argument list); RQ does not store the name elsewhere in
    plain text.

    ``cursor`` is a decimal offset (:func:`encode_offset_cursor`). Listing
    across queues merges each registry's top ``offset + limit`` entries by
    score, so the page is exact for any number of queues.

    Raises :class:`ValidationError` for a malformed cursor and
    :class:`AdapterError` when redis is unreachable or the walk outlives
    its timeout.
    """
    try:
        offset = decode_offset_cursor(cursor)
    except ValueError as exc:
        raise ValidationError(str(exc)) from exc
    bounded = max(1, min(int(limit), DLQ_LIST_MAX_LIMIT))
    scrubber = redaction or RedactionEngine()
    try:
        return await offload(
            _list_failed_registry,
            rq_app,
            queue,
            bounded,
            offset,
            scrubber,
            engine_name,
            timeout=_OFFLOAD_TIMEOUT,
        )
    except OffloadTimeoutError as exc:
        raise AdapterError(
            f"list_dead_letters timed out after {_OFFLOAD_TIMEOUT:g}s waiting on redis",
        ) from exc
    except Z4JError:
        raise
    except Exception as exc:
        raise AdapterError(f"list_dead_letters failed: {exc}") from exc


def _list_failed_registry(
    rq_app: Any,
    queue: str | None,
    limit: int,
    offset: int,
    redaction: RedactionEngine,
    engine_name: str,
) -> DeadLetterPage:
    """Synchronous registry walk; runs on the offload pool."""
    targets = _registry_targets(rq_app, queue)
    fetch_end = offset + limit
    total = 0
    # (score, job_id, queue_name, connection); highest score first.
    candidates: list[tuple[float, str, str, Any]] = []
    for queue_name, connection in targets:
        key = _failed_registry_key(queue_name)
        total += int(connection.zcard(key))
        for member, score in connection.zrevrange(key, 0, fetch_end - 1, withscores=True):
            candidates.append((float(score), _as_text(member), queue_name, connection))
    candidates.sort(key=lambda item: (-item[0], item[2], item[1]))
    window = candidates[offset:fetch_end]
    entries = [
        _entry_for(job_id, queue_name, connection, redaction)
        for _score, job_id, queue_name, connection in window
    ]
    next_cursor = encode_offset_cursor(fetch_end) if fetch_end < total else None
    return DeadLetterPage(
        entries=entries,
        next_cursor=next_cursor,
        total=total,
        engine=engine_name,
    )


def _registry_targets(rq_app: Any, queue: str | None) -> list[tuple[str, Any]]:
    """``(queue_name, connection)`` pairs whose failed registries to page."""
    fallback = _connection_for(rq_app)
    known: list[tuple[str, Any]] = []
    for q in _iter_queues(rq_app):
        name = getattr(q, "name", None)
        connection = getattr(q, "connection", None) or fallback
        if isinstance(name, str) and name and connection is not None:
            known.append((name, connection))
    known.sort(key=lambda item: item[0])
    if queue is None:
        return known
    selected = [item for item in known if item[0] == queue]
    if selected:
        return selected
    # A queue the app has not declared still has a registry key; page it
    # directly when we have a connection, so a queue drained by a worker
    # that is now gone remains listable.
    if fallback is None:
        raise AdapterError(
            f"list_dead_letters: queue {queue!r} is unknown and no redis connection is available"
        )
    return [(queue, fallback)]


def _failed_registry_key(queue_name: str) -> str:
    try:
        from rq.registry import FailedJobRegistry  # type: ignore[import-not-found]

        template = str(FailedJobRegistry.key_template)
    except Exception:
        template = "rq:failed:{0}"
    return template.format(queue_name)


def _job_key(job_id: str) -> str:
    try:
        from rq.job import Job  # type: ignore[import-not-found]

        key = Job.key_for(job_id)
    except Exception:
        return f"rq:job:{job_id}"
    # RQ 1.x returns the key as bytes, RQ 2.x as str; the hash lookup needs
    # the text form either way (str(bytes) would spell "b'rq:job:...'").
    if isinstance(key, bytes | bytearray):
        return bytes(key).decode("utf-8")
    return str(key)


def _entry_for(
    job_id: str,
    queue_name: str,
    connection: Any,
    redaction: RedactionEngine,
) -> DeadLetterEntry:
    try:
        raw = list(connection.hmget(_job_key(job_id), list(_JOB_HASH_FIELDS)))
    except Exception:
        raw = []
    raw.extend([None] * (len(_JOB_HASH_FIELDS) - len(raw)))
    description, origin, ended_at, exc_info = raw[: len(_JOB_HASH_FIELDS)]
    task_name = _task_name_from_description(_as_text(description), redaction)
    origin_text = _as_text(origin)
    return DeadLetterEntry(
        task_id=job_id,
        task_name=task_name,
        queue=origin_text or queue_name,
        failed_at=_parse_rq_timestamp(_as_text(ended_at)),
        error_excerpt=redact_error_excerpt(_decode_exc_info(exc_info), redaction),
        # RQ stores ``retries_left``, not a count of executions; reporting a
        # remaining budget as "attempts" would be misleading.
        attempts=None,
    )


def _task_name_from_description(description: str, redaction: RedactionEngine) -> str:
    """The callable part of RQ's ``"pkg.func(arg, kw=...)"`` description."""
    head = description.split("(", 1)[0].strip()
    if not head:
        return ""
    scrubbed = redaction.scrub(head)
    text = scrubbed if isinstance(scrubbed, str) else str(scrubbed)
    return text[:500]


def _decode_exc_info(raw: Any) -> str:
    """RQ stores ``exc_info`` zlib-compressed; older rows are plain text."""
    if raw is None:
        return ""
    if isinstance(raw, bytes):
        try:
            return zlib.decompress(raw).decode("utf-8", errors="replace")
        except zlib.error:
            return raw.decode("utf-8", errors="replace")
    return str(raw)


def _parse_rq_timestamp(text: str) -> datetime | None:
    if not text:
        return None
    for layout in _RQ_TIMESTAMP_FORMATS:
        try:
            return datetime.strptime(text, layout).replace(tzinfo=UTC)
        except ValueError:
            continue
    return None


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


__all__ = ["list_dead_letters_action", "requeue_dead_letter_action"]
