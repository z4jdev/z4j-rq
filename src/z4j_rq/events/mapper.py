"""Translate RQ Job state into z4j :class:`Event` shapes.

This module is the single source of truth for the event payload
shape on the RQ side. Every RQ capture path (callbacks, worker-wrap,
future broker pub/sub) ultimately calls :func:`build_event` so the
brain receives a uniform structure regardless of which path
captured it.

Security notes:

- We **never** pass ``job.args`` or ``job.kwargs`` through unchanged.
  RQ defaults to pickle for job args; deserializing and forwarding
  them would (a) violate the no-pickle rule and
  (b) leak whatever the user put in there. Instead we forward
  ``job.description`` (RQ's own string repr - already redacted of
  type info) plus the redacted result/exception summary.
- The redaction engine runs again on the brain (defense in depth),
  but the agent must not transmit anything it would not want
  ingested.
"""

from __future__ import annotations

import functools
import logging
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any, TypeVar
from uuid import uuid4

from z4j_core.models import Event, EventKind
from z4j_core.redaction.engine import RedactionEngine

from z4j_rq.meta import TaskMeta, get_meta

logger = logging.getLogger("z4j.adapter.rq.events.mapper")

# Engine name string used in every RQ-emitted event. Kept in this
# module so the engine and the events package stay decoupled.
RQ_ENGINE_NAME = "rq"

_F = TypeVar("_F", bound=Callable[..., Any])


# Plus: every field RQ lazy-deserializes off the
# stored pickle blob. Reading ANY of these on a Job that was
# fetched fresh from Redis (not loaded by a worker) triggers
# ``rq.job.Job._deserialize_data`` which calls
# ``self.serializer.loads(self.data)`` - pickle.loads on attacker-
# controlled bytes, inside the agent process, which holds the
# HMAC signing key. The earlier fix covered ``args`` and
# ``kwargs`` only; ``func_name`` and ``instance`` were added
# because an audit found ``retry_task_action`` still passed
# ``job.func_name`` into ``queue.enqueue_call`` despite the
# args/kwargs gate above it.
_PICKLE_FIELDS_FULL: frozenset[str] = frozenset(
    {"args", "kwargs", "func_name", "instance"},
)

# Subset the event-extraction path is allowed to enforce. ``build_event``
# legitimately reads ``func_name`` for the event payload (the worker
# has already deserialized the job by the time this runs - we read a
# Python attribute on already-loaded state, not a fresh pickle.loads).
# We still forbid ``args`` / ``kwargs`` so future drift on the
# event path can't quietly re-open the hole.
_PICKLE_FIELDS_EVENT_SAFE: frozenset[str] = frozenset({"args", "kwargs"})


class _PickleFieldAccessTripwire:
    """Wraps a Job and raises on reads of any forbidden pickle field.

    Reading any of ``args`` / ``kwargs`` / ``func_name`` / ``instance``
    on a real ``rq.job.Job`` (when the job has not already been
    worker-loaded) triggers RQ's lazy pickle deserialization of
    attacker-controlled bytes inside the agent process. The exact
    forbidden set is configurable so the event-extraction path
    (which legitimately needs ``func_name`` post-deserialization) can
    use a narrower set than the retry path (which must never touch
    any of the four).

    See (args/kwargs closed) and (func_name closure).
    """

    __slots__ = ("_forbidden", "_wrapped")

    def __init__(
        self,
        wrapped: Any,
        forbidden: frozenset[str] = _PICKLE_FIELDS_FULL,
    ) -> None:
        object.__setattr__(self, "_wrapped", wrapped)
        object.__setattr__(self, "_forbidden", forbidden)

    def __getattr__(self, name: str) -> Any:
        if name in self._forbidden:
            raise AssertionError(
                f"forbidden read of job.{name} on a tripwire-wrapped Job - "
                f"RQ stores {name} inside the same pickle blob as args / "
                "kwargs / func_name / instance, and the agent must never "
                "deserialize that blob. If you genuinely "
                "need this value, route it through brain-supplied "
                "parameters (task_name / override_args / override_kwargs) "
                "and never via the stored Job."
            )
        return getattr(self._wrapped, name)


# Backwards-compatible alias - the class was renamed in 1.6.7 to
# reflect its broader scope.
# External code importing the old name keeps working.
_ArgsKwargsAccessTripwire = _PickleFieldAccessTripwire


def assert_no_args_kwargs_access(fn: _F) -> _F:
    """Decorator: fail loudly if ``fn`` reads forbidden Job pickle fields.

    Applied to functions that take a ``job`` keyword argument and run
    on the event-extraction path (where ``func_name`` is a legitimate
    payload field but ``args`` / ``kwargs`` are not). For the retry
    path - which must never touch ANY of the four fields - use the
    stricter ``assert_no_pickle_field_access`` decorator below.

    Promoted this from a docstring promise to a runtime check.
    Kept the name (event-path semantics unchanged) and added
    the stricter sibling for the retry path.
    """

    @functools.wraps(fn)
    def _guarded(*args: Any, **kwargs: Any) -> Any:
        if "job" in kwargs and kwargs["job"] is not None:
            kwargs = {
                **kwargs,
                "job": _PickleFieldAccessTripwire(
                    kwargs["job"],
                    forbidden=_PICKLE_FIELDS_EVENT_SAFE,
                ),
            }
        return fn(*args, **kwargs)

    return _guarded  # type: ignore[return-value]


def make_strict_tripwire(job: Any) -> Any:
    """Return a Job-shaped proxy that forbids reads of all 4 pickle fields.

    Use in retry-path tests to assert ``retry_task_action`` /
    ``bulk_retry_action`` / ``requeue_dead_letter_action`` never read
    ``args`` / ``kwargs`` / ``func_name`` / ``instance``. The proxy
    forwards every other attribute access to the wrapped object, so
    safe reads (``id``, ``origin``, ``get_status``) keep working.
    """
    return _PickleFieldAccessTripwire(job, forbidden=_PICKLE_FIELDS_FULL)


# Maximum length we ever forward for ``job.description`` and the
# exception summary. RQ has no limit - a pathological task could
# attach a megabyte-long repr - and the brain enforces its own
# payload-size cap, but we trim early so the local SQLite buffer
# doesn't churn either.
_MAX_DESCRIPTION_BYTES = 4096
_MAX_EXC_SUMMARY_BYTES = 4096


@assert_no_args_kwargs_access
def build_event(
    *,
    kind: EventKind,
    job: Any,
    redaction: RedactionEngine,
    extra: dict[str, Any] | None = None,
) -> Event:
    """Construct an :class:`Event` from an ``rq.job.Job``.

    ``job`` is duck-typed - any object with ``.id``, ``.func_name``,
    ``.origin`` (queue name), ``.description``, ``.created_at``,
    ``.started_at``, ``.ended_at``, ``.exc_info`` works. Tests pass a
    minimal fake; production passes a real :class:`rq.job.Job`.

    The returned Event is ready for the agent's outbound buffer.
    Re-running redaction on the brain catches any leak (defense in
    depth) - but we never rely on that.
    """
    func_name = _safe_str(getattr(job, "func_name", "unknown"))
    queue_name = _safe_str(getattr(job, "origin", "default"))
    description = _safe_truncate(
        _safe_str(getattr(job, "description", "")),
        _MAX_DESCRIPTION_BYTES,
    )
    # Worker identity - RQ stamps ``worker_name`` onto a Job when a
    # worker picks it up. If the field is empty (queued event, or
    # no worker claimed it yet) we fall back to RQ's
    # ``get_current_job()`` helper which surfaces the worker via the
    # active task. The brain's WorkerRepository rolls this up into
    # the Workers page (reads ``payload->>'worker'`` per audit §1.7).
    worker_name = _safe_str(getattr(job, "worker_name", "")) or _current_worker_name()

    # Apply per-task TaskMeta overrides. RQ jobs carry the original
    # function reference on ``job.func`` (only after Job.fetch + a
    # cache hit). Falling back to None means "no overrides found"
    # which is the right default.
    func_obj = _resolve_func_obj(job)
    meta: TaskMeta | None = get_meta(func_obj)

    payload: dict[str, Any] = {
        "task_name": func_name,
        "queue": queue_name,
        "description": description,
    }
    if worker_name:
        payload["worker"] = worker_name

    occurred_at = _resolve_occurred_at(kind, job)

    if kind is EventKind.TASK_FAILED:
        exc_info = _safe_str(getattr(job, "exc_info", "") or "")
        payload["exception"] = _safe_truncate(exc_info, _MAX_EXC_SUMMARY_BYTES)

    if extra:
        payload.update(extra)

    payload = redaction.scrub(payload)

    if meta and meta.tags:
        # Tags are post-redaction so they always survive - they're
        # operator-supplied metadata, not user input.
        payload["tags"] = list(meta.tags)
    if meta and meta.priority:
        payload["priority"] = meta.priority

    placeholder = uuid4()  # project_id and agent_id are stamped on the brain
    return Event(
        id=uuid4(),
        project_id=placeholder,
        agent_id=placeholder,
        engine=RQ_ENGINE_NAME,
        task_id=_safe_str(getattr(job, "id", "")),
        kind=kind,
        occurred_at=occurred_at,
        data=payload,
    )


def _current_worker_name() -> str:
    """Best-effort worker id for the currently-executing job.

    RQ exposes ``get_current_job()`` inside a worker process; the
    returned Job carries ``worker_name`` (set by the worker when it
    picked the job up). Outside a worker context (called from a
    dispatcher process) we return an empty string and the mapper
    simply omits the field.
    """
    try:
        from rq import get_current_job  # type: ignore[import-not-found]
    except ImportError:
        return ""
    try:
        cur = get_current_job()
    except Exception:
        return ""
    if cur is None:
        return ""
    return _safe_str(getattr(cur, "worker_name", ""))


def _safe_str(value: Any) -> str:
    """Coerce any value into a string without ever raising.

    Some RQ field accesses can raise (e.g. ``job.func_name`` when
    the job's import path no longer resolves). The capture path runs
    inside a worker process - letting a stringification raise here
    would crash the user's task.
    """
    if value is None:
        return ""
    try:
        return str(value)
    except Exception:
        return "<unstringifiable>"


def _safe_truncate(value: str, limit: int) -> str:
    """Trim ``value`` to ``limit`` UTF-8 bytes without splitting a codepoint."""
    if not value:
        return value
    encoded = value.encode("utf-8", errors="replace")
    if len(encoded) <= limit:
        return value
    truncated = encoded[:limit].decode("utf-8", errors="ignore")
    return truncated + "…[truncated]"


def _resolve_func_obj(job: Any) -> Any | None:
    """Return ``job.func`` if RQ has it cached; else None.

    RQ resolves ``job.func`` on first access by importing the
    function. We *want* the import (so ``@z4j_meta`` can be read) but
    we never let an import error escape: a partially-broken task
    should still produce events.
    """
    try:
        return job.func
    except Exception:
        return None


def _resolve_occurred_at(kind: EventKind, job: Any) -> datetime:
    """Pick the most accurate timestamp on the job for this event kind.

    RQ stores ``created_at`` / ``started_at`` / ``ended_at`` per job.
    We prefer the timestamp that semantically matches the event kind;
    falling back to ``datetime.now(UTC)`` keeps the event ingestible
    even if the field is missing.
    """
    field_map = {
        EventKind.TASK_RECEIVED: "enqueued_at",
        EventKind.TASK_STARTED: "started_at",
        EventKind.TASK_SUCCEEDED: "ended_at",
        EventKind.TASK_FAILED: "ended_at",
    }
    attr = field_map.get(kind)
    if attr:
        candidate = getattr(job, attr, None)
        if isinstance(candidate, datetime):
            if candidate.tzinfo is None:
                candidate = candidate.replace(tzinfo=UTC)
            return candidate
    return datetime.now(UTC)


__all__ = [
    "RQ_ENGINE_NAME",
    "assert_no_args_kwargs_access",
    "build_event",
    "make_strict_tripwire",
]
