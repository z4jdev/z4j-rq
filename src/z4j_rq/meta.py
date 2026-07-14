"""The ``@z4j_meta`` decorator for RQ tasks.

Optional metadata helper users can stack on top of an RQ task
function (whether decorated with ``@job`` or just enqueued via
``queue.enqueue(fn, ...)``) to give z4j per-task hints - redaction
overrides, tags, expected duration, skip/sample flags.

The key property of this decorator is that it is **pure metadata**.
It attaches a ``__z4j_meta__`` attribute to the decorated function
and returns the function unchanged. It does not wrap the function.
It does not change its signature. It does not affect RQ behavior in
any way. If the user uninstalls z4j entirely, every ``@z4j_meta``
call becomes a no-op.

Example::

    from rq.decorators import job
    from z4j_rq import z4j_meta


    @job("emails", connection=redis_conn)
    @z4j_meta(redact_kwargs=["email"], tags=["billing"], deadline_ms=5000)
    def send_invoice(user_id, email, amount): ...

The implementation is intentionally identical to the Celery version
(:mod:`z4j_celery.meta`) so users moving between engines find the
same surface. See ``docs/ADAPTER.md §3.7`` for the full user-facing
documentation.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import Any, TypeVar

F = TypeVar("F", bound=Callable[..., Any])

META_ATTR = "__z4j_meta__"
"""Name of the attribute ``@z4j_meta`` attaches to a task function.

Other ``z4j-rq`` modules (event mapper, discovery) read the
attribute directly to apply per-task overrides. Clients should not
rely on this attribute name - it is an internal contract between
``z4j-rq`` modules.
"""


@dataclass(frozen=True, slots=True)
class TaskMeta:
    """Normalized per-task z4j metadata attached via ``@z4j_meta``."""

    redact_kwargs: frozenset[str] = field(default_factory=frozenset)
    keep_kwargs: frozenset[str] | None = None
    redact_result: bool = False
    tags: tuple[str, ...] = ()
    priority: str | None = None
    expected_duration_ms: int | None = None
    deadline_ms: int | None = None
    skip: bool = False
    sample_rate: float = 1.0


def z4j_meta(
    *,
    redact_kwargs: Iterable[str] | None = None,
    keep_kwargs: Iterable[str] | None = None,
    redact_result: bool = False,
    tags: Iterable[str] | None = None,
    priority: str | None = None,
    expected_duration_ms: int | None = None,
    deadline_ms: int | None = None,
    skip: bool = False,
    sample_rate: float = 1.0,
) -> Callable[[F], F]:
    """Attach z4j metadata to an RQ task function.

    See :class:`TaskMeta` for the meaning of each argument.

    The decorator is a **no-op** at call time - the wrapped function
    runs exactly as if the decorator were not there. The only thing
    the decorator does is set a single attribute on the function
    object.

    Raises:
        ValueError: ``sample_rate`` is not in ``[0.0, 1.0]``.
        ValueError: ``priority`` is not a recognized level.
    """
    if not 0.0 <= sample_rate <= 1.0:
        raise ValueError("sample_rate must be in [0.0, 1.0]")
    valid_priorities = {"critical", "high", "normal", "low", None}
    if priority not in valid_priorities:
        raise ValueError(
            f"priority must be one of {valid_priorities - {None}}, got {priority!r}",
        )

    meta = TaskMeta(
        redact_kwargs=frozenset(redact_kwargs or ()),
        keep_kwargs=frozenset(keep_kwargs) if keep_kwargs is not None else None,
        redact_result=redact_result,
        tags=tuple(tags or ()),
        priority=priority,
        expected_duration_ms=expected_duration_ms,
        deadline_ms=deadline_ms,
        skip=skip,
        sample_rate=sample_rate,
    )

    def decorator(func: F) -> F:
        setattr(func, META_ATTR, meta)
        return func

    return decorator


def get_meta(func: Any) -> TaskMeta | None:
    """Return the :class:`TaskMeta` attached to ``func``, or None."""
    if func is None:
        return None
    return getattr(func, META_ATTR, None)


__all__ = ["META_ATTR", "TaskMeta", "get_meta", "z4j_meta"]
