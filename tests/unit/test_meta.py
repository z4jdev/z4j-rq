"""Tests for the ``@z4j_meta`` decorator."""

from __future__ import annotations

import pytest

from z4j_rq.meta import META_ATTR, TaskMeta, get_meta, z4j_meta


def test_decorator_attaches_meta_attribute() -> None:
    @z4j_meta(tags=["billing"])
    def fn() -> None:
        ...
    meta = getattr(fn, META_ATTR)
    assert isinstance(meta, TaskMeta)
    assert meta.tags == ("billing",)


def test_decorator_is_noop_at_call_time() -> None:
    @z4j_meta(redact_kwargs=["email"])
    def fn(x: int) -> int:
        return x + 1
    assert fn(1) == 2


def test_get_meta_returns_none_when_missing() -> None:
    def plain() -> None:
        ...
    assert get_meta(plain) is None


def test_get_meta_returns_attached_meta() -> None:
    @z4j_meta(deadline_ms=5000)
    def fn() -> None:
        ...
    assert get_meta(fn).deadline_ms == 5000


def test_invalid_sample_rate_raises() -> None:
    with pytest.raises(ValueError):
        z4j_meta(sample_rate=2.0)


def test_invalid_priority_raises() -> None:
    with pytest.raises(ValueError):
        z4j_meta(priority="ultra")


def test_valid_priorities_accepted() -> None:
    for p in ("critical", "high", "normal", "low"):
        @z4j_meta(priority=p)
        def fn() -> None:
            ...
        assert get_meta(fn).priority == p


def test_redact_kwargs_normalized_to_frozenset() -> None:
    @z4j_meta(redact_kwargs=["a", "b", "a"])
    def fn() -> None:
        ...
    meta = get_meta(fn)
    assert meta.redact_kwargs == frozenset({"a", "b"})
