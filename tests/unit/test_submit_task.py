"""Tests for ``RqEngineAdapter.submit_task``.

The bare-agent dispatcher's v1.1.0 ``schedule.fire`` path routes
brain-side scheduler ticks to ``engine.submit_task(...)``. These tests
pin the contract for the RQ engine: the adapter MUST advertise
``submit_task`` in capabilities and MUST translate
``(name, args, kwargs, queue)`` into a real ``Queue.enqueue`` call on
the configured queue.

Without these tests an RQ-backed app on z4j-bare 1.1.0 would silently
fail at the engine boundary and we'd only catch it in production.
"""

from __future__ import annotations

from typing import Any

import pytest
from z4j_rq.engine import RqEngineAdapter


@pytest.mark.asyncio
class TestSubmitTask:
    async def test_capability_advertised(self, rq_app) -> None:
        """``submit_task`` MUST be in capabilities() so the bare
        dispatcher's ``schedule.fire`` path will accept the engine.
        """
        adapter = RqEngineAdapter(rq_app=rq_app)
        assert "submit_task" in adapter.capabilities()

    async def test_enqueues_on_default_queue(self, rq_app) -> None:
        """No ``queue`` kwarg -> enqueues on the default queue."""
        # FakeQueue from conftest doesn't ship `enqueue`; add it
        # locally so the adapter's call lands somewhere observable.
        # The real RQ Queue.enqueue takes (name, *args, **kwargs).
        from z4j_rq.engine import RqEngineAdapter as _Adapter

        _patch_enqueue(rq_app)

        adapter = _Adapter(rq_app=rq_app)
        result = await adapter.submit_task(
            "myapp.tasks.send_email",
            args=("alice@example.com",),
            kwargs={"template": "welcome"},
        )

        assert result.status == "success"
        assert result.result["task_id"].startswith("submit-")
        assert result.result["engine"] == "rq"

        q = rq_app.queue_for_name("default")
        assert q.submit_calls == [
            {
                "name": "myapp.tasks.send_email",
                "args": ("alice@example.com",),
                "kwargs": {"template": "welcome"},
            },
        ]

    async def test_enqueues_on_named_queue(self, rq_app) -> None:
        """``queue`` kwarg routes to that queue, creating it if needed."""
        _patch_enqueue(rq_app)
        adapter = RqEngineAdapter(rq_app=rq_app)

        result = await adapter.submit_task(
            "myapp.tasks.heavy",
            args=(),
            kwargs={"x": 1},
            queue="high-priority",
        )

        assert result.status == "success"
        q = rq_app.queue_for_name("high-priority")
        assert len(q.submit_calls) == 1
        assert q.submit_calls[0]["name"] == "myapp.tasks.heavy"
        assert q.submit_calls[0]["kwargs"] == {"x": 1}

    async def test_eta_schedules_at_absolute_timestamp(self, rq_app) -> None:
        from datetime import UTC, datetime, timedelta

        _patch_enqueue(rq_app)
        adapter = RqEngineAdapter(rq_app=rq_app)
        target = datetime.now(UTC) + timedelta(minutes=5)

        result = await adapter.submit_task(
            "myapp.tasks.delayed",
            args=(1,),
            kwargs={"key": "value"},
            eta=target.timestamp(),
        )

        assert result.status == "success"
        q = rq_app.queue_for_name("default")
        assert q.submit_calls == []
        assert q.scheduled_calls == [
            {
                "at": target,
                "name": "myapp.tasks.delayed",
                "args": (1,),
                "kwargs": {"key": "value"},
            }
        ]

    async def test_priority_is_rejected_instead_of_silently_ignored(self, rq_app) -> None:
        _patch_enqueue(rq_app)
        adapter = RqEngineAdapter(rq_app=rq_app)

        result = await adapter.submit_task("myapp.tasks.x", priority=9)

        assert result.status == "failed"
        assert "priority" in (result.error or "")
        q = rq_app.queue_for_name("default")
        assert q.submit_calls == []
        assert q.scheduled_calls == []

    async def test_broker_failure_returns_failed_result(self, rq_app) -> None:
        """If ``Queue.enqueue`` raises, the adapter returns a clean
        ``CommandResult(status="failed", error=...)`` instead of
        bubbling the exception out into the dispatcher loop.
        """

        def boom(*_a: Any, **_k: Any) -> None:
            raise RuntimeError("redis unreachable")

        q = rq_app.queue_for_name("default")
        q.enqueue = boom  # type: ignore[attr-defined]

        adapter = RqEngineAdapter(rq_app=rq_app)
        result = await adapter.submit_task("myapp.tasks.x")

        assert result.status == "failed"
        assert "redis unreachable" in (result.error or "")


def _patch_enqueue(rq_app) -> None:
    """Add a recording ``enqueue`` method to the test FakeQueue.

    The conftest FakeQueue ships ``enqueue_call`` (kwarg-form) for the
    retry tests, but the v1.1.0 ``submit_task`` calls ``enqueue(name,
    *args, **kwargs)`` (positional). We patch the real-RQ shape onto
    every queue this app vends so the adapter's new call site lands on
    a recorder.
    """
    from dataclasses import dataclass

    @dataclass
    class _FakeJob:
        id: str

    def make_enqueue(queue):
        def _enqueue(name, *args, **kwargs):
            new_id = f"submit-{len(queue.submit_calls) + 1}"
            queue.submit_calls.append(
                {
                    "name": name,
                    "args": tuple(args),
                    "kwargs": dict(kwargs),
                }
            )
            return _FakeJob(id=new_id)

        return _enqueue

    def make_enqueue_at(queue):
        def _enqueue_at(at, name, *args, **kwargs):
            new_id = f"submit-{len(queue.scheduled_calls) + 1}"
            queue.scheduled_calls.append(
                {
                    "at": at,
                    "name": name,
                    "args": tuple(kwargs.pop("args", args)),
                    "kwargs": dict(kwargs.pop("kwargs", {})),
                }
            )
            return _FakeJob(id=new_id)

        return _enqueue_at

    original = rq_app.queue_for_name

    def patched(name: str):
        q = original(name)
        if not hasattr(q, "submit_calls"):
            q.submit_calls = []  # type: ignore[attr-defined]
            q.scheduled_calls = []  # type: ignore[attr-defined]
            q.enqueue = make_enqueue(q)  # type: ignore[attr-defined]
            q.enqueue_at = make_enqueue_at(q)  # type: ignore[attr-defined]
        return q

    rq_app.queue_for_name = patched  # type: ignore[method-assign]
    # Also patch the "default" that already exists.
    default = rq_app._queues["default"]
    if not hasattr(default, "submit_calls"):
        default.submit_calls = []  # type: ignore[attr-defined]
        default.scheduled_calls = []  # type: ignore[attr-defined]
        default.enqueue = make_enqueue(default)  # type: ignore[attr-defined]
        default.enqueue_at = make_enqueue_at(default)  # type: ignore[attr-defined]
