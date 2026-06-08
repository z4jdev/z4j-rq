"""Smoke + protocol-conformance tests for :class:`RqEngineAdapter`."""

from __future__ import annotations

import asyncio
import inspect

import pytest

from z4j_core.protocols import QueueEngineAdapter

from z4j_rq.capabilities import DEFAULT_CAPABILITIES
from z4j_rq.engine import RqEngineAdapter


class TestRqEngineAdapterShape:
    """Static shape - Protocol conformance + capability honesty."""

    def test_satisfies_queue_engine_adapter_protocol(self, rq_app):
        adapter = RqEngineAdapter(rq_app=rq_app)
        # ``runtime_checkable`` Protocol - structural match is enough.
        assert isinstance(adapter, QueueEngineAdapter)

    def test_engine_name_is_rq(self, rq_app):
        adapter = RqEngineAdapter(rq_app=rq_app)
        assert adapter.name == "rq"

    def test_capabilities_match_default_set(self, rq_app):
        adapter = RqEngineAdapter(rq_app=rq_app)
        assert adapter.capabilities() == set(DEFAULT_CAPABILITIES)

    def test_capabilities_omit_engine_constraints(self, rq_app):
        """Capabilities RQ engine genuinely cannot support."""
        adapter = RqEngineAdapter(rq_app=rq_app)
        caps = adapter.capabilities()
        for absent in (
            "restart_worker",
            "rate_limit",
            "pool_grow",
            "pool_shrink",
            "add_consumer",
            "cancel_consumer",
        ):
            assert absent not in caps, (
                f"{absent} must not be in RQ capabilities - see "
                "docs/MULTI_ENGINE_PLAN.md §5"
            )


class TestRqEngineAdapterAsyncMethods:
    """Every Protocol method on the adapter is async / async-gen."""

    @pytest.mark.parametrize(
        "method_name",
        [
            "discover_tasks",
            "list_queues",
            "list_workers",
            "get_task",
            "retry_task",
            "cancel_task",
            "purge_queue",
            "bulk_retry",
            "requeue_dead_letter",
            "rate_limit",
            "restart_worker",
        ],
    )
    def test_method_is_coroutine(self, rq_app, method_name):
        adapter = RqEngineAdapter(rq_app=rq_app)
        method = getattr(adapter, method_name)
        assert inspect.iscoroutinefunction(method), (
            f"{method_name} must be `async def`"
        )

    @pytest.mark.parametrize(
        "method_name", ["subscribe_events", "subscribe_registry_changes"],
    )
    def test_subscribe_methods_are_async_generators(self, rq_app, method_name):
        adapter = RqEngineAdapter(rq_app=rq_app)
        method = getattr(adapter, method_name)
        assert inspect.isasyncgenfunction(method), (
            f"{method_name} must be `async def ... yield`"
        )


class TestUnsupportedActionsReportHonestly:
    """Methods we declare as absent return clear failure messages.

    The brain reads ``capabilities()`` and refuses to dispatch
    these - but if anything bypasses the capability check, the
    adapter must still degrade gracefully with a message that
    explains the engine constraint.
    """

    @pytest.mark.asyncio
    async def test_bulk_retry_now_succeeds(self, rq_app):
        """Promoted from honest-absence to shipped feature in v2026.5."""
        adapter = RqEngineAdapter(rq_app=rq_app)
        result = await adapter.bulk_retry({}, max=10)
        assert result.status == "success"
        assert "retried" in (result.result or {})

    @pytest.mark.asyncio
    async def test_dlq_now_succeeds(self, rq_app, queued_job):
        """Promoted from honest-absence to shipped feature in v2026.5.

        R7 H-2 + R8 H-1: the fallback path requires brain-supplied
        task_name AND overrides because it routes through
        retry_task_action.
        """
        adapter = RqEngineAdapter(rq_app=rq_app)
        result = await adapter.requeue_dead_letter(
            queued_job.id,
            task_name="myapp.tasks.send_email",
            override_args=(),
            override_kwargs={},
        )
        assert result.status == "success"

    @pytest.mark.asyncio
    async def test_restart_worker_returns_failed_with_explanation(self, rq_app):
        adapter = RqEngineAdapter(rq_app=rq_app)
        result = await adapter.restart_worker("worker-1")
        assert result.status == "failed"
        assert "remote-control" in result.error.lower()

    @pytest.mark.asyncio
    async def test_rate_limit_returns_failed_with_explanation(self, rq_app):
        adapter = RqEngineAdapter(rq_app=rq_app)
        result = await adapter.rate_limit("myapp.tasks.work", "10/m")
        assert result.status == "failed"
        assert "rate-limit" in result.error.lower()


class TestHealthDictShape:
    """Heartbeat health snapshot must always include the expected keys."""

    def test_health_includes_redis_marker(self, rq_app):
        adapter = RqEngineAdapter(rq_app=rq_app)
        health = adapter.get_health()
        # RQ is Redis-only by engine design; the adapter must say so.
        assert health["broker_type"] == "redis"
        assert "broker_connected" in health
        assert "queue_depths" in health


class TestSubscribeEventsDelivery:
    """Events pushed onto the queue surface from subscribe_events()."""

    @pytest.mark.asyncio
    async def test_subscribe_events_yields_enqueued_event(self, rq_app):
        from datetime import UTC, datetime
        from uuid import uuid4

        from z4j_core.models import Event, EventKind

        adapter = RqEngineAdapter(rq_app=rq_app)
        placeholder = uuid4()
        ev = Event(
            id=uuid4(),
            project_id=placeholder,
            agent_id=placeholder,
            engine="rq",
            task_id="t-1",
            kind=EventKind.TASK_SUCCEEDED,
            occurred_at=datetime.now(UTC),
            data={},
        )
        # Emulate the worker-wrap sink hopping back via the loop.
        adapter._enqueue_event(ev)

        async def _first() -> Event:
            async for e in adapter.subscribe_events():
                return e
            raise AssertionError("no event yielded")

        got = await asyncio.wait_for(_first(), timeout=1.0)
        assert got.task_id == "t-1"
