"""Tests for the new ``bulk_retry`` + ``requeue_dead_letter`` actions."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest

from z4j_rq.actions.bulk_retry import bulk_retry_action
from z4j_rq.actions.dlq import requeue_dead_letter_action


# ---------------------------------------------------------------------------
# bulk_retry
# ---------------------------------------------------------------------------


class TestBulkRetryExplicitIds:
    @pytest.mark.asyncio
    async def test_retries_every_explicit_id(self, rq_app, queued_job):
        # Queue a second job so we have two ids to test.
        from tests.unit.conftest import FakeJob  # type: ignore[import-not-found]

        j2 = FakeJob(id="job-2", func_name="myapp.other",
                     args=(), kwargs={})
        rq_app.register(j2)

        result = await bulk_retry_action(
            rq_app,
            filter={"task_ids": [queued_job.id, j2.id]},
            max=10,
        )
        assert result.status == "success"
        assert result.result["retried"] == 2
        assert result.result["skipped"] == 0
        assert result.result["source"] == "explicit_ids"
        assert len(result.result["new_task_ids"]) == 2

    @pytest.mark.asyncio
    async def test_caps_at_max(self, rq_app, queued_job):
        result = await bulk_retry_action(
            rq_app,
            filter={"task_ids": [queued_job.id, "ghost-1", "ghost-2"]},
            max=1,
        )
        assert result.result["retried"] == 1
        # Ghosts 1 and 2 don't run because we capped to 1.
        assert "ghost-1" not in result.result.get("errors", {})

    @pytest.mark.asyncio
    async def test_missing_ids_counted_as_skipped_not_errors(self, rq_app):
        result = await bulk_retry_action(
            rq_app,
            filter={"task_ids": ["ghost-1", "ghost-2"]},
            max=10,
        )
        assert result.status == "success"
        assert result.result["retried"] == 0
        assert result.result["skipped"] == 2

    @pytest.mark.asyncio
    async def test_above_absolute_max_marks_capped(self, rq_app):
        huge = [f"id-{i}" for i in range(50_000)]
        result = await bulk_retry_action(
            rq_app,
            filter={"task_ids": huge},
            max=20_000,
        )
        assert result.result["capped"] is True


class TestBulkRetryRegistrySweep:
    @pytest.mark.asyncio
    async def test_uses_rq_app_stub_when_registry_unavailable(self, rq_app):
        # The test env doesn't have the rq.registry module; the
        # action accepts a stub via ``rq_app.failed_job_ids(limit=)``
        # that lets us exercise the registry path.
        rq_app.failed_job_ids = lambda *, limit: []  # type: ignore[attr-defined]

        result = await bulk_retry_action(rq_app, max=10)
        assert result.status == "success"
        assert result.result["source"] == "failed_registry"
        assert result.result["retried"] == 0


class TestBulkRetryEmptyInput:
    @pytest.mark.asyncio
    async def test_no_ids_returns_zeros(self, rq_app):
        result = await bulk_retry_action(rq_app, filter={}, max=10)
        assert result.status == "success"
        assert result.result["retried"] == 0


# ---------------------------------------------------------------------------
# requeue_dead_letter
# ---------------------------------------------------------------------------


class TestDlqRequeue:
    @pytest.mark.asyncio
    async def test_falls_back_to_generic_retry_when_registry_unavailable(
        self, rq_app, queued_job,
    ):
        """In the unit-test env rq.registry isn't importable, so the
        action falls through to the generic retry path. The result
        should mark the source as ``dlq_fallback``."""
        result = await requeue_dead_letter_action(
            rq_app, task_id=queued_job.id,
        )
        assert result.status == "success"
        assert result.result["source"] == "dlq_fallback"

    @pytest.mark.asyncio
    async def test_missing_job_id_returns_success_noop(self, rq_app):
        result = await requeue_dead_letter_action(
            rq_app, task_id="ghost-id",
        )
        # Fallback path treats missing ids as idempotent no-op success.
        assert result.status == "success"


# ---------------------------------------------------------------------------
# Capability promotion contract
# ---------------------------------------------------------------------------


class TestCapabilitiesPromoted:
    def test_bulk_retry_now_in_capabilities(self):
        from z4j_rq.capabilities import DEFAULT_CAPABILITIES
        assert "bulk_retry" in DEFAULT_CAPABILITIES
        assert "requeue_dead_letter" in DEFAULT_CAPABILITIES

    def test_engine_constraints_still_absent(self):
        from z4j_rq.capabilities import DEFAULT_CAPABILITIES
        for absent in (
            "restart_worker",
            "rate_limit",
            "pool_grow",
            "pool_shrink",
            "add_consumer",
            "cancel_consumer",
        ):
            assert absent not in DEFAULT_CAPABILITIES
