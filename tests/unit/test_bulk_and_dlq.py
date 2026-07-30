"""Tests for the new ``bulk_retry`` + ``requeue_dead_letter`` actions."""

from __future__ import annotations

from typing import Any

import pytest
from z4j_rq.actions.bulk_retry import bulk_retry_action
from z4j_rq.actions.dlq import requeue_dead_letter_action

# ---------------------------------------------------------------------------
# bulk_retry
# ---------------------------------------------------------------------------


def _all_empty_overrides(*task_ids: str) -> dict[str, dict[str, Any]]:
    """Shorthand for tests: supply empty-but-explicit overrides per id.

    bulk_retry refuses any id that lacks an override entry.
    Tests that aren't exercising the missing-override path use this
    helper to pass the safety gate cleanly.
    """
    return {tid: {"args": [], "kwargs": {}} for tid in task_ids}


def _all_task_names(*task_ids: str, name: str = "myapp.tasks.work") -> dict[str, str]:
    """Shorthand for tests: supply a uniform task_name per id.

    bulk_retry refuses any id that lacks a task_name entry.
    Tests that aren't exercising the missing-task_name path use this
    helper to pass the safety gate cleanly.
    """
    return dict.fromkeys(task_ids, name)


class TestBulkRetryExplicitIds:
    @pytest.mark.asyncio
    async def test_retries_every_explicit_id(self, rq_app, queued_job):
        # Queue a second job so we have two ids to test.
        from tests.unit.conftest import FakeJob  # type: ignore[import-not-found]

        j2 = FakeJob(id="job-2", func_name="myapp.other", args=(), kwargs={})
        rq_app.register(j2)

        result = await bulk_retry_action(
            rq_app,
            filter={
                "task_ids": [queued_job.id, j2.id],
                "overrides": _all_empty_overrides(queued_job.id, j2.id),
                "task_names": _all_task_names(queued_job.id, j2.id),
            },
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
            filter={
                "task_ids": [queued_job.id, "ghost-1", "ghost-2"],
                # ``max=1`` clips to the first id only - that's the
                # only id we need to supply an override for.
                "overrides": _all_empty_overrides(queued_job.id),
                "task_names": _all_task_names(queued_job.id),
            },
            max=1,
        )
        assert result.result["retried"] == 1
        # Ghosts 1 and 2 don't run because we capped to 1.
        assert "ghost-1" not in result.result.get("errors", {})

    @pytest.mark.asyncio
    async def test_missing_ids_counted_as_skipped_not_errors(self, rq_app):
        result = await bulk_retry_action(
            rq_app,
            filter={
                "task_ids": ["ghost-1", "ghost-2"],
                "overrides": _all_empty_overrides("ghost-1", "ghost-2"),
                "task_names": _all_task_names("ghost-1", "ghost-2"),
            },
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
            filter={
                "task_ids": huge,
                # Only the first ``effective_max=10_000`` ids are
                # actually processed; overrides for those are enough
                # to pass the safety gate.
                "overrides": _all_empty_overrides(*huge[:10_000]),
                "task_names": _all_task_names(*huge[:10_000]),
            },
            max=20_000,
        )
        assert result.result["capped"] is True

    @pytest.mark.asyncio
    async def test_id_without_override_is_not_pickle_reconstructed_r7_h2(
        self,
        rq_app,
        queued_job,
    ):
        """/ CX-M17: an id WITHOUT an explicit override is never
        pickle-reconstructed. It is requeued by reference instead (which is
        unavailable in this rq-less fake, so it fails closed for that id);
        the id WITH an override reconstructs normally. The batch is a per-id
        summary, not an all-or-nothing refusal."""
        from tests.unit.conftest import FakeJob  # type: ignore[import-not-found]

        j2 = FakeJob(id="job-2", func_name="myapp.other", args=(), kwargs={})
        rq_app.register(j2)

        result = await bulk_retry_action(
            rq_app,
            filter={
                "task_ids": [queued_job.id, j2.id],
                # Only one override - the other id must NOT be reconstructed.
                "overrides": _all_empty_overrides(queued_job.id),
                "task_names": _all_task_names(queued_job.id, j2.id),
            },
            max=10,
        )
        assert result.status == "success"
        # queued_job (has override) reconstructed; j2 (no override) is not.
        assert result.result["retried"] == 1
        assert j2.id in result.result["errors"]
        # Exactly ONE reconstruction enqueue (queued_job); j2 was never
        # pickle-reconstructed.
        queue = rq_app.queue_for_name(queued_job.origin)
        assert len(queue.enqueue_calls) == 1

    @pytest.mark.asyncio
    async def test_override_without_task_name_is_not_pickle_reconstructed_r8_h1(
        self,
        rq_app,
        queued_job,
    ):
        """An id with an explicit override but NO brain-supplied
        task_name must not be reconstructed (that would need job.func_name
        from pickle). It is recorded as a per-id error; the id with both an
        override and a task_name reconstructs."""
        from tests.unit.conftest import FakeJob  # type: ignore[import-not-found]

        j2 = FakeJob(id="job-2", func_name="myapp.other", args=(), kwargs={})
        rq_app.register(j2)

        result = await bulk_retry_action(
            rq_app,
            filter={
                "task_ids": [queued_job.id, j2.id],
                "overrides": _all_empty_overrides(queued_job.id, j2.id),
                # Only one task_name - j2 has an override but no task_name.
                "task_names": _all_task_names(queued_job.id),
            },
            max=10,
        )
        assert result.status == "success"
        assert result.result["retried"] == 1
        assert j2.id in result.result["errors"]
        assert "task_name" in result.result["errors"][j2.id]
        # Exactly ONE reconstruction enqueue (queued_job); j2 was never
        # pickle-reconstructed.
        queue = rq_app.queue_for_name(queued_job.origin)
        assert len(queue.enqueue_calls) == 1

    @pytest.mark.asyncio
    async def test_bulk_retry_passes_task_name_per_job_r8_h1(
        self,
        rq_app,
        queued_job,
    ):
        """Regression: each job's retry enqueues with its own task_name."""
        from tests.unit.conftest import FakeJob  # type: ignore[import-not-found]

        j2 = FakeJob(id="job-2", func_name="myapp.other", args=(), kwargs={})
        rq_app.register(j2)

        result = await bulk_retry_action(
            rq_app,
            filter={
                "task_ids": [queued_job.id, j2.id],
                "overrides": _all_empty_overrides(queued_job.id, j2.id),
                # Distinct names per id; assert each lands as the
                # enqueue ``func``, not the stored job.func_name.
                "task_names": {
                    queued_job.id: "tasks.send_email",
                    j2.id: "tasks.send_sms",
                },
            },
            max=10,
        )
        assert result.status == "success"
        queue = rq_app.queue_for_name(queued_job.origin)
        funcs = [c["func"] for c in queue.enqueue_calls]
        assert "tasks.send_email" in funcs
        assert "tasks.send_sms" in funcs


class TestBulkRetryNoRegistrySweepRH4:
    @pytest.mark.asyncio
    async def test_no_ids_is_noop_not_broker_sweep(self, rq_app):
        # RH4: a bulk_retry with no explicit task_ids must NOT walk the broker's
        # FailedJobRegistry (that retried EVERY failed job on the shared broker,
        # including other projects' jobs the brain never ownership-verified). It
        # is now a clean no-op; the brain resolves project-owned ids and supplies
        # them explicitly. The old sweep stub must never be consulted.
        swept = {"called": False}

        def _stub(*, limit):
            swept["called"] = True
            return ["foreign-1", "foreign-2"]

        rq_app.failed_job_ids = _stub  # type: ignore[attr-defined]

        result = await bulk_retry_action(rq_app, max=10)
        assert result.status == "success"
        assert result.result["retried"] == 0
        assert result.result["source"] == "no_explicit_ids"
        assert swept["called"] is False  # the broker was NEVER swept


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
        self,
        rq_app,
        queued_job,
    ):
        """In the unit-test env rq.registry isn't importable, so the
        action falls through to the generic retry path. The result
        should mark the source as ``dlq_fallback``.

        The fallback runs in the agent process and
        therefore requires brain-supplied task_name AND override args /
        kwargs.
        """
        result = await requeue_dead_letter_action(
            rq_app,
            task_id=queued_job.id,
            task_name="myapp.tasks.send_email",
            override_args=(),
            override_kwargs={},
        )
        assert result.status == "success"
        assert result.result["source"] == "dlq_fallback"

    @pytest.mark.asyncio
    async def test_missing_job_id_returns_success_noop(self, rq_app):
        result = await requeue_dead_letter_action(
            rq_app,
            task_id="ghost-id",
        )
        # Fallback path treats missing ids as idempotent no-op success.
        assert result.status == "success"

    @pytest.mark.asyncio
    async def test_fallback_refuses_without_overrides_r7_h2(
        self,
        rq_app,
        queued_job,
    ):
        """Regression: the dlq fallback fails closed too."""
        result = await requeue_dead_letter_action(
            rq_app,
            task_id=queued_job.id,
            task_name="myapp.tasks.send_email",
            # override_args / override_kwargs intentionally absent
        )
        # The fallback delegates to retry_task_action which refuses
        # without overrides; the failure should be visible to the
        # operator with the same language.
        assert result.status == "failed"
        assert "override_args" in result.error

    @pytest.mark.asyncio
    async def test_fallback_refuses_without_task_name_r8_h1(
        self,
        rq_app,
        queued_job,
    ):
        """Regression: the dlq fallback fails closed without task_name."""
        result = await requeue_dead_letter_action(
            rq_app,
            task_id=queued_job.id,
            override_args=(),
            override_kwargs={},
            # task_name intentionally absent
        )
        assert result.status == "failed"
        assert "task_name" in result.error


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
