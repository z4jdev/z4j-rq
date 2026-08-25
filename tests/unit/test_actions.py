"""Action-helper unit tests using the FakeRqApp."""

from __future__ import annotations

import sys
import time
from types import ModuleType

import pytest
from z4j_rq.actions.cancel import cancel_task_action
from z4j_rq.actions.purge import purge_queue_action
from z4j_rq.actions.retry import retry_task_action
from z4j_rq.events.mapper import make_strict_tripwire


class TestRetry:
    @pytest.mark.asyncio
    async def test_retry_queued_job_creates_new_job(self, rq_app, queued_job):
        # Brain MUST supply task_name, override_args, override_kwargs.
        result = await retry_task_action(
            rq_app,
            task_id=queued_job.id,
            task_name="myapp.tasks.send_email",
            override_args=("u-1",),
            override_kwargs={"email": "x@example.com"},
        )
        assert result.status == "success"
        assert result.result["previous_task_id"] == queued_job.id
        assert result.result["task_id"].startswith("new-")
        queue = rq_app.queue_for_name(queued_job.origin)
        assert queue.enqueue_calls[-1]["args"] == ("u-1",)
        assert queue.enqueue_calls[-1]["kwargs"] == {"email": "x@example.com"}
        # Enqueue must use the brain-supplied task_name, not
        # job.func_name (which would lazy-pickle-load).
        assert queue.enqueue_calls[-1]["func"] == "myapp.tasks.send_email"

    @pytest.mark.asyncio
    async def test_retry_overrides_args_kwargs(self, rq_app, queued_job):
        result = await retry_task_action(
            rq_app,
            task_id=queued_job.id,
            task_name="myapp.tasks.send_email",
            override_args=("u-2",),
            override_kwargs={"email": "y@example.com"},
        )
        assert result.status == "success"
        queue = rq_app.queue_for_name(queued_job.origin)
        assert queue.enqueue_calls[-1]["args"] == ("u-2",)
        assert queue.enqueue_calls[-1]["kwargs"] == {"email": "y@example.com"}

    @pytest.mark.asyncio
    async def test_retry_missing_job_is_noop_success(self, rq_app):
        result = await retry_task_action(rq_app, task_id="ghost")
        assert result.status == "success"
        assert result.result["noop"] is True
        assert result.result["reason"] == "job_not_found"

    @pytest.mark.asyncio
    async def test_retry_started_job_refused(self, rq_app, started_job):
        result = await retry_task_action(rq_app, task_id=started_job.id)
        assert result.status == "failed"
        assert "still running" in result.error

    @pytest.mark.asyncio
    async def test_retry_eta_too_far_past_refused(self, rq_app, queued_job):
        far_past = time.time() - 600  # 10 minutes ago
        result = await retry_task_action(
            rq_app,
            task_id=queued_job.id,
            task_name="myapp.tasks.send_email",
            override_args=(),
            override_kwargs={},
            eta=far_past,
        )
        assert result.status == "failed"
        assert "past" in result.error.lower()

    @pytest.mark.asyncio
    async def test_retry_eta_too_far_future_refused(self, rq_app, queued_job):
        far_future = time.time() + 86400 * 400  # 400 days
        result = await retry_task_action(
            rq_app,
            task_id=queued_job.id,
            task_name="myapp.tasks.send_email",
            override_args=(),
            override_kwargs={},
            eta=far_future,
        )
        assert result.status == "failed"
        assert "future" in result.error.lower()

    @pytest.mark.asyncio
    async def test_retry_valid_eta_schedules_not_enqueues(self, rq_app, queued_job):
        # B18: an in-bounds eta must SCHEDULE the retry for that time, not
        # enqueue it immediately and silently drop the schedule.
        target = time.time() + 1800  # 30 minutes out
        result = await retry_task_action(
            rq_app,
            task_id=queued_job.id,
            task_name="myapp.tasks.send_email",
            override_args=("u-9",),
            override_kwargs={"email": "z@example.com"},
            eta=target,
        )
        assert result.status == "success"
        assert result.result["scheduled_for"] == target
        queue = rq_app.queue_for_name(queued_job.origin)
        # Scheduled, NOT immediately enqueued.
        assert queue.enqueue_calls == []
        assert len(queue.scheduled_calls) == 1
        sched = queue.scheduled_calls[0]
        assert sched["func"] == "myapp.tasks.send_email"
        assert sched["args"] == ("u-9",)
        assert sched["kwargs"] == {"email": "z@example.com"}

    @pytest.mark.asyncio
    async def test_retry_without_overrides_requeues_by_reference_never_pickle_r7_h2(
        self,
        rq_app,
        queued_job,
    ):
        """/ CX-M17: with no operator overrides, retry re-runs the
        original failed job BY REFERENCE (FailedJobRegistry.requeue), which
        never deserializes job.args / job.kwargs / job.func_name in the
        agent process. It must NEVER fall back to pickle reconstruction.
        In this fake env ``rq`` is not installed, so requeue-by-reference
        is unavailable and the action fails closed -- but it must fail
        closed WITHOUT ever enqueuing a reconstructed job.
        """
        result = await retry_task_action(
            rq_app,
            task_id=queued_job.id,
            task_name="myapp.tasks.send_email",
        )
        assert result.status == "failed"
        # New fail-closed message: not in a FailedJobRegistry + no overrides.
        assert "FailedJobRegistry" in result.error
        assert "retry with different inputs" in result.error
        # Critically: NOTHING was reconstruct-enqueued (no pickle path).
        queue = rq_app.queue_for_name(queued_job.origin)
        assert queue.enqueue_calls == []

    @pytest.mark.asyncio
    async def test_retry_accepts_brain_supplied_args_r7_h2(
        self,
        rq_app,
        queued_job,
    ):
        """Regression: the override path is the only safe path."""
        result = await retry_task_action(
            rq_app,
            task_id=queued_job.id,
            task_name="myapp.tasks.send_email",
            override_args=("safe-arg",),
            override_kwargs={"safe": True},
        )
        assert result.status == "success"
        queue = rq_app.queue_for_name(queued_job.origin)
        assert queue.enqueue_calls[-1]["args"] == ("safe-arg",)
        assert queue.enqueue_calls[-1]["kwargs"] == {"safe": True}
        # Empty overrides count as "explicitly supplied" - the
        # operator chose to retry with no inputs.
        rq_app2_job = queued_job  # reuse same job; new enqueue id will differ
        result_empty = await retry_task_action(
            rq_app,
            task_id=rq_app2_job.id,
            task_name="myapp.tasks.send_email",
            override_args=(),
            override_kwargs={},
        )
        assert result_empty.status == "success"
        assert queue.enqueue_calls[-1]["args"] == ()
        assert queue.enqueue_calls[-1]["kwargs"] == {}

    @pytest.mark.asyncio
    async def test_retry_refuses_without_brain_supplied_task_name_r8_h1(
        self,
        rq_app,
        queued_job,
    ):
        """Regression: omitting task_name must fail closed.

        Reading job.func_name in the agent process triggers RQ's
        ``_deserialize_data`` (same pickle blob as args/kwargs). The
        retry action must refuse rather than fall back to the stored
        ``job.func_name``.
        """
        result = await retry_task_action(
            rq_app,
            task_id=queued_job.id,
            override_args=("safe",),
            override_kwargs={"safe": True},
            # task_name intentionally absent
        )
        assert result.status == "failed"
        assert "task_name" in result.error
        assert "pickle" in result.error.lower()
        queue = rq_app.queue_for_name(queued_job.origin)
        assert queue.enqueue_calls == []

    @pytest.mark.asyncio
    async def test_retry_refuses_empty_task_name_r8_h1(self, rq_app, queued_job):
        """Empty string task_name is rejected like None."""
        result = await retry_task_action(
            rq_app,
            task_id=queued_job.id,
            task_name="",
            override_args=(),
            override_kwargs={},
        )
        assert result.status == "failed"
        assert "task_name" in result.error

    @pytest.mark.asyncio
    async def test_retry_never_reads_pickle_fields_r8_h1(
        self,
        rq_app,
        queued_job,
    ):
        """Regression: retry must never read any of the four
        pickle-load-trigger fields on the fetched Job.

        Wraps the registered Job in a tripwire proxy that raises
        ``AssertionError`` on any read of ``args`` / ``kwargs`` /
        ``func_name`` / ``instance``. The retry path must succeed
        without tripping any of them; if a future change reads any
        of those fields the proxy raises and this test fails loudly.
        """
        rq_app._jobs[queued_job.id] = make_strict_tripwire(queued_job)
        result = await retry_task_action(
            rq_app,
            task_id=queued_job.id,
            task_name="myapp.tasks.send_email",
            override_args=("u-1",),
            override_kwargs={"email": "x@example.com"},
        )
        assert result.status == "success"
        queue = rq_app.queue_for_name(queued_job.origin)
        assert queue.enqueue_calls[-1]["func"] == "myapp.tasks.send_email"


class TestCancel:
    @pytest.mark.asyncio
    async def test_cancel_queued_job_removes_it(self, rq_app, queued_job):
        result = await cancel_task_action(rq_app, task_id=queued_job.id)
        assert result.status == "success"
        assert result.result["soft"] is False
        assert queued_job.cancelled is True

    @pytest.mark.asyncio
    async def test_cancel_finished_job_is_noop(self, rq_app, finished_job):
        result = await cancel_task_action(rq_app, task_id=finished_job.id)
        assert result.status == "success"
        assert result.result["noop"] is True
        assert "already_finished" in result.result["reason"]

    @pytest.mark.asyncio
    async def test_cancel_missing_job_is_noop(self, rq_app):
        result = await cancel_task_action(rq_app, task_id="ghost")
        assert result.status == "success"
        assert result.result["noop"] is True

    @pytest.mark.asyncio
    async def test_started_cancel_reports_published_but_unverified(
        self,
        rq_app,
        started_job,
        monkeypatch,
    ):
        calls = []

        rq_module = ModuleType("rq")
        rq_module.__path__ = []  # type: ignore[attr-defined]
        command_module = ModuleType("rq.command")

        def send_stop_job_command(connection, task_id):
            calls.append((connection, task_id))

        command_module.send_stop_job_command = send_stop_job_command  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, "rq", rq_module)
        monkeypatch.setitem(sys.modules, "rq.command", command_module)

        result = await cancel_task_action(rq_app, task_id=started_job.id)

        assert result.status == "success"
        assert result.result == {
            "task_id": started_job.id,
            "soft": True,
            "note": "stop command published; job termination was not verified",
        }
        assert calls == [(rq_app.connection, started_job.id)]


class TestPurge:
    @pytest.mark.asyncio
    async def test_purge_with_force_succeeds_without_token(self, rq_app, queued_job):
        result = await purge_queue_action(
            rq_app,
            queue_name=queued_job.origin,
            force=True,
        )
        assert result.status == "success"
        assert result.result["queue"] == queued_job.origin
        # purged jobs go away
        assert rq_app.queue_for_name(queued_job.origin).count == 0

    @pytest.mark.asyncio
    async def test_purge_without_token_refused(self, rq_app, queued_job):
        result = await purge_queue_action(
            rq_app,
            queue_name=queued_job.origin,
        )
        assert result.status == "failed"
        assert "confirm_token" in result.error

    @pytest.mark.asyncio
    async def test_purge_with_correct_token_succeeds(self, rq_app, queued_job, monkeypatch):
        from z4j_core.purge_token import legacy_purge_confirm_token

        queue = rq_app.queue_for_name(queued_job.origin)
        # No Z4J_HMAC_SECRET in this env -> only the legacy unkeyed token is
        # available, which is OFF by default now; opt into the grace window.
        monkeypatch.setenv("Z4J_ACCEPT_LEGACY_PURGE_TOKEN", "1")
        token = legacy_purge_confirm_token(
            queue_name=queue.name,
            queue_depth=queue.count,
        )
        result = await purge_queue_action(
            rq_app,
            queue_name=queue.name,
            confirm_token=token,
        )
        assert result.status == "success"

    @pytest.mark.asyncio
    async def test_purge_with_stale_token_refused(self, rq_app, queued_job):
        from z4j_core.purge_token import legacy_purge_confirm_token

        queue = rq_app.queue_for_name(queued_job.origin)
        # Token derived for a different depth - should fail
        token = legacy_purge_confirm_token(
            queue_name=queue.name,
            queue_depth=queue.count + 99,
        )
        result = await purge_queue_action(
            rq_app,
            queue_name=queue.name,
            confirm_token=token,
        )
        assert result.status == "failed"
        assert "stale" in result.error.lower() or "token" in result.error.lower()

    @pytest.mark.asyncio
    async def test_purge_above_threshold_refused_without_force(
        self,
        rq_app,
        monkeypatch,
    ):
        # Force a ridiculously low threshold and queue 5 jobs.
        monkeypatch.setenv("Z4J_PURGE_THRESHOLD", "2")
        from dataclasses import dataclass

        from z4j_core.purge_token import legacy_purge_confirm_token

        @dataclass
        class _Job:
            id: str
            origin: str = "hot"

        queue = rq_app.queue_for_name("hot")
        for i in range(5):
            queue.jobs.append(_Job(id=f"j{i}"))

        # Threshold refusal fires before the token check; token shape is
        # irrelevant here but supplied for realism.
        token = legacy_purge_confirm_token(queue_name="hot", queue_depth=queue.count)
        result = await purge_queue_action(
            rq_app,
            queue_name="hot",
            confirm_token=token,
        )
        assert result.status == "failed"
        assert "Z4J_PURGE_THRESHOLD" in result.error
