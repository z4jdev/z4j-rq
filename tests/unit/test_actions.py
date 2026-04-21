"""Action-helper unit tests using the FakeRqApp."""

from __future__ import annotations

import time

import pytest

from z4j_rq.actions.cancel import cancel_task_action
from z4j_rq.actions.purge import purge_queue_action
from z4j_rq.actions.retry import retry_task_action


class TestRetry:
    @pytest.mark.asyncio
    async def test_retry_queued_job_creates_new_job(self, rq_app, queued_job):
        result = await retry_task_action(rq_app, task_id=queued_job.id)
        assert result.status == "success"
        assert result.result["previous_task_id"] == queued_job.id
        assert result.result["task_id"].startswith("new-")
        # New job re-uses the same args/kwargs
        queue = rq_app.queue_for_name(queued_job.origin)
        assert queue.enqueue_calls[-1]["args"] == queued_job.args
        assert queue.enqueue_calls[-1]["kwargs"] == queued_job.kwargs

    @pytest.mark.asyncio
    async def test_retry_overrides_args_kwargs(self, rq_app, queued_job):
        result = await retry_task_action(
            rq_app,
            task_id=queued_job.id,
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
            rq_app, task_id=queued_job.id, eta=far_past,
        )
        assert result.status == "failed"
        assert "past" in result.error.lower()

    @pytest.mark.asyncio
    async def test_retry_eta_too_far_future_refused(self, rq_app, queued_job):
        far_future = time.time() + 86400 * 400  # 400 days
        result = await retry_task_action(
            rq_app, task_id=queued_job.id, eta=far_future,
        )
        assert result.status == "failed"
        assert "future" in result.error.lower()


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


class TestPurge:
    @pytest.mark.asyncio
    async def test_purge_with_force_succeeds_without_token(self, rq_app, queued_job):
        result = await purge_queue_action(
            rq_app, queue_name=queued_job.origin, force=True,
        )
        assert result.status == "success"
        assert result.result["queue"] == queued_job.origin
        # purged jobs go away
        assert rq_app.queue_for_name(queued_job.origin).count == 0

    @pytest.mark.asyncio
    async def test_purge_without_token_refused(self, rq_app, queued_job):
        result = await purge_queue_action(
            rq_app, queue_name=queued_job.origin,
        )
        assert result.status == "failed"
        assert "confirm_token" in result.error

    @pytest.mark.asyncio
    async def test_purge_with_correct_token_succeeds(self, rq_app, queued_job):
        from z4j_rq.actions.purge import _derive_token

        queue = rq_app.queue_for_name(queued_job.origin)
        token = _derive_token(queue.name, queue.count)
        result = await purge_queue_action(
            rq_app,
            queue_name=queue.name,
            confirm_token=token,
        )
        assert result.status == "success"

    @pytest.mark.asyncio
    async def test_purge_with_stale_token_refused(self, rq_app, queued_job):
        from z4j_rq.actions.purge import _derive_token

        queue = rq_app.queue_for_name(queued_job.origin)
        # Token derived for a different depth - should fail
        token = _derive_token(queue.name, queue.count + 99)
        result = await purge_queue_action(
            rq_app,
            queue_name=queue.name,
            confirm_token=token,
        )
        assert result.status == "failed"
        assert "stale" in result.error.lower() or "token" in result.error.lower()

    @pytest.mark.asyncio
    async def test_purge_above_threshold_refused_without_force(
        self, rq_app, monkeypatch,
    ):
        # Force a ridiculously low threshold and queue 5 jobs.
        monkeypatch.setenv("Z4J_PURGE_THRESHOLD", "2")
        from dataclasses import dataclass

        from z4j_rq.actions.purge import _derive_token

        @dataclass
        class _Job:
            id: str
            origin: str = "hot"

        queue = rq_app.queue_for_name("hot")
        for i in range(5):
            queue.jobs.append(_Job(id=f"j{i}"))

        token = _derive_token("hot", queue.count)
        result = await purge_queue_action(
            rq_app, queue_name="hot", confirm_token=token,
        )
        assert result.status == "failed"
        assert "Z4J_PURGE_THRESHOLD" in result.error
