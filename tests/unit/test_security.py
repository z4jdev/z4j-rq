"""Adversarial test pass for z4j-rq.

These tests are the security counterpart to the Celery adversarial
sweep already in the repo. They pin the invariants the multi-engine
plan §3 marks as non-negotiable for every adapter.

Each test name doubles as the threat statement: re-read it in 6
months and you should immediately know what regression it is
guarding against.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from z4j_core.models import EventKind
from z4j_core.redaction.engine import RedactionEngine

from z4j_rq.events import callbacks as cb_mod
from z4j_rq.events.mapper import build_event
from z4j_rq.events.worker_wrap import RqWorkerHook


# ---------------------------------------------------------------------------
# T1 - mapper never forwards raw job.args / job.kwargs (no-pickle rule)
# ---------------------------------------------------------------------------


class TestMapperDoesNotForwardArgsOrKwargs:
    """CLAUDE.md §2.3: no pickled bytes leave the agent unredacted.

    Even if the user puts plaintext secrets into ``kwargs`` (a common
    anti-pattern with RQ because it pickles by default), the agent
    must not ship them. The mapper drops args/kwargs entirely; only
    the engine-supplied ``description`` string survives, post-redaction.
    """

    def test_secret_in_kwargs_does_not_leak(self, queued_job):
        queued_job.kwargs = {
            "stripe_secret_key": "sk_live_DO_NOT_LEAK",
            "password": "hunter2",
        }
        ev = build_event(
            kind=EventKind.TASK_STARTED,
            job=queued_job,
            redaction=RedactionEngine(),
        )
        flat = repr(ev.data)
        assert "sk_live_DO_NOT_LEAK" not in flat
        assert "hunter2" not in flat

    def test_secret_in_positional_args_does_not_leak(self, queued_job):
        queued_job.args = ("user-1", "Bearer eyJtokenheader.payload.sig")
        ev = build_event(
            kind=EventKind.TASK_STARTED,
            job=queued_job,
            redaction=RedactionEngine(),
        )
        flat = repr(ev.data)
        assert "Bearer eyJtokenheader" not in flat


# ---------------------------------------------------------------------------
# T2 - exception payloads are bounded
# ---------------------------------------------------------------------------


class TestExceptionPayloadBounded:
    """A multi-MB traceback must NOT inflate every event.

    The local SQLite buffer is bounded; an unbounded exception would
    burn through the buffer faster than the brain can drain it,
    forcing eviction of legitimate events.
    """

    def test_extreme_traceback_truncated(self, queued_job):
        queued_job.exc_info = "X" * 1_000_000  # 1 MB
        ev = build_event(
            kind=EventKind.TASK_FAILED,
            job=queued_job,
            redaction=RedactionEngine(),
        )
        assert len(ev.data["exception"].encode("utf-8")) <= 5_000


# ---------------------------------------------------------------------------
# T3 - capture callbacks never raise into the host process
# ---------------------------------------------------------------------------


class TestCaptureCallbacksAreBoundary:
    """A bug in z4j must not crash the user's RQ worker.

    CLAUDE.md §2.2: every host-callback boundary is wrapped. The
    callbacks intentionally swallow EVERY exception (including ones
    raised inside ``build_event`` itself).
    """

    def test_callback_swallows_mapper_exception(self, queued_job, monkeypatch):
        # Force the mapper to raise - simulates an internal bug.
        from z4j_rq.events import callbacks as cb

        def _boom(**_kwargs: Any) -> None:
            raise RuntimeError("synthetic mapper bug")

        monkeypatch.setattr(cb, "build_event", _boom)
        cb.install(sink=lambda _ev: None, redaction=RedactionEngine())
        try:
            # MUST NOT raise - host worker would die otherwise.
            cb.capture_success(queued_job)
            cb.capture_failure(queued_job)
            cb.capture_started(queued_job)
            cb.capture_stopped(queued_job)
        finally:
            cb.uninstall()

    def test_callback_with_no_sink_is_noop(self, queued_job):
        from z4j_rq.events import callbacks as cb
        cb.uninstall()  # ensure clean slate
        # Must not raise even though no sink is installed.
        cb.capture_success(queued_job)


# ---------------------------------------------------------------------------
# T4 - worker-wrap monkey-patch is install/uninstall safe
# ---------------------------------------------------------------------------


class TestWorkerWrapLifecycle:
    """The patch into ``rq.Worker.perform_job`` must be reversible.

    If `disconnect_signals` is called (e.g. agent shutdown), the
    original method MUST come back exactly as it was. Otherwise a
    later restart of the agent would double-wrap, double-emit, and
    leave broken behaviour after agent death.
    """

    def test_install_uninstall_when_rq_missing_is_silent(self):
        # In the unit-test env we don't have rq installed. install()
        # must silently no-op rather than crash the engine adapter.
        hook = RqWorkerHook(sink=lambda _e: None, redaction=RedactionEngine())
        hook.install()
        hook.uninstall()  # should also no-op cleanly

    def test_install_is_idempotent(self):
        hook = RqWorkerHook(sink=lambda _e: None, redaction=RedactionEngine())
        hook.install()
        hook.install()  # second call must not double-wrap nor raise
        hook.uninstall()


# ---------------------------------------------------------------------------
# T5 - purge confirm-token is constant-time-comparable, not bypassable
# ---------------------------------------------------------------------------


class TestPurgeTokenIsAuthoritative:
    """Audit H13 invariant - depth-aware confirm token gate.

    A forged token (or a token computed against a stale depth) must
    NOT bypass the guard. The action returns ``failed`` rather than
    raising, so the dashboard surfaces the rejection cleanly.
    """

    @pytest.mark.asyncio
    async def test_empty_string_token_rejected(self, rq_app, queued_job):
        from z4j_rq.actions.purge import purge_queue_action
        result = await purge_queue_action(
            rq_app, queue_name=queued_job.origin, confirm_token="",
        )
        assert result.status == "failed"

    @pytest.mark.asyncio
    async def test_garbage_token_rejected(self, rq_app, queued_job):
        from z4j_rq.actions.purge import purge_queue_action
        result = await purge_queue_action(
            rq_app,
            queue_name=queued_job.origin,
            confirm_token="0" * 64,  # plausible-looking sha256 hex
        )
        assert result.status == "failed"


# ---------------------------------------------------------------------------
# T6 - retry refuses ETA windows that would silently surprise the user
# ---------------------------------------------------------------------------


class TestRetryEtaBounds:
    """Audit H14 invariant - ETA is bounded both directions."""

    @pytest.mark.asyncio
    async def test_retry_eta_far_past_refused(self, rq_app, queued_job):
        from z4j_rq.actions.retry import retry_task_action
        far_past = datetime(2000, 1, 1, tzinfo=UTC).timestamp()
        result = await retry_task_action(
            rq_app, task_id=queued_job.id, eta=far_past,
        )
        assert result.status == "failed"

    @pytest.mark.asyncio
    async def test_retry_eta_far_future_refused(self, rq_app, queued_job):
        from z4j_rq.actions.retry import retry_task_action
        far_future = datetime(2030, 1, 1, tzinfo=UTC).timestamp()
        # 2030 is more than 365 days from "now" in 2026 tests
        result = await retry_task_action(
            rq_app, task_id=queued_job.id, eta=far_future,
        )
        assert result.status == "failed"


# ---------------------------------------------------------------------------
# T7 - capabilities() does not lie
# ---------------------------------------------------------------------------


class TestCapabilitiesDoNotLie:
    """Every capability the adapter advertises is actually implemented.

    The dashboard gates buttons on this set. Lying here puts a button
    in the UI that throws a runtime error when clicked - exactly the
    bad UX the multi-engine plan §3 N5 warns against.
    """

    def test_every_advertised_capability_has_method(self, rq_app):
        from z4j_rq.engine import RqEngineAdapter
        adapter = RqEngineAdapter(rq_app=rq_app)
        # Map capability name -> method name on the adapter.
        method_for = {
            "submit_task": "submit_task",
            "retry_task": "retry_task",
            "cancel_task": "cancel_task",
            "purge_queue": "purge_queue",
            "bulk_retry": "bulk_retry",
            "requeue_dead_letter": "requeue_dead_letter",
        }
        for cap in adapter.capabilities():
            method_name = method_for.get(cap)
            assert method_name, (
                f"capability {cap!r} has no documented method binding "
                "in the test - either add it or remove the capability"
            )
            assert callable(getattr(adapter, method_name))


# ---------------------------------------------------------------------------
# T8 - install/uninstall on the engine adapter is safe under churn
# ---------------------------------------------------------------------------


class TestEngineAdapterChurn:
    """connect_signals / disconnect_signals can be called repeatedly
    in a long-running process (e.g. on transport reconnect). It must
    be safe - no leaked patches, no leaked sinks, no monkey-patch
    drift over time.
    """

    def test_repeated_connect_disconnect_is_safe(self, rq_app):
        from z4j_rq.engine import RqEngineAdapter
        adapter = RqEngineAdapter(rq_app=rq_app)
        for _ in range(5):
            adapter.connect_signals()
            adapter.disconnect_signals()
        # If we got here without raising, the lifecycle is clean.
        assert adapter._worker_hook is None
        assert cb_mod._SINK is None
