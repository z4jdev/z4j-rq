"""Tests for :mod:`z4j_rq.events.mapper`."""

from __future__ import annotations

from datetime import UTC, datetime

from z4j_core.models import EventKind
from z4j_core.redaction.engine import RedactionEngine

from z4j_rq.events.mapper import build_event
from z4j_rq.meta import z4j_meta


class TestBuildEvent:
    def test_started_event_has_engine_rq(self, queued_job):
        ev = build_event(
            kind=EventKind.TASK_STARTED,
            job=queued_job,
            redaction=RedactionEngine(),
        )
        assert ev.engine == "rq"
        assert ev.kind == EventKind.TASK_STARTED
        assert ev.task_id == queued_job.id

    def test_succeeded_event_uses_ended_at_timestamp(self, finished_job):
        ev = build_event(
            kind=EventKind.TASK_SUCCEEDED,
            job=finished_job,
            redaction=RedactionEngine(),
        )
        assert ev.occurred_at == finished_job.ended_at

    def test_failed_event_includes_truncated_exception(self, queued_job):
        queued_job.exc_info = "Boom!\n" + ("x" * 10_000)
        ev = build_event(
            kind=EventKind.TASK_FAILED,
            job=queued_job,
            redaction=RedactionEngine(),
        )
        exc = ev.data["exception"]
        assert exc.startswith("Boom!")
        # Bounded - must not echo the full 10k payload
        assert len(exc.encode("utf-8")) <= 5_000

    def test_event_drops_args_kwargs_security_invariant(self, queued_job):
        """SECURITY: raw args/kwargs must NEVER leak into the event payload."""
        queued_job.kwargs = {"password": "supersecret-do-not-leak"}
        ev = build_event(
            kind=EventKind.TASK_STARTED,
            job=queued_job,
            redaction=RedactionEngine(),
        )
        flat = repr(ev.data)
        assert "supersecret-do-not-leak" not in flat

    def test_naive_timestamp_coerced_to_utc(self, queued_job):
        queued_job.started_at = datetime(2026, 4, 15, 10, 0, 0)  # naive
        ev = build_event(
            kind=EventKind.TASK_STARTED,
            job=queued_job,
            redaction=RedactionEngine(),
        )
        assert ev.occurred_at.tzinfo is not None
        assert ev.occurred_at.utcoffset().total_seconds() == 0

    def test_meta_tags_attached_to_payload(self, queued_job):
        @z4j_meta(tags=["billing", "critical"], priority="high")
        def some_task() -> None:
            ...

        queued_job.func = some_task
        ev = build_event(
            kind=EventKind.TASK_STARTED,
            job=queued_job,
            redaction=RedactionEngine(),
        )
        assert ev.data["tags"] == ["billing", "critical"]
        assert ev.data["priority"] == "high"

    def test_unstringifiable_field_does_not_crash(self, queued_job):
        class _Bad:
            def __str__(self) -> str:
                raise RuntimeError("nope")
        queued_job.description = _Bad()  # type: ignore[assignment]
        ev = build_event(
            kind=EventKind.TASK_STARTED,
            job=queued_job,
            redaction=RedactionEngine(),
        )
        assert ev.data["description"] == "<unstringifiable>"

    def test_safe_against_missing_timestamps(self, queued_job):
        queued_job.started_at = None
        ev = build_event(
            kind=EventKind.TASK_STARTED,
            job=queued_job,
            redaction=RedactionEngine(),
        )
        # Falls back to "now" - must still be tz-aware.
        assert ev.occurred_at.tzinfo is not None
        assert (datetime.now(UTC) - ev.occurred_at).total_seconds() < 5
