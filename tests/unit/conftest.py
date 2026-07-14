"""Shared fixtures for z4j-rq unit tests.

Fakes here let the entire z4j-rq surface run without ``rq`` /
``redis`` actually installed. Integration tests (separate matrix
leg) use real RQ + a real Redis container.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import pytest


@dataclass
class FakeJob:
    """Minimal stand-in for ``rq.job.Job``."""

    id: str
    func_name: str = "myapp.tasks.work"
    origin: str = "default"
    description: str = "myapp.tasks.work()"
    args: tuple[Any, ...] = ()
    kwargs: dict[str, Any] = field(default_factory=dict)
    status: str = "queued"
    enqueued_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    started_at: datetime | None = None
    ended_at: datetime | None = None
    exc_info: str = ""
    func: Any | None = None
    cancelled: bool = False

    def get_status(self) -> str:
        return self.status

    def cancel(self) -> None:
        self.cancelled = True
        self.status = "canceled"


@dataclass
class FakeQueue:
    """Minimal stand-in for ``rq.Queue``."""

    name: str = "default"
    jobs: list[FakeJob] = field(default_factory=list)
    enqueue_calls: list[dict[str, Any]] = field(default_factory=list)
    scheduled_calls: list[dict[str, Any]] = field(default_factory=list)
    purged: bool = False

    @property
    def count(self) -> int:
        return len(self.jobs)

    def empty(self) -> None:
        self.purged = True
        self.jobs.clear()

    def get_jobs(self, start: int, end: int) -> list[FakeJob]:
        return self.jobs[start : end + 1]

    def enqueue_call(
        self,
        *,
        func: str,
        args: Iterable[Any] = (),
        kwargs: dict[str, Any] | None = None,
    ) -> FakeJob:
        new_id = f"new-{len(self.enqueue_calls) + 1}"
        record = {
            "func": func,
            "args": tuple(args),
            "kwargs": dict(kwargs or {}),
        }
        self.enqueue_calls.append(record)
        new_job = FakeJob(
            id=new_id,
            func_name=func,
            origin=self.name,
            args=tuple(args),
            kwargs=dict(kwargs or {}),
        )
        self.jobs.append(new_job)
        return new_job

    def create_job(
        self,
        func: str,
        *,
        args: Iterable[Any] = (),
        kwargs: dict[str, Any] | None = None,
        status: Any = "scheduled",
        **_extra: Any,
    ) -> FakeJob:
        # Mirrors rq.Queue.create_job: builds a job WITHOUT enqueuing it.
        return FakeJob(
            id=f"sched-{len(self.scheduled_calls) + 1}",
            func_name=func,
            origin=self.name,
            args=tuple(args),
            kwargs=dict(kwargs or {}),
            status=str(getattr(status, "value", status)),
        )

    def schedule_job(self, job: FakeJob, at: Any, **_extra: Any) -> FakeJob:
        # Mirrors rq.Queue.schedule_job: records the scheduled time.
        self.scheduled_calls.append(
            {"func": job.func_name, "args": job.args, "kwargs": job.kwargs, "at": at},
        )
        return job


class FakeConnection:
    """Minimal Redis-like surface used by health checks."""

    def __init__(self) -> None:
        self.pinged = 0

    def ping(self) -> bool:
        self.pinged += 1
        return True


class FakeRqApp:
    """Duck-typed rq_app the engine adapter accepts.

    Production wraps a real ``redis.Redis`` instance; tests use this
    minimal surface so the adapter logic can be exercised without
    importing the rq package at all.
    """

    def __init__(self) -> None:
        self.connection = FakeConnection()
        self._queues: dict[str, FakeQueue] = {"default": FakeQueue("default")}
        self._jobs: dict[str, FakeJob] = {}

    @property
    def queues(self) -> list[FakeQueue]:
        return list(self._queues.values())

    def queue_for_name(self, name: str) -> FakeQueue:
        if name not in self._queues:
            self._queues[name] = FakeQueue(name)
        return self._queues[name]

    def queue_for(self, job: FakeJob) -> FakeQueue:
        return self.queue_for_name(job.origin)

    def fetch_job(self, task_id: str) -> FakeJob | None:
        return self._jobs.get(task_id)

    # Test helpers --------------------------------------------------

    def register(self, job: FakeJob) -> None:
        self._jobs[job.id] = job
        self.queue_for_name(job.origin).jobs.append(job)


@pytest.fixture
def rq_app() -> FakeRqApp:
    return FakeRqApp()


@pytest.fixture
def queued_job(rq_app: FakeRqApp) -> FakeJob:
    job = FakeJob(
        id="job-1",
        func_name="myapp.tasks.send_email",
        args=("u-1",),
        kwargs={"email": "x@example.com"},
    )
    rq_app.register(job)
    return job


@pytest.fixture
def started_job(rq_app: FakeRqApp) -> FakeJob:
    job = FakeJob(
        id="job-running",
        status="started",
        started_at=datetime.now(UTC),
    )
    rq_app.register(job)
    return job


@pytest.fixture
def finished_job(rq_app: FakeRqApp) -> FakeJob:
    job = FakeJob(
        id="job-done",
        status="finished",
        ended_at=datetime.now(UTC),
    )
    rq_app.register(job)
    return job
