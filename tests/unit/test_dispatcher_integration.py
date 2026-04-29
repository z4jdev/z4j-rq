"""End-to-end dispatcher integration: real RQ engine + bare dispatcher.

Phase 2 of the v1.1.0 schedule.fire verification matrix. The unit
tests in ``test_submit_task.py`` prove the engine method itself
works; the bare dispatcher's ``TestScheduleFire`` proves the
dispatch routing with a fake engine. This file proves the COMPOSITION:
a real ``schedule.fire`` CommandFrame, handed to a real
CommandDispatcher wired to a real ``RqEngineAdapter``, results in
a real ``Queue.enqueue`` call and a successful ``command_result``
frame.

If this passes for every engine, the v1.1.0 brain-side scheduler
path is wired correctly across the entire engine matrix.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from z4j_bare.buffer import BufferStore
from z4j_bare.dispatcher import CommandDispatcher
from z4j_core.transport.frames import CommandFrame, CommandPayload

from z4j_rq.engine import RqEngineAdapter

from tests.unit.test_submit_task import _patch_enqueue


@pytest.fixture
def buf(tmp_path: Path) -> BufferStore:
    store = BufferStore(path=tmp_path / "buf.sqlite")
    yield store
    store.close()


@pytest.mark.asyncio
async def test_schedule_fire_end_to_end_through_dispatcher(
    rq_app, buf: BufferStore,
) -> None:
    """A schedule.fire CommandFrame for the RQ engine must:
    1. Survive the bare dispatcher's _dispatch_schedule_fire route
    2. Reach RqEngineAdapter.submit_task with the right kwargs
    3. Land on the actual RQ queue
    4. Produce a success command_result frame on the buffer
    """
    _patch_enqueue(rq_app)
    engine = RqEngineAdapter(rq_app=rq_app)
    dispatcher = CommandDispatcher(
        engines={"rq": engine},
        schedulers={},  # no scheduler adapter — proves the fix works
        buffer=buf,
    )

    frame = CommandFrame(
        id="cmd_e2e_rq_01",
        payload=CommandPayload(
            action="schedule.fire",
            target={},
            parameters={
                "schedule_id": "sched-1",
                "schedule_name": "nightly-cleanup",
                "task_name": "myapp.tasks.cleanup",
                "engine": "rq",
                "queue": "high-priority",
                "args": ["arg1"],
                "kwargs": {"verbose": True},
                "fire_id": "fire-1",
            },
        ),
        hmac="deadbeef" * 8,
    )

    await dispatcher.handle(frame)

    # The RQ queue saw the enqueue with the brain's payload values.
    q = rq_app.queue_for_name("high-priority")
    assert q.submit_calls == [
        {
            "name": "myapp.tasks.cleanup",
            "args": ("arg1",),
            "kwargs": {"verbose": True},
        },
    ]

    # The dispatcher emitted a success command_result frame.
    entries = buf.drain(10)
    results = [e for e in entries if e.kind == "command_result"]
    assert len(results) == 1
    parsed = json.loads(results[0].payload.decode("utf-8"))
    assert parsed["payload"]["status"] == "success"
    assert parsed["payload"]["result"]["engine"] == "rq"
