"""The corrected RQ fork boundary preserves child context and parent state."""

from __future__ import annotations

import json
import os
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from rq import Worker as StockWorker
from rq.utils import import_attribute
from z4j_rq.worker import Worker

pytestmark = pytest.mark.skipif(not hasattr(os, "fork"), reason="POSIX fork worker")


def _worker() -> Worker:
    worker = Worker.__new__(Worker)
    worker.name = "test-worker"
    worker._horse_pid = 0
    worker.procline = Mock()
    worker.main_work_horse = Mock()
    return worker


@pytest.mark.parametrize("prior_job", [None, "parent-context"])
def test_parent_does_not_write_job_environment(monkeypatch, prior_job) -> None:
    environment = {} if prior_job is None else {"RQ_JOB_ID": prior_job}
    environment["RQ_WORKER_ID"] = "parent-worker-context"
    expected = environment.copy()
    monkeypatch.setattr("z4j_rq.worker.os.environ", environment)
    monkeypatch.setattr("z4j_rq.worker.os.fork", lambda: 12345)
    setpgrp = Mock()
    monkeypatch.setattr("z4j_rq.worker.os.setpgrp", setpgrp)
    worker = _worker()

    for i in range(100):
        worker.fork_work_horse(SimpleNamespace(id=f"unique-job-{i}"), None)

    assert environment == expected
    assert worker.horse_pid == 12345
    setpgrp.assert_not_called()
    worker.main_work_horse.assert_not_called()
    assert worker.procline.call_count == 100


@pytest.mark.parametrize("version, isolation", [("1.10.1", "session"), ("2.12.0", "process-group")])
def test_child_sets_environment_before_job_and_exits(monkeypatch, version, isolation) -> None:
    environment = {"RQ_JOB_ID": "previous", "RQ_WORKER_ID": "previous"}
    monkeypatch.setattr("z4j_rq.worker.os.environ", environment)
    monkeypatch.setattr("z4j_rq.worker.os.fork", lambda: 0)
    calls = []
    monkeypatch.setattr("z4j_rq.worker.rq_version", version)
    monkeypatch.setattr("z4j_rq.worker.os.setsid", lambda: calls.append("session"))
    monkeypatch.setattr("z4j_rq.worker.os.setpgrp", lambda: calls.append("process-group"))

    def exit_child(code):
        calls.append(("exit", code))
        raise SystemExit(code)

    monkeypatch.setattr("z4j_rq.worker.os._exit", exit_child)
    worker = _worker()
    job = SimpleNamespace(id="current-job")
    queue = object()

    def run_child(actual_job, actual_queue):
        assert actual_job is job and actual_queue is queue
        assert environment == {"RQ_JOB_ID": "current-job", "RQ_WORKER_ID": worker.name}
        assert calls == [isolation]
        calls.append("job")

    worker.main_work_horse = run_child
    with pytest.raises(SystemExit) as raised:
        worker.fork_work_horse(job, queue)
    assert raised.value.code == 0
    assert calls == [isolation, "job", ("exit", 0)]
    worker.procline.assert_not_called()


def test_failed_fork_keeps_environment_and_does_not_execute(monkeypatch) -> None:
    environment = {"RQ_JOB_ID": "parent-context"}
    monkeypatch.setattr("z4j_rq.worker.os.environ", environment)
    failure = OSError("fork unavailable")

    def fail_fork():
        raise failure

    monkeypatch.setattr("z4j_rq.worker.os.fork", fail_fork)
    worker = _worker()
    with pytest.raises(OSError) as raised:
        worker.fork_work_horse(SimpleNamespace(id="never-executed"), None)
    assert raised.value is failure
    assert environment == {"RQ_JOB_ID": "parent-context"}
    assert worker.horse_pid == 0
    worker.main_work_horse.assert_not_called()
    worker.procline.assert_not_called()


def test_real_fork_exposes_job_context_only_to_child(monkeypatch) -> None:
    monkeypatch.setenv("RQ_JOB_ID", "parent-context")
    monkeypatch.setenv("RQ_WORKER_ID", "parent-worker")
    read_fd, write_fd = os.pipe()
    worker = _worker()

    def run_child(job, queue):
        os.close(read_fd)
        payload = json.dumps(
            {"job": os.environ["RQ_JOB_ID"], "worker": os.environ["RQ_WORKER_ID"]},
        ).encode()
        os.write(write_fd, payload)
        os.close(write_fd)

    worker.main_work_horse = run_child
    try:
        worker.fork_work_horse(SimpleNamespace(id="child-job"), None)
        os.close(write_fd)
        write_fd = -1
        with os.fdopen(read_fd, "rb") as pipe:
            read_fd = -1
            observed = json.loads(pipe.read())
        _, status = os.waitpid(worker.horse_pid, 0)
        assert os.waitstatus_to_exitcode(status) == 0
        assert observed == {"job": "child-job", "worker": "test-worker"}
        assert os.environ["RQ_JOB_ID"] == "parent-context"
        assert os.environ["RQ_WORKER_ID"] == "parent-worker"
    finally:
        if read_fd >= 0:
            os.close(read_fd)
        if write_fd >= 0:
            os.close(write_fd)


def test_cli_class_is_explicit_and_inherits_rq_execution() -> None:
    assert import_attribute("z4j_rq.worker.Worker") is Worker
    assert issubclass(Worker, StockWorker)
    assert Worker.execute_job is StockWorker.execute_job
    assert Worker.monitor_work_horse is StockWorker.monitor_work_horse
    assert StockWorker.fork_work_horse is not Worker.fork_work_horse
