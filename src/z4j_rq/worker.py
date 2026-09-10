"""RQ fork worker that keeps per-job environment changes in the child.

Select explicitly with ``rq worker --worker-class z4j_rq.worker.Worker``.
Stock RQ assigns every job ID to ``os.environ`` in its long-lived parent.
glibc retains distinct ``setenv`` values, so this grows native memory even
when Python allocations are released. The job environment belongs in the
short-lived work horse; its parent needs only the child's process ID.

The fork boundary below is adapted from RQ's BSD-licensed Worker. See
``RQ_LICENSE`` in this package for the upstream copyright and license.
Monitoring, timeouts, callbacks, retries and shutdown remain inherited from
the installed RQ version. Importing this module does not patch stock workers.
"""

from __future__ import annotations

import os
import time
from typing import TYPE_CHECKING

from rq import Worker as RqWorker
from rq import __version__ as rq_version

if TYPE_CHECKING:
    from rq.job import Job
    from rq.queue import Queue


class Worker(RqWorker):
    """Opt-in POSIX fork worker without parent-side job-ID accumulation."""

    def fork_work_horse(self, job: Job, queue: Queue) -> None:
        """Set job environment only after entering the work-horse child."""
        child_pid = os.fork()
        if child_pid == 0:
            os.environ["RQ_WORKER_ID"] = self.name
            os.environ["RQ_JOB_ID"] = job.id
            # Preserve RQ 1.x's session isolation; RQ 2.0 switched to a
            # separate process group in the existing session.
            if int(rq_version.split(".", 1)[0]) < 2:
                os.setsid()
            else:
                os.setpgrp()
            self.main_work_horse(job, queue)
            os._exit(0)  # RQ's child normally exits inside main_work_horse.

        self._horse_pid = child_pid
        self.procline(f"Forked {child_pid} at {time.time()}")


__all__ = ["Worker"]
