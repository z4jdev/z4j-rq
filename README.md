# z4j-rq

[![PyPI version](https://img.shields.io/pypi/v/z4j-rq.svg)](https://pypi.org/project/z4j-rq/)
[![Python](https://img.shields.io/pypi/pyversions/z4j-rq.svg)](https://pypi.org/project/z4j-rq/)
[![License](https://img.shields.io/pypi/l/z4j-rq.svg)](https://github.com/z4jdev/z4j-rq/blob/main/LICENSE)

The RQ engine adapter for [z4j](https://z4j.com).

Streams supported RQ job lifecycle events from your workers to z4j
and accepts operator control actions from the dashboard. Pair with
z4j-rqscheduler to manage periodic schedules.

## Compatibility

- RQ 1.10.1+ (no upper bound)
- Python 3.11+

Full per-adapter matrix at <https://docs.z4j.com/reference/compatibility/>.

## What it ships

| Capability | Notes |
|---|---|
| Job lifecycle events | started, succeeded, failed, revoked (canceled) |
| Job discovery | runtime registry of queue names + worker introspection |
| Submit / retry / cancel | direct against the RQ queue |
| Bulk retry | retries brain-resolved, project-owned explicit IDs by reference; never sweeps the broker-wide failed registry |
| Purge queue | with confirm-token guard |
| Reconcile task | via Redis-backed job hash lookup |

## Install

```bash
pip install z4j-rq z4j-rqscheduler
```

Pair with a framework adapter:

```bash
pip install z4j-django  z4j-rq z4j-rqscheduler   # Django
pip install z4j-flask   z4j-rq z4j-rqscheduler   # Flask
pip install z4j-fastapi z4j-rq z4j-rqscheduler   # FastAPI
pip install z4j-bare    z4j-rq z4j-rqscheduler   # framework-free worker
```

## Pairs with

- [`z4j-rqscheduler`](https://github.com/z4jdev/z4j-rqscheduler), schedule adapter for rq-scheduler

## Reliability

- For POSIX fork workers, select the memory-corrected worker through RQ's
  standard CLI extension point:

  ```bash
  rq worker --worker-class z4j_rq.worker.Worker default
  ```

  Keep your existing queues, Redis options, settings module and z4j startup
  integration. In Python, import `Worker` from `z4j_rq.worker` instead of `rq`.
  The worker sets each job's environment only in its child process, preventing
  the parent-side native allocation growth reproduced with stock RQ on glibc.
  RQ retains execution, monitoring, timeout, retry and shutdown handling; the
  RQ 1.x session and RQ 2.x process-group behavior are preserved.
- Selecting this class is explicit. Installing the adapter alone does not
  replace stock or custom workers. `SimpleWorker`, `SpawnWorker` and Windows
  workers are outside this fork-worker repair. If you retain an affected stock
  fork worker, use `--max-jobs 50000` with a process manager configured to
  restart successful exits. Normal supervision and application memory limits
  remain useful with either worker.
- Lifecycle-capture failures are isolated from RQ workers and job hooks;
  capture hooks make no brain network request inline.
- The in-process event queue and SQLite outbound buffer are bounded. Queue
  overflow drops new events and buffer pressure evicts oldest rows; both losses
  are logged.

## Documentation

Full docs at [docs.z4j.com/engines/rq/](https://docs.z4j.com/engines/rq/).

## License

Apache-2.0, see [LICENSE](LICENSE).
The fork boundary adapted from RQ retains its BSD-2-Clause notice in
[`RQ_LICENSE`](src/z4j_rq/RQ_LICENSE), included in the installed package.

## Links

- Homepage: https://z4j.com
- Documentation: https://docs.z4j.com
- PyPI: https://pypi.org/project/z4j-rq/
- Issues: https://github.com/z4jdev/z4j-rq/issues
- Changelog: [CHANGELOG.md](CHANGELOG.md)
- Security: security@z4j.com (see [SECURITY.md](SECURITY.md))
