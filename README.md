# z4j-rq

[![PyPI version](https://img.shields.io/pypi/v/z4j-rq.svg)](https://pypi.org/project/z4j-rq/)
[![Python](https://img.shields.io/pypi/pyversions/z4j-rq.svg)](https://pypi.org/project/z4j-rq/)
[![License](https://img.shields.io/pypi/l/z4j-rq.svg)](https://github.com/z4jdev/z4j-rq/blob/main/LICENSE)

The RQ engine adapter for [z4j](https://z4j.com).

Streams supported RQ job lifecycle events from your workers to z4j
and accepts operator control actions from the dashboard. Pair with
z4j-rqscheduler to manage periodic schedules.

## Compatibility

- RQ 1.10.1+ and <3 (capped below the RQ 3.0 breaking-major rewrite)
- Python 3.11+

Full per-adapter matrix at <https://z4j.dev/reference/compatibility/>.

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

- Lifecycle-capture failures are isolated from RQ workers and job hooks;
  capture hooks make no brain network request inline.
- The in-process event queue and SQLite outbound buffer are bounded. Queue
  overflow drops new events and buffer pressure evicts oldest rows; both losses
  are logged.

## Documentation

Full docs at [z4j.dev/engines/rq/](https://z4j.dev/engines/rq/).

## License

Apache-2.0, see [LICENSE](LICENSE).

## Links

- Homepage: https://z4j.com
- Documentation: https://z4j.dev
- PyPI: https://pypi.org/project/z4j-rq/
- Issues: https://github.com/z4jdev/z4j-rq/issues
- Changelog: [CHANGELOG.md](CHANGELOG.md)
- Security: security@z4j.com (see [SECURITY.md](SECURITY.md))
