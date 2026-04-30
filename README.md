# z4j-rq

[![PyPI version](https://img.shields.io/pypi/v/z4j-rq.svg)](https://pypi.org/project/z4j-rq/)
[![Python](https://img.shields.io/pypi/pyversions/z4j-rq.svg)](https://pypi.org/project/z4j-rq/)
[![License](https://img.shields.io/pypi/l/z4j-rq.svg)](https://github.com/z4jdev/z4j-rq/blob/main/LICENSE)

The RQ engine adapter for [z4j](https://z4j.com).

Streams every RQ job lifecycle event from your workers to the z4j brain
and accepts operator control actions from the dashboard. Pair with
z4j-rqscheduler to manage periodic schedules.

## What it ships

| Capability | Notes |
|---|---|
| Job lifecycle events | enqueued, started, finished, failed, deferred, scheduled |
| Job discovery | runtime registry of queue names + worker introspection |
| Submit / retry / cancel | direct against the RQ queue |
| Bulk retry | filter-driven; re-enqueues matching jobs from the failed registry |
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

- [`z4j-rqscheduler`](https://github.com/z4jdev/z4j-rqscheduler) — schedule adapter for rq-scheduler

## Reliability

- No exception from the adapter ever propagates back into your RQ
  workers or job hooks.
- Events buffer locally when the brain is unreachable; workers never
  block on network I/O.

## Documentation

Full docs at [z4j.dev/engines/rq/](https://z4j.dev/engines/rq/).

## License

Apache-2.0 — see [LICENSE](LICENSE).

## Links

- Homepage: https://z4j.com
- Documentation: https://z4j.dev
- PyPI: https://pypi.org/project/z4j-rq/
- Issues: https://github.com/z4jdev/z4j-rq/issues
- Changelog: [CHANGELOG.md](CHANGELOG.md)
- Security: security@z4j.com (see [SECURITY.md](SECURITY.md))
