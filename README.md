# z4j-rq

[![PyPI version](https://img.shields.io/pypi/v/z4j-rq.svg)](https://pypi.org/project/z4j-rq/)
[![Python](https://img.shields.io/pypi/pyversions/z4j-rq.svg)](https://pypi.org/project/z4j-rq/)
[![License](https://img.shields.io/pypi/l/z4j-rq.svg)](https://github.com/z4jdev/z4j-rq/blob/main/LICENSE)


**License:** Apache 2.0
**Status:** v1.0.0 - first public release alongside `z4j-celery` and `z4j-dramatiq`.

The RQ ([Redis Queue](https://python-rq.org/)) queue-engine adapter
for z4j. Drop into any RQ install (bare-Python, Django+`django-rq`,
FastAPI, anything) and z4j observes every job your workers run -
without modifying your task code.

## Install

```bash
# As part of the z4j stack:
pip install z4j[rq]

# Or just this package:
pip install z4j-rq
```

## What it ships on day 1

| Capability | Status | Notes |
|---|---|---|
| Event capture (received / started / succeeded / failed) | ✅ | Worker-wrap (every job) + per-job callbacks (opt-in) |
| Discovery | ✅ | Walks queues + StartedJobRegistry + FinishedJobRegistry |
| `retry` | ✅ | Re-enqueues by id; refuses if job is currently running |
| `cancel` | ⚠️ | Queued jobs removed; running jobs get a stop-command (RQ ≥1.13) |
| `purge_queue` | ✅ | With confirm-token + Z4J_PURGE_THRESHOLD guard |

## What it deliberately does NOT ship

| Capability | Why |
|---|---|
| `bulk_retry` | Deferred to v1.1 |
| `requeue_dead_letter` | Deferred to v1.1 (RQ has FailedJobRegistry) |
| `restart_worker` | RQ workers expose no remote-control channel - never |
| `rate_limit` | RQ has no per-task rate-limit primitive - never |
| `pool grow / shrink / consumer ops` | RQ has no pool concept - never |

The adapter advertises only what it implements via
`capabilities()` - the dashboard hides every button it can't honor,
so users never click an action that would silently fail.

## Scheduler pairing

Use [`z4j-rqscheduler`](https://github.com/z4jdev/z4j-rqscheduler) for
`rq-scheduler` periodic jobs on the Schedules page.

## Documentation

- [RQ engine guide](https://z4j.dev/engines/rq/)
- [Architecture](https://z4j.dev/concepts/architecture/)
- [Adapter protocol](https://z4j.dev/concepts/adapter-axes/)

## License

Apache 2.0 - see [LICENSE](LICENSE).

## Links

- Homepage: <https://z4j.com>
- Documentation: <https://z4j.dev>
- Issues: <https://github.com/z4jdev/z4j-rq/issues>
- Changelog: [CHANGELOG.md](CHANGELOG.md)
- Security: `security@z4j.com` (see [SECURITY.md](SECURITY.md))
