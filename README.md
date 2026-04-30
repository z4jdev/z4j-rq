# z4j-rq

[![PyPI version](https://img.shields.io/pypi/v/z4j-rq.svg)](https://pypi.org/project/z4j-rq/)
[![Python](https://img.shields.io/pypi/pyversions/z4j-rq.svg)](https://pypi.org/project/z4j-rq/)
[![License](https://img.shields.io/pypi/l/z4j-rq.svg)](https://github.com/z4jdev/z4j-rq/blob/main/LICENSE)

The RQ engine adapter for [z4j](https://z4j.com).

Streams RQ job lifecycle events to the z4j brain and accepts
control actions (retry, cancel, bulk retry, purge) from the
dashboard. Pair with z4j-rqscheduler to surface periodic
schedules.

## Install

```bash
pip install z4j-rq z4j-rqscheduler
```

## Pairs with

- [`z4j-rqscheduler`](https://github.com/z4jdev/z4j-rqscheduler) — schedule adapter for rq-scheduler

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
