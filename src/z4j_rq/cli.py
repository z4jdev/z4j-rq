"""z4j-rq CLI: ``z4j-rq doctor | check | status | version``.

Probes upstream ``rq`` library, ``z4j-rq`` adapter, and ``REDIS_URL``
env var (informational). Engines are libraries; the CLI is for
isolating "is the adapter installed and importable" questions.
The framework's doctor calls into the same probes automatically.
"""

from __future__ import annotations

from z4j_bare.cli import make_engine_main

main = make_engine_main(
    "rq",
    upstream_package="rq",
    broker_env="REDIS_URL",
)


__all__ = ["main"]
