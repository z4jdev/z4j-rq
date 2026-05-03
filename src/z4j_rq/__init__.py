"""z4j-rq - RQ queue engine adapter for z4j.

Public API:

- :class:`RqEngineAdapter` - the adapter to pass to
  :func:`z4j_bare.install_agent` (or to a framework adapter that
  forwards to the same install path).
- :func:`z4j_meta` - optional per-task metadata decorator for
  redaction overrides, tagging, and skip flags.
- :class:`TaskMeta` - normalized per-task metadata struct.
- :func:`register_worker_bootstrap` - auto-bootstrap helper that
  z4j-bare's CLI uses; bare-Python projects can call it explicitly
  if they construct their RQ Worker manually.

Licensed under Apache License 2.0.
"""

from __future__ import annotations

from z4j_rq.engine import RqEngineAdapter
from z4j_rq.meta import TaskMeta, z4j_meta
from z4j_rq.worker_bootstrap import register_worker_bootstrap

__version__ = "1.4.0"

__all__ = [
    "RqEngineAdapter",
    "TaskMeta",
    "__version__",
    "register_worker_bootstrap",
    "z4j_meta",
]
