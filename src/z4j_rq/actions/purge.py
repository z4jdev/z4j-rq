"""``purge_queue`` action - empty an RQ queue.

Mirror of the Celery purge action's safety properties (audit H13 / M-7):

- The brain attaches a ``confirm_token`` -- a keyed
  ``HMAC(project_secret, "purge|queue|depth")`` (see
  ``z4j_core.purge_token``) -- to every purge command. The adapter
  recomputes it locally against the *current* depth + its own
  per-project secret and rejects the action on mismatch. Keying (M-7)
  means a depth-observer cannot forge or refresh a token. A pre-1.7
  unkeyed token is accepted during a grace window (with a warning).
- ``force=True`` bypasses both the token check and the depth-
  threshold guard. Reserved for emergency tooling.
- The ``Z4J_PURGE_THRESHOLD`` env var caps the depth above which
  the adapter refuses without ``force=True`` even if the token
  matches. Default 10 000.

RQ's purge primitive is ``Queue.empty()`` which atomically deletes
all queued jobs from Redis. There is no equivalent for jobs already
fetched into a worker - those continue to run.
"""

from __future__ import annotations

import logging
import os
from typing import Any

from z4j_core.models import CommandResult
from z4j_core.purge_token import (
    accept_legacy_from_env,
    verify_purge_confirm_token,
)
from z4j_core.transport.hmac import decode_agent_hmac_secret

from z4j_rq._offload import OffloadTimeoutError, indeterminate_timeout_result, offload

logger = logging.getLogger("z4j.adapter.rq.actions.purge")

_DEFAULT_THRESHOLD = 10_000
_OFFLOAD_TIMEOUT = 10.0


def _resolve_agent_secret() -> bytes | None:
    """Raw per-project secret for keying the confirm token, or None.

    Reads + decodes ``Z4J_HMAC_SECRET`` the same way frame signing does;
    None (absent/undecodable) leaves only the legacy unkeyed token
    verifiable during the grace window.
    """
    raw = os.environ.get("Z4J_HMAC_SECRET")
    if not raw:
        return None
    try:
        return decode_agent_hmac_secret(raw)
    except ValueError:
        return None


async def purge_queue_action(  # noqa: PLR0911  guard + offload timeout branches
    rq_app: Any,
    *,
    queue_name: str,
    confirm_token: str | None = None,
    force: bool = False,
) -> CommandResult:
    """Empty ``queue_name`` after token + threshold checks."""
    # Resolving the queue and reading ``Queue.count`` both hit Redis
    # synchronously (redis-py is pure-sync); offload to a thread so a
    # broker slowdown cannot freeze the agent's single event loop.
    try:
        queue, depth = await offload(
            _resolve_and_count, rq_app, queue_name, timeout=_OFFLOAD_TIMEOUT
        )
    except OffloadTimeoutError:
        # RL2: this is the DEPTH-PROBE read, which runs BEFORE queue.empty().
        # A read is side-effect-free, so a timeout is a clean, retryable
        # failure -- NOT the indeterminate outcome reserved for a mutation that
        # may have half-applied.
        return CommandResult(
            status="failed",
            error=(
                f"refusing to purge {queue_name!r}: the depth probe timed out "
                f"after {_OFFLOAD_TIMEOUT:g}s (a read; no messages were purged, "
                "safe to retry when Redis is responsive)."
            ),
        )
    except Exception as exc:
        return CommandResult(status="failed", error=f"purge failed: {exc}")

    if queue is None:
        return CommandResult(
            status="failed",
            error=f"queue {queue_name!r} not resolvable",
        )

    if not force:
        if depth is None:
            # 1.7.1 (H9): unmeasurable depth is NOT an empty queue -- refuse
            # rather than let a failed probe relax the threshold/token gate.
            return CommandResult(
                status="failed",
                error=(
                    f"refusing to purge {queue_name!r}: could not read the "
                    "queue depth (the broker may be unreachable). Retry when "
                    "the broker is responsive, or re-issue with force=true."
                ),
            )
        threshold = _threshold()
        if depth > threshold:
            return CommandResult(
                status="failed",
                error=(
                    f"refusing to purge {queue_name!r}: depth {depth} "
                    f"exceeds Z4J_PURGE_THRESHOLD={threshold}. Re-issue "
                    f"with force=true if this is intentional."
                ),
            )
        accepted, used_legacy = verify_purge_confirm_token(
            provided=confirm_token or "",
            queue_name=queue_name,
            queue_depth=depth,
            secret=_resolve_agent_secret(),
            accept_legacy=accept_legacy_from_env(),
        )
        if not accepted:
            return CommandResult(
                status="failed",
                error=(
                    "purge confirm_token missing or stale (queue depth "
                    "may have changed); re-issue from the dashboard"
                ),
            )
        if used_legacy:
            logger.warning(
                "z4j purge_queue: accepted a LEGACY unkeyed confirm_token "
                "for queue %r -- the issuer is pre-1.7. Upgrade the brain "
                "so it sends a keyed HMAC token; legacy acceptance is "
                "removed in a future release.",
                queue_name,
            )

    # ``Queue.empty()`` runs a Lua script against Redis synchronously;
    # offload it too.
    try:
        await offload(queue.empty, timeout=_OFFLOAD_TIMEOUT)
    except OffloadTimeoutError:
        return indeterminate_timeout_result(
            "purge_queue",
            _OFFLOAD_TIMEOUT,
            hint="the queue may still be purged",
        )
    except Exception as exc:
        return CommandResult(status="failed", error=f"purge failed: {exc}")
    return CommandResult(
        status="success",
        result={"queue": queue_name, "purged": depth},
    )


def _resolve_and_count(rq_app: Any, queue_name: str) -> tuple[Any | None, int | None]:
    """Resolve a Queue handle and read its depth in one blocking hop.

    Both steps can touch Redis synchronously, so they are called together
    off the event loop. Returns ``(None, None)`` when the queue is
    unresolvable, and ``(queue, None)`` when the queue resolves but its
    depth cannot be read -- distinct from a genuine ``(queue, 0)`` empty
    queue (1.7.1 H9).
    """
    queue = _resolve_queue(rq_app, queue_name)
    if queue is None:
        return None, None
    try:
        depth: int | None = int(getattr(queue, "count", 0) or 0)
    except Exception:
        # 1.7.1 (H9): the depth probe FAILED (broker unreachable). Do NOT
        # report 0 -- the caller would treat it as an empty queue, pass the
        # threshold + confirm-token gates, and purge everything once the
        # connection recovers.
        depth = None
    return queue, depth


def _resolve_queue(rq_app: Any, queue_name: str) -> Any | None:
    """Get a Queue handle without instantiating duplicates if rq_app exposes one."""
    factory = getattr(rq_app, "queue_for_name", None)
    if callable(factory):
        try:
            return factory(queue_name)
        except Exception:
            return None
    try:
        from rq import Queue  # type: ignore[import-not-found]
    except ImportError:
        return None
    connection = getattr(rq_app, "connection", None)
    if connection is None and hasattr(rq_app, "ping"):
        connection = rq_app
    if connection is None:
        return None
    try:
        return Queue(name=queue_name, connection=connection)
    except Exception:
        return None


def _threshold() -> int:
    raw = os.environ.get("Z4J_PURGE_THRESHOLD")
    if not raw:
        return _DEFAULT_THRESHOLD
    try:
        return max(0, int(raw))
    except ValueError:
        return _DEFAULT_THRESHOLD


__all__ = ["purge_queue_action"]
