"""Capability tokens advertised by the RQ engine adapter.

RQ has a narrower surface than Celery - RQ's worker model has no
remote-control channel (no remote restart, no remote pool grow/
shrink, no remote rate-limit). But within the "data-plane"
actions, z4j-rq is now feature-complete: retry, cancel, purge,
bulk_retry, requeue_dead_letter all ship in this release.

See `docs/MULTI_ENGINE_PLAN.md` §5 for the per-engine matrix and
§3 N5 for why honest capability reporting is a non-negotiable.
"""

from __future__ import annotations

DEFAULT_CAPABILITIES: frozenset[str] = frozenset(
    {
        "submit_task",
        # Per-task data-plane actions - shipped in v2026.5
        "retry_task",
        "cancel_task",
        "purge_queue",
        # v1.1 promotions - both ship in v2026.5 alongside the
        # RQ+Dramatiq GA. See docs/MULTI_ENGINE_PLAN.md §7 scope
        # cuts list; these were originally deferred but round-2
        # landed them together.
        "bulk_retry",
        "requeue_dead_letter",
    },
)
"""Actions implemented in :class:`z4j_rq.engine.RqEngineAdapter`.

Honest absences (NOT in this set):

- ``restart_worker`` - never; RQ workers expose no remote control
- ``rate_limit`` - never; RQ has no per-task rate-limit primitive
- ``pool_grow`` / ``pool_shrink`` - never; RQ has no pool concept
- ``add_consumer`` / ``cancel_consumer`` - never; same reason

The dashboard reads this set and hides the corresponding buttons
when the project's agent is RQ-only - so a user never clicks a
button that would have failed silently.
"""


__all__ = ["DEFAULT_CAPABILITIES"]
