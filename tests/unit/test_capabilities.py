"""Capability set is a contract - these tests freeze it.

Day-1 RQ ships exactly three capabilities. If anyone tries to add or
remove a capability without updating ``docs/MULTI_ENGINE_PLAN.md §5``,
this test fails first and they have to think about it.
"""

from __future__ import annotations

from z4j_rq.capabilities import DEFAULT_CAPABILITIES


def test_default_capabilities_frozen() -> None:
    # v2026.5 GA capability set - see docs/MULTI_ENGINE_PLAN.md §5.
    # ``bulk_retry`` and ``requeue_dead_letter`` were originally
    # deferred but round-2 landed them in time for release.
    assert (
        frozenset(
            {
                "submit_task",
                "retry_task",
                "cancel_task",
                "purge_queue",
                "bulk_retry",
                "requeue_dead_letter",
            },
        )
        == DEFAULT_CAPABILITIES
    )


def test_default_capabilities_is_frozen_type() -> None:
    """frozenset, not set - accidental mutation must fail."""
    assert isinstance(DEFAULT_CAPABILITIES, frozenset)


def test_no_remote_control_capabilities() -> None:
    """RQ engine has no remote-control story - these MUST be absent."""
    for absent in ("restart_worker", "rate_limit", "pool_grow", "pool_shrink"):
        assert absent not in DEFAULT_CAPABILITIES


def test_engine_only_absences() -> None:
    """Capabilities RQ can NEVER support (engine-level constraints)."""
    for absent in ("rate_limit", "restart_worker", "pool_grow", "pool_shrink"):
        assert absent not in DEFAULT_CAPABILITIES
