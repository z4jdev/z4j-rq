"""``list_dead_letters`` over RQ's ``FailedJobRegistry`` (the ``dlq.list`` read side).

A hermetic redis-py stand-in reproduces RQ's key layout (``rq:failed:<queue>``
sorted set scored by expiry, ``rq:job:<id>`` hash with zlib-compressed
``exc_info`` and ``utcformat`` timestamps) so the walk, the paging, and the
no-pickle rule are exercised without a Redis server.
"""

from __future__ import annotations

import zlib
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest
from z4j_core.errors import AdapterError, ValidationError
from z4j_core.models import DEAD_LETTER_EXCERPT_MAX_CHARS, DeadLetterPage
from z4j_core.redaction import REDACTED, RedactionEngine
from z4j_rq.actions.dlq import list_dead_letters_action
from z4j_rq.engine import RqEngineAdapter


class FakeRedis:
    """Just enough redis-py for the listing: sorted sets and hashes."""

    def __init__(self) -> None:
        self.zsets: dict[str, dict[str, float]] = {}
        self.hashes: dict[str, dict[str, bytes]] = {}
        self.hmget_fields: list[list[str]] = []
        self.fail_with: Exception | None = None

    # -- writes used by the seed helper ------------------------------------
    def zadd(self, key: str, mapping: dict[str, float]) -> None:
        self.zsets.setdefault(key, {}).update(mapping)

    def hset(self, key: str, mapping: dict[str, bytes]) -> None:
        self.hashes.setdefault(key, {}).update(mapping)

    # -- reads the listing is allowed to make ------------------------------
    def ping(self) -> bool:
        return True

    def zcard(self, key: str) -> int:
        if self.fail_with is not None:
            raise self.fail_with
        return len(self.zsets.get(key, {}))

    def zrevrange(self, key: str, start: int, end: int, withscores: bool = False) -> list[Any]:
        items = sorted(self.zsets.get(key, {}).items(), key=lambda kv: (-kv[1], kv[0]))
        window = items[start : None if end == -1 else end + 1]
        if withscores:
            return [(member.encode(), score) for member, score in window]
        return [member.encode() for member, _ in window]

    def hmget(self, key: str, fields: list[str]) -> list[bytes | None]:
        self.hmget_fields.append(list(fields))
        stored = self.hashes.get(key, {})
        return [stored.get(field) for field in fields]


def _seed(
    conn: FakeRedis,
    *,
    queue: str,
    job_id: str,
    score: float,
    description: str = "myapp.tasks.send_email('u-1', email='x@example.com')",
    exc_info: str = "Traceback (most recent call last):\n  ...\nValueError: boom",
    ended_at: str = "2026-10-02T12:00:00.123456Z",
    compress: bool = True,
) -> None:
    conn.zadd(f"rq:failed:{queue}", {job_id: score})
    conn.hset(
        f"rq:job:{job_id}",
        {
            "description": description.encode(),
            "origin": queue.encode(),
            "ended_at": ended_at.encode(),
            "exc_info": zlib.compress(exc_info.encode()) if compress else exc_info.encode(),
            # The pickled callable + arguments. Reading it would be RCE.
            "data": b"\x80\x04pickle-bomb",
            "meta": b"\x80\x04pickle-bomb",
        },
    )


def _app(conn: FakeRedis, *queues: str) -> Any:
    return SimpleNamespace(
        connection=conn,
        queues=[SimpleNamespace(name=name, connection=conn) for name in queues],
    )


@pytest.fixture
def conn() -> FakeRedis:
    return FakeRedis()


class TestListing:
    async def test_newest_first_with_parsed_fields(self, conn: FakeRedis) -> None:
        _seed(conn, queue="default", job_id="old", score=100.0, ended_at="2026-10-01T00:00:00Z")
        _seed(conn, queue="default", job_id="new", score=300.0)
        _seed(conn, queue="default", job_id="mid", score=200.0)

        page = await list_dead_letters_action(_app(conn, "default"), redaction=RedactionEngine())

        assert isinstance(page, DeadLetterPage)
        assert page.engine == "rq"
        assert page.total == 3
        assert page.next_cursor is None
        assert [e.task_id for e in page.entries] == ["new", "mid", "old"]
        newest = page.entries[0]
        assert newest.task_name == "myapp.tasks.send_email"  # argument list dropped
        assert newest.queue == "default"
        assert newest.failed_at == datetime(2026, 10, 2, 12, 0, 0, 123456, tzinfo=UTC)
        assert newest.error_excerpt.endswith("ValueError: boom")
        assert newest.attempts is None
        # Older RQ rows use the second-precision layout.
        assert page.entries[2].failed_at == datetime(2026, 10, 1, tzinfo=UTC)

    async def test_pages_with_offset_cursor(self, conn: FakeRedis) -> None:
        for i in range(5):
            _seed(conn, queue="default", job_id=f"job-{i}", score=float(i))
        app = _app(conn, "default")

        first = await list_dead_letters_action(app, limit=2)
        assert [e.task_id for e in first.entries] == ["job-4", "job-3"]
        assert first.total == 5
        assert first.next_cursor == "2"

        second = await list_dead_letters_action(app, limit=2, cursor=first.next_cursor)
        assert [e.task_id for e in second.entries] == ["job-2", "job-1"]
        assert second.next_cursor == "4"

        last = await list_dead_letters_action(app, limit=2, cursor=second.next_cursor)
        assert [e.task_id for e in last.entries] == ["job-0"]
        assert last.next_cursor is None

    async def test_merges_across_queues_by_score(self, conn: FakeRedis) -> None:
        _seed(conn, queue="emails", job_id="e-1", score=50.0)
        _seed(conn, queue="default", job_id="d-1", score=75.0)
        _seed(conn, queue="emails", job_id="e-2", score=100.0)
        app = _app(conn, "default", "emails")

        page = await list_dead_letters_action(app, limit=2)
        assert [(e.queue, e.task_id) for e in page.entries] == [
            ("emails", "e-2"),
            ("default", "d-1"),
        ]
        assert page.total == 3
        assert page.next_cursor == "2"

        rest = await list_dead_letters_action(app, limit=2, cursor=page.next_cursor)
        assert [(e.queue, e.task_id) for e in rest.entries] == [("emails", "e-1")]
        assert rest.next_cursor is None

    async def test_queue_filter_restricts_to_that_registry(self, conn: FakeRedis) -> None:
        _seed(conn, queue="emails", job_id="e-1", score=50.0)
        _seed(conn, queue="default", job_id="d-1", score=75.0)
        page = await list_dead_letters_action(_app(conn, "default", "emails"), queue="emails")
        assert [e.task_id for e in page.entries] == ["e-1"]
        assert page.total == 1

    async def test_undeclared_queue_is_paged_directly_by_name(self, conn: FakeRedis) -> None:
        _seed(conn, queue="orphaned", job_id="o-1", score=1.0)
        page = await list_dead_letters_action(_app(conn, "default"), queue="orphaned")
        assert [e.task_id for e in page.entries] == ["o-1"]

    async def test_empty_registry_is_an_empty_page(self, conn: FakeRedis) -> None:
        page = await list_dead_letters_action(_app(conn, "default"))
        assert page.entries == []
        assert page.total == 0
        assert page.next_cursor is None

    async def test_limit_is_clamped_and_at_least_one(self, conn: FakeRedis) -> None:
        for i in range(3):
            _seed(conn, queue="default", job_id=f"job-{i}", score=float(i))
        page = await list_dead_letters_action(_app(conn, "default"), limit=0)
        assert len(page.entries) == 1
        assert page.next_cursor == "1"
        page = await list_dead_letters_action(_app(conn, "default"), limit=10_000)
        assert len(page.entries) == 3


class TestSafety:
    async def test_never_reads_pickled_fields(self, conn: FakeRedis) -> None:
        _seed(conn, queue="default", job_id="job-1", score=1.0)
        await list_dead_letters_action(_app(conn, "default"))
        assert conn.hmget_fields, "the listing must read the job hash through hmget"
        for fields in conn.hmget_fields:
            assert "data" not in fields
            assert "meta" not in fields

    async def test_excerpt_is_redacted_and_bounded(self, conn: FakeRedis) -> None:
        secret_trace = "x" * 2000 + "\nRuntimeError: api_key=AKIAIOSFODNN7EXAMPLE rejected"
        _seed(conn, queue="default", job_id="job-1", score=1.0, exc_info=secret_trace)
        page = await list_dead_letters_action(_app(conn, "default"), redaction=RedactionEngine())
        excerpt = page.entries[0].error_excerpt
        assert excerpt == REDACTED
        assert len(excerpt) <= DEAD_LETTER_EXCERPT_MAX_CHARS

    async def test_long_clean_excerpt_keeps_the_tail(self, conn: FakeRedis) -> None:
        trace = ("  File 'x.py', line 1, in f\n" * 100) + "KeyError: 'tenant'"
        _seed(conn, queue="default", job_id="job-1", score=1.0, exc_info=trace)
        page = await list_dead_letters_action(_app(conn, "default"))
        excerpt = page.entries[0].error_excerpt
        assert excerpt.endswith("KeyError: 'tenant'")
        assert len(excerpt) <= DEAD_LETTER_EXCERPT_MAX_CHARS

    async def test_uncompressed_exc_info_is_tolerated(self, conn: FakeRedis) -> None:
        _seed(conn, queue="default", job_id="job-1", score=1.0, exc_info="plain", compress=False)
        page = await list_dead_letters_action(_app(conn, "default"))
        assert page.entries[0].error_excerpt == "plain"

    async def test_missing_job_hash_still_lists_the_id(self, conn: FakeRedis) -> None:
        conn.zadd("rq:failed:default", {"ghost": 1.0})
        page = await list_dead_letters_action(_app(conn, "default"))
        entry = page.entries[0]
        assert entry.task_id == "ghost"
        assert entry.task_name == ""
        assert entry.queue == "default"
        assert entry.failed_at is None
        assert entry.error_excerpt == ""

    async def test_malformed_cursor_is_a_validation_error(self, conn: FakeRedis) -> None:
        with pytest.raises(ValidationError, match="cursor"):
            await list_dead_letters_action(_app(conn, "default"), cursor="page-2")

    async def test_redis_failure_is_an_adapter_error(self, conn: FakeRedis) -> None:
        conn.fail_with = ConnectionError("redis down")
        with pytest.raises(AdapterError, match="redis down"):
            await list_dead_letters_action(_app(conn, "default"))

    async def test_no_connection_for_unknown_queue_is_an_adapter_error(self) -> None:
        app = SimpleNamespace(queues=[])
        with pytest.raises(AdapterError, match="unknown"):
            await list_dead_letters_action(app, queue="x")


class TestEngineAdapter:
    def test_capability_is_advertised(self, conn: FakeRedis) -> None:
        adapter = RqEngineAdapter(rq_app=_app(conn, "default"))
        assert "list_dead_letters" in adapter.capabilities()

    async def test_engine_method_delegates_with_its_redaction(self, conn: FakeRedis) -> None:
        _seed(
            conn,
            queue="default",
            job_id="job-1",
            score=1.0,
            exc_info="RuntimeError: token=AKIAIOSFODNN7EXAMPLE rejected",
        )
        adapter = RqEngineAdapter(rq_app=_app(conn, "default"))
        page = await adapter.list_dead_letters("default", limit=5)
        assert page.engine == "rq"
        assert page.entries[0].error_excerpt == REDACTED


class TestKeyLayoutMatchesRq:
    """The hermetic fake mirrors RQ's real key templates."""

    def test_registry_and_job_keys(self) -> None:
        pytest.importorskip("rq")
        from rq.job import Job
        from rq.registry import FailedJobRegistry
        from z4j_rq.actions.dlq import _failed_registry_key, _job_key

        assert _failed_registry_key("emails") == FailedJobRegistry.key_template.format("emails")
        assert _failed_registry_key("emails") == "rq:failed:emails"
        # RQ 1.x hands the key back as bytes, RQ 2.x as str; both spell the
        # same hash key once decoded, and that text form is what we look up.
        rq_key = Job.key_for("abc")
        if isinstance(rq_key, bytes):
            rq_key = rq_key.decode("utf-8")
        assert _job_key("abc") == rq_key == "rq:job:abc"

    def test_timestamp_layout_matches_rq_utcformat(self) -> None:
        pytest.importorskip("rq")
        from rq.utils import utcformat
        from z4j_rq.actions.dlq import _parse_rq_timestamp

        moment = datetime(2026, 10, 2, 12, 30, 45, 654321, tzinfo=UTC)
        assert _parse_rq_timestamp(utcformat(moment)) == moment
