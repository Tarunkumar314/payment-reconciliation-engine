"""
Pytest fixtures for real-Postgres integration tests.

Isolation strategy: savepoint-based rollback
─────────────────────────────────────────────
Each test is fully isolated without touching the schema, using:

  BEGIN  (T_outer on a fresh AsyncConnection — never committed)
    └─ AsyncSession with join_transaction_mode="create_savepoint"
         Every session.commit() the router calls becomes:
           RELEASE SAVEPOINT saN  +  SAVEPOINT saN+1
         T_outer stays open; nothing reaches disk.
  ROLLBACK  (at fixture teardown — undoes every write the test made)

Why savepoints and not TRUNCATE:
  - Zero schema churn between tests
  - Faster: no table scans or FK cascade walks
  - Semantically correct: every write is genuinely undone, not deleted

Why NullPool:
  asyncpg binds a connection object to the coroutine context that created
  it.  With a pool, the schema-setup fixture and per-test fixtures can
  receive the same physical connection from the pool in different event-loop
  coroutine contexts, causing asyncpg's "another operation is in progress"
  error. NullPool gives each engine.connect() call a brand-new TCP
  connection, owned by exactly one coroutine context. This is the correct
  pool strategy for asyncpg + pytest.

  NullPool is on test_engine ONLY. app/database.py's production engine
  (pool_size=10, max_overflow=20) is not modified.

Why asyncio.run() for schema setup:
  pytest-asyncio manages its own event loop per test-function scope. A
  session-scoped async fixture runs in a *different* loop instance than the
  function-scoped async fixtures, meaning any asyncpg connection object
  created in the session fixture would be "owned" by a now-dead loop when
  the function fixtures try to use the engine. Using asyncio.run() for
  schema creation keeps that DDL work in a completely separate, isolated
  event loop that fully exits before pytest-asyncio's per-test loops start.
"""

import os
import asyncio
import pytest
import pytest_asyncio
from unittest.mock import patch
from httpx import AsyncClient, ASGITransport
from sqlalchemy.pool import NullPool
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from dotenv import load_dotenv

from app.main import app
from app.database import get_db
from app.models.base import Base
from app.models import Account, Transaction, LedgerEntry, OutboxEvent  # noqa: F401 — ensures ORM metadata is registered

load_dotenv()

TEST_DATABASE_URL = os.environ["TEST_DATABASE_URL"]

# NullPool: no connection reuse across coroutine contexts.
# Scoped to this test engine only — does not touch app/database.py.
test_engine = create_async_engine(TEST_DATABASE_URL, poolclass=NullPool, echo=False)



# ── Schema lifecycle ──────────────────────────────────────────────────────────

@pytest.fixture(scope="session", autouse=True)
def setup_schema():
    """
    Creates all ORM tables before any test; drops them after the last.

    Uses asyncio.run() deliberately — keeps schema DDL in its own isolated
    event loop, completely separate from pytest-asyncio's per-test loops.
    This prevents asyncpg connection objects from being "stranded" on a
    session-scoped loop when function-scoped fixtures try to use the engine.
    """
    async def _create():
        async with test_engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def _drop():
        async with test_engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)

    asyncio.run(_create())
    yield
    asyncio.run(_drop())


# ── Per-test savepoint isolation ──────────────────────────────────────────────

@pytest_asyncio.fixture
async def db_connection(setup_schema):
    """
    Opens a fresh connection (NullPool guarantees it's never shared),
    starts T_outer (BEGIN), yields, then unconditionally rolls back.

    All writes made through db_session during the test are undone here.
    """
    async with test_engine.connect() as conn:
        await conn.begin()
        yield conn
        await conn.rollback()


@pytest_asyncio.fixture
async def db_session(db_connection):
    """
    Wraps the test connection in an AsyncSession with savepoint mode.

    With join_transaction_mode="create_savepoint":
      - session.commit()  →  RELEASE SAVEPOINT saN + SAVEPOINT saN+1
      - T_outer remains open and uncommitted throughout the test
      - db_connection.rollback() at teardown undoes everything
    """
    session = AsyncSession(
        bind=db_connection,
        join_transaction_mode="create_savepoint",
        expire_on_commit=False,
    )
    yield session
    await session.close()


@pytest_asyncio.fixture
async def client(db_session):
    """
    ASGI test client with get_db overridden to yield the test session.

    Critical: all router DB operations must go through the same session
    (and therefore the same connection and transaction) as the fixture.
    If get_db were allowed to spin up a new session from the production
    engine, it would operate on a different connection outside T_outer
    and the rollback would not affect it.
    """
    async def _override_get_db():
        yield db_session

    app.dependency_overrides[get_db] = _override_get_db
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac
    app.dependency_overrides.clear()


# ── Redis fixtures for idempotency / fraud velocity tests ─────────────────────
#
# Why real Redis (not fakeredis):
#   We use real Postgres for ledger tests to catch NUMERIC precision, ENUM
#   constraints, and FK cascade behaviour that SQLite doesn't enforce.  The
#   same principle applies here: real Redis exercises actual pipeline
#   semantics, sorted-set scoring, and TTL expiry behaviour.
#
# Isolation strategy — DB index + key namespace:
#   Redis doesn't have savepoints.  Instead we:
#     1. Use Redis DB 1 (DB 0 = production, DB 1 = test) — hard-wired.
#     2. Give each test a unique UUID4 namespace prefix.  All keys written by
#        the idempotency and velocity services are prefixed with this value,
#        so two concurrent test runs never collide.
#     3. FLUSHDB at fixture teardown to leave the test DB clean.
#
# How the prefix is injected:
#   The services use key formats "idempotency:{key}" and "velocity:{account}".
#   We wrap the redis client in a thin KeyPrefixRedis adapter that prepends
#   the namespace to every key operation.  This is simpler than patching the
#   format strings inside each service.

import uuid as _uuid
import redis.asyncio as _aioredis

TEST_REDIS_URL = os.environ.get("TEST_REDIS_URL", "redis://localhost:6379/1")


class _PrefixedRedis:
    """
    Transparent proxy around a redis.asyncio.Redis that prepends `prefix:`
    to every key argument.  Supports all commands used by the services:
    GET, SET, ZADD, ZREMRANGEBYSCORE, ZCARD, EXPIRE, TTL, and pipeline().
    """

    def __init__(self, client: _aioredis.Redis, prefix: str):
        self._r = client
        self._prefix = prefix

    def _k(self, key: str) -> str:
        return f"{self._prefix}:{key}"

    async def get(self, key: str):
        return await self._r.get(self._k(key))

    async def set(self, key: str, value, **kwargs):
        return await self._r.set(self._k(key), value, **kwargs)

    async def zadd(self, key: str, mapping: dict, **kwargs):
        return await self._r.zadd(self._k(key), mapping, **kwargs)

    async def zcard(self, key: str):
        return await self._r.zcard(self._k(key))

    async def zremrangebyscore(self, key: str, min, max):
        return await self._r.zremrangebyscore(self._k(key), min, max)

    async def expire(self, key: str, seconds: int):
        return await self._r.expire(self._k(key), seconds)

    async def ttl(self, key: str):
        return await self._r.ttl(self._k(key))

    async def ping(self):
        return await self._r.ping()

    def pipeline(self, transaction: bool = True) -> "_PrefixedPipeline":
        return _PrefixedPipeline(self._r.pipeline(transaction=transaction), self._prefix)

    async def aclose(self):
        pass  # shared client — caller manages lifecycle


class _PrefixedPipeline:
    """Pipeline proxy that applies the same key prefix to queued commands."""

    def __init__(self, pipe, prefix: str):
        self._pipe = pipe
        self._prefix = prefix

    def _k(self, key: str) -> str:
        return f"{self._prefix}:{key}"

    def zadd(self, key: str, mapping: dict, **kwargs):
        self._pipe.zadd(self._k(key), mapping, **kwargs)
        return self

    def zremrangebyscore(self, key: str, min, max):
        self._pipe.zremrangebyscore(self._k(key), min, max)
        return self

    def zcard(self, key: str):
        self._pipe.zcard(self._k(key))
        return self

    def expire(self, key: str, seconds: int):
        self._pipe.expire(self._k(key), seconds)
        return self

    async def execute(self):
        return await self._pipe.execute()

    async def __aenter__(self):
        await self._pipe.__aenter__()
        return self

    async def __aexit__(self, *args):
        return await self._pipe.__aexit__(*args)


@pytest.fixture
def test_redis():
    """
    Real async Redis client pointed at DB 1 (test database).

    Each test gets a unique prefix; the DB is flushed at teardown.
    The fixture yields a (_PrefixedRedis, raw_client) tuple so tests
    can inspect keys directly via the raw client if needed.

    Why sync fixture + asyncio.run() for teardown (mirrors setup_schema)
    ────────────────────────────────────────────────────────────────────
    pytest-asyncio runs async fixture teardown via plugin.py::finalizer →
    runner.run(async_finalizer()), which creates a BRAND-NEW event loop
    (loop B).  The `raw` Redis client lazily establishes connections during
    the test in loop A (the per-test loop pytest-asyncio manages).  When
    teardown runs in loop B, the asyncio connection layer sees those
    sockets as belonging to a dead loop: asyncio.open_connection() raises
    CancelledError, the pool retries, times out, and surfaces as:

        redis.exceptions.TimeoutError: Timeout connecting to server

    This is the exact same "stranded connection" problem that NullPool +
    asyncio.run() solved for asyncpg.  The fix is identical: make the
    fixture synchronous so teardown runs in a plain Python call frame, then
    use asyncio.run() with a FRESH Redis client that has no prior
    connections.  The fresh client owns its TCP connection from birth to
    close inside one self-contained loop — no stranding possible.

    `raw` itself (loop-A connections) is left for GC; the pool connections
    are closed when the OS reclaims the sockets after loop A exits.
    """
    raw = _aioredis.from_url(TEST_REDIS_URL, encoding="utf-8", decode_responses=True)
    prefix = str(_uuid.uuid4())
    prefixed = _PrefixedRedis(raw, prefix)
    yield prefixed, raw

    # Teardown: fresh client in its own isolated loop — never shares
    # connections with loop A, so no CancelledError / TimeoutError.
    async def _flush():
        client = _aioredis.from_url(TEST_REDIS_URL, encoding="utf-8", decode_responses=True)
        try:
            await client.flushdb()
        finally:
            await client.aclose()

    asyncio.run(_flush())


@pytest_asyncio.fixture
async def client_with_redis(db_session, test_redis):
    """
    ASGI client + DB override + real prefixed Redis injected.

    The prefixed Redis is patched in at the router and service level so
    every key written during the test carries the unique namespace prefix.
    Tests can pre-seed the store via the `test_redis` fixture and the
    router will see those entries through the same prefixed client.
    """
    prefixed_redis, raw_redis = test_redis

    async def _override_get_db():
        yield db_session

    app.dependency_overrides[get_db] = _override_get_db

    with (
        patch("app.routers.ledger.redis_client", prefixed_redis),
        patch("app.services.idempotency.redis_client", prefixed_redis, create=True),
        patch("app.services.fraud.redis_client", prefixed_redis, create=True),
    ):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            yield ac, prefixed_redis

    app.dependency_overrides.clear()

