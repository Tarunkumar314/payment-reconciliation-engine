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
from httpx import AsyncClient, ASGITransport
from sqlalchemy.pool import NullPool
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from dotenv import load_dotenv

from app.main import app
from app.database import get_db
from app.models.base import Base
from app.models import Account, Transaction, LedgerEntry  # noqa: F401 — ensures ORM metadata is registered

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
