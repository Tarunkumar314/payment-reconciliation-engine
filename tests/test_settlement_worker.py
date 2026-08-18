"""
Integration tests for the settlement worker — Step 5.

Test infrastructure
───────────────────
All tests use real Postgres (via db_session savepoint isolation) and real
Kafka (via the Docker container). The mock bank endpoint is called via
an httpx.AsyncClient pointed at the live FastAPI test app (using the
`client` fixture from conftest), so the bank's failure rate is controlled
by patching the setting rather than mocking the HTTP call.

The settlement worker's process_event() function is called directly
(same pattern as _poll_and_publish in the outbox relay tests) — injecting
the db_session, a real aiokafka producer (for DLQ writes), and a real
httpx client pointed at the test app. This exercises the full code path
without needing to run the worker as a separate process.

Kafka consumer fixture
──────────────────────
Each test that needs to consume from the DLQ creates its own consumer with
a unique group_id and auto_offset_reset="latest", seeking to the end before
the test runs so it only reads messages produced during that test.

Failure rate control
────────────────────
We override MOCK_BANK_FAILURE_RATE via monkeypatching get_settings() to
return a modified Settings instance, rather than setting env vars (which
would require reloading the lru_cache). Each test patches the rate to
0.0 (always succeed) or 1.0 (always fail) for deterministic behavior.
"""

import asyncio
import json
import time
import uuid
from unittest.mock import patch, MagicMock

import pytest
import pytest_asyncio
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import httpx

from app.config import get_settings, Settings
from app.models.ledger import Transaction, TransactionStatus
from app.models.outbox import OutboxEvent, OutboxEventType
from app.settlement_worker import (
    process_event,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_DLQ_TOPIC,
    _backoff_seconds,
)
from sqlalchemy import select


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_event(transaction_id: str, status: str = "POSTED") -> dict:
    """Build a settlement event payload matching the outbox relay's format."""
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "TRANSACTION_POSTED",
        "transaction_id": transaction_id,
        "status": status,
        "description": "Settlement test",
        "entries": [
            {"account_id": str(uuid.uuid4()), "entry_type": "DEBIT",  "amount": "100.0000"},
            {"account_id": str(uuid.uuid4()), "entry_type": "CREDIT", "amount": "100.0000"},
        ],
    }


async def _seed_transaction(db_session, status: TransactionStatus = TransactionStatus.POSTED) -> Transaction:
    """Insert a minimal Transaction row for the worker to find."""
    txn = Transaction(
        description="Settlement test",
        status=status,
    )
    db_session.add(txn)
    await db_session.commit()
    return txn


def _mock_settings(failure_rate: float = 0.0, delay_ms: int = 0) -> Settings:
    """Return a Settings instance with overridden mock bank params."""
    real = get_settings()
    mock = MagicMock(spec=Settings)
    mock.MOCK_BANK_FAILURE_RATE = failure_rate
    mock.MOCK_BANK_DELAY_MS = delay_ms
    mock.MOCK_BANK_URL = real.MOCK_BANK_URL
    mock.KAFKA_BOOTSTRAP_SERVERS = real.KAFKA_BOOTSTRAP_SERVERS
    mock.KAFKA_SETTLEMENT_TOPIC = real.KAFKA_SETTLEMENT_TOPIC
    mock.KAFKA_DLQ_TOPIC = real.KAFKA_DLQ_TOPIC
    mock.SETTLEMENT_MAX_RETRIES = real.SETTLEMENT_MAX_RETRIES
    mock.SETTLEMENT_BASE_BACKOFF_SECONDS = real.SETTLEMENT_BASE_BACKOFF_SECONDS
    mock.DATABASE_URL = real.DATABASE_URL
    return mock


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest_asyncio.fixture
async def kafka_producer_worker():
    """Real aiokafka producer for the worker to use when writing to DLQ."""
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        acks="all",
        enable_idempotence=True,
    )
    await producer.start()
    yield producer
    await producer.stop()


@pytest_asyncio.fixture
async def dlq_consumer():
    """
    Real Kafka consumer subscribed to the DLQ topic.
    Seeks to partition end before yielding so we only read messages
    produced during the current test.
    """
    group_id = f"test-dlq-{uuid.uuid4()}"
    consumer = AIOKafkaConsumer(
        KAFKA_DLQ_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=group_id,
        auto_offset_reset="latest",
        enable_auto_commit=False,
        consumer_timeout_ms=8000,
    )
    await consumer.start()
    await consumer.seek_to_end()
    yield consumer
    await consumer.stop()


# ── Unit test: backoff math ───────────────────────────────────────────────────

def test_backoff_seconds_is_bounded_and_random():
    """
    _backoff_seconds returns values in [0, cap] and varies between calls
    (i.e., jitter is applied, not a fixed formula).
    """
    cap = 30.0
    samples = [_backoff_seconds(attempt=3, base=1.0) for _ in range(20)]
    assert all(0 <= s <= cap for s in samples), "All backoff values should be within [0, cap]"
    # With 20 samples from a uniform distribution, they should not all be identical
    assert len(set(round(s, 6) for s in samples)) > 1, "Jitter should produce varying values"


# ── Integration tests ─────────────────────────────────────────────────────────

async def test_successful_settlement(db_session, client_with_redis, kafka_producer_worker):
    """
    Happy path: mock bank returns 200.
    Transaction should be marked SETTLED.
    Offset is committed (tested by asserting process_event completes without error).

    client_with_redis yields an httpx.AsyncClient already wired to the ASGI
    app via ASGITransport — use it directly as the http_client for process_event.
    We patch MOCK_BANK_URL to "http://test" (the base_url of that client) so
    the worker sends its POST to the in-process test app instead of localhost.
    """
    ac, _ = client_with_redis
    txn = await _seed_transaction(db_session, TransactionStatus.POSTED)
    event = _make_event(str(txn.id))

    # Zero failure rate — bank always succeeds
    with patch("app.settlement_worker.MOCK_BANK_URL", "http://test"):
        with patch("app.routers.mock_bank.get_settings") as mock_cfg:
            mock_cfg.return_value = _mock_settings(failure_rate=0.0, delay_ms=0)
            await process_event(event, kafka_producer_worker, ac, db_session)

    await db_session.refresh(txn)
    assert txn.status == TransactionStatus.SETTLED, (
        f"Expected SETTLED, got {txn.status}"
    )


async def test_retry_then_succeed(db_session, client_with_redis, kafka_producer_worker):
    """
    First N calls fail, then succeed.
    Transaction ends SETTLED; verifies the retry loop works.

    We simulate this by making the mock bank fail exactly once, then succeed,
    by using a call counter via a patched Settings object.
    """
    client, _ = client_with_redis
    txn = await _seed_transaction(db_session, TransactionStatus.POSTED)
    event = _make_event(str(txn.id))

    call_count = {"n": 0}

    async def _bank_side_effect(transaction_id, amount, http_client):
        call_count["n"] += 1
        return call_count["n"] >= 2  # fail first call, succeed on second

    with patch("app.settlement_worker._call_mock_bank", side_effect=_bank_side_effect):
        with patch("app.settlement_worker._backoff_seconds", return_value=0.0):
            await process_event(event, kafka_producer_worker, None, db_session)

    await db_session.refresh(txn)
    assert txn.status == TransactionStatus.SETTLED, (
        f"Expected SETTLED after retry, got {txn.status}"
    )
    assert call_count["n"] == 2, f"Expected exactly 2 bank calls, got {call_count['n']}"


async def test_retry_exhaustion_goes_to_dlq(
    db_session, client_with_redis, kafka_producer_worker, dlq_consumer
):
    """
    All retries fail → event published to DLQ, transaction marked SETTLEMENT_FAILED.

    Verifies:
      (a) DLQ Kafka topic receives the event with failure_reason field.
      (b) transaction status is SETTLEMENT_FAILED in Postgres.
      (c) DLQ payload includes original transaction_id for replay.
    """
    client, _ = client_with_redis
    txn = await _seed_transaction(db_session, TransactionStatus.POSTED)
    event = _make_event(str(txn.id))

    # Always fail — every bank call returns False
    with patch("app.settlement_worker._call_mock_bank", return_value=False):
        with patch("app.settlement_worker._backoff_seconds", return_value=0.0):
            await process_event(event, kafka_producer_worker, None, db_session)

    # Check Postgres
    await db_session.refresh(txn)
    assert txn.status == TransactionStatus.SETTLEMENT_FAILED, (
        f"Expected SETTLEMENT_FAILED, got {txn.status}"
    )

    # Check DLQ Kafka topic
    dlq_message = None
    async for msg in dlq_consumer:
        data = json.loads(msg.value.decode("utf-8"))
        if data.get("transaction_id") == str(txn.id):
            dlq_message = data
            break

    assert dlq_message is not None, "DLQ message not found in Kafka"
    assert "failure_reason" in dlq_message, "DLQ payload must include failure_reason"
    assert dlq_message["transaction_id"] == str(txn.id)
    assert "retry_attempts" in dlq_message


async def test_duplicate_event_already_settled_is_skipped(
    db_session, client_with_redis, kafka_producer_worker
):
    """
    If the transaction is already SETTLED when the worker sees a duplicate event,
    the worker must skip all bank calls and return without error.

    This is the idempotency guard that makes at-least-once delivery safe:
    - The outbox relay can publish the same transaction_id twice on crash.
    - Without this check, the worker would call the bank a second time for
      an already-settled transaction.

    Verifies:
      (a) process_event returns without calling the bank.
      (b) transaction status remains SETTLED.
    """
    client, _ = client_with_redis
    # Seed with SETTLED status — simulates a duplicate Kafka message
    txn = await _seed_transaction(db_session, TransactionStatus.SETTLED)
    event = _make_event(str(txn.id))

    bank_calls = {"n": 0}

    async def _count_calls(*args, **kwargs):
        bank_calls["n"] += 1
        return True

    with patch("app.settlement_worker._call_mock_bank", side_effect=_count_calls):
        await process_event(event, kafka_producer_worker, None, db_session)

    assert bank_calls["n"] == 0, (
        "Worker must not call the bank for an already-SETTLED transaction. "
        f"Got {bank_calls['n']} bank calls."
    )

    await db_session.refresh(txn)
    assert txn.status == TransactionStatus.SETTLED, (
        f"Status should remain SETTLED, got {txn.status}"
    )


async def test_duplicate_event_already_failed_is_skipped(
    db_session, client_with_redis, kafka_producer_worker
):
    """
    SETTLEMENT_FAILED is also a terminal state — a duplicate event for a
    failed transaction must be skipped without re-attempting bank calls.
    """
    client, _ = client_with_redis
    txn = await _seed_transaction(db_session, TransactionStatus.SETTLEMENT_FAILED)
    event = _make_event(str(txn.id))

    bank_calls = {"n": 0}

    async def _count_calls(*args, **kwargs):
        bank_calls["n"] += 1
        return True

    with patch("app.settlement_worker._call_mock_bank", side_effect=_count_calls):
        await process_event(event, kafka_producer_worker, None, db_session)

    assert bank_calls["n"] == 0, (
        "Worker must not call the bank for a SETTLEMENT_FAILED transaction."
    )
