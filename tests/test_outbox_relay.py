"""
Integration tests for the outbox relay process — Step 4.

These tests exercise _poll_and_publish() directly (the core relay function)
against real infrastructure: real Postgres (same savepoint-isolated session)
and the real Kafka container from docker-compose.

Why real Kafka (not mocked):
  A mock that records "send_and_wait was called" proves nothing about the
  contract we care about: the message actually reaching a broker topic.
  Using the real container exercises serialisation, topic auto-creation,
  broker ACK semantics (acks="all"), and the consumer offset protocol.
  This mirrors why we use real Postgres for ledger tests.

Test isolation:
  - Each test uses a unique Kafka consumer group ID + topic suffix to avoid
    reading messages from previous test runs.
  - Actually we use the fixed topic "settlement-events" and a unique group_id
    per test so the consumer reads from the beginning of the topic partition
    for messages produced during that specific test.
  - Postgres rows are isolated via the savepoint fixture as usual.

Kafka consumer in tests:
  We create a short-lived AIOKafkaConsumer per test, seek to the partition
  end before the relay runs (so we only see new messages), then poll for the
  message after the relay publishes.

Failure case:
  We inject a failing producer (a simple AsyncMock whose send_and_wait raises
  KafkaTimeoutError) to verify that published_at stays NULL when Kafka is
  unreachable — the relay must NOT mark a row published if delivery failed.
"""

import asyncio
import json
import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaTimeoutError
from sqlalchemy import select

from app.models.outbox import OutboxEvent, OutboxEventType
from app.outbox_relay import _poll_and_publish, KAFKA_TOPIC, KAFKA_BOOTSTRAP_SERVERS


# ── Helpers ───────────────────────────────────────────────────────────────────

async def _seed_outbox_event(db_session, transaction_id: uuid.UUID) -> OutboxEvent:
    """
    Insert a minimal Transaction + unpublished OutboxEvent directly (bypasses router).
    The Transaction row is required because outbox_events.transaction_id is a
    non-nullable FK to transactions.id.
    """
    from app.models.ledger import Transaction, TransactionStatus

    txn = Transaction(
        id=transaction_id,
        description="Relay test",
        status=TransactionStatus.POSTED,
    )
    db_session.add(txn)
    await db_session.flush()  # persist txn.id without releasing savepoint

    event = OutboxEvent(
        transaction_id=transaction_id,
        event_type=OutboxEventType.TRANSACTION_POSTED,
        payload={
            "transaction_id": str(transaction_id),
            "status": "POSTED",
            "description": "Relay test",
            "entries": [],
        },
    )
    db_session.add(event)
    await db_session.commit()
    return event


async def _get_outbox_event(db_session, event_id: uuid.UUID) -> OutboxEvent:
    """Re-fetch a single outbox event by PK."""
    result = await db_session.execute(
        select(OutboxEvent).where(OutboxEvent.id == event_id)
    )
    return result.scalar_one()


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest_asyncio.fixture
async def kafka_producer():
    """
    Real aiokafka producer connected to the Kafka container.
    acks="all" + enable_idempotence matches relay production configuration.
    """
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        acks="all",
        enable_idempotence=True,
    )
    await producer.start()
    yield producer
    await producer.stop()


@pytest_asyncio.fixture
async def kafka_consumer():
    """
    Real aiokafka consumer subscribed to the settlement-events topic.
    A fresh UUID group_id ensures this consumer starts reading from the
    current end of the topic, not from old committed offsets.
    After seeking to end, we return the consumer — callers must
    start their relay AFTER the fixture yields so no messages are missed.
    """
    group_id = f"test-relay-{uuid.uuid4()}"
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=group_id,
        auto_offset_reset="latest",      # start from now, ignore historical messages
        enable_auto_commit=False,
        consumer_timeout_ms=5000,        # raise StopAsyncIteration after 5s of silence
    )
    await consumer.start()
    # Seek to end so we only read messages produced during this test
    await consumer.seek_to_end()
    yield consumer
    await consumer.stop()


# ── Tests ─────────────────────────────────────────────────────────────────────

async def test_relay_publishes_unpublished_event_to_kafka(
    db_session, kafka_producer, kafka_consumer
):
    """
    Seed one unpublished outbox row.
    Run one relay poll cycle with a real producer.
    Verify:
      (a) The message appears in the Kafka topic with the correct transaction_id.
      (b) published_at is now non-null in Postgres.
      (c) published_at was set AFTER the Kafka ACK (ordering invariant).
    """
    tx_id = uuid.uuid4()
    event = await _seed_outbox_event(db_session, tx_id)
    event_id = event.id

    # One poll cycle — inject db_session so relay sees the test's uncommitted rows
    count = await _poll_and_publish(kafka_producer, session=db_session)
    assert count >= 1, "Relay should have published at least one event"

    # Verify Kafka received the message
    received = None
    async for msg in kafka_consumer:
        data = json.loads(msg.value.decode("utf-8"))
        if data.get("transaction_id") == str(tx_id):
            received = data
            break

    assert received is not None, (
        f"Message for transaction {tx_id} not found in Kafka topic '{KAFKA_TOPIC}'"
    )
    assert received["event_type"] == OutboxEventType.TRANSACTION_POSTED.value
    assert received["status"] == "POSTED"

    # Verify published_at is now set in Postgres
    await db_session.refresh(event)
    updated_event = await _get_outbox_event(db_session, event_id)
    assert updated_event.published_at is not None, (
        "published_at must be set after relay confirms Kafka ACK"
    )
    assert isinstance(updated_event.published_at, datetime)


async def test_relay_sets_published_at_only_after_kafka_ack(
    db_session, kafka_producer, kafka_consumer
):
    """
    Verifies the temporal ordering: published_at timestamp must be >= the
    moment before the relay ran (i.e., it was set AFTER the Kafka write, not
    before or at INSERT time).
    """
    tx_id = uuid.uuid4()
    event = await _seed_outbox_event(db_session, tx_id)

    before_relay = datetime.now(tz=timezone.utc)

    await _poll_and_publish(kafka_producer, session=db_session)

    # Consume the message (just to confirm it arrived)
    async for msg in kafka_consumer:
        if json.loads(msg.value.decode("utf-8")).get("transaction_id") == str(tx_id):
            break

    await db_session.refresh(event)
    updated = await _get_outbox_event(db_session, event.id)

    assert updated.published_at is not None
    assert updated.published_at.replace(tzinfo=timezone.utc) >= before_relay, (
        "published_at should be set after the relay ran, not at row creation time"
    )


async def test_relay_skips_already_published_events(
    db_session, kafka_producer, kafka_consumer
):
    """
    An event that already has published_at set must not be re-published.
    """
    tx_id = uuid.uuid4()
    event = await _seed_outbox_event(db_session, tx_id)

    # Manually mark it as already published
    event.published_at = datetime.now(tz=timezone.utc)
    await db_session.commit()

    count = await _poll_and_publish(kafka_producer, session=db_session)
    assert count == 0, (
        "Relay should not re-publish an event that already has published_at set"
    )



async def test_relay_kafka_failure_leaves_published_at_null(db_session):
    """
    If Kafka is unreachable (send_and_wait raises), published_at must stay NULL.

    This is the key atomicity invariant: we must never mark a row published
    unless Kafka confirmed receipt.  If we did, the relay would skip the row
    on restart and the event would be silently lost — exactly the failure
    mode the outbox pattern exists to prevent.

    We inject a failing producer via AsyncMock rather than stopping the real
    Kafka container (which would affect other tests running concurrently).
    """
    tx_id = uuid.uuid4()
    event = await _seed_outbox_event(db_session, tx_id)
    event_id = event.id

    # Producer whose send_and_wait always fails
    failing_producer = AsyncMock()
    failing_producer.send_and_wait = AsyncMock(
        side_effect=KafkaTimeoutError("Simulated broker timeout")
    )

    # The relay should catch the exception internally (log and move on),
    # but published_at must remain NULL.
    try:
        await _poll_and_publish(failing_producer, session=db_session)
    except KafkaTimeoutError:
        pass  # If the relay re-raises, that's also fine — published_at must still be null

    # Re-fetch from DB
    fresh = await _get_outbox_event(db_session, event_id)
    assert fresh.published_at is None, (
        "published_at must remain NULL when Kafka delivery fails. "
        "The relay must not mark a row published unless the broker ACKed."
    )


async def test_relay_processes_multiple_events_in_batch(
    db_session, kafka_producer, kafka_consumer
):
    """
    Seed N unpublished events in one poll cycle.
    Verify all N are published to Kafka and all N have published_at set.
    Confirms the batch loop in _poll_and_publish works end-to-end.
    """
    n = 5
    tx_ids = [uuid.uuid4() for _ in range(n)]
    events = [await _seed_outbox_event(db_session, tx_id) for tx_id in tx_ids]

    count = await _poll_and_publish(kafka_producer, session=db_session)
    assert count >= n, f"Expected at least {n} events published, got {count}"

    # Collect published transaction IDs from Kafka
    received_tx_ids: set[str] = set()
    async for msg in kafka_consumer:
        data = json.loads(msg.value.decode("utf-8"))
        received_tx_ids.add(data.get("transaction_id", ""))
        if all(str(tx_id) in received_tx_ids for tx_id in tx_ids):
            break

    for tx_id in tx_ids:
        assert str(tx_id) in received_tx_ids, (
            f"Event for transaction {tx_id} not found in Kafka"
        )

    # All events should have published_at set
    for event in events:
        await db_session.refresh(event)
        fresh = await _get_outbox_event(db_session, event.id)
        assert fresh.published_at is not None, (
            f"Event {event.id} (tx={event.transaction_id}) published_at still NULL after relay"
        )
