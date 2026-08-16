"""
Integration tests for the transactional outbox — Step 4.

Scope
─────
These tests verify the atomic write contract that makes the outbox pattern
correct:
  1. Every successful POST /transactions inserts exactly one OutboxEvent row.
  2. That row is inserted in the SAME database transaction as the ledger
     entries — if the ledger write commits, the outbox row exists; they are
     never split across separate commits.
  3. published_at is NULL on insert (the relay hasn't run yet).
  4. The payload is self-contained: it includes transaction_id, status,
     description, and all entries so the consumer (Step 5) never needs to
     re-query the ledger.
  5. event_type correctly reflects the transaction status:
       POSTED         → TRANSACTION_POSTED
       HELD_FOR_REVIEW → TRANSACTION_HELD
  6. No outbox row is written for requests that short-circuit before the
     ledger write (idempotency replay, validation errors).

What we do NOT test here
────────────────────────
- The relay polling loop (tested manually or in a separate integration suite
  that requires a live Kafka broker).
- Kafka delivery or message format (Step 5 scope).
- The at-least-once duplicate scenario (requires relay process to crash
  mid-cycle — not feasible in a unit/integration test).
"""

import time
import uuid
from decimal import Decimal

from sqlalchemy import select
from app.models.outbox import OutboxEvent, OutboxEventType
from app.services.fraud import VELOCITY_MAX_COUNT


# ── Helpers ───────────────────────────────────────────────────────────────────

def _tx_payload(acc1_id: str, acc2_id: str, amount: str = "75.0000") -> dict:
    return {
        "description": "Outbox test transaction",
        "entries": [
            {"account_id": acc1_id, "entry_type": "DEBIT",  "amount": amount},
            {"account_id": acc2_id, "entry_type": "CREDIT", "amount": amount},
        ],
    }


async def _create_accounts(client) -> tuple[str, str]:
    r1 = await client.post("/accounts", json={"name": "Src", "account_type": "ASSET",   "currency": "USD"})
    assert r1.status_code == 201
    r2 = await client.post("/accounts", json={"name": "Dst", "account_type": "REVENUE", "currency": "USD"})
    assert r2.status_code == 201
    return r1.json()["id"], r2.json()["id"]


# ── Tests ─────────────────────────────────────────────────────────────────────

async def test_outbox_row_created_on_successful_transaction(client_with_redis, db_session):
    """
    A successful POST /transactions must produce exactly one OutboxEvent row
    in the same DB transaction (and therefore visible to the same db_session
    once the savepoint releases).
    """
    client, _ = client_with_redis
    acc1, acc2 = await _create_accounts(client)

    resp = await client.post("/transactions", json=_tx_payload(acc1, acc2))
    assert resp.status_code == 201

    tx_id = resp.json()["id"]

    # Query through the same db_session that the router used — savepoint isolation
    # guarantees this session sees exactly what the router committed.
    result = await db_session.execute(
        select(OutboxEvent).where(OutboxEvent.transaction_id == uuid.UUID(tx_id))
    )
    events = result.scalars().all()

    assert len(events) == 1, f"Expected 1 outbox event, found {len(events)}"


async def test_outbox_published_at_is_null_on_insert(client_with_redis, db_session):
    """
    published_at must be NULL on insert — the relay hasn't run yet.
    If this were non-null at insert time, the relay would skip the row
    and the event would never be delivered to Kafka.
    """
    client, _ = client_with_redis
    acc1, acc2 = await _create_accounts(client)

    resp = await client.post("/transactions", json=_tx_payload(acc1, acc2))
    assert resp.status_code == 201

    tx_id = resp.json()["id"]
    result = await db_session.execute(
        select(OutboxEvent).where(OutboxEvent.transaction_id == uuid.UUID(tx_id))
    )
    event = result.scalar_one()

    assert event.published_at is None, (
        f"published_at should be NULL on insert, got {event.published_at}"
    )


async def test_outbox_payload_is_self_contained(client_with_redis, db_session):
    """
    The outbox payload must contain all information the consumer needs:
    transaction_id, status, description, and entries with account_id,
    entry_type, and amount.

    A self-contained payload means the consumer never needs to re-query the
    ledger — the event is the source of truth for downstream processing.
    """
    client, _ = client_with_redis
    acc1, acc2 = await _create_accounts(client)

    resp = await client.post("/transactions", json=_tx_payload(acc1, acc2, amount="123.4500"))
    assert resp.status_code == 201
    tx_data = resp.json()
    tx_id = tx_data["id"]

    result = await db_session.execute(
        select(OutboxEvent).where(OutboxEvent.transaction_id == uuid.UUID(tx_id))
    )
    event = result.scalar_one()
    payload = event.payload

    # Top-level fields
    assert payload["transaction_id"] == tx_id
    assert payload["status"] == "POSTED"
    assert payload["description"] == "Outbox test transaction"

    # Entries
    assert len(payload["entries"]) == 2
    amounts = {e["entry_type"]: Decimal(e["amount"]) for e in payload["entries"]}
    assert amounts["DEBIT"] == Decimal("123.4500")
    assert amounts["CREDIT"] == Decimal("123.4500")

    account_ids_in_payload = {e["account_id"] for e in payload["entries"]}
    assert acc1 in account_ids_in_payload
    assert acc2 in account_ids_in_payload


async def test_outbox_event_type_posted_for_normal_transaction(client_with_redis, db_session):
    """event_type is TRANSACTION_POSTED when the transaction is within velocity threshold."""
    client, _ = client_with_redis
    acc1, acc2 = await _create_accounts(client)

    resp = await client.post("/transactions", json=_tx_payload(acc1, acc2))
    assert resp.status_code == 201
    assert resp.json()["status"] == "POSTED"

    result = await db_session.execute(
        select(OutboxEvent).where(
            OutboxEvent.transaction_id == uuid.UUID(resp.json()["id"])
        )
    )
    event = result.scalar_one()
    assert event.event_type == OutboxEventType.TRANSACTION_POSTED


async def test_outbox_event_type_held_for_velocity_breach(client_with_redis, db_session):
    """event_type is TRANSACTION_HELD when the velocity threshold is exceeded."""
    client, redis = client_with_redis
    acc1, acc2 = await _create_accounts(client)

    # Pre-seed the sorted set to simulate being over threshold
    now_ms = int(time.time() * 1000)
    key = f"velocity:{acc1}"
    for i in range(VELOCITY_MAX_COUNT + 1):
        await redis.zadd(key, {f"synthetic-{i}": now_ms - i})

    resp = await client.post("/transactions", json=_tx_payload(acc1, acc2))
    assert resp.status_code == 201
    assert resp.json()["status"] == "HELD_FOR_REVIEW"

    result = await db_session.execute(
        select(OutboxEvent).where(
            OutboxEvent.transaction_id == uuid.UUID(resp.json()["id"])
        )
    )
    event = result.scalar_one()
    assert event.event_type == OutboxEventType.TRANSACTION_HELD
    assert event.payload["status"] == "HELD_FOR_REVIEW"


async def test_outbox_idempotency_replay_does_not_create_new_row(client_with_redis, db_session):
    """
    Replaying a request with a known Idempotency-Key must NOT insert a new
    outbox row — the idempotency guard short-circuits before the ledger write,
    so no new event is emitted.

    This verifies that the outbox and the idempotency guard compose correctly:
    a retried-but-idempotent request does not double-publish to Kafka.
    """
    client, _ = client_with_redis
    acc1, acc2 = await _create_accounts(client)
    key = str(uuid.uuid4())
    payload = _tx_payload(acc1, acc2)

    # First request — creates the transaction and the outbox row
    resp1 = await client.post("/transactions", json=payload, headers={"Idempotency-Key": key})
    assert resp1.status_code == 201
    tx_id = resp1.json()["id"]

    # Second request — should replay from Redis cache, no new DB write
    resp2 = await client.post("/transactions", json=payload, headers={"Idempotency-Key": key})
    assert resp2.status_code == 200  # replay

    # Still only ONE outbox row for this transaction
    result = await db_session.execute(
        select(OutboxEvent).where(OutboxEvent.transaction_id == uuid.UUID(tx_id))
    )
    events = result.scalars().all()
    assert len(events) == 1, (
        f"Idempotency replay must not create a second outbox row. Found {len(events)}."
    )


async def test_outbox_no_row_on_validation_failure(client_with_redis, db_session):
    """
    A transaction rejected at validation (missing account, unbalanced entries)
    must not create any outbox row — the router raises before the write block.
    """
    client, _ = client_with_redis
    fake_id = str(uuid.uuid4())

    # Reference a non-existent account — triggers 404 before the write block
    resp = await client.post("/transactions", json={
        "description": "Should fail",
        "entries": [
            {"account_id": fake_id, "entry_type": "DEBIT",  "amount": "50.00"},
            {"account_id": str(uuid.uuid4()), "entry_type": "CREDIT", "amount": "50.00"},
        ],
    })
    assert resp.status_code == 404

    result = await db_session.execute(select(OutboxEvent))
    all_events = result.scalars().all()
    assert len(all_events) == 0, (
        f"No outbox rows should exist after a validation failure. Found {len(all_events)}."
    )
