"""
Integration tests for fraud velocity checking on POST /transactions.

Key behaviours verified:
  1. Transactions within the threshold → POSTED status.
  2. Transactions exceeding the threshold → HELD_FOR_REVIEW status,
     but still written to the ledger (audit trail preserved).
  3. A HELD_FOR_REVIEW transaction still produces correct ledger entries
     (balances are updated — the hold is a status flag, not a rejection).
  4. The velocity window is per-account: two *different* accounts each
     staying under threshold should both be POSTED.
  5. Unit-level: sorted set correctly prunes old entries (rolling window).
"""

import time
import uuid
from decimal import Decimal
import pytest
from httpx import AsyncClient

from app.services.fraud import check_velocity, VELOCITY_MAX_COUNT, VELOCITY_WINDOW_SECONDS


# ── Helper ────────────────────────────────────────────────────────────────────

def _tx_payload(acc1_id: str, acc2_id: str) -> dict:
    return {
        "description": "Velocity test transaction",
        "entries": [
            {"account_id": acc1_id, "entry_type": "DEBIT",  "amount": "10.0000"},
            {"account_id": acc2_id, "entry_type": "CREDIT", "amount": "10.0000"},
        ],
    }


async def _create_accounts(client: AsyncClient) -> tuple[str, str]:
    r1 = await client.post("/accounts", json={"name": "Source", "account_type": "ASSET", "currency": "USD"})
    assert r1.status_code == 201
    r2 = await client.post("/accounts", json={"name": "Dest", "account_type": "REVENUE", "currency": "USD"})
    assert r2.status_code == 201
    return r1.json()["id"], r2.json()["id"]


# ── API-level tests ───────────────────────────────────────────────────────────

async def test_transactions_within_threshold_are_posted(client_with_redis):
    """
    N transactions within the velocity window (N ≤ VELOCITY_MAX_COUNT)
    must all be written as POSTED.
    """
    client, _ = client_with_redis
    acc1, acc2 = await _create_accounts(client)
    payload = _tx_payload(acc1, acc2)

    for i in range(VELOCITY_MAX_COUNT):
        resp = await client.post("/transactions", json=payload)
        assert resp.status_code == 201, f"Transaction {i+1} failed: {resp.text}"
        assert resp.json()["status"] == "POSTED", (
            f"Transaction {i+1} should be POSTED but got {resp.json()['status']}"
        )


async def test_transaction_exceeding_threshold_is_held(client_with_redis):
    """
    The (VELOCITY_MAX_COUNT + 1)-th transaction in the window must be
    written as HELD_FOR_REVIEW, not POSTED or rejected.

    Crucially, the ledger entry must still exist — the hold is a status flag,
    not a rejection.  We verify this by checking that the balance reflects
    ALL (N+1) transactions.
    """
    client, _ = client_with_redis
    acc1, acc2 = await _create_accounts(client)
    payload = _tx_payload(acc1, acc2)

    # Fill up to threshold
    for _ in range(VELOCITY_MAX_COUNT):
        resp = await client.post("/transactions", json=payload)
        assert resp.status_code == 201

    # The next one should be held
    resp = await client.post("/transactions", json=payload)
    assert resp.status_code == 201, "HELD_FOR_REVIEW is still written — expect 201"
    assert resp.json()["status"] == "HELD_FOR_REVIEW", (
        f"Expected HELD_FOR_REVIEW, got {resp.json()['status']}"
    )

    # Balance should reflect N+1 transactions × $10 = $(N+1)*10
    balance_resp = await client.get(f"/accounts/{acc1}/balance")
    expected_balance = Decimal("10.0000") * (VELOCITY_MAX_COUNT + 1)
    actual_balance = Decimal(balance_resp.json()["balance"])
    assert actual_balance == expected_balance, (
        f"HELD_FOR_REVIEW transaction was not written to the ledger. "
        f"Expected {expected_balance}, got {actual_balance}"
    )


async def test_held_transaction_has_correct_entries(client_with_redis):
    """
    A HELD_FOR_REVIEW transaction must have both ledger entries (DEBIT +
    CREDIT) present in the response, with correct amounts.
    """
    client, redis = client_with_redis
    acc1, acc2 = await _create_accounts(client)

    # Pre-seed the sorted set to simulate being over threshold
    # We insert VELOCITY_MAX_COUNT + 1 synthetic entries with current timestamps
    now_ms = int(time.time() * 1000)
    key = f"velocity:{acc1}"
    for i in range(VELOCITY_MAX_COUNT + 1):
        await redis.zadd(key, {f"synthetic-{i}": now_ms - i})

    payload = _tx_payload(acc1, acc2)
    resp = await client.post("/transactions", json=payload)
    assert resp.status_code == 201
    data = resp.json()

    assert data["status"] == "HELD_FOR_REVIEW"
    assert len(data["entries"]) == 2

    amounts = {e["entry_type"]: Decimal(e["amount"]) for e in data["entries"]}
    assert amounts["DEBIT"] == Decimal("10.0000")
    assert amounts["CREDIT"] == Decimal("10.0000")


async def test_velocity_is_per_account_not_global(client_with_redis):
    """
    Velocity limits apply per-account.  Two independent account pairs
    each sending N transactions must both stay POSTED; the counts don't
    bleed across account pairs.
    """
    client, _ = client_with_redis

    # Pair 1
    r1 = await client.post("/accounts", json={"name": "A1", "account_type": "ASSET", "currency": "USD"})
    r2 = await client.post("/accounts", json={"name": "A2", "account_type": "REVENUE", "currency": "USD"})
    acc1a, acc1b = r1.json()["id"], r2.json()["id"]

    # Pair 2
    r3 = await client.post("/accounts", json={"name": "B1", "account_type": "ASSET", "currency": "USD"})
    r4 = await client.post("/accounts", json={"name": "B2", "account_type": "REVENUE", "currency": "USD"})
    acc2a, acc2b = r3.json()["id"], r4.json()["id"]

    for _ in range(VELOCITY_MAX_COUNT):
        resp1 = await client.post("/transactions", json=_tx_payload(acc1a, acc1b))
        resp2 = await client.post("/transactions", json=_tx_payload(acc2a, acc2b))
        assert resp1.json()["status"] == "POSTED"
        assert resp2.json()["status"] == "POSTED"


# ── Unit-level tests for the sorted-set rolling window ────────────────────────

async def test_sorted_set_rolling_window_evicts_old_entries(test_redis):
    """
    Directly verifies that check_velocity() uses a rolling window, not a
    fixed window anchored to the first event.

    Strategy: pre-seed the sorted set with entries that are just outside the
    window (older than VELOCITY_WINDOW_SECONDS ago).  These should be evicted
    before the count is taken, leaving only 0 recent entries.  Adding one new
    entry via check_velocity() should therefore produce count=1, well below
    VELOCITY_MAX_COUNT → returns False (not over threshold).

    Both the test's zadd() and check_velocity()'s internal zadd() go through
    the same _PrefixedRedis instance, so they operate on the same key space
    without leaking into other tests.
    """
    prefixed_redis, _ = test_redis
    account_id = str(uuid.uuid4())
    key = f"velocity:{account_id}"

    # Insert VELOCITY_MAX_COUNT entries with timestamps from 2 windows ago
    old_timestamp_ms = int(time.time() * 1000) - (VELOCITY_WINDOW_SECONDS * 2 * 1000)
    for i in range(VELOCITY_MAX_COUNT):
        await prefixed_redis.zadd(key, {f"old-entry-{i}": old_timestamp_ms - i})

    # Now call check_velocity — old entries should be pruned
    exceeded = await check_velocity(prefixed_redis, account_id)
    assert not exceeded, (
        "Old entries (outside rolling window) should have been evicted. "
        "Velocity check incorrectly flagged as exceeded."
    )

    # Confirm only 1 entry remains (the one just added by check_velocity)
    count = await prefixed_redis.zcard(key)
    assert count == 1, f"Expected 1 entry after eviction, found {count}"


async def test_sorted_set_counts_only_recent_entries(test_redis):
    """
    Pre-seed with VELOCITY_MAX_COUNT - 1 recent entries (within window).
    check_velocity() adds one more → total = VELOCITY_MAX_COUNT → not exceeded.
    One more call → total = VELOCITY_MAX_COUNT + 1 → exceeded.
    """
    prefixed_redis, _ = test_redis
    account_id = str(uuid.uuid4())
    key = f"velocity:{account_id}"

    now_ms = int(time.time() * 1000)
    for i in range(VELOCITY_MAX_COUNT - 1):
        await prefixed_redis.zadd(key, {f"recent-{i}": now_ms - i * 100})  # 100 ms apart, all in window

    # N-1 pre-seeded + 1 from check_velocity = N → not exceeded
    result1 = await check_velocity(prefixed_redis, account_id)
    assert not result1, f"At threshold boundary (count={VELOCITY_MAX_COUNT}), should not be exceeded"

    # N + 1 from this second call → exceeded
    result2 = await check_velocity(prefixed_redis, account_id)
    assert result2, f"Over threshold (count={VELOCITY_MAX_COUNT + 1}), should be exceeded"
