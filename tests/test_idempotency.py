"""
Integration tests for idempotency on POST /transactions.

Key behaviours verified:
  1. First request with a key → 201, transaction written to ledger.
  2. Second request with the same key → 200, same response body, ledger
     NOT touched (entry count stays at 2, no duplicate rows).
  3. Request without a key → always 201, always creates a new transaction.
  4. Different key on same payload → 201, creates a second distinct
     transaction (idempotency scope is key, not payload hash).
"""

import json
import uuid
from decimal import Decimal
from httpx import AsyncClient
import pytest


# Helper to build a balanced transaction payload for two account IDs.
def _tx_payload(acc1_id: str, acc2_id: str, description: str = "Idempotency test") -> dict:
    return {
        "description": description,
        "entries": [
            {"account_id": acc1_id, "entry_type": "DEBIT",  "amount": "50.0000"},
            {"account_id": acc2_id, "entry_type": "CREDIT", "amount": "50.0000"},
        ],
    }


async def _create_accounts(client: AsyncClient) -> tuple[str, str]:
    """Create two accounts and return their IDs."""
    r1 = await client.post("/accounts", json={"name": "Wallet A", "account_type": "ASSET", "currency": "USD"})
    assert r1.status_code == 201
    r2 = await client.post("/accounts", json={"name": "Revenue B", "account_type": "REVENUE", "currency": "USD"})
    assert r2.status_code == 201
    return r1.json()["id"], r2.json()["id"]


async def test_idempotency_first_request_creates_transaction(client_with_redis):
    """
    A request with a fresh Idempotency-Key must create a new transaction
    and respond with 201.
    """
    client, _ = client_with_redis
    acc1, acc2 = await _create_accounts(client)
    key = str(uuid.uuid4())

    resp = await client.post(
        "/transactions",
        json=_tx_payload(acc1, acc2),
        headers={"Idempotency-Key": key},
    )
    assert resp.status_code == 201
    data = resp.json()
    assert data["status"] == "POSTED"
    assert len(data["entries"]) == 2


async def test_idempotency_duplicate_key_returns_cached_response(client_with_redis):
    """
    A second request with the *same* Idempotency-Key must:
      - Return HTTP 200 (replay, not new resource)
      - Return the exact same JSON body (same transaction ID, same entries)
      - NOT write a second transaction to the ledger
    """
    client, _ = client_with_redis
    acc1, acc2 = await _create_accounts(client)
    key = str(uuid.uuid4())
    payload = _tx_payload(acc1, acc2)

    # First request
    resp1 = await client.post("/transactions", json=payload, headers={"Idempotency-Key": key})
    assert resp1.status_code == 201
    body1 = resp1.json()

    # Second request — same key, same payload
    resp2 = await client.post("/transactions", json=payload, headers={"Idempotency-Key": key})
    assert resp2.status_code == 200, "Replay must return 200, not 201"
    body2 = resp2.json()

    # Response bodies must be identical (same transaction ID, same amounts)
    assert body1["id"] == body2["id"], "Replayed transaction ID must match original"
    assert body1["entries"] == body2["entries"]

    # Ledger must have exactly 2 entries (not 4 from a duplicate write)
    balance_resp = await client.get(f"/accounts/{acc1}/balance")
    balance = Decimal(balance_resp.json()["balance"])
    assert balance == Decimal("50.0000"), (
        f"Expected balance $50 (one transaction), got ${balance} — "
        "duplicate ledger write occurred"
    )


async def test_idempotency_different_key_creates_new_transaction(client_with_redis):
    """
    Two requests with *different* Idempotency-Keys must produce two
    distinct transaction records, even if the payload is identical.
    """
    client, _ = client_with_redis
    acc1, acc2 = await _create_accounts(client)
    payload = _tx_payload(acc1, acc2)

    resp1 = await client.post("/transactions", json=payload, headers={"Idempotency-Key": str(uuid.uuid4())})
    resp2 = await client.post("/transactions", json=payload, headers={"Idempotency-Key": str(uuid.uuid4())})

    assert resp1.status_code == 201
    assert resp2.status_code == 201
    assert resp1.json()["id"] != resp2.json()["id"], "Different keys must produce different transactions"

    # Two transactions × $50 = $100 balance
    balance_resp = await client.get(f"/accounts/{acc1}/balance")
    assert Decimal(balance_resp.json()["balance"]) == Decimal("100.0000")


async def test_idempotency_no_key_always_creates_new_transaction(client_with_redis):
    """
    Requests *without* an Idempotency-Key are non-idempotent by definition.
    Each call must produce a fresh transaction regardless of payload equality.
    """
    client, _ = client_with_redis
    acc1, acc2 = await _create_accounts(client)
    payload = _tx_payload(acc1, acc2)

    resp1 = await client.post("/transactions", json=payload)
    resp2 = await client.post("/transactions", json=payload)

    assert resp1.status_code == 201
    assert resp2.status_code == 201
    assert resp1.json()["id"] != resp2.json()["id"]


async def test_idempotency_key_is_scoped_to_24h(client_with_redis):
    """
    Verifies the key is stored in Redis after a successful commit.
    We inspect the fake Redis store directly rather than waiting 24 hours.
    """
    client, redis = client_with_redis
    acc1, acc2 = await _create_accounts(client)
    key = str(uuid.uuid4())

    resp = await client.post(
        "/transactions",
        json=_tx_payload(acc1, acc2),
        headers={"Idempotency-Key": key},
    )
    assert resp.status_code == 201

    # Key must now exist in Redis
    cached = await redis.get(f"idempotency:{key}")
    assert cached is not None, "Key should be stored in Redis after commit"

    # TTL must be set (> 0) and ≤ 86400 s
    ttl = await redis.ttl(f"idempotency:{key}")
    assert 0 < ttl <= 86_400, f"Expected TTL in (0, 86400], got {ttl}"
