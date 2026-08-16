"""
Integration tests for the ledger API endpoints.

These tests run against a real PostgreSQL database (reconcile_test_db),
giving accurate enforcement of:
  - NUMERIC(18,4) precision
  - ENUM type constraints
  - UUID foreign key integrity
  - Savepoint / nested transaction semantics

Fixtures (db_session, client) are defined in conftest.py.
Each test is fully isolated: all writes are rolled back after the test.
"""

from decimal import Decimal
from httpx import AsyncClient


async def test_create_account_and_post_transaction(client: AsyncClient):
    """
    Happy path: two accounts created, one balanced double-entry transaction
    posted, balances verified from raw ledger entry sums.
    """
    # 1. Create an ASSET account (normal balance = debits - credits)
    resp1 = await client.post("/accounts", json={
        "name": "User Wallet",
        "account_type": "ASSET",
        "currency": "USD"
    })
    assert resp1.status_code == 201, resp1.text
    acc1 = resp1.json()

    # 2. Create a REVENUE account (normal balance = credits - debits)
    resp2 = await client.post("/accounts", json={
        "name": "Merchant Account",
        "account_type": "REVENUE",
        "currency": "USD"
    })
    assert resp2.status_code == 201, resp2.text
    acc2 = resp2.json()

    # 3. Post a balanced double-entry transaction:
    #      DEBIT  User Wallet     $100 (money leaves the asset account)
    #      CREDIT Merchant Account $100 (revenue recognised)
    tx_payload = {
        "description": "Payment for order #1",
        "entries": [
            {"account_id": acc1["id"], "entry_type": "DEBIT",  "amount": "100.0000"},
            {"account_id": acc2["id"], "entry_type": "CREDIT", "amount": "100.0000"},
        ]
    }
    tx_resp = await client.post("/transactions", json=tx_payload)
    assert tx_resp.status_code == 201, tx_resp.text
    tx_data = tx_resp.json()
    assert tx_data["description"] == "Payment for order #1"
    assert len(tx_data["entries"]) == 2
    assert tx_data["status"] == "POSTED"

    # 4. Verify calculated balances
    bal1_resp = await client.get(f"/accounts/{acc1['id']}/balance")
    assert bal1_resp.status_code == 200
    assert Decimal(bal1_resp.json()["balance"]) == Decimal("100.0000")

    bal2_resp = await client.get(f"/accounts/{acc2['id']}/balance")
    assert bal2_resp.status_code == 200
    assert Decimal(bal2_resp.json()["balance"]) == Decimal("100.0000")


async def test_post_unbalanced_transaction_rejected(client: AsyncClient):
    """
    Pydantic rejects unbalanced transactions at the API boundary (422).
    The DB is never touched.
    """
    resp1 = await client.post("/accounts", json={
        "name": "Wallet A", "account_type": "ASSET", "currency": "USD"
    })
    acc1 = resp1.json()

    resp2 = await client.post("/accounts", json={
        "name": "Wallet B", "account_type": "ASSET", "currency": "USD"
    })
    acc2 = resp2.json()

    tx_resp = await client.post("/transactions", json={
        "description": "Unbalanced Payment",
        "entries": [
            {"account_id": acc1["id"], "entry_type": "DEBIT",  "amount": "100.00"},
            {"account_id": acc2["id"], "entry_type": "CREDIT", "amount": "50.00"},
        ]
    })
    assert tx_resp.status_code == 422
    assert "Unbalanced double-entry transaction" in tx_resp.text


async def test_transaction_with_currency_mismatch_rejected(client: AsyncClient):
    """
    Router rejects transactions that span accounts with different currencies (400).
    Balance enforcement is the DB layer; currency guard is the business layer.
    """
    resp1 = await client.post("/accounts", json={
        "name": "USD Wallet", "account_type": "ASSET", "currency": "USD"
    })
    acc1 = resp1.json()

    resp2 = await client.post("/accounts", json={
        "name": "EUR Wallet", "account_type": "ASSET", "currency": "EUR"
    })
    acc2 = resp2.json()

    tx_resp = await client.post("/transactions", json={
        "description": "Cross Currency Error",
        "entries": [
            {"account_id": acc1["id"], "entry_type": "DEBIT",  "amount": "100.00"},
            {"account_id": acc2["id"], "entry_type": "CREDIT", "amount": "100.00"},
        ]
    })
    assert tx_resp.status_code == 400
    assert "All accounts in a transaction must use the same currency" in tx_resp.json()["detail"]


async def test_transaction_references_nonexistent_account(client: AsyncClient):
    """
    Router returns 404 when an entry references an account ID that does not
    exist in the DB — not just a Pydantic error, a real DB lookup failure.
    """
    import uuid
    fake_id = str(uuid.uuid4())
    real_resp = await client.post("/accounts", json={
        "name": "Real Account", "account_type": "ASSET", "currency": "USD"
    })
    real_id = real_resp.json()["id"]

    tx_resp = await client.post("/transactions", json={
        "description": "Missing account",
        "entries": [
            {"account_id": real_id, "entry_type": "DEBIT",  "amount": "50.00"},
            {"account_id": fake_id, "entry_type": "CREDIT", "amount": "50.00"},
        ]
    })
    assert tx_resp.status_code == 404
    assert fake_id in tx_resp.json()["detail"]


async def test_isolation_between_tests(client: AsyncClient):
    """
    Confirms that data written in one test does not leak into another.
    If rollback isolation works correctly, GET /accounts returns exactly
    the accounts created within this test — not those from prior tests.
    """
    # Create one account
    await client.post("/accounts", json={
        "name": "Isolation Check Account", "account_type": "ASSET", "currency": "USD"
    })

    list_resp = await client.get("/accounts")
    assert list_resp.status_code == 200
    accounts = list_resp.json()

    # Should see exactly 1 account — those from other tests were rolled back
    assert len(accounts) == 1
    assert accounts[0]["name"] == "Isolation Check Account"
