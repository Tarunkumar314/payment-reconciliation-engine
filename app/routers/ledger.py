import uuid
from decimal import Decimal
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Header, Response, status
from fastapi.encoders import jsonable_encoder
import json
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.database import get_db
from app.models.ledger import Account, Transaction, LedgerEntry, AccountType, EntryType, TransactionStatus
from app.redis_client import redis_client
from app.schemas import (
    AccountCreate,
    AccountResponse,
    AccountBalanceResponse,
    TransactionCreate,
    TransactionResponse,
)
from app.services.idempotency import get_cached_response, store_response
from app.services.fraud import check_velocity

router = APIRouter(tags=["Ledger"])


@router.post(
    "/accounts",
    response_model=AccountResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a ledger account"
)
async def create_account(
    account_in: AccountCreate,
    db: AsyncSession = Depends(get_db)
):
    account = Account(
        name=account_in.name,
        account_type=account_in.account_type,
        currency=account_in.currency.upper()
    )
    db.add(account)
    await db.commit()
    await db.refresh(account)
    return account


@router.get(
    "/accounts",
    response_model=List[AccountResponse],
    summary="List all ledger accounts"
)
async def list_accounts(
    db: AsyncSession = Depends(get_db)
):
    result = await db.execute(select(Account).order_by(Account.created_at.desc()))
    return result.scalars().all()


@router.get(
    "/accounts/{account_id}/balance",
    response_model=AccountBalanceResponse,
    summary="Get calculated account balance from ledger entries"
)
async def get_account_balance(
    account_id: uuid.UUID,
    db: AsyncSession = Depends(get_db)
):
    account = await db.get(Account, account_id)
    if not account:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Account with ID '{account_id}' not found."
        )

    # Calculate sum of debits and sum of credits
    debit_query = select(func.coalesce(func.sum(LedgerEntry.amount), Decimal("0"))).where(
        LedgerEntry.account_id == account_id,
        LedgerEntry.entry_type == EntryType.DEBIT
    )
    credit_query = select(func.coalesce(func.sum(LedgerEntry.amount), Decimal("0"))).where(
        LedgerEntry.account_id == account_id,
        LedgerEntry.entry_type == EntryType.CREDIT
    )

    debit_result = await db.execute(debit_query)
    credit_result = await db.execute(credit_query)

    total_debits: Decimal = debit_result.scalar_one()
    total_credits: Decimal = credit_result.scalar_one()

    # Asset & Expense accounts have a normal DEBIT balance (Debits - Credits)
    # Liability, Equity, Revenue accounts have a normal CREDIT balance (Credits - Debits)
    if account.account_type in (AccountType.ASSET, AccountType.EXPENSE):
        balance = total_debits - total_credits
    else:
        balance = total_credits - total_debits

    return AccountBalanceResponse(
        account_id=account.id,
        account_name=account.name,
        currency=account.currency,
        balance=balance
    )


@router.post(
    "/transactions",
    response_model=TransactionResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Post a balanced double-entry transaction",
    description=(
        "Accepts an optional **Idempotency-Key** header. "
        "If provided and the key has been seen within the last 24 hours, "
        "the previously cached response is returned without touching the ledger. "
        "A fraud velocity check (rolling 60-second window per account) "
        "may write the transaction as HELD_FOR_REVIEW instead of POSTED."
    ),
)
async def create_transaction(
    tx_in: TransactionCreate,
    db: AsyncSession = Depends(get_db),
    idempotency_key: Optional[str] = Header(
        default=None,
        alias="Idempotency-Key",
        description="Client-generated unique key (UUID recommended). Requests with the same key within 24 h return the cached response.",
    ),
):
    # ── 1. Idempotency check (before any DB work) ─────────────────────────────
    # If the caller supplied a key and we have a cached response, return it
    # immediately.  The ledger is not touched, the status code is re-sent as
    # 200 (not 201) to signal "replayed response, not a new resource".
    if idempotency_key:
        cached = await get_cached_response(redis_client, idempotency_key)
        if cached is not None:
            return Response(
                content=cached,
                status_code=status.HTTP_200_OK,
                media_type="application/json",
            )

    # ── 2. Account validation ─────────────────────────────────────────────────
    account_ids = list({entry.account_id for entry in tx_in.entries})

    result = await db.execute(select(Account).where(Account.id.in_(account_ids)))
    existing_accounts = {acc.id: acc for acc in result.scalars().all()}

    missing_account_ids = [str(aid) for aid in account_ids if aid not in existing_accounts]
    if missing_account_ids:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"The following account IDs were not found: {', '.join(missing_account_ids)}"
        )

    currencies = {existing_accounts[aid].currency for aid in account_ids}
    if len(currencies) > 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"All accounts in a transaction must use the same currency. Found currencies: {list(currencies)}"
        )

    # ── 3. Fraud velocity check (before ledger write) ─────────────────────────
    # We check velocity against each *unique* account involved in this
    # transaction.  If *any* account is over threshold the whole transaction
    # is held — a multi-account split cannot circumvent the limit.
    #
    # check_velocity() records this request in the sorted set BEFORE we know
    # whether the DB write will succeed.  If the DB write later fails and the
    # caller retries (without an idempotency key), we may count the attempt
    # twice.  This is intentional: we prefer false positives (over-counting)
    # over false negatives (under-counting) in a fraud context.
    held = False
    for aid in account_ids:
        if await check_velocity(redis_client, str(aid)):
            held = True
            break  # one account over threshold is enough; no need to check others

    tx_status = TransactionStatus.HELD_FOR_REVIEW if held else TransactionStatus.POSTED

    # ── 4. Atomic ledger write ────────────────────────────────────────────────
    async with db.begin_nested() if db.in_transaction() else db.begin():
        transaction = Transaction(
            description=tx_in.description,
            status=tx_status,
        )
        db.add(transaction)
        await db.flush()  # Generates transaction.id

        for entry_in in tx_in.entries:
            ledger_entry = LedgerEntry(
                transaction_id=transaction.id,
                account_id=entry_in.account_id,
                entry_type=entry_in.entry_type,
                amount=entry_in.amount,
            )
            db.add(ledger_entry)

    # ── 5. Build response ─────────────────────────────────────────────────────
    result = await db.execute(
        select(Transaction)
        .options(selectinload(Transaction.entries))
        .where(Transaction.id == transaction.id)
    )
    saved_tx = result.scalar_one()

    # Serialise via Pydantic so the cached copy is byte-for-byte identical
    # to what FastAPI would have sent, including decimal precision.
    response_data = TransactionResponse.model_validate(saved_tx)
    response_json = json.dumps(jsonable_encoder(response_data))

    # ── 6. Store idempotency key AFTER successful commit ─────────────────────
    # Storing before commit risks caching a response for a transaction that
    # never actually landed in the ledger (e.g. DB error between SET and commit).
    if idempotency_key:
        await store_response(redis_client, idempotency_key, response_json)

    return Response(
        content=response_json,
        status_code=status.HTTP_201_CREATED,
        media_type="application/json",
    )

