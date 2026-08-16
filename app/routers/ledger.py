import uuid
from decimal import Decimal
from typing import List
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.database import get_db
from app.models.ledger import Account, Transaction, LedgerEntry, AccountType, EntryType, TransactionStatus
from app.schemas import (
    AccountCreate,
    AccountResponse,
    AccountBalanceResponse,
    TransactionCreate,
    TransactionResponse,
)

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
    summary="Post a balanced double-entry transaction"
)
async def create_transaction(
    tx_in: TransactionCreate,
    db: AsyncSession = Depends(get_db)
):
    # Extract unique account IDs participating in this transaction
    account_ids = list({entry.account_id for entry in tx_in.entries})

    # Fetch accounts from DB to verify existence and currency match
    result = await db.execute(select(Account).where(Account.id.in_(account_ids)))
    existing_accounts = {acc.id: acc for acc in result.scalars().all()}

    missing_account_ids = [str(aid) for aid in account_ids if aid not in existing_accounts]
    if missing_account_ids:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"The following account IDs were not found: {', '.join(missing_account_ids)}"
        )

    # Validate multi-currency consistency across entries
    currencies = {existing_accounts[aid].currency for aid in account_ids}
    if len(currencies) > 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"All accounts in a transaction must use the same currency. Found currencies: {list(currencies)}"
        )

    # Atomic insertion inside a transaction block
    async with db.begin_nested() if db.in_transaction() else db.begin():
        transaction = Transaction(
            description=tx_in.description,
            status=TransactionStatus.POSTED
        )
        db.add(transaction)
        await db.flush()  # Generates transaction.id

        for entry_in in tx_in.entries:
            ledger_entry = LedgerEntry(
                transaction_id=transaction.id,
                account_id=entry_in.account_id,
                entry_type=entry_in.entry_type,
                amount=entry_in.amount
            )
            db.add(ledger_entry)

    # Re-fetch transaction with entries eagerly loaded for response serialization
    result = await db.execute(
        select(Transaction)
        .options(selectinload(Transaction.entries))
        .where(Transaction.id == transaction.id)
    )
    saved_tx = result.scalar_one()

    return saved_tx
