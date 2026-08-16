import uuid
from datetime import datetime
from decimal import Decimal
from typing import List, Optional
from pydantic import BaseModel, Field, ConfigDict, model_validator

from app.models.ledger import AccountType, EntryType, TransactionStatus


class AccountCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=100, json_schema_extra={"example": "User Cash Wallet"})
    account_type: AccountType = Field(..., json_schema_extra={"example": AccountType.ASSET})
    currency: str = Field(default="USD", min_length=3, max_length=3, json_schema_extra={"example": "USD"})


class AccountResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    name: str
    account_type: AccountType
    currency: str
    created_at: datetime


class LedgerEntryCreate(BaseModel):
    account_id: uuid.UUID
    entry_type: EntryType
    amount: Decimal = Field(..., gt=Decimal("0"), json_schema_extra={"example": "100.0000"})


class LedgerEntryResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    transaction_id: uuid.UUID
    account_id: uuid.UUID
    entry_type: EntryType
    amount: Decimal
    created_at: datetime


class TransactionCreate(BaseModel):
    description: Optional[str] = Field(None, max_length=255, json_schema_extra={"example": "Payment for order #1001"})
    entries: List[LedgerEntryCreate]

    @model_validator(mode="after")
    def validate_double_entry_balance(self) -> "TransactionCreate":
        if len(self.entries) < 2:
            raise ValueError("A double-entry transaction must contain at least 2 entries.")

        total_debits = sum(
            entry.amount for entry in self.entries if entry.entry_type == EntryType.DEBIT
        )
        total_credits = sum(
            entry.amount for entry in self.entries if entry.entry_type == EntryType.CREDIT
        )

        if total_debits != total_credits:
            raise ValueError(
                f"Unbalanced double-entry transaction: Total DEBIT ({total_debits}) "
                f"does not equal Total CREDIT ({total_credits})."
            )

        return self


class TransactionResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    description: Optional[str]
    status: TransactionStatus
    created_at: datetime
    entries: List[LedgerEntryResponse]


class AccountBalanceResponse(BaseModel):
    account_id: uuid.UUID
    account_name: str
    currency: str
    balance: Decimal
