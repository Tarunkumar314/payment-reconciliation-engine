import uuid
from decimal import Decimal
import pytest
from pydantic import ValidationError

from app.schemas import TransactionCreate, LedgerEntryCreate
from app.models.ledger import EntryType, AccountType


def test_transaction_schema_balanced():
    acc1 = uuid.uuid4()
    acc2 = uuid.uuid4()

    tx = TransactionCreate(
        description="Transfer $50",
        entries=[
            LedgerEntryCreate(account_id=acc1, entry_type=EntryType.DEBIT, amount=Decimal("50.00")),
            LedgerEntryCreate(account_id=acc2, entry_type=EntryType.CREDIT, amount=Decimal("50.00")),
        ]
    )
    assert len(tx.entries) == 2


def test_transaction_schema_unbalanced_raises_validation_error():
    acc1 = uuid.uuid4()
    acc2 = uuid.uuid4()

    with pytest.raises(ValidationError) as exc_info:
        TransactionCreate(
            description="Unbalanced Transfer",
            entries=[
                LedgerEntryCreate(account_id=acc1, entry_type=EntryType.DEBIT, amount=Decimal("100.00")),
                LedgerEntryCreate(account_id=acc2, entry_type=EntryType.CREDIT, amount=Decimal("50.00")),
            ]
        )
    assert "Unbalanced double-entry transaction" in str(exc_info.value)


def test_transaction_schema_insufficient_entries_raises_validation_error():
    acc1 = uuid.uuid4()

    with pytest.raises(ValidationError) as exc_info:
        TransactionCreate(
            description="Single Entry",
            entries=[
                LedgerEntryCreate(account_id=acc1, entry_type=EntryType.DEBIT, amount=Decimal("100.00")),
            ]
        )
    assert "must contain at least 2 entries" in str(exc_info.value)
