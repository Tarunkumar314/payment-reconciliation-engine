"""
Mock bank endpoint — POST /mock-bank/settle

Purpose
───────
This endpoint simulates a downstream bank's settlement API.  It exists
purely so the settlement worker has something realistic to exercise:
  - Configurable random failure rate (MOCK_BANK_FAILURE_RATE env var, default 20%)
  - Artificial response delay (MOCK_BANK_DELAY_MS env var, default 100ms)

Both parameters are set to extreme values in tests:
  - MOCK_BANK_FAILURE_RATE=1.0  → always fail (exercises retry/DLQ path)
  - MOCK_BANK_FAILURE_RATE=0.0  → never fail (exercises happy path)
  - MOCK_BANK_DELAY_MS=0        → no delay in tests for speed

Why it lives in the FastAPI app rather than as a separate service:
  - Shares the same process in dev/test — no extra container needed.
  - The settlement worker calls it via HTTP (httpx), same as it would call
    a real bank, so the interface contract is identical.
  - In production, MOCK_BANK_URL would point to a real bank API and this
    router would not be mounted.

Request body: { "transaction_id": "<uuid>", "amount": "<decimal>" }
Response 200: { "status": "accepted", "reference": "<uuid>" }
Response 503: { "detail": "Bank temporarily unavailable" }
"""

import asyncio
import random
import uuid

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel

from app.config import get_settings

router = APIRouter(prefix="/mock-bank", tags=["Mock Bank"])


class BankSettleRequest(BaseModel):
    transaction_id: str
    amount: str


class BankSettleResponse(BaseModel):
    status: str
    reference: str


@router.post(
    "/settle",
    response_model=BankSettleResponse,
    summary="Simulate bank settlement (configurable failure rate)",
)
async def mock_bank_settle(payload: BankSettleRequest) -> BankSettleResponse:
    settings = get_settings()

    # Simulate network/processing latency
    if settings.MOCK_BANK_DELAY_MS > 0:
        await asyncio.sleep(settings.MOCK_BANK_DELAY_MS / 1000)

    # Simulate random bank-side failure
    if random.random() < settings.MOCK_BANK_FAILURE_RATE:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Bank temporarily unavailable",
        )

    return BankSettleResponse(
        status="accepted",
        reference=str(uuid.uuid4()),
    )
