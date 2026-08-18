"""
Settlement worker — Kafka consumer on the settlement-events topic.

Architecture
────────────
Standalone async process, run as: python -m app.settlement_worker

Consumes from the 'settlement-events' topic (produced by the outbox relay).
For each event, calls the mock bank endpoint (POST /mock-bank/settle).
On success: marks the transaction SETTLED in Postgres, commits the Kafka offset.
On failure: retries with exponential backoff + jitter, up to SETTLEMENT_MAX_RETRIES.
After exhausting retries: publishes to 'settlement-events-dlq', marks
  the transaction SETTLEMENT_FAILED, commits the Kafka offset.

Delivery guarantee: at-least-once (mirrors the relay)
──────────────────────────────────────────────────────
Kafka delivers at-least-once, not exactly-once. The relay (Step 4) can
publish the same transaction_id more than once if it crashes between the
Kafka ACK and the Postgres UPDATE of published_at.

The worker handles duplicates by checking transaction status before calling
the bank:
  - SETTLED          → skip, commit offset (already done)
  - SETTLEMENT_FAILED → skip, commit offset (already terminal, sits in DLQ)
  - anything else    → proceed with settlement

This check is the idempotency guard for the worker. Without it, a duplicated
Kafka message would call the bank twice for the same transaction — potentially
double-settling, or consuming retry budget on work that already succeeded.

Offset commit strategy: manual, at terminal state only
───────────────────────────────────────────────────────
enable_auto_commit=False. The offset is committed ONLY when the event reaches
a terminal state:
  - SETTLED: immediately after the DB write
  - DLQ'd + SETTLEMENT_FAILED: after both the DLQ publish and the DB write

What breaks with auto-commit:
  The consumer group commits offsets on a timer (~5 seconds by default).
  If the worker crashes after auto-commit but before the DB write, the offset
  has advanced — the message will never be redelivered. The transaction stays
  in POSTED status indefinitely, silently un-settled.
  With manual commit, a crash before the commit means the message is
  redelivered on restart. The status check above deduplicates it if the DB
  write already succeeded.

Exponential backoff + jitter
─────────────────────────────
Each retry waits: clamp(2^attempt × base, 0, max_cap) × rand(0.5, 1.5)

Why jitter specifically:
  Backoff alone (1s, 2s, 4s...) reduces load on the bank but preserves
  correlation. If N workers all fail at the same moment (e.g. after a
  partition rebalance or a thundering herd), they all retry on the same
  schedule and hammer the bank in synchronized waves.
  Jitter breaks the correlation: each worker picks a different delay from
  within the backoff window, spreading the retry load uniformly over time.
  The difference is between "smaller spikes at predictable intervals" and
  "smooth constant load" — only jitter achieves the second.

DLQ
───
After SETTLEMENT_MAX_RETRIES failures, the event is published to
'settlement-events-dlq' with the original payload plus a 'failure_reason'
field. Ops tooling can subscribe to the DLQ topic for alerting without
polling the DB. The full payload enables replay: once the bank recovers,
a script can re-publish DLQ events to 'settlement-events' to reprocess them.
"""

import asyncio
import json
import logging
import os
import random
import signal
import sys
from datetime import datetime, timezone

import httpx
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker

from app.config import get_settings
from app.models.ledger import Transaction, TransactionStatus

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("settlement_worker")

# ── Configuration ─────────────────────────────────────────────────────────────

_settings = get_settings()

DATABASE_URL: str = _settings.DATABASE_URL
KAFKA_BOOTSTRAP_SERVERS: str = _settings.KAFKA_BOOTSTRAP_SERVERS
KAFKA_SETTLEMENT_TOPIC: str = _settings.KAFKA_SETTLEMENT_TOPIC
KAFKA_DLQ_TOPIC: str = _settings.KAFKA_DLQ_TOPIC
MOCK_BANK_URL: str = _settings.MOCK_BANK_URL
SETTLEMENT_MAX_RETRIES: int = _settings.SETTLEMENT_MAX_RETRIES
SETTLEMENT_BASE_BACKOFF_SECONDS: float = _settings.SETTLEMENT_BASE_BACKOFF_SECONDS
CONSUMER_GROUP_ID: str = os.environ.get("SETTLEMENT_CONSUMER_GROUP", "settlement-worker-group")

# ── Database ──────────────────────────────────────────────────────────────────

engine = create_async_engine(DATABASE_URL, pool_size=2, max_overflow=0, echo=False)
SessionLocal = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

# ── Graceful shutdown ─────────────────────────────────────────────────────────

_shutdown_event = asyncio.Event()


def _handle_signal(sig, frame):
    log.info("Received signal %s — initiating graceful shutdown", sig)
    _shutdown_event.set()


# ── Backoff helper ────────────────────────────────────────────────────────────

def _backoff_seconds(attempt: int, base: float = SETTLEMENT_BASE_BACKOFF_SECONDS) -> float:
    """
    Exponential backoff with full jitter.

    Formula: rand(0, min(cap, base * 2^attempt))
    Cap is 30 seconds — prevents unbounded waits on later retries.
    Full jitter (rand from 0 to max) gives the best load distribution
    across a fleet of workers retrying simultaneously.
    """
    cap = 30.0
    ceiling = min(cap, base * (2 ** attempt))
    return random.uniform(0, ceiling)


# ── Core settlement logic ─────────────────────────────────────────────────────

async def _get_transaction_status(session: AsyncSession, transaction_id: str) -> TransactionStatus | None:
    """Return the current status of a transaction, or None if not found."""
    result = await session.execute(
        select(Transaction.status).where(Transaction.id == transaction_id)
    )
    row = result.scalar_one_or_none()
    return row


async def _mark_transaction(
    session: AsyncSession,
    transaction_id: str,
    new_status: TransactionStatus,
) -> None:
    """Update transaction status. Commits immediately."""
    await session.execute(
        update(Transaction)
        .where(Transaction.id == transaction_id)
        .values(status=new_status)
    )
    await session.commit()


async def _call_mock_bank(transaction_id: str, amount: str, http_client: httpx.AsyncClient) -> bool:
    """
    POST to the mock bank endpoint.
    Returns True on success (HTTP 200), False on any failure.
    """
    try:
        resp = await http_client.post(
            f"{MOCK_BANK_URL}/mock-bank/settle",
            json={"transaction_id": transaction_id, "amount": amount},
            timeout=10.0,
        )
        return resp.status_code == 200
    except Exception as exc:
        log.warning("Mock bank call raised: %s", exc)
        return False


async def process_event(
    event_data: dict,
    producer: AIOKafkaProducer,
    http_client: httpx.AsyncClient,
    session: AsyncSession,
) -> None:
    """
    Process one settlement event end-to-end.

    Steps:
      1. Parse transaction_id and amount from payload.
      2. Check current DB status — skip if already terminal.
      3. Retry loop: call mock bank with exponential backoff + jitter.
      4a. Success → mark SETTLED.
      4b. Exhausted → publish to DLQ, mark SETTLEMENT_FAILED.

    This function is called by the consumer loop. The caller commits the
    Kafka offset only AFTER this function returns successfully.

    Raises on unexpected errors so the caller can decide whether to
    retry or DLQ.
    """
    transaction_id: str = event_data.get("transaction_id", "")
    # Amount: sum entries or use total if present; fall back to "0"
    entries = event_data.get("entries", [])
    total_amount = str(
        sum(
            float(e["amount"])
            for e in entries
            if e.get("entry_type") == "DEBIT"
        ) or 0
    )

    # ── Step 2: Idempotency check ──────────────────────────────────────────────
    # At-least-once delivery means we may see this transaction_id more than once.
    # If it's already in a terminal state, skip all bank calls and return.
    current_status = await _get_transaction_status(session, transaction_id)

    if current_status is None:
        log.warning("Transaction %s not found in DB — skipping", transaction_id)
        return

    TERMINAL = {TransactionStatus.SETTLED, TransactionStatus.SETTLEMENT_FAILED}
    if current_status in TERMINAL:
        log.info(
            "Transaction %s already in terminal state %s — skipping duplicate event",
            transaction_id, current_status.value,
        )
        return

    # ── Step 3: Retry loop with exponential backoff + jitter ──────────────────
    last_error: str = "unknown"
    for attempt in range(SETTLEMENT_MAX_RETRIES + 1):
        if attempt > 0:
            delay = _backoff_seconds(attempt - 1)
            log.info(
                "Retry %d/%d for tx %s — waiting %.2fs",
                attempt, SETTLEMENT_MAX_RETRIES, transaction_id, delay,
            )
            await asyncio.sleep(delay)

        success = await _call_mock_bank(transaction_id, total_amount, http_client)

        if success:
            # ── Step 4a: Success ───────────────────────────────────────────────
            await _mark_transaction(session, transaction_id, TransactionStatus.SETTLED)
            log.info("Transaction %s SETTLED after %d attempt(s)", transaction_id, attempt + 1)
            return

        last_error = f"Mock bank returned failure on attempt {attempt + 1}"

    # ── Step 4b: Exhausted retries → DLQ ──────────────────────────────────────
    log.warning(
        "Transaction %s exhausted %d retries — publishing to DLQ",
        transaction_id, SETTLEMENT_MAX_RETRIES,
    )

    dlq_payload = json.dumps({
        **event_data,
        "failure_reason": last_error,
        "failed_at": datetime.now(tz=timezone.utc).isoformat(),
        "retry_attempts": SETTLEMENT_MAX_RETRIES + 1,
    }).encode("utf-8")

    await producer.send_and_wait(
        KAFKA_DLQ_TOPIC,
        value=dlq_payload,
        key=transaction_id.encode("utf-8"),
    )

    await _mark_transaction(session, transaction_id, TransactionStatus.SETTLEMENT_FAILED)
    log.info("Transaction %s marked SETTLEMENT_FAILED", transaction_id)


# ── Consumer loop ─────────────────────────────────────────────────────────────

async def run_worker(
    consumer: AIOKafkaConsumer | None = None,
    producer: AIOKafkaProducer | None = None,
    session: AsyncSession | None = None,
) -> None:
    """
    Main consumer loop.

    Parameters are optional — when None, the worker creates its own
    real connections.  When provided (tests), the caller's fixtures are
    used, giving full control over Kafka, DB state, and session isolation.

    enable_auto_commit=False: offsets are committed ONLY after the event
    reaches a terminal state (SETTLED or DLQ'd + SETTLEMENT_FAILED).
    See module docstring for why auto-commit breaks this use case.
    """
    _owns_resources = consumer is None
    http_client: httpx.AsyncClient | None = None

    if _owns_resources:
        consumer = AIOKafkaConsumer(
            KAFKA_SETTLEMENT_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=CONSUMER_GROUP_ID,
            auto_offset_reset="earliest",
            enable_auto_commit=False,       # manual commit — see module docstring
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        )
        producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            acks="all",
            enable_idempotence=True,
        )
        await consumer.start()
        await producer.start()
        log.info(
            "Settlement worker started — topic=%s group=%s",
            KAFKA_SETTLEMENT_TOPIC, CONSUMER_GROUP_ID,
        )

    http_client = httpx.AsyncClient()

    try:
        async for msg in consumer:
            if _shutdown_event.is_set():
                break

            # Deserialise if not already done by the fixture
            if isinstance(msg.value, bytes):
                event_data = json.loads(msg.value.decode("utf-8"))
            else:
                event_data = msg.value

            log.info(
                "Received event tx=%s partition=%d offset=%d",
                event_data.get("transaction_id", "?"),
                msg.partition,
                msg.offset,
            )

            _owns_session = session is None
            _session = session if not _owns_session else SessionLocal()

            try:
                await process_event(event_data, producer, http_client, _session)
                # Commit offset ONLY after reaching terminal state
                await consumer.commit()
            except Exception:
                log.exception(
                    "Unhandled error processing tx=%s — offset not committed, will redeliver",
                    event_data.get("transaction_id", "?"),
                )
                # Do NOT commit — message will be redelivered on restart
            finally:
                if _owns_session:
                    await _session.close()

    finally:
        await http_client.aclose()
        if _owns_resources:
            await consumer.stop()
            await producer.stop()
            await engine.dispose()
            log.info("Settlement worker stopped cleanly")


def main() -> None:
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    try:
        asyncio.run(run_worker())
    except KeyboardInterrupt:
        pass
    sys.exit(0)


if __name__ == "__main__":
    main()
