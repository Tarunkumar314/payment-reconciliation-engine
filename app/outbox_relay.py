"""
Outbox relay — polls Postgres for unpublished events and delivers them to Kafka.

Architecture
────────────
This is a standalone async process, completely separate from the FastAPI app.
It is run as:  python -m app.outbox_relay

It is NOT a FastAPI endpoint or a background task on the web process.
Reasons for separation:
  1. Independent scaling: if event volume spikes, you scale the relay without
     scaling the API.
  2. No coupling to the HTTP request lifecycle: the relay runs at its own pace
     regardless of API traffic.
  3. Clean failure isolation: a relay crash does not affect API availability.
     Events accumulate in the outbox table until the relay restarts.

Delivery guarantee: at-least-once
──────────────────────────────────
The relay marks published_at ONLY after Kafka's send_and_wait() returns
(meaning the broker has acknowledged the write with acks="all").

If the relay crashes between the Kafka ACK and the Postgres UPDATE:
  - The outbox row's published_at stays NULL.
  - On restart, the relay will re-select this row and publish it again.
  - Kafka receives a duplicate message for the same transaction_id.

This is at-least-once delivery: every event is guaranteed to reach Kafka,
but may appear more than once on crash boundaries.

Why this is acceptable:
  - Step 3's idempotency work gave every transaction a UUID that is embedded
    in the outbox payload as transaction_id.
  - The settlement consumer (Step 5) will use transaction_id as its own
    deduplication key (ON CONFLICT DO NOTHING in its own idempotency table).
  - Exactly-once would require Kafka transactional producers + read_committed
    consumers tied to the Postgres commit in a 2PC — significant complexity
    for a marginal gain when the consumer already has a natural idempotency key.

Poll interval: 2 seconds (configurable via OUTBOX_POLL_INTERVAL_SECONDS env var).

Batch size: 100 rows per poll (configurable via OUTBOX_BATCH_SIZE env var).
Batching keeps relay lag bounded: if the relay falls behind (e.g. after a
restart), it catches up 100 events per 2-second cycle rather than processing
one at a time.

Ordering note: we ORDER BY created_at ASC to publish events in creation order.
This gives the consumer a best-effort ordered stream. It is not a strict
guarantee (two events with identical created_at milliseconds can arrive in
either order), but for financial events it is good enough — consumers must
be idempotent regardless of order for other reasons (network redelivery, etc).
"""

import asyncio
import json
import logging
import os
import signal
import sys
from datetime import datetime, timezone

from aiokafka import AIOKafkaProducer
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker

from app.config import get_settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("outbox_relay")

# ── Configuration ─────────────────────────────────────────────────────────────

_settings = get_settings()

DATABASE_URL: str = _settings.DATABASE_URL
KAFKA_BOOTSTRAP_SERVERS: str = _settings.KAFKA_BOOTSTRAP_SERVERS
KAFKA_TOPIC: str = "settlement-events"
POLL_INTERVAL_SECONDS: float = float(os.environ.get("OUTBOX_POLL_INTERVAL_SECONDS", "2"))
BATCH_SIZE: int = int(os.environ.get("OUTBOX_BATCH_SIZE", "100"))

# ── Database setup ────────────────────────────────────────────────────────────
# The relay gets its own engine — it is a separate process with no connection
# to the FastAPI app's engine.  pool_size=2 is sufficient: one connection for
# SELECT (reading unpublished rows) and one for UPDATE (marking published).
# In practice a single connection handles both sequentially, but two provides
# headroom without waste.

engine = create_async_engine(DATABASE_URL, pool_size=2, max_overflow=0, echo=False)
SessionLocal = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

# ── Graceful shutdown ─────────────────────────────────────────────────────────

_shutdown_event = asyncio.Event()


def _handle_signal(sig, frame):
    log.info("Received signal %s — initiating graceful shutdown", sig)
    _shutdown_event.set()


# ── Core relay logic ──────────────────────────────────────────────────────────

async def _poll_and_publish(producer: AIOKafkaProducer, session: AsyncSession | None = None) -> int:
    """
    One poll cycle: fetch up to BATCH_SIZE unpublished rows, publish each to
    Kafka, then mark them published in Postgres.

    Returns the number of events published this cycle (useful for logging).

    `session` parameter:
      - When provided (tests), uses the caller's existing session.  This allows
        test fixtures to inject a savepoint-isolated session so the relay sees
        rows written by the test without needing a separate DB connection.
      - When None (production relay loop), creates a fresh session via
        SessionLocal for each poll cycle and commits/closes it internally.

    Why we mark each row individually rather than bulk-updating after the loop:
    If the relay crashes mid-batch, only the rows that were individually updated
    will be marked published.  The rest will be re-polled on restart and
    re-published (at-least-once, not exactly-once — consistent with our
    delivery guarantee).

    An alternative is to collect all published IDs and bulk-UPDATE at the end.
    That's slightly more efficient per batch but means a crash after Kafka ACKs
    but before the bulk UPDATE republishes the entire batch.  Per-row updates
    minimise the duplicate-delivery blast radius on crash.
    """
    from app.models.outbox import OutboxEvent  # noqa: PLC0415

    _owns_session = session is None
    if _owns_session:
        session = SessionLocal()

    try:
        result = await session.execute(
            select(OutboxEvent)
            .where(OutboxEvent.published_at.is_(None))
            .order_by(OutboxEvent.created_at.asc())
            .limit(BATCH_SIZE)
            .with_for_update(skip_locked=True)
        )
        events = result.scalars().all()

        if not events:
            return 0

        published_count = 0
        for event in events:
            message_bytes = json.dumps(
                {
                    "event_id": str(event.id),
                    "event_type": event.event_type.value,
                    "created_at": event.created_at.isoformat(),
                    **event.payload,
                }
            ).encode("utf-8")

            # Raises on Kafka failure — caller (run_relay) catches and logs.
            # published_at UPDATE below is intentionally AFTER this line.
            await producer.send_and_wait(
                KAFKA_TOPIC,
                value=message_bytes,
                key=str(event.transaction_id).encode("utf-8"),
            )

            await session.execute(
                update(OutboxEvent)
                .where(OutboxEvent.id == event.id)
                .values(published_at=datetime.now(tz=timezone.utc))
            )
            await session.commit()
            published_count += 1
            log.debug("Published event %s (tx=%s)", event.id, event.transaction_id)

        return published_count

    finally:
        if _owns_session:
            await session.close()


async def run_relay() -> None:
    """
    Main relay loop.

    1. Creates an aiokafka producer with acks="all" for durability.
       acks="all" means the broker leader AND all in-sync replicas must ACK
       before send_and_wait returns.  With our single-broker dev setup this
       is equivalent to acks=1, but the setting is correct for production.
    2. Polls Postgres every POLL_INTERVAL_SECONDS seconds.
    3. Exits cleanly on SIGTERM/SIGINT via _shutdown_event.
    """
    log.info(
        "Outbox relay starting — bootstrap=%s topic=%s poll=%.1fs batch=%d",
        KAFKA_BOOTSTRAP_SERVERS,
        KAFKA_TOPIC,
        POLL_INTERVAL_SECONDS,
        BATCH_SIZE,
    )

    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        acks="all",           # wait for all in-sync replicas — safest durability setting
        enable_idempotence=True,  # producer-level dedup within a single session
        value_serializer=None,    # we handle serialisation manually (explicit bytes)
        compression_type="gzip",  # bandwidth-efficient for JSON payloads
    )

    await producer.start()
    log.info("Kafka producer connected")

    try:
        while not _shutdown_event.is_set():
            try:
                count = await _poll_and_publish(producer)
                if count:
                    log.info("Published %d event(s) this cycle", count)
            except Exception:
                # Log and continue — transient Kafka or Postgres errors should
                # not crash the relay.  The next poll cycle will retry.
                log.exception("Error during poll cycle — will retry after %.1fs", POLL_INTERVAL_SECONDS)

            # Wait for next cycle, but wake immediately on shutdown signal.
            try:
                await asyncio.wait_for(_shutdown_event.wait(), timeout=POLL_INTERVAL_SECONDS)
            except asyncio.TimeoutError:
                pass  # normal — timeout means it's time for the next poll

    finally:
        log.info("Relay shutting down — flushing producer")
        await producer.stop()
        await engine.dispose()
        log.info("Relay stopped cleanly")


def main() -> None:
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    try:
        asyncio.run(run_relay())
    except KeyboardInterrupt:
        pass  # already handled via signal
    sys.exit(0)


if __name__ == "__main__":
    main()
