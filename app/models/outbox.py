"""
OutboxEvent ORM model — transactional outbox for settlement events.

Schema decisions
────────────────
- id: UUID PK, consistent with every other table in this schema.
- transaction_id: FK to transactions.id with CASCADE DELETE.  If a
  transaction is ever deleted (administrative correction), its outbox row
  goes with it automatically — no orphaned relay jobs.
- event_type: a narrow ENUM rather than a free-form string so the relay
  and consumer can switch on a well-known vocabulary without defensive
  parsing.
- payload: JSONB (native Postgres binary JSON) rather than TEXT or JSON.
  JSONB is stored parsed, supports GIN indexing if we ever need to query
  inside the payload, and is de-facto standard for event payloads in
  Postgres-based outboxes.
- created_at: server-side timestamp — the moment the row was written.
  Used for lag monitoring: NOW() - created_at tells you how old an
  unpublished event is.
- published_at: nullable.  NULL means "not yet delivered to Kafka."
  Non-null means the relay confirmed Kafka ACKed the write.  A partial
  index on (published_at) WHERE published_at IS NULL keeps relay queries
  O(unpublished) rather than O(all events).
"""

import enum
import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy import String, DateTime, ForeignKey, Enum as SQLEnum, func, Index
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base


class OutboxEventType(str, enum.Enum):
    TRANSACTION_POSTED = "TRANSACTION_POSTED"
    TRANSACTION_HELD = "TRANSACTION_HELD"


class OutboxEvent(Base):
    __tablename__ = "outbox_events"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    transaction_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("transactions.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    event_type: Mapped[OutboxEventType] = mapped_column(
        SQLEnum(OutboxEventType), nullable=False
    )
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    published_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True, default=None
    )

    __table_args__ = (
        # Partial index: only covers unpublished rows.
        # As the relay keeps up, this index shrinks toward zero entries —
        # relay SELECT queries stay fast regardless of total event volume.
        Index(
            "ix_outbox_events_unpublished",
            "created_at",
            postgresql_where=(published_at == None),  # noqa: E711 — SQLAlchemy requires `== None` not `is None`
        ),
    )
