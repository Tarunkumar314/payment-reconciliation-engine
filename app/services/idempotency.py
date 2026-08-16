"""
Idempotency guard for POST /transactions.

Design decisions
────────────────
1. Redis SET NX EX (set-if-not-exists with TTL) is used instead of a
   Postgres UNIQUE column because:
   - It adds zero schema surface to the ledger tables.
   - TTL-based expiry is native to Redis; SQL requires a scheduled job or
     a WHERE expires_at > NOW() predicate on every read.
   - A single SET NX is atomic (compare-and-set in one RTT).

2. The key is stored AFTER the DB commit, never before.
   If we wrote the key first and the DB write failed, subsequent requests
   with the same key would receive a cached "success" response that has no
   corresponding ledger record — a phantom success.  Storing after commit
   ensures the cached response and the ledger record are always consistent.

3. We cache the full JSON response body (serialised by the caller), not
   just the transaction ID.  This makes cache retrieval O(1) and keeps the
   idempotency layer ignorant of the DB schema.

Key format:  idempotency:{idempotency_key}
TTL:         24 hours (86400 seconds)
"""

from typing import Optional
import redis.asyncio as aioredis

IDEMPOTENCY_TTL_SECONDS = 86_400  # 24 hours
_KEY_PREFIX = "idempotency:"


def _make_key(idempotency_key: str) -> str:
    return f"{_KEY_PREFIX}{idempotency_key}"


async def get_cached_response(
    redis: aioredis.Redis,
    idempotency_key: str,
) -> Optional[str]:
    """
    Returns the previously cached JSON response string for this key,
    or None if the key has never been seen (or has expired).
    """
    return await redis.get(_make_key(idempotency_key))


async def store_response(
    redis: aioredis.Redis,
    idempotency_key: str,
    response_json: str,
) -> None:
    """
    Persists the serialised response body in Redis with a 24-hour TTL.

    Must be called ONLY after the DB transaction has successfully committed.
    Uses SET with EX (absolute TTL) and NX (only set if not exists) so that
    a concurrent request that also passed the pre-check cannot overwrite a
    response that was just stored by the first request.
    """
    await redis.set(
        _make_key(idempotency_key),
        response_json,
        ex=IDEMPOTENCY_TTL_SECONDS,
        nx=True,  # idempotent: don't overwrite if a race already stored it
    )
