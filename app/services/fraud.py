"""
Fraud velocity checker for POST /transactions.

Design decisions — sorted set vs. plain counter
────────────────────────────────────────────────
A naive implementation uses INCR + EXPIRE:

    INCR  velocity:{account_id}
    EXPIRE velocity:{account_id} 60

This breaks at window boundaries.  Consider an account allowed 5 txns/60s:

    t=00  txns 1-5 arrive  → count=5, window expires at t=60
    t=59  txn 6 arrives    → count=6, blocked  ✓
    t=61  window resets    → count=0
    t=61  txns 7-11 arrive → count=5, all allowed  ✓

The account sent 10 transactions between t=00 and t=61 — only 1 second
more than one window — but the counter saw at most 5 at any one time.

A sorted set stores a timestamp as the *score* of each member:

    ZADD  velocity:{account_id} <now_ms> <unique_member>
    ZREMRANGEBYSCORE velocity:{account_id} 0 <now_ms - window_ms>
    ZCARD velocity:{account_id}

Before counting, we evict all entries older than `window_ms` ago.
The window is therefore always "the last N seconds from this exact moment",
not "since the key was first created".  A boundary burst is structurally
impossible.  The three commands are sent in a single pipeline to avoid
multiple round-trips; they have no inter-command data dependency.

Key format:   velocity:{account_id}
Window:       60 seconds  (configurable via VELOCITY_WINDOW_SECONDS)
Threshold:    10 per window (configurable via VELOCITY_MAX_COUNT)
TTL:          window_ms * 2 — prevents orphaned keys for idle accounts

Result semantics
─────────────────
Returns True  → threshold exceeded → caller should use HELD_FOR_REVIEW
Returns False → within threshold   → caller should use POSTED
"""

import time
import uuid

import redis.asyncio as aioredis

VELOCITY_WINDOW_SECONDS: int = 60
VELOCITY_MAX_COUNT: int = 10

_KEY_PREFIX = "velocity:"


def _make_key(account_id: str) -> str:
    return f"{_KEY_PREFIX}{account_id}"


async def check_velocity(
    redis: aioredis.Redis,
    account_id: str,
) -> bool:
    """
    Records this request in the sorted set for `account_id`, prunes entries
    outside the rolling window, then returns True if the count now exceeds
    VELOCITY_MAX_COUNT.

    The three Redis commands are pipelined: single TCP round-trip.
    """
    now_ms = int(time.time() * 1000)
    window_start_ms = now_ms - (VELOCITY_WINDOW_SECONDS * 1000)
    key = _make_key(account_id)

    # Unique member per call — we store timestamps as scores, but two
    # requests arriving within the same millisecond would collide without
    # a unique member.  A UUID4 suffix is cheap and collision-free.
    member = str(uuid.uuid4())

    async with redis.pipeline(transaction=False) as pipe:
        # 1. Record this request
        pipe.zadd(key, {member: now_ms})
        # 2. Evict entries older than the window
        pipe.zremrangebyscore(key, 0, window_start_ms)
        # 3. Count surviving entries (includes the one just added)
        pipe.zcard(key)
        # 4. Refresh TTL so idle-account keys don't accumulate forever
        pipe.expire(key, VELOCITY_WINDOW_SECONDS * 2)
        results = await pipe.execute()

    count: int = results[2]  # zcard result
    return count > VELOCITY_MAX_COUNT
