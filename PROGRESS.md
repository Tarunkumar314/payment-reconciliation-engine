# Project Progress

## Verified complete (confirmed by running it, not by agent summary)

- **Step 1**: Skeleton — Postgres/Redis/Kafka (KRaft, no Zookeeper) + FastAPI `/health`
  — verified via `docker compose ps` and curl

- **Step 2**: Double-entry ledger schema + `POST /transactions` — real Postgres test isolation
  (savepoint pattern: `join_transaction_mode="create_savepoint"` + outer `ROLLBACK` at teardown;
  event-loop-per-fixture-scope bug fixed via `asyncio.run()` inside sync fixtures for both
  schema setup and Redis teardown)

- **Step 3**: Idempotency guard (Redis, 24h TTL) + Redis sliding-window fraud velocity check
  — real Redis on DB 1 with UUID-namespaced key isolation per test

- **Step 4**: Outbox pattern — atomic `(transaction + ledger_entries + outbox_event)` in one
  `commit()`, polling relay publishes to Kafka and stamps `published_at` — real Kafka in tests

- **Step 5**: Settlement worker + full Docker wiring for all 6 services
  — verified end-to-end in running Docker Compose:
    - 4 transactions posted → outbox relay published → worker consumed → all 4 `SETTLED` in DB
    - Retry-then-succeed observed live: TX `7137d4e1` got 503 on attempt 1, retried, 200 on
      attempt 2, `SETTLED after 2 attempt(s)` — log lines confirmed in real container output
    - DLQ publish confirmed before offset commit on retry-exhaustion path: `send_and_wait`
      at `settlement_worker.py:264` blocks until broker ACK; `consumer.commit()` at line 342
      only executes after `process_event()` returns — code shown directly, not summarised
  — bugs found and fixed in this step:
    - `begin_nested()` silent-rollback: `get_db`'s implicit outer transaction was never
      committed; `RELEASE SAVEPOINT` merges into parent but doesn't write to disk — fixed
      with explicit `await db.commit()` in `create_transaction`
    - Kafka `ADVERTISED_LISTENERS` was `localhost:9092` only — containers got the right
      bootstrap address but were redirected to `localhost` post-handshake and lost
      connectivity — fixed with dual listeners: `EXTERNAL://localhost:9092` (host/test suite)
      and `INTERNAL://kafka:29092` (container-to-container)
    - `outbox_relay` was missing entirely from `docker-compose.yml` — added with
      `depends_on: kafka: condition: service_healthy`
    - `app` service had no `command:` — bare `python3` exits immediately causing restart loop
      — fixed with `uvicorn app.main:app --host 0.0.0.0 --port 8000`

## Not yet verified in this session
- DLQ exhaustion path (retry-then-succeed was observed; full retry exhaustion + DLQ publish
  was not triggered in the live run — confirmed via code only, not live log lines)
- `tests/test_settlement_worker.py` has not been run with `pytest` — tests exist and pass
  in isolation but were not executed as a full suite in this session

## Standing conventions (don't relitigate these)
- Real Postgres/Redis/Kafka in every test — never mocks, never SQLite, never fakeredis
- Async teardown: `asyncio.run()` inside sync fixtures to avoid stranded asyncpg/redis
  connections across event-loop boundaries (see `conftest.py`)
- Container-to-container calls use Docker service names: `kafka:29092`, `http://app:8000`
- Host/test suite uses: `localhost:9092` (Kafka EXTERNAL listener), `localhost:8000`
- `enable_auto_commit=False` on all Kafka consumers — offset committed only after terminal state
- Outbox relay is a separate process/container, not a background thread inside the app

## Next
- Step 6: not started
