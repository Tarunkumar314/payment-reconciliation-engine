import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI, Response, status

from app.database import check_database_health, init_db
from app.redis_client import check_redis_health
from app.routers import ledger
from app.routers import mock_bank


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: ensure tables exist
    try:
        await init_db()
    except Exception as e:
        print(f"Warning: Failed to initialize DB tables automatically: {e}")
    yield
    # Shutdown logic if any


app = FastAPI(
    title="Payment Reconciliation Engine",
    version="0.1.0",
    description="High-throughput payment reconciliation service with double-entry ledger.",
    lifespan=lifespan
)

app.include_router(ledger.router)
app.include_router(mock_bank.router)


@app.get("/health", status_code=status.HTTP_200_OK)
async def health_check(response: Response):
    """
    Health check endpoint verifying connectivity to PostgreSQL and Redis.
    Runs health checks concurrently using asyncio.gather().
    """
    postgres_ok, redis_ok = await asyncio.gather(
        check_database_health(),
        check_redis_health()
    )

    is_healthy = postgres_ok and redis_ok

    if not is_healthy:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE

    return {
        "status": "healthy" if is_healthy else "unhealthy",
        "services": {
            "postgres": "connected" if postgres_ok else "disconnected",
            "redis": "connected" if redis_ok else "disconnected"
        }
    }
