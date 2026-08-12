import redis.asyncio as redis
from app.config import get_settings

settings = get_settings()

redis_client = redis.from_url(
    settings.REDIS_URL,
    encoding="utf-8",
    decode_responses=True
)

async def check_redis_health() -> bool:
    """Sends PING command to Redis server."""
    try:
        return await redis_client.ping()
    except Exception:
        return False
