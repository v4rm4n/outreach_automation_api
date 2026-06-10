# - outreach_automation_api/services/redis.py -

import asyncio

import redis.asyncio as redis
from redis import RedisError

from config import STORECFG
from services import ECHO

class RedisManager:
    def __init__(self):
        self.client = None
        self.connected = False

    async def connect(self):
        try:
            self.client = redis.from_url(
                STORECFG["REDIS_URL"],
                protocol=2,
                decode_responses=True,
                max_connections=STORECFG["REDIS_MAX_CONNECTIONS"]
                )
            await asyncio.wait_for(self.client.ping(), timeout=3.0)
            self.connected = True
            ECHO.info("Connected to Redis")
        except (RedisError, asyncio.TimeoutError) as e:
            self.connected = False
            ECHO.error("redis connection failed", error=str(e))
            raise RuntimeError("Redis connection failed")

    async def close(self):
        if self.client:
            await self.client.aclose()
            self.connected = False

    def get_client(self):
        if not self.connected or self.client is None:
            raise RuntimeError("Not connect to Redis")
        return self.client

REDIS = RedisManager()