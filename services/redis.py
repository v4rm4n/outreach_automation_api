# - outreach_automation_api/services/redis.py -

import asyncio
import time

import redis.asyncio as redis
from redis import RedisError

from config import STORECFG
from services import ECHO

async def check_instagram_rate_limit(account_id: str = "global", limit: int = 20, window_sec: int = 60) -> bool:
    """
    Checks if we are allowed to send an Instagram DM using a Sliding Window algorithm.
    Returns True if allowed, False if we need to back off.
    """
    # Assuming your REDIS manager has a get_client() method returning the async redis connection
    client = REDIS.get_client()
    if not client:
        raise RuntimeError("Redis client not initialized for rate limiting.")

    key = f"rate_limit:instagram:{account_id}"
    now = time.time()
    
    # We use a transaction pipeline so all commands execute atomically in Redis
    async with client.pipeline(transaction=True) as pipe:
        # 1. Remove timestamps older than the current window (now - 60 seconds)
        pipe.zremrangebyscore(key, 0, now - window_sec)
        
        # 2. Add the current request's timestamp to the sorted set
        # Using the exact timestamp as both the score and the member
        pipe.zadd(key, {str(now): now})
        
        # 3. Count how many requests occurred in the last 60 seconds
        pipe.zcard(key)
        
        # 4. Set a TTL on the key so it cleans itself up if the worker goes idle
        pipe.expire(key, window_sec)
        
        results = await pipe.execute()
        
    # The result of zcard is the 3rd operation in our pipeline
    current_count = results[2]
    
    if current_count > limit:
        # We went over the limit. Remove the token we just added so it doesn't count against future checks.
        await client.zrem(key, str(now))
        return False
        
    return True

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