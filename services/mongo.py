# - outreach_automation_api/services/mongo.py -

import asyncio

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError

from config import STORECFG
from services import ECHO


class MongoManager:
    def __init__(self):
        self.client: AsyncIOMotorClient | None = None
        self.db = None
        self.connected = False

    async def connect(self):
        try:
            self.client = AsyncIOMotorClient(
                STORECFG["MONGO_URL"],
                maxPoolSize=STORECFG["MONGO_MAX_CONNECTIONS"],
                serverSelectionTimeoutMS=3000,
            )
            # motor is lazy — ping forces actual connection
            await asyncio.wait_for(
                self.client.admin.command("ping"),
                timeout=3.0
            )
            self.db = self.client[STORECFG["MONGO_DB_NAME"]]
            self.connected = True
            ECHO.info("Connected to MongoDB")
        except (ConnectionFailure, ServerSelectionTimeoutError, asyncio.TimeoutError) as e:
            self.connected = False
            ECHO.error("MongoDB connection failed", error=str(e))
            raise RuntimeError("MongoDB connection failed")

    async def close(self):
        if self.client:
            self.client.close()
            self.connected = False

    async def create_indexes(self):
        if not self.connected:
            raise RuntimeError("Not connected to MongoDB")
        
        db = self.get_db()
        
        # 1. Users Layer
        await db["users"].create_index("email", unique=True)
        
        # 2. Campaigns Layer (Compound to optimize dashboard fetches sorted by latest)
        await db["campaigns"].create_index([("user_id", 1), ("created_at", -1)])
        
        # 3. Creators Layer (Enables identical handles on different social media channels)
        await db["creators"].create_index([("handle", 1), ("platform", 1)], unique=True)
        
        # 4. Templates Layer 
        await db["templates"].create_index("user_id")
        
        # 5. Messages / Log State Layer
        await db["messages"].create_index([("status", 1), ("scheduled_at", 1)])
        await db["messages"].create_index("campaign_id")
        
        # 6. Dispatch Jobs (CRITICAL: DB-level Idempotency circuit breaker)
        await db["dispatch_jobs"].create_index(
            [("campaign_id", 1), ("creator_id", 1)], 
            unique=True
        )
        await db["dispatch_jobs"].create_index("status")
        
        # 7. Operational Health Layer (Compound index for time-window alert sorting)
        await db["critical_alerts"].create_index([("campaign_id", 1), ("created_at", -1)])
        
        ECHO.info("MongoDB structural and uniqueness indexes created successfully")


    def get_db(self):
        if not self.connected or self.db is None:
            raise RuntimeError("Not connected to MongoDB")
        return self.db

    def get_collection(self, name: str):
        return self.get_db()[name]


MONGO = MongoManager()