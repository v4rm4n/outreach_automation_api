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

    def get_db(self):
        if not self.connected or self.db is None:
            raise RuntimeError("Not connected to MongoDB")
        return self.db

    def get_collection(self, name: str):
        return self.get_db()[name]


MONGO = MongoManager()