# - outreach_automation_api/services/rabbit.py -

import asyncio
import aio_pika

from aio_pika.abc import AbstractRobustConnection, AbstractChannel
from aio_pika.exceptions import AMQPConnectionError

from config import STORECFG
from services import ECHO


class RabbitManager:
    def __init__(self):
        self.connection: AbstractRobustConnection | None = None
        self.connected = False

    async def connect(self):
        try:
            self.connection = await asyncio.wait_for(
                aio_pika.connect_robust(STORECFG["RABBIT_URL"]),
                timeout=5.0
            )
            self.connected = True
            ECHO.info("Connected to RabbitMQ")
        except (AMQPConnectionError, asyncio.TimeoutError) as e:
            self.connected = False
            ECHO.error("RabbitMQ connection failed", error=str(e))
            raise RuntimeError("RabbitMQ connection failed")

    async def close(self):
        if self.connection:
            await self.connection.close()
            self.connected = False

    async def get_channel(self) -> AbstractChannel:
        if not self.connected or self.connection is None:
            raise RuntimeError("Not connected to RabbitMQ")
        return await self.connection.channel()


RABBIT = RabbitManager()