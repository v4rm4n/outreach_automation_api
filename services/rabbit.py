# - outreach_automation_api/services/rabbit.py -

import json
import asyncio
import aio_pika
from aio_pika.abc import AbstractRobustConnection
from aio_pika.pool import Pool

from config import STORECFG
from services import ECHO
from .topology import TopologyConfig

class RabbitManager:
    def __init__(self):
        self.connection: AbstractRobustConnection | None = None
        self.channel_pool: Pool | None = None
        self.connected = False
        self._stop_event = asyncio.Event()
        self._maintenance_task = None

    async def connect(self):
        if self.connected:
            return

        try:
            self.connection = await aio_pika.connect_robust(
                STORECFG["RABBIT_URL"], 
                timeout=10
            )
            # A pool of channels prevents bottlenecking during high-throughput publishing
            self.channel_pool = Pool(self.connection.channel, max_size=10)
            self.connected = True
            
            # Start background connection monitor
            self._maintenance_task = asyncio.create_task(self._maintain_connection())
            ECHO.info("Connected to RabbitMQ")
        except Exception as e:
            self.connected = False
            ECHO.error(f"Connection failed: {e}")
            raise RuntimeError("RabbitMQ connection failed")

    async def publish_task(self, exchange_name: str, routing_key: str, payload: dict, delay_ms: int = 0):
        """Publishes a payload to an exchange, optionally with a delay."""
        if not self.channel_pool:
            raise RuntimeError("RabbitMQ channel pool not initialized.")

        async with self.channel_pool.acquire() as channel:
            exchange = await channel.get_exchange(exchange_name, ensure=False)
            
            headers = {}
            if delay_ms > 0:
                headers["x-delay"] = delay_ms

            await exchange.publish(
                aio_pika.Message(
                    body=json.dumps(payload).encode("utf-8"),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    content_type="application/json",
                    headers=headers
                ),
                routing_key=routing_key,
            )
            ECHO.debug(f"Published to '{exchange_name}' with key '{routing_key}' (Delay: {delay_ms}ms)")

    def get_channel_pool(self) -> Pool:
        if not self.channel_pool:
            raise RuntimeError("Not connected to RabbitMQ")
        return self.channel_pool

    async def _maintain_connection(self):
        """Background task to monitor connection state."""
        while not self._stop_event.is_set():
            if self.connection and self.connection.is_closed:
                ECHO.warning("Connection lost. aio_pika will attempt auto-reconnect...")
            await asyncio.sleep(5)

    async def close(self):
        self._stop_event.set()
        if self._maintenance_task:
            self._maintenance_task.cancel()
            
        if self.channel_pool:
            await self.channel_pool.close()
            
        if self.connection and not self.connection.is_closed:
            await self.connection.close()
            self.connected = False
            ECHO.info("Connection closed gracefully.")

    async def setup_topology(self, config: TopologyConfig):
        """Deploy YAML definitions to the broker."""
        if not self.channel_pool:
            raise RuntimeError("Cannot setup topology: Channel pool not initialized.")

        async with self.channel_pool.acquire() as channel:
            ECHO.info("Deploying topology...")

            # 1. Exchanges
            exchanges = {}
            for ex in config.exchanges:
                exchanges[ex.name] = await channel.declare_exchange(
                    ex.name,
                    type=ex.kind,
                    durable=True,
                    arguments=ex.arguments
                )

            # 2. Queues
            queues = {}
            for q in config.queues:
                queues[q.name] = await channel.declare_queue(
                    q.name,
                    durable=True,
                    arguments=q.arguments
                )

            # 3. Bindings
            for b in config.bindings:
                if b.destination_type == "queue":
                    if b.destination in queues and b.source in exchanges:
                        await queues[b.destination].bind(
                            exchanges[b.source], routing_key=b.routing_key
                        )
            ECHO.info("Topology deployment complete.")

RABBIT = RabbitManager()