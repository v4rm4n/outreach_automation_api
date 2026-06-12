# --- outreach_automation_api/services/http_manager.py ---

import httpx
from services import ECHO

class HTTPManager:
    """
    A singleton, asynchronous HTTP client manager built on top of httpx. 
    Provides a centralized connection pool for the application to 
    interface with external APIs safely and efficiently.
    """
    _instance = None

    def __new__(cls):
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        # Bulletproof gate to ensure initialization properties are only declared ONCE
        if not hasattr(self, "_state_initialized"):
            self.client: httpx.AsyncClient | None = None
            self.initialized_flag = False
            self._state_initialized = True

    async def initialize(self):
        """
        Sets up the underlying AsyncClient with high-concurrency limits.
        """
        if self.initialized_flag:
            ECHO.debug("[purple]http_manager:[/] Global HTTP client already initialized. Skipping.")
            return

        try:
            # Configure the pool for high concurrency
            limits = httpx.Limits(
                max_keepalive_connections=100, # Keep 100 pipes warm and open
                max_connections=200,           # Pool ceiling for concurrent inflight requests
                keepalive_expiry=30.0          # Keep idle connections alive for 30s
            )
            
            # Global timeout of 10s; can be cleanly overridden per-request if needed
            self.client = httpx.AsyncClient(
                limits=limits,
                timeout=10.0,
                follow_redirects=True
            )
            self.initialized_flag = True
            ECHO.debug("[purple]http_manager:[/] Global HTTP client successfully initialized.")
        except Exception as e:
            self.initialized_flag = False
            ECHO.error(f"[purple]http_manager:[/] Initialization failed: {e}")
            raise RuntimeError("HTTP Manager failed to initialize.") from e

    async def close(self):
        """
        Gracefully drains and closes the connection pool.
        """
        if self.client:
            await self.client.aclose()
            self.client = None
            self.initialized_flag = False
            ECHO.debug("[purple]http_manager:[/] Global HTTP client closed gracefully.")

    def get_client(self) -> httpx.AsyncClient:
        """
        Returns the singleton httpx.AsyncClient instance.
        """
        if not self.initialized_flag or self.client is None:
            raise RuntimeError("HTTPManager client is not initialized! Call await HTTP.initialize() first.")
        return self.client

# Singleton instance
HTTP = HTTPManager()