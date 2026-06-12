# --- outreach_automation_api/tests/constant_load.py ---

import asyncio
import time
from datetime import datetime, timedelta, timezone
import httpx
from services import ECHO

BASE_URL = "http://localhost:8000"
CAMPAIGN_ID = "6a2be8e84359afd3c8fa79b7" # Your actual campaign ID
API_URL = f"{BASE_URL}/campaigns/{CAMPAIGN_ID}/creators"

AUTH_TOKEN = (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJzdWIiOiI2YTJhOGU2ZDJmMTFiMzZjYWRkOTBlZjciLCJyb2xlIjoidXNlciIsImlhdCI6MTc4MTE3Mzg3MiwiZXhwIjoxNzgxNzc4NjcyfQ."
    "qjCgPDVKtmnKHhaoIP2WMbvlyVlleASXw_Vx0BdfgV4"
)

HEADERS = {
    "Authorization": f"Bearer {AUTH_TOKEN}",
    "Content-Type": "application/json"
}

def get_future_iso_timestamp() -> str:
    tz = timezone(timedelta(hours=5, minutes=30))
    return (datetime.now(tz) + timedelta(seconds=5)).isoformat(timespec='seconds')

async def fire_request(client: httpx.AsyncClient, req_id: int):
    # Using your validated creator ID to ensure the API accepts it
    payload = {
        "creator_ids": ["6a2be6e9d04ef534bb380ea5"], 
        "scheduled_for": get_future_iso_timestamp()
    }
    try:
        response = await client.post(API_URL, headers=HEADERS, json=payload)
        if response.status_code == 200:
            ECHO.info(f"Request {req_id}: Success")
        else:
            ECHO.warning(f"Request {req_id}: Failed ({response.status_code})")
    except Exception as e:
        ECHO.error(f"Network error on {req_id}: {e}")

async def main():
    duration_seconds = 60
    requests_per_second = 2
    total_requests = duration_seconds * requests_per_second
    
    ECHO.info(f"[bold cyan]=== STARTING SUSTAINED LOAD TEST ===")
    ECHO.info(f"Target: {requests_per_second} req/sec for {duration_seconds} seconds ({total_requests} total)\n")

    async with httpx.AsyncClient(timeout=10.0) as client:
        start_time = time.time()
        
        for i in range(1, total_requests + 1):
            # Fire the request asynchronously so we don't block the loop
            asyncio.create_task(fire_request(client, i))
            
            # Sleep exactly enough to maintain the requests_per_second rate
            await asyncio.sleep(1.0 / requests_per_second)
            
        # Wait a moment for the final inflight requests to finish
        await asyncio.sleep(2)
        
    actual_duration = time.time() - start_time
    ECHO.info(f"\n=== LOAD TEST COMPLETE ===")
    ECHO.info(f"Duration: {actual_duration:.2f} seconds")

if __name__ == "__main__":
    asyncio.run(main())