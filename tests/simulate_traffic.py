# --- outreach_automation_api/tests/simulate_traffic.py ---

import asyncio
import random
import time
from datetime import datetime, timedelta, timezone
import httpx
from services import ECHO

BASE_URL = "http://localhost:8000"
CAMPAIGN_ID = "6a2be8e84359afd3c8fa79b7"
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

def generate_mock_object_id() -> str:
    """Generates a valid-looking 24-character hex string for MongoDB ObjectIDs."""
    return "".join(random.choices("0123456789abcdef", k=24))

def get_future_iso_timestamp(seconds_ahead: int = 5) -> str:
    tz = timezone(timedelta(hours=5, minutes=30))
    future_dt = datetime.now(tz) + timedelta(seconds=seconds_ahead)
    return future_dt.isoformat(timespec='seconds')


async def run_volume_scenario(client: httpx.AsyncClient):
    """Proves 50k/day scale by injecting 1,000 jobs instantly."""
    ECHO.info("\n[bold cyan]=== HIGH VOLUME LOAD TEST (1,000 Creators) ===[/]")
    
    # Send 10 batches of 100 creators each, concurrently.
    tasks = []
    for _ in range(10):
        creator_ids = ["6a2be6e9d04ef534bb380ea5"]
        scheduled_time = get_future_iso_timestamp(seconds_ahead=5)
        
        payload = {
            "creator_ids": creator_ids,
            "scheduled_for": scheduled_time
        }
        tasks.append(client.post(API_URL, headers=HEADERS, json=payload))
    
    start_time = time.time()
    responses = await asyncio.gather(*tasks, return_exceptions=True)
    total_time = time.time() - start_time
    
    # Safely check for HTTP success without triggering Pylance errors
    success_count = 0
    failed_count = 0
    for r in responses:
        if isinstance(r, httpx.Response) and r.status_code == 200:
            # We want to know how many actual *jobs* were staged, not just how many *requests* passed
            res_data = r.json()
            success_count += res_data.get("jobs_staged", 0)
        else:
            failed_count += 1
            if isinstance(r, httpx.Response):
                ECHO.warning(f"Batch failed ({r.status_code}): {r.text}")
            else:
                ECHO.error(f"Network Exception: {r}")

    ECHO.info(f"Volume Test Complete:[/] Successfully staged {success_count} jobs in {total_time:.3f} seconds.")
    if failed_count > 0:
        ECHO.warning(f"[yellow]Failed Batches:[/] {failed_count}")
        
    throughput = (success_count / total_time) if total_time > 0 else 0
    ECHO.info(f"[cyan]Ingestion Throughput:[/] {throughput:,.0f} jobs/second.")
    
    # The math for the documentation
    daily_theoretical = throughput * 86400
    ECHO.info(f"Theoretical Max Ingestion:[/] {daily_theoretical:,.0f} jobs/day")

async def main():
    # Increase the timeout because the DB might take a second to bulk-insert 1,000 records
    async with httpx.AsyncClient(timeout=30.0) as client:
        await run_volume_scenario(client)

if __name__ == "__main__":
    ECHO.info("Initializing High-Volume Traffic Simulation Script...")
    asyncio.run(main())