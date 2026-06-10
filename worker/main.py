# - outreach_automation_api/worker/main.py -

import os
import asyncio

from config import APPCFG, APICFG
from services import configure_logging, ECHO
from services import MONGO, RABBIT, REDIS

configure_logging(
    log_level = APICFG["LOG_LEVEL"],
    dev = APPCFG["DEV_MODE"]
)

async def main():
    try:
        await MONGO.connect()
        await RABBIT.connect()
        await REDIS.connect()
    except RuntimeError:
        ECHO.error("Resource initialization failed")
        os._exit(1)
    try:
        # await run_worker_loop()
        pass
    finally:
        await MONGO.close()
        await RABBIT.close()
        await REDIS.close()
    
if __name__ == "__main__":
    asyncio.run(main())