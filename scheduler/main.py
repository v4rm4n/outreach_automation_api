# - outreach_automation_api/scheduler/main.py -

import os
import asyncio

from config import APPCFG, APICFG
from services import load_topology_config, configure_logging
from services import ECHO, MONGO, RABBIT

configure_logging(
    log_level = APICFG["LOG_LEVEL"],
    dev = APPCFG["DEV_MODE"]
)

async def main():
    try:
        await MONGO.connect()
        await RABBIT.connect()
        topology_cfg = load_topology_config("topology.yaml")
        await RABBIT.setup_topology(topology_cfg)
    except RuntimeError:
        ECHO.error("Resource initialization failed")
        os._exit(1)

    try:
        ECHO.info("`main` routine initiated!")
        pass
    
    finally:
        await MONGO.close()
        await RABBIT.close()

if __name__ == "__main__":
    asyncio.run(main())