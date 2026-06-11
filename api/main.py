# - outreach_automation_api/api/main.py -

import os
import uvicorn

from contextlib import asynccontextmanager
from fastapi import FastAPI
# TODO: Remember to configure during frontend integration
# from fastapi.middleware.cors import CORSMiddleware

from config import APPCFG, APICFG
from services import load_topology_config, configure_logging
from services import ECHO, REDIS, MONGO, RABBIT

# Import sub-routers
from .auth import auth_router
from .creators import creator_router
from .templates import templates_router
from .campaigns import campaigns_router

configure_logging(
    log_level = APPCFG["LOG_LEVEL"],
    dev = APPCFG["DEV_MODE"]
)

@asynccontextmanager
async def lifespan(_app: FastAPI):
    try:
        await REDIS.connect()
        await MONGO.connect()
        await MONGO.create_indexes()
        await RABBIT.connect()
        topology_cfg = load_topology_config("topology.yaml")
        await RABBIT.setup_topology(topology_cfg)
    except RuntimeError:
        ECHO.error("Resource initialization failed")
        os._exit(1)
    
    try:
        ECHO.info(f"Serving @ {APICFG["UVICORN_HOST"]}:{APICFG["UVICORN_PORT"]}")
        yield
    
    finally:
        await REDIS.close()
        await MONGO.close()
        await RABBIT.close()

app = FastAPI(lifespan = lifespan)

# TODO: Remember to configure during frontend integration
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["http://localhost:5173"],
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )

@app.get("/")
async def root():
    ECHO.info("Root endpoint hit!")
    return f"Outreach Automation API v{APPCFG["VERSION"]}"

app.include_router(auth_router)
app.include_router(creator_router)
app.include_router(templates_router)
app.include_router(campaigns_router)

if __name__ == "__main__":
    uvicorn.run(
        app = "main:app" if APICFG["UVICORN_RELOAD"] else app,
        log_level = APPCFG["LOG_LEVEL"],
        host = APICFG["UVICORN_HOST"],
        port = APICFG["UVICORN_PORT"],
        reload = APICFG["UVICORN_RELOAD"],
    )