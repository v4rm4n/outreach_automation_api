# - outreach_automation_api/config/config.py

import os

from dotenv import load_dotenv

from . import validators

load_dotenv()

APPCFG = {
    # Maintain in `pyproject.toml` too
    "VERSION": "0.1.0",
    "LOG_LEVEL": validators.logging.log_level("LOG_LEVEL", "INFO"),
    "DEV_MODE":  validators.common.boolean("DEV_MODE", True),
    "IG_USERNAME": os.getenv("IG_USERNAME", "cherry_pie"),
    "IG_PASSWORD": os.getenv("IG_PASSWORD", "perry_chie"),
}

APICFG = {
    "UVICORN_HOST": validators.uvicorn.host("UVICORN_HOST", "0.0.0.0"),
    "UVICORN_PORT": validators.uvicorn.port("UVICORN_PORT", 8000),
    "UVICORN_RELOAD": validators.common.boolean("UVICORN_RELOAD", False),
}

AUTHCFG = {
    # openssl rand -hex 32
    "JWT_SECRET": os.getenv("JWT_SECRET", "cda15d346583947f309aae95a0adf787e00ee2cdc1b073804bdfe3a4896ae1ae"),
    "JWT_ALGORITHM": "HS256",
    "JWT_EXPIRE_MINUTES": 60 * 24 * 7,
    "JWT_REFRESH_EXPIRE_MINUTES": 60 * 24 * 30,
}

STORECFG = {
    "REDIS_URL": validators.storage.redis_url("REDIS_URL", "redis://localhost:6379/0"),
    "REDIS_MAX_CONNECTIONS": int(os.getenv("REDIS_MAX_CONNECTIONS", 10)),
    "MONGO_URL": os.getenv("MONGO_URL", "mongodb://localhost:27017"),
    "MONGO_DB_NAME": os.getenv("MONGO_DB_NAME", "outreach_automation"),
    "MONGO_MAX_CONNECTIONS": int(os.getenv("MONGO_MAX_CONNECTIONS", 10)),
    "RABBIT_URL": os.getenv("RABBIT_URL", "amqp://guest:guest@localhost:5672/"),
}