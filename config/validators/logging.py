# - outreach_automation_api/config/validators/logging.py -

import os

def log_level(key: str, default: str) -> str:
    valid = {"DEBUG", "INFO", "WARNING", "CRITICAL"}
    val = os.getenv(key, default).upper()
    if val not in valid:
        raise ValueError(f"{key}={val!r} invalid, must be one of {valid}")
    return val