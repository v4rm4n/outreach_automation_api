# - outreach_automation_api/config/validators/common.py

import os

def boolean(key: str, default: bool) -> bool:
    valid_true = {"true", "1", "yes"}
    valid_false = {"false", "0", "no"}
    val = os.getenv(key, str(default)).lower()
    if val in valid_true:
        return True
    if val in valid_false:
        return False
    raise ValueError(f"{key}={val!r} invalid, must be one of {valid_true | valid_false}")