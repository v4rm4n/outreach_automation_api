# - outreach_automation_api/config/validators/storage.py -

import os
import re

def redis_url(key: str, default: str) -> str:
    val = os.getenv(key, default)
    pattern = r"^rediss?://(?:[^:@]*(?::[^@]*)?@)?[^:/]+(:\d+)?(/\d+)?$"
    if not re.match(pattern, val):
        raise ValueError(f"{key}={val!r} is not a valid Redis URL, expected redis://[:password@]host[:port][/db]")
    return val