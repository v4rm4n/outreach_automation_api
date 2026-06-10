# - outreach_automation_api/config/validators/uvicorn.py

import os

def host(key: str, default: str) -> str:
    import socket
    val = os.getenv(key, default)
    try:
        socket.inet_pton(socket.AF_INET, val)
        return val
    except OSError:
        pass
    try:
        socket.inet_pton(socket.AF_INET6, val)
        return val
    except OSError:
        pass
    raise ValueError(f"{key}={val!r} is not a valid IPv4 or IPv6 address")

def port(key: str, default: int) -> int:
    val = os.getenv(key, str(default))
    try:
        port = int(val)
    except ValueError:
        raise ValueError(f"{key}={val!r} is not a valid integer")
    if not (1 <= port <= 65535):
        raise ValueError(f"{key}={port} out of range, must be 1-65535")
    return port
