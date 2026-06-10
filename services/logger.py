# - outreach_automation_api/services/logger.py -

import structlog
import logging

def get_log_level(level: str) -> int:
    return getattr(logging, level)

def configure_logging(log_level: str, dev: bool = False) -> None:
    level = get_log_level(log_level)
    
    shared_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
    ]

    processors = shared_processors + [
        structlog.dev.ConsoleRenderer() if dev else structlog.processors.JSONRenderer()
    ]

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
    )

    logging.basicConfig(level=level)

ECHO: structlog.stdlib.BoundLogger = structlog.get_logger("ch355")