"""Logging configuration with Betterstack support."""
import logging
import sys
from logtail import LogtailHandler
from src import settings

logger = logging.getLogger("file_metadata_sync")
logger.setLevel(getattr(logging, settings.LOG_LEVEL, logging.INFO))

detailed_formatter = logging.Formatter(
    fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(detailed_formatter)
logger.addHandler(console_handler)

log_file = settings.LOGS_DIR / "app.log"
file_handler = logging.FileHandler(log_file)
file_handler.setFormatter(detailed_formatter)
logger.addHandler(file_handler)

if settings.BETTERSTACK_SOURCE_TOKEN:
    try:
        handler_kwargs = {"source_token": settings.BETTERSTACK_SOURCE_TOKEN}
        if settings.BETTERSTACK_INGEST_HOST:
            handler_kwargs["host"] = settings.BETTERSTACK_INGEST_HOST
        betterstack_handler = LogtailHandler(**handler_kwargs)
        betterstack_handler.setLevel(logging.DEBUG)
        betterstack_handler.setFormatter(detailed_formatter)
        logger.addHandler(betterstack_handler)
        host_info = settings.BETTERSTACK_INGEST_HOST or "default (in.logs.betterstack.com)"
        logger.info(f"Betterstack logging enabled (host: {host_info})")
    except Exception as e:
        logger.warning(f"Failed to initialize Betterstack logging: {e}")
else:
    logger.info("Betterstack logging not configured (BETTERSTACK_SOURCE_TOKEN not set)")

logger.propagate = False

