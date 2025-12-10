#!/usr/bin/env python3
"""Run a single sync cycle then exit."""
import sys
from pathlib import Path

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src import settings
from src.logging_conf import logger
from src.sync import run_sync_cycle


def main():
    try:
        settings.validate_config()
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)

    logger.info("Starting single sync cycle...")
    uploaded, errors, duration = run_sync_cycle(
        settings.SYNC_SOURCE_PATHS,
        max_workers=settings.SYNC_WORKERS
    )

    if duration < 60:
        duration_str = f"{duration:.1f}s"
    else:
        duration_str = f"{duration/60:.1f}m"

    logger.info(f"Complete: {uploaded} uploaded, {errors} errors in {duration_str}")
    sys.exit(0 if errors == 0 else 1)


if __name__ == "__main__":
    main()
