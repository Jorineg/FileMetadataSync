#!/usr/bin/env python3
"""Run a full metadata scan without rclone sync."""
import sys
from pathlib import Path

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src import settings
from src.logging_conf import logger
from src.db import Database
from src.sync import full_scan_metadata


def main():
    # Validate config
    try:
        settings.validate_config()
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)

    db = Database()
    total_processed = 0
    total_errors = 0

    try:
        for source_path_str in settings.SYNC_SOURCE_PATHS:
            source_path = Path(source_path_str)
            if not source_path.exists():
                logger.warning(f"Source path does not exist: {source_path}")
                continue

            processed, errors = full_scan_metadata(db, source_path)
            total_processed += processed
            total_errors += errors
    finally:
        db.close()

    logger.info(f"Total: {total_processed} processed, {total_errors} errors")


if __name__ == "__main__":
    main()

