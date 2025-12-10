"""Main application entry point."""
import time
import signal
import sys
from pathlib import Path

from src import settings
from src.logging_conf import logger
from src.db import Database
from src.sync import sync_source, full_scan_metadata


class App:
    def __init__(self):
        self.running = True
        self.db: Database = None

    def signal_handler(self, signum, frame):
        logger.info(f"Received signal {signum}, shutting down...")
        self.running = False

    def run_sync_cycle(self):
        """Run one sync cycle for all source paths."""
        total_processed = 0
        total_errors = 0

        for source_path_str in settings.SYNC_SOURCE_PATHS:
            source_path = Path(source_path_str)
            if not source_path.exists():
                logger.warning(f"Source path does not exist, skipping: {source_path}")
                continue

            logger.info(f"Syncing source: {source_path}")
            processed, errors = sync_source(self.db, source_path)
            total_processed += processed
            total_errors += errors

        return total_processed, total_errors

    def run_full_scan(self):
        """Run full metadata scan for all source paths."""
        total_processed = 0
        total_errors = 0

        for source_path_str in settings.SYNC_SOURCE_PATHS:
            source_path = Path(source_path_str)
            if not source_path.exists():
                logger.warning(f"Source path does not exist, skipping: {source_path}")
                continue

            logger.info(f"Full scan: {source_path}")
            processed, errors = full_scan_metadata(self.db, source_path)
            total_processed += processed
            total_errors += errors

        return total_processed, total_errors

    def run(self):
        """Main run loop."""
        # Setup signal handlers
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

        # Validate config
        try:
            settings.validate_config()
        except ValueError as e:
            logger.error(f"Configuration error: {e}")
            sys.exit(1)

        logger.info("FileMetadataSync starting")
        logger.info(f"Source paths: {settings.SYNC_SOURCE_PATHS}")
        logger.info(f"S3 bucket: {settings.S3_BUCKET}")
        logger.info(f"Sync interval: {settings.SYNC_INTERVAL}s")

        # Connect to database
        self.db = Database()

        try:
            while self.running:
                try:
                    processed, errors = self.run_sync_cycle()
                    logger.info(f"Sync cycle complete: {processed} files processed, {errors} errors")
                except Exception as e:
                    logger.error(f"Error in sync cycle: {e}")

                # Wait for next cycle
                if self.running:
                    logger.debug(f"Sleeping for {settings.SYNC_INTERVAL}s")
                    for _ in range(settings.SYNC_INTERVAL):
                        if not self.running:
                            break
                        time.sleep(1)
        finally:
            if self.db:
                self.db.close()
            logger.info("FileMetadataSync stopped")


def main():
    app = App()
    app.run()


if __name__ == "__main__":
    main()

