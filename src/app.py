"""Main application entry point."""
import time
import signal
import sys

from src import settings
from src.logging_conf import logger
from src.sync import run_sync_cycle


class App:
    def __init__(self):
        self.running = True

    def signal_handler(self, signum, frame):
        logger.info(f"Received signal {signum}, shutting down...")
        self.running = False

    def run(self):
        """Main run loop."""
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

        # Validate config
        try:
            settings.validate_config()
        except ValueError as e:
            logger.error(f"Configuration error: {e}")
            sys.exit(1)

        # Get worker count
        max_workers = settings.SYNC_WORKERS

        logger.info("FileMetadataSync starting")
        logger.info(f"Source paths: {settings.SYNC_SOURCE_PATHS}")
        logger.info(f"Bucket: {settings.S3_BUCKET}")
        logger.info(f"Workers: {max_workers}")
        logger.info(f"Sync interval: {settings.SYNC_INTERVAL}s")

        try:
            while self.running:
                try:
                    uploaded, errors, duration = run_sync_cycle(
                        settings.SYNC_SOURCE_PATHS,
                        max_workers=max_workers
                    )
                    
                    # Human-readable duration
                    if duration < 60:
                        duration_str = f"{duration:.1f}s"
                    elif duration < 3600:
                        duration_str = f"{duration/60:.1f}m"
                    else:
                        duration_str = f"{duration/3600:.1f}h"

                    logger.info(
                        f"Sync complete: {uploaded} uploaded, {errors} errors "
                        f"in {duration_str}"
                    )

                except Exception as e:
                    logger.error(f"Error in sync cycle: {e}", exc_info=True)

                # Wait for next cycle (interruptible)
                if self.running:
                    logger.debug(f"Sleeping for {settings.SYNC_INTERVAL}s")
                    for _ in range(settings.SYNC_INTERVAL):
                        if not self.running:
                            break
                        time.sleep(1)

        except Exception as e:
            logger.error(f"Fatal error: {e}", exc_info=True)
        finally:
            logger.info("FileMetadataSync stopped")


def main():
    app = App()
    app.run()


if __name__ == "__main__":
    main()
