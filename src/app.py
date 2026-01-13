"""
FileMetadataSync - Hybrid mode: real-time watcher + daily full scan.

- Watcher: monitors filesystem for create/modify/move events (real-time)
- Scheduler: triggers full scan at configured hour (default 3 AM)
- Optional full scan on startup
- No external cron needed - everything runs inside the container
"""
import signal
import sys
import threading
import time
from datetime import datetime

from src import settings
from src.logging_conf import logger
from src.sync import run_full_scan, process_watcher_events
from src.watcher import FileWatcher


class App:
    def __init__(self):
        self.running = True
        self.watcher: FileWatcher | None = None
        self._last_full_scan_date: str | None = None
        self._is_scanning = False
        self._scan_lock = threading.Lock()

    def signal_handler(self, signum, frame):
        logger.info(f"Received signal {signum}, shutting down...")
        self.running = False

    def _on_watcher_events(self, events):
        """Callback when watcher has events ready to process."""
        if not events:
            return
        try:
            registered, updated, unchanged, errors = process_watcher_events(
                events, settings.SYNC_SOURCE_PATHS
            )
            if registered or updated or errors:
                logger.info(
                    f"Watcher batch: {registered} registered, {updated} updated, "
                    f"{unchanged} unchanged, {errors} errors"
                )
            if registered > 0:
                pass
        except Exception as e:
            logger.error(f"Error processing watcher events: {e}", exc_info=True)

    def _should_run_scheduled_scan(self) -> bool:
        """Check if it's time for daily scheduled full scan."""
        # Don't run if another scan is in progress
        if self._is_scanning:
            return False
        
        now = datetime.now()
        today = now.strftime("%Y-%m-%d")
        
        # Already ran today?
        if self._last_full_scan_date == today:
            return False
        
        # Is it the configured hour?
        return now.hour == settings.FULL_SCAN_HOUR

    def _run_full_scan(self, reason: str = "scheduled"):
        """Execute full scan with lock to prevent concurrent runs."""
        with self._scan_lock:
            if self._is_scanning:
                logger.debug("Scan already in progress, skipping")
                return
            self._is_scanning = True

        logger.info(f"Starting full scan ({reason})...")
        try:
            stats = run_full_scan(settings.SYNC_SOURCE_PATHS, settings.SYNC_WORKERS)
            self._last_full_scan_date = datetime.now().strftime("%Y-%m-%d")
            logger.info(
                f"Full scan complete: {stats.registered} registered, {stats.updated} updated, "
                f"{stats.skipped_unchanged} unchanged, {stats.soft_deleted} soft-deleted, "
                f"{stats.errors} errors in {stats.duration_human}"
            )
            if stats.registered > 0:
                pass
        except Exception as e:
            logger.error(f"Full scan failed: {e}", exc_info=True)
        finally:
            self._is_scanning = False

    def _start_uploader(self):
        """Run uploader in a separate thread with crash recovery."""
        from src.uploader import Uploader
        uploader = Uploader()
        while self.running:
            try:
                uploader.run()
            except Exception as e:
                logger.error(f"Uploader thread crashed: {e}")
                time.sleep(10)

    def run(self):
        """Main run loop."""
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

        try:
            settings.validate_config()
        except ValueError as e:
            logger.error(f"Configuration error: {e}")
            sys.exit(1)

        logger.info("FileMetadataSync starting (CAS Hybrid Mode)")
        logger.info(f"Source paths: {settings.SYNC_SOURCE_PATHS}")
        logger.info(f"Bucket: {settings.S3_BUCKET}")
        logger.info(f"Workers: {settings.SYNC_WORKERS}")
        logger.info(f"Full scan hour: {settings.FULL_SCAN_HOUR}:00")

        try:
            # Start uploader thread
            uploader_thread = threading.Thread(target=self._start_uploader, daemon=True)
            uploader_thread.start()

            # Optional initial full scan
            if settings.FULL_SCAN_ON_STARTUP:
                self._run_full_scan(reason="startup")
            else:
                logger.info("Skipping startup full scan (FULL_SCAN_ON_STARTUP=false)")

            # Start watcher
            self.watcher = FileWatcher(
                source_paths=settings.SYNC_SOURCE_PATHS,
                on_events_ready=self._on_watcher_events,
                debounce_seconds=settings.DEBOUNCE_SECONDS,
                ignore_patterns=settings.IGNORE_PATTERNS
            )
            self.watcher.start()

            # Main loop - check for scheduled full scan
            while self.running:
                if self._should_run_scheduled_scan():
                    self._run_full_scan(reason="scheduled")
                time.sleep(60)  # Check every minute

        except Exception as e:
            logger.error(f"Fatal error: {e}", exc_info=True)
        finally:
            if self.watcher:
                self.watcher.stop()
            logger.info("FileMetadataSync stopped")


def main():
    app = App()
    app.run()


if __name__ == "__main__":
    main()
