import time
import mimetypes
from pathlib import Path
from src import settings
from src.logging_conf import logger
from src.sync import get_supabase_client

# IMPORTANT: Memory limit for NAS devices (Synology DS923+ has limited RAM).
# Files are loaded entirely into memory before upload. Exceeding this risks OOM.
# Large files should use streaming upload (not yet implemented).
MAX_UPLOAD_SIZE_MB = 100
MAX_UPLOAD_SIZE_BYTES = MAX_UPLOAD_SIZE_MB * 1024 * 1024


class Uploader:
    def __init__(self):
        self.running = True
        self.client = None

    def run(self):
        """Infinite loop to process the upload queue."""
        logger.info("Uploader thread started")
        while self.running:
            try:
                if not self.client:
                    self.client = get_supabase_client()
                
                result = self.client.rpc("dequeue_upload_batch", {
                    "p_batch_size": 5,
                    "p_path_prefixes": settings.SYNC_SOURCE_PATHS
                }).execute()
                batch = result.data or []
                
                if not batch:
                    time.sleep(10)
                    continue
                
                for item in batch:
                    if not self.running:
                        break
                    self.process_upload(item)
                    
            except Exception as e:
                logger.error(f"Uploader loop error: {e}")
                time.sleep(10)
                self.client = None

    def process_upload(self, item):
        content_hash = item["content_hash"]
        full_path = item["full_path"]
        size_bytes = item.get("size_bytes", 0)
        local_path = Path(full_path)
        
        try:
            # Check file exists
            if not local_path.exists():
                self._mark_failed(content_hash, "File missing on disk")
                logger.warning(f"⊘ Missing: {local_path.name}")
                return

            # Check file size - skip files too large for memory upload
            actual_size = local_path.stat().st_size
            if actual_size > MAX_UPLOAD_SIZE_BYTES:
                size_mb = actual_size / 1024 / 1024
                reason = f"File too large: {size_mb:.0f}MB (max {MAX_UPLOAD_SIZE_MB}MB)"
                self._mark_skipped(content_hash, reason)
                logger.info(f"⊘ Skipped: {local_path.name} - {reason}")
                return

            content_type, _ = mimetypes.guess_type(str(local_path))
            
            with open(local_path, 'rb') as f:
                file_data = f.read()
            
            # Content-addressable storage: just the hash, no extension
            self.client.storage.from_(settings.S3_BUCKET).upload(
                path=content_hash,
                file=file_data,
                file_options={"content-type": content_type or "application/octet-stream", "upsert": "true"}
            )
            
            self.client.rpc("mark_upload_complete", {
                "p_hash": content_hash,
                "p_storage_path": content_hash,
                "p_mime_type": content_type or "application/octet-stream"
            }).execute()
            
            logger.info(f"✓ Uploaded: {content_hash[:10]}... ({len(file_data) / 1024 / 1024:.1f}MB, {local_path.name})")

        except Exception as e:
            error_msg = str(e)[:500]
            self._mark_failed(content_hash, error_msg)
            logger.error(f"✗ Failed: {content_hash[:10]}... - {error_msg}")

    def _mark_failed(self, content_hash: str, error: str):
        """Mark upload as failed (will be retried)."""
        try:
            self.client.rpc("mark_upload_failed", {"p_hash": content_hash, "p_error": error}).execute()
        except Exception:
            pass

    def _mark_skipped(self, content_hash: str, reason: str):
        """Mark upload as skipped (permanent, no retry)."""
        try:
            self.client.rpc("mark_upload_skipped", {"p_hash": content_hash, "p_reason": reason}).execute()
        except Exception:
            pass

    def reset_stuck_uploads(self):
        """Reset any uploads stuck in 'uploading' state (called on startup)."""
        try:
            if not self.client:
                self.client = get_supabase_client()
            result = self.client.rpc("reset_stuck_uploads").execute()
            count = result.data if result.data else 0
            if count:
                logger.info(f"Reset {count} stuck uploads on startup")
        except Exception as e:
            logger.warning(f"Could not reset stuck uploads: {e}")
