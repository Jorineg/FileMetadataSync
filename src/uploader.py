"""S3 uploader with PostgREST queue management."""
import time
import mimetypes
from pathlib import Path

from supabase import create_client, ClientOptions

from src import settings
from src.logging_conf import logger
from src.postgrest import PostgRESTClient

# Memory limit for NAS devices
MAX_UPLOAD_SIZE_MB = 100
MAX_UPLOAD_SIZE_BYTES = MAX_UPLOAD_SIZE_MB * 1024 * 1024


def get_storage_client():
    """
    Get Supabase client for S3 storage operations.
    
    TODO: This uses service_role key which has full DB access.
    Options to fix:
    1. Direct S3/MinIO access with storage-only credentials
    2. Pre-signed URL generation from server
    3. Storage-only JWT token
    
    For now, we use this for storage ONLY - DB operations go through PostgREST.
    """
    return create_client(
        settings.SUPABASE_URL,
        settings.SUPABASE_SERVICE_KEY,
        options=ClientOptions(storage_client_timeout=120)
    )


class Uploader:
    def __init__(self):
        self.running = True
        self.db = None  # PostgREST client for DB operations
        self.storage = None  # Supabase client for S3 storage only

    def run(self):
        """Infinite loop to process the upload queue."""
        logger.info("Uploader thread started")
        while self.running:
            try:
                if not self.db:
                    self.db = PostgRESTClient()
                if not self.storage:
                    self.storage = get_storage_client()
                
                batch = self.db.dequeue_upload_batch(5, settings.SYNC_SOURCE_PATHS)
                
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
                self.db = None
                self.storage = None

    def process_upload(self, item):
        content_hash = item["content_hash"]
        full_path = item["full_path"]
        local_path = Path(full_path)
        
        try:
            # Check file exists
            if not local_path.exists():
                self.db.mark_upload_failed(content_hash, "File missing on disk")
                logger.warning(f"⊘ Missing: {local_path.name}")
                return

            # Check file size
            actual_size = local_path.stat().st_size
            if actual_size > MAX_UPLOAD_SIZE_BYTES:
                size_mb = actual_size / 1024 / 1024
                reason = f"File too large: {size_mb:.0f}MB (max {MAX_UPLOAD_SIZE_MB}MB)"
                self.db.mark_upload_skipped(content_hash, reason)
                logger.info(f"⊘ Skipped: {local_path.name} - {reason}")
                return

            content_type, _ = mimetypes.guess_type(str(local_path))
            
            with open(local_path, 'rb') as f:
                file_data = f.read()
            
            # Upload to S3 storage (uses Supabase client)
            self.storage.storage.from_(settings.S3_BUCKET).upload(
                path=content_hash,
                file=file_data,
                file_options={"content-type": content_type or "application/octet-stream", "upsert": "true"}
            )
            
            # Mark complete via PostgREST
            self.db.mark_upload_complete(content_hash, content_hash, content_type or "application/octet-stream")
            
            logger.info(f"✓ Uploaded: {content_hash[:10]}... ({len(file_data) / 1024 / 1024:.1f}MB, {local_path.name})")

        except Exception as e:
            error_msg = str(e)[:500]
            self.db.mark_upload_failed(content_hash, error_msg)
            logger.error(f"✗ Failed: {content_hash[:10]}... - {error_msg}")

    def reset_stuck_uploads(self):
        """Reset any uploads stuck in 'uploading' state (called on startup)."""
        try:
            if not self.db:
                self.db = PostgRESTClient()
            count = self.db.reset_stuck_uploads()
            if count:
                logger.info(f"Reset {count} stuck uploads on startup")
        except Exception as e:
            logger.warning(f"Could not reset stuck uploads: {e}")
