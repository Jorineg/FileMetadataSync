import time
import mimetypes
from pathlib import Path
from src import settings
from src.logging_conf import logger
from src.sync import get_supabase_client

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
                
                # Fetch a batch of pending uploads including the local path we need
                # dequeue_upload_batch is atomic and marks items as 'uploading'
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
                self.client = None # Force client recreation

    def process_upload(self, item):
        content_hash = item["content_hash"]
        full_path = item["full_path"]
        local_path = Path(full_path)
        
        try:
            if not local_path.exists():
                logger.warning(f"Physical file missing at {full_path} for hash {content_hash}")
                self.client.rpc("mark_upload_failed", {"p_hash": content_hash, "p_error": "Physical file missing"}).execute()
                return

            # Prepare storage path (Content-Addressable)
            extension = local_path.suffix.lower()
            storage_path = f"files/{content_hash}{extension}"
            
            # Check if file already exists in storage (fail-safe)
            # though the 'uploaded' status should normally prevent reaching here
            
            with open(local_path, 'rb') as f:
                file_data = f.read()
            
            content_type, _ = mimetypes.guess_type(str(local_path))
            
            # Perform upload to storage
            self.client.storage.from_(settings.S3_BUCKET).upload(
                path=storage_path,
                file=file_data,
                file_options={"content-type": content_type or "application/octet-stream", "upsert": "true"}
            )
            
            # Update DB status
            self.client.rpc("mark_upload_complete", {
                "p_hash": content_hash,
                "p_storage_path": storage_path,
                "p_mime_type": content_type or "application/octet-stream"
            }).execute()
            
            logger.info(f"✓ Uploaded content: {content_hash[:10]}... (from {local_path.name})")

        except Exception as e:
            error_msg = str(e)
            logger.error(f"✗ Failed to upload {content_hash[:10]}: {error_msg}")
            try:
                self.client.rpc("mark_upload_failed", {"p_hash": content_hash, "p_error": error_msg}).execute()
            except Exception:
                pass
