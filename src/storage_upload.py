"""Upload files using Supabase Storage REST API."""
import os
import re
import unicodedata
from pathlib import Path
from typing import Optional
from supabase import create_client, Client
import time

from src import settings
from src.logging_conf import logger

# Character replacements for filename sanitization
CHAR_REPLACEMENTS = {
    'ä': 'ae', 'ö': 'oe', 'ü': 'ue', 'ß': 'ss',
    'Ä': 'Ae', 'Ö': 'Oe', 'Ü': 'Ue',
    '[': '(', ']': ')',
    '{': '(', '}': ')',
    '#': '_', '%': '_', '&': '_', '*': '_',
    '<': '_', '>': '_', '|': '_', '"': '_',
    '?': '_', '\\': '_', ':': '_',
}


def sanitize_filename(name: str) -> str:
    """
    Sanitize filename for S3/Supabase Storage compatibility.
    Replaces special characters while preserving readability.
    """
    # Apply explicit replacements first
    for char, replacement in CHAR_REPLACEMENTS.items():
        name = name.replace(char, replacement)
    
    # Normalize unicode (é -> e, ñ -> n, etc.)
    name = unicodedata.normalize('NFKD', name)
    name = ''.join(c for c in name if not unicodedata.combining(c))
    
    # Replace any remaining non-ASCII with underscore
    name = re.sub(r'[^\x00-\x7F]', '_', name)
    
    # Collapse multiple underscores
    name = re.sub(r'_+', '_', name)
    
    return name


def sanitize_path(path: str) -> str:
    """Sanitize entire storage path (preserves / separators)."""
    return "/".join(sanitize_filename(segment) for segment in path.split('/'))


class StorageUploader:
    """Upload files to Supabase Storage via REST API."""

    def __init__(self):
        self._client: Optional[Client] = None
        self._connect()

    def _connect(self):
        self._client = create_client(settings.SUPABASE_URL, settings.SUPABASE_SERVICE_KEY)
        logger.info("Supabase Storage client initialized")

    def ensure_bucket_exists(self, bucket_name: str) -> bool:
        """Create bucket if it doesn't exist."""
        try:
            buckets = self._client.storage.list_buckets()
            bucket_names = [b.name for b in buckets]
            if bucket_name in bucket_names:
                logger.debug(f"Bucket {bucket_name} exists")
                return True

            # Create bucket
            self._client.storage.create_bucket(bucket_name, options={"public": False})
            logger.info(f"Created bucket: {bucket_name}")
            return True
        except Exception as e:
            # Bucket might already exist
            if "already exists" in str(e).lower():
                return True
            logger.error(f"Failed to ensure bucket exists: {e}")
            return False

    def upload_file(self, local_path: Path, bucket: str, storage_path: str, retries: int = 3) -> Optional[str]:
        """
        Upload a file to Supabase Storage with retry logic.
        Returns the sanitized storage path on success, None on failure.
        """
        # Sanitize path for S3 compatibility (umlauts, brackets, accents)
        sanitized_path = sanitize_path(storage_path)
        
        for attempt in range(retries):
            try:
                with open(local_path, "rb") as f:
                    file_data = f.read()

                # Determine content type
                import mimetypes
                content_type, _ = mimetypes.guess_type(str(local_path))
                content_type = content_type or "application/octet-stream"

                # Upload with upsert to handle existing files
                result = self._client.storage.from_(bucket).upload(
                    sanitized_path,
                    file_data,
                    file_options={"content-type": content_type, "upsert": "true"}
                )

                logger.debug(f"Uploaded: {sanitized_path}")
                return sanitized_path  # Return sanitized path for DB
                
            except Exception as e:
                if attempt < retries - 1:
                    wait_time = 2 ** attempt  # Exponential backoff
                    logger.warning(f"Upload attempt {attempt + 1} failed for {local_path.name}: {e}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Failed to upload {local_path} after {retries} attempts: {e}")
                    return None

    def file_exists(self, bucket: str, storage_path: str) -> bool:
        """Check if file exists in storage."""
        try:
            folder = str(Path(storage_path).parent)
            if folder == ".":
                folder = ""
            filename = Path(storage_path).name

            files = self._client.storage.from_(bucket).list(folder)
            return any(f.get("name") == filename for f in files)
        except Exception as e:
            logger.debug(f"Error checking if file exists: {e}")
            return False

    def close(self):
        logger.debug("Supabase Storage client closed")
        self._client = None
