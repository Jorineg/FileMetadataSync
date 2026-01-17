"""PostgREST client for database operations."""
import httpx
from typing import Optional
from datetime import datetime, timezone

from src import settings
from src.logging_conf import logger

# Timeout for DB operations
CLIENT_TIMEOUT = 120.0


class PostgRESTClient:
    """HTTP client for PostgREST API with FMS service authentication."""
    
    def __init__(self):
        self.base_url = settings.POSTGREST_URL
        self.headers = {
            "X-API-Key": settings.FMS_SERVICE_SECRET,
            "Content-Type": "application/json",
            "Prefer": "return=representation",
        }
        self._client = httpx.Client(timeout=CLIENT_TIMEOUT)
    
    def close(self):
        """Close HTTP client."""
        self._client.close()
    
    # ========================================
    # File Operations
    # ========================================
    
    def fetch_path_map(self) -> dict[str, str]:
        """Fetch all existing file paths from DB. Returns: {full_path: content_hash}"""
        path_map = {}
        page_size = 2000
        offset = 0
        
        while True:
            try:
                url = f"{self.base_url}/files"
                params = {
                    "select": "full_path,content_hash",
                    "order": "id",  # Stable ordering for pagination
                    "offset": offset,
                    "limit": page_size,
                }
                response = self._client.get(url, headers=self.headers, params=params)
                response.raise_for_status()
                data = response.json()
                
                if not data:
                    break
                for row in data:
                    path_map[row["full_path"]] = row["content_hash"]
                if len(data) < page_size:
                    break
                offset += page_size
            except Exception as e:
                logger.error(f"Failed to fetch path map (offset={offset}): {e}")
                break
        
        return path_map
    
    def upsert_file_contents(self, content_hash: str, size_bytes: int, mime_type: Optional[str]) -> bool:
        """Upsert into file_contents table."""
        try:
            url = f"{self.base_url}/file_contents"
            headers = {**self.headers, "Prefer": "resolution=merge-duplicates"}
            now = datetime.now(timezone.utc).isoformat()
            data = {
                "content_hash": content_hash,
                "size_bytes": size_bytes,
                "mime_type": mime_type,
                "db_updated_at": now,
            }
            response = self._client.post(url, headers=headers, json=data)
            response.raise_for_status()
            return True
        except Exception as e:
            logger.error(f"Failed to upsert file_contents {content_hash}: {e}")
            return False
    
    def upsert_file(self, file_record: dict) -> bool:
        """Upsert into files table."""
        try:
            url = f"{self.base_url}/files"
            headers = {**self.headers, "Prefer": "resolution=merge-duplicates"}
            # Use full_path for conflict detection (not the auto-generated id PK)
            params = {"on_conflict": "full_path"}
            response = self._client.post(url, headers=headers, params=params, json=file_record)
            response.raise_for_status()
            return True
        except Exception as e:
            # Log response body for debugging auth issues
            resp_body = getattr(getattr(e, 'response', None), 'text', 'no response body')
            logger.error(f"Failed to upsert file {file_record.get('full_path')}: {e} | Response: {resp_body[:500]}")
            return False
    
    def update_last_seen(self, full_path: str) -> bool:
        """Update last_seen_at for a file."""
        try:
            url = f"{self.base_url}/files"
            params = {"full_path": f"eq.{full_path}"}
            now = datetime.now(timezone.utc).isoformat()
            data = {"last_seen_at": now}
            response = self._client.patch(url, headers=self.headers, params=params, json=data)
            response.raise_for_status()
            return True
        except Exception as e:
            logger.warning(f"Failed to update last_seen_at for {full_path}: {e}")
            return False
    
    def mark_deleted(self, path_prefix: str, before_timestamp: str) -> int:
        """Soft-delete files not seen since timestamp."""
        try:
            url = f"{self.base_url}/files"
            params = {
                "last_seen_at": f"lt.{before_timestamp}",
                "deleted_at": "is.null",
                "full_path": f"like.{path_prefix}*",
            }
            now = datetime.now(timezone.utc).isoformat()
            data = {"deleted_at": now}
            response = self._client.patch(url, headers=self.headers, params=params, json=data)
            response.raise_for_status()
            result = response.json()
            return len(result) if result else 0
        except Exception as e:
            logger.error(f"Failed to soft-delete files for {path_prefix}: {e}")
            return 0
    
    # ========================================
    # Upload Queue Operations (RPC)
    # ========================================
    
    def dequeue_upload_batch(self, batch_size: int, path_prefixes: list[str]) -> list[dict]:
        """Dequeue files pending upload."""
        try:
            url = f"{self.base_url}/rpc/dequeue_upload_batch"
            data = {"p_batch_size": batch_size, "p_path_prefixes": path_prefixes}
            response = self._client.post(url, headers=self.headers, json=data)
            response.raise_for_status()
            return response.json() or []
        except Exception as e:
            logger.error(f"Failed to dequeue upload batch: {e}")
            return []
    
    def mark_upload_complete(self, content_hash: str, storage_path: str, mime_type: str) -> bool:
        """Mark upload as complete."""
        try:
            url = f"{self.base_url}/rpc/mark_upload_complete"
            data = {"p_hash": content_hash, "p_storage_path": storage_path, "p_mime_type": mime_type}
            response = self._client.post(url, headers=self.headers, json=data)
            response.raise_for_status()
            return True
        except Exception as e:
            logger.error(f"Failed to mark upload complete {content_hash}: {e}")
            return False
    
    def mark_upload_failed(self, content_hash: str, error: str) -> bool:
        """Mark upload as failed."""
        try:
            url = f"{self.base_url}/rpc/mark_upload_failed"
            data = {"p_hash": content_hash, "p_error": error[:500]}
            response = self._client.post(url, headers=self.headers, json=data)
            response.raise_for_status()
            return True
        except Exception as e:
            logger.error(f"Failed to mark upload failed {content_hash}: {e}")
            return False
    
    def mark_upload_skipped(self, content_hash: str, reason: str) -> bool:
        """Mark upload as skipped (permanent)."""
        try:
            url = f"{self.base_url}/rpc/mark_upload_skipped"
            data = {"p_hash": content_hash, "p_reason": reason[:500]}
            response = self._client.post(url, headers=self.headers, json=data)
            response.raise_for_status()
            return True
        except Exception as e:
            logger.error(f"Failed to mark upload skipped {content_hash}: {e}")
            return False
    
    def reset_stuck_uploads(self) -> int:
        """Reset uploads stuck in 'uploading' state."""
        try:
            url = f"{self.base_url}/rpc/reset_stuck_uploads"
            response = self._client.post(url, headers=self.headers, json={})
            response.raise_for_status()
            return response.json() or 0
        except Exception as e:
            logger.warning(f"Failed to reset stuck uploads: {e}")
            return 0


# Module-level client instance
_client: Optional[PostgRESTClient] = None


def get_postgrest_client() -> PostgRESTClient:
    """Get or create PostgREST client."""
    global _client
    if _client is None:
        _client = PostgRESTClient()
    return _client

