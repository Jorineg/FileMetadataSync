"""Supabase REST API database backend (works over HTTPS/443)."""
from typing import Optional
from datetime import datetime
from supabase import create_client, Client

from src import settings
from src.logging_conf import logger


class RestDatabase:
    """Supabase REST API client."""

    def __init__(self):
        self._client: Optional[Client] = None
        self._connect()

    def _connect(self):
        try:
            self._client = create_client(settings.SUPABASE_URL, settings.SUPABASE_SERVICE_KEY)
            logger.info("Supabase REST client initialized")
        except Exception as e:
            logger.error(f"Failed to initialize Supabase client: {e}")
            raise

    def commit(self):
        # REST API auto-commits
        pass

    def rollback(self):
        # REST API doesn't support transactions
        pass

    def get_storage_object_id(self, bucket_id: str, object_name: str) -> Optional[str]:
        """Look up storage object ID by bucket and name."""
        try:
            result = self._client.from_("storage.objects").select("id").eq("bucket_id", bucket_id).eq("name", object_name).limit(1).execute()
            if result.data and len(result.data) > 0:
                return str(result.data[0]["id"])
            return None
        except Exception as e:
            # storage.objects might not be directly accessible via REST
            # Try using an RPC function instead
            logger.debug(f"Direct storage.objects query failed: {e}, trying RPC")
            try:
                result = self._client.rpc("get_storage_object_id", {
                    "p_bucket_id": bucket_id,
                    "p_object_name": object_name
                }).execute()
                if result.data:
                    return str(result.data)
                return None
            except Exception as e2:
                logger.warning(f"Failed to get storage object ID for {bucket_id}/{object_name}: {e2}")
                return None

    def _serialize_datetime(self, dt: Optional[datetime]) -> Optional[str]:
        """Convert datetime to ISO string for JSON."""
        if dt is None:
            return None
        return dt.isoformat()

    def upsert_file(self, file_data: dict) -> Optional[str]:
        """Insert or update file record by storage_object_id. Returns file ID."""
        storage_object_id = file_data.get('storage_object_id')
        if not storage_object_id:
            logger.warning("Cannot upsert file without storage_object_id")
            return None

        # Prepare data for REST API (serialize datetimes)
        data = {
            "storage_object_id": storage_object_id,
            "filename": file_data.get("filename"),
            "folder_path": file_data.get("folder_path"),
            "content_hash": file_data.get("content_hash"),
            "file_created_at": self._serialize_datetime(file_data.get("file_created_at")),
            "file_modified_at": self._serialize_datetime(file_data.get("file_modified_at")),
            "file_created_by": file_data.get("file_created_by"),
            "filesystem_inode": file_data.get("filesystem_inode"),
            "filesystem_access_rights": file_data.get("filesystem_access_rights"),
            "filesystem_attributes": file_data.get("filesystem_attributes"),
            "auto_extracted_metadata": file_data.get("auto_extracted_metadata"),
        }

        try:
            # Check for existing
            existing = self._client.from_("files").select("id").eq("storage_object_id", storage_object_id).limit(1).execute()

            if existing.data and len(existing.data) > 0:
                # Update
                file_id = existing.data[0]["id"]
                self._client.from_("files").update(data).eq("id", file_id).execute()
                return str(file_id)
            else:
                # Insert
                result = self._client.from_("files").insert(data).execute()
                if result.data and len(result.data) > 0:
                    return str(result.data[0]["id"])
                return None
        except Exception as e:
            logger.error(f"Failed to upsert file: {e}")
            return None

    def get_all_file_hashes(self) -> dict[str, str]:
        """
        Fetch all existing file hashes from DB in one query.
        Returns dict: {content_hash: storage_object_id}
        """
        try:
            # Paginate to handle large datasets
            all_hashes = {}
            page_size = 1000
            offset = 0
            
            while True:
                result = self._client.from_("files").select("content_hash,storage_object_id").range(offset, offset + page_size - 1).execute()
                if not result.data:
                    break
                for row in result.data:
                    if row.get("content_hash"):
                        all_hashes[row["content_hash"]] = row["storage_object_id"]
                if len(result.data) < page_size:
                    break
                offset += page_size
            
            logger.info(f"Loaded {len(all_hashes)} existing file hashes from DB")
            return all_hashes
        except Exception as e:
            logger.error(f"Failed to fetch file hashes: {e}")
            return {}

    def close(self):
        # REST client doesn't need explicit close
        logger.info("Supabase REST client closed")
        self._client = None

