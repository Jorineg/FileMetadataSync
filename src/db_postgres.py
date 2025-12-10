"""Direct PostgreSQL database backend (requires port 5432 access)."""
import time
from typing import Optional
from contextlib import contextmanager
import psycopg2
from psycopg2 import OperationalError, InterfaceError
import psycopg2.extras

from src import settings
from src.logging_conf import logger


def is_connection_error(exc: Exception) -> bool:
    """Check if exception indicates a connection problem."""
    if isinstance(exc, (OperationalError, InterfaceError)):
        return True
    error_msg = str(exc).lower()
    indicators = ['connection', 'server closed', 'network', 'timeout', 'could not connect',
                  'terminating connection', 'connection refused', 'no route to host',
                  'connection reset', 'broken pipe', 'ssl connection', 'server unexpectedly closed']
    return any(ind in error_msg for ind in indicators)


class PostgresDatabase:
    """PostgreSQL direct connection with auto-reconnection."""

    def __init__(self):
        self._conn: Optional[psycopg2.extensions.connection] = None
        self._connection_valid = False
        self._connect()

    def _connect(self) -> bool:
        delay = settings.DB_RECONNECT_DELAY
        while True:
            try:
                if self._conn:
                    try:
                        self._conn.close()
                    except Exception:
                        pass
                self._conn = psycopg2.connect(dsn=settings.PG_DSN, connect_timeout=settings.DB_CONNECT_TIMEOUT)
                self._connection_valid = True
                logger.info("PostgreSQL direct connection established")
                return True
            except Exception as e:
                self._connection_valid = False
                logger.warning(f"Failed to connect to PostgreSQL: {e}. Retrying in {delay}s...")
                time.sleep(delay)
                delay = min(delay * 2, settings.DB_MAX_RECONNECT_DELAY)

    def _ensure_connected(self):
        if self._connection_valid and self._conn:
            try:
                with self._conn.cursor() as cur:
                    cur.execute("SELECT 1")
                return
            except Exception as e:
                logger.warning(f"Connection test failed: {e}")
                self._connection_valid = False
        logger.info("Reconnecting to PostgreSQL...")
        self._connect()

    @contextmanager
    def get_cursor(self):
        self._ensure_connected()
        cur = self._conn.cursor()
        try:
            yield cur
        finally:
            cur.close()

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def get_storage_object_id(self, bucket_id: str, object_name: str) -> Optional[str]:
        """Look up storage object ID by bucket and name."""
        with self.get_cursor() as cur:
            cur.execute("""
                SELECT id FROM storage.objects
                WHERE bucket_id = %s AND name = %s
            """, (bucket_id, object_name))
            row = cur.fetchone()
            return str(row[0]) if row else None

    def upsert_file(self, file_data: dict) -> Optional[str]:
        """Insert or update file record by storage_object_id. Returns file ID."""
        with self.get_cursor() as cur:
            storage_object_id = file_data.get('storage_object_id')
            if not storage_object_id:
                logger.warning("Cannot upsert file without storage_object_id")
                return None

            cur.execute("SELECT id FROM files WHERE storage_object_id = %s", (storage_object_id,))
            existing = cur.fetchone()

            if existing:
                cur.execute("""
                    UPDATE files SET
                        filename = %s,
                        folder_path = %s,
                        content_hash = %s,
                        file_created_at = %s,
                        file_modified_at = %s,
                        file_created_by = %s,
                        filesystem_inode = %s,
                        filesystem_access_rights = %s,
                        filesystem_attributes = %s,
                        auto_extracted_metadata = %s,
                        db_updated_at = NOW()
                    WHERE id = %s
                    RETURNING id
                """, (
                    file_data.get('filename'),
                    file_data.get('folder_path'),
                    file_data.get('content_hash'),
                    file_data.get('file_created_at'),
                    file_data.get('file_modified_at'),
                    file_data.get('file_created_by'),
                    file_data.get('filesystem_inode'),
                    psycopg2.extras.Json(file_data.get('filesystem_access_rights')),
                    psycopg2.extras.Json(file_data.get('filesystem_attributes')),
                    psycopg2.extras.Json(file_data.get('auto_extracted_metadata')),
                    existing[0]
                ))
                return str(cur.fetchone()[0])

            cur.execute("""
                INSERT INTO files (
                    storage_object_id, filename, folder_path, content_hash,
                    file_created_at, file_modified_at, file_created_by,
                    filesystem_inode, filesystem_access_rights,
                    filesystem_attributes, auto_extracted_metadata
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                storage_object_id,
                file_data.get('filename'),
                file_data.get('folder_path'),
                file_data.get('content_hash'),
                file_data.get('file_created_at'),
                file_data.get('file_modified_at'),
                file_data.get('file_created_by'),
                file_data.get('filesystem_inode'),
                psycopg2.extras.Json(file_data.get('filesystem_access_rights')),
                psycopg2.extras.Json(file_data.get('filesystem_attributes')),
                psycopg2.extras.Json(file_data.get('auto_extracted_metadata'))
            ))
            return str(cur.fetchone()[0])

    def get_all_file_hashes(self) -> dict[str, str]:
        """
        Fetch all existing file hashes from DB in one query.
        Returns dict: {content_hash: storage_object_id}
        """
        with self.get_cursor() as cur:
            cur.execute("SELECT content_hash, storage_object_id FROM files WHERE content_hash IS NOT NULL")
            rows = cur.fetchall()
            result = {row[0]: str(row[1]) for row in rows}
            logger.info(f"Loaded {len(result)} existing file hashes from DB")
            return result

    def close(self):
        if self._conn:
            try:
                self._conn.close()
                logger.info("PostgreSQL connection closed")
            except Exception as e:
                logger.warning(f"Error closing connection: {e}")
            finally:
                self._conn = None
                self._connection_valid = False

