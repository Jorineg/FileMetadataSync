"""
File sync with single-phase workers for maximum efficiency.

Architecture:
- Load existing hashes from DB upfront
- Workers process files independently (hash → compare → upload → insert)
- Each worker has own Supabase client (thread-safe)
- UUID-based storage paths (no sanitization needed)
- Single file read for unchanged files (hash only)
- Compensating transaction on DB failure (delete from storage)
"""
import os
import time
import uuid
import hashlib
from pathlib import Path
from dataclasses import dataclass
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from supabase import create_client

from src import settings
from src.logging_conf import logger


# Constants
HASH_CHUNK_SIZE = 65536  # 64KB chunks for streaming hash
DEFAULT_WORKERS = 6  # Good for NAS with 4 threads


@dataclass
class SyncStats:
    """Statistics for a sync run."""
    total_files: int = 0
    skipped_unchanged: int = 0
    uploaded: int = 0
    errors: int = 0
    start_time: float = 0
    end_time: float = 0

    @property
    def duration_seconds(self) -> float:
        return self.end_time - self.start_time

    @property
    def duration_human(self) -> str:
        d = self.duration_seconds
        if d < 60:
            return f"{d:.1f}s"
        elif d < 3600:
            return f"{d/60:.1f}m"
        else:
            return f"{d/3600:.1f}h"


def compute_hash_streaming(filepath: Path) -> Optional[str]:
    """Compute SHA256 hash without loading entire file into memory."""
    try:
        hasher = hashlib.sha256()
        with open(filepath, 'rb') as f:
            for chunk in iter(lambda: f.read(HASH_CHUNK_SIZE), b''):
                hasher.update(chunk)
        return hasher.hexdigest()
    except Exception as e:
        logger.warning(f"Failed to hash {filepath}: {e}")
        return None


def extract_file_metadata(filepath: Path, source_base: Path) -> dict:
    """Extract metadata using stat() - no file read needed."""
    try:
        st = os.stat(filepath)
    except Exception as e:
        logger.error(f"Failed to stat {filepath}: {e}")
        return {}

    # Relative path for folder_path
    try:
        rel_path = filepath.relative_to(source_base)
        if rel_path.parent != Path("."):
            folder_path = f"{source_base.name}/{rel_path.parent}"
        else:
            folder_path = source_base.name
    except ValueError:
        folder_path = str(filepath.parent)

    # Filesystem attributes
    import stat as stat_module
    mode = st.st_mode
    
    fs_attributes = {
        "size_bytes": st.st_size,
        "mode_octal": oct(mode)[-3:],
        "uid": st.st_uid,
        "gid": st.st_gid,
        "is_symlink": filepath.is_symlink(),
    }

    # Auto-extracted metadata
    import mimetypes
    mime_type, _ = mimetypes.guess_type(str(filepath))
    
    auto_metadata = {
        "mime_type": mime_type,
        "extension": filepath.suffix.lower() if filepath.suffix else None,
        "original_filename": filepath.name,
        "original_path": str(filepath),
        "source_base": str(source_base),
    }

    return {
        "filename": filepath.name,
        "folder_path": folder_path,
        "file_created_at": datetime.fromtimestamp(st.st_ctime, tz=timezone.utc).isoformat(),
        "file_modified_at": datetime.fromtimestamp(st.st_mtime, tz=timezone.utc).isoformat(),
        "filesystem_attributes": fs_attributes,
        "auto_extracted_metadata": auto_metadata,
    }


class FileWorker:
    """
    Worker that processes files independently.
    Each worker has its own Supabase client (thread-safe).
    """

    def __init__(self, existing_hashes: set, bucket: str, source_base: Path):
        self.existing_hashes = existing_hashes
        self.bucket = bucket
        self.source_base = source_base
        self._client = create_client(settings.SUPABASE_URL, settings.SUPABASE_SERVICE_KEY)

    def process_file(self, filepath: Path) -> tuple[str, bool, str]:
        """
        Process a single file: hash → compare → upload → insert.
        Returns: (filename, success, message)
        """
        filename = filepath.name

        # Step 1: Compute hash (streaming, memory-efficient)
        content_hash = compute_hash_streaming(filepath)
        if not content_hash:
            return filename, False, "hash failed"

        # Step 2: Check if unchanged
        if content_hash in self.existing_hashes:
            return filename, True, "unchanged"

        # Step 3: Extract metadata (stat only, no file read)
        metadata = extract_file_metadata(filepath, self.source_base)
        if not metadata:
            return filename, False, "metadata extraction failed"

        # Step 4: Generate UUID for storage path
        storage_uuid = str(uuid.uuid4())
        extension = filepath.suffix.lower() if filepath.suffix else ""
        storage_path = f"{storage_uuid}{extension}"

        # Step 5: Upload file
        try:
            with open(filepath, 'rb') as f:
                file_data = f.read()

            import mimetypes
            content_type, _ = mimetypes.guess_type(str(filepath))
            content_type = content_type or "application/octet-stream"

            self._client.storage.from_(self.bucket).upload(
                storage_path,
                file_data,
                file_options={"content-type": content_type}
            )
        except Exception as e:
            return filename, False, f"upload failed: {e}"

        # Step 6: Insert to DB
        db_record = {
            "storage_path": storage_path,
            "content_hash": content_hash,
            **metadata,
        }

        try:
            result = self._client.from_("files").insert(db_record).execute()
            if not result.data:
                raise Exception("No data returned from insert")
            file_id = result.data[0].get("id", "unknown")
        except Exception as e:
            # Compensating transaction: delete from storage
            try:
                self._client.storage.from_(self.bucket).remove([storage_path])
                logger.debug(f"Cleaned up orphan: {storage_path}")
            except Exception as cleanup_error:
                logger.warning(f"Failed to cleanup orphan {storage_path}: {cleanup_error}")
            return filename, False, f"db insert failed: {e}"

        return filename, True, f"uploaded → {file_id}"


def scan_filesystem(source_path: Path) -> list[Path]:
    """Scan directory and return list of file paths (excludes hidden)."""
    files = []
    for root, dirs, filenames in os.walk(source_path):
        dirs[:] = [d for d in dirs if not d.startswith('.')]
        for filename in filenames:
            if not filename.startswith('.'):
                files.append(Path(root) / filename)
    return files


def fetch_existing_hashes(supabase_url: str, supabase_key: str) -> set:
    """Fetch all existing content hashes from DB."""
    client = create_client(supabase_url, supabase_key)
    all_hashes = set()
    page_size = 1000
    offset = 0

    while True:
        result = client.from_("files").select("content_hash").range(offset, offset + page_size - 1).execute()
        if not result.data:
            break
        for row in result.data:
            if row.get("content_hash"):
                all_hashes.add(row["content_hash"])
        if len(result.data) < page_size:
            break
        offset += page_size

    return all_hashes


def sync_source(source_path: Path, max_workers: int = DEFAULT_WORKERS) -> SyncStats:
    """
    Sync a source directory to Supabase Storage.
    
    Returns SyncStats with timing and counts.
    """
    stats = SyncStats()
    stats.start_time = time.time()
    bucket = settings.S3_BUCKET

    # Phase 1: Load existing hashes from DB
    logger.info("Loading existing hashes from database...")
    existing_hashes = fetch_existing_hashes(settings.SUPABASE_URL, settings.SUPABASE_SERVICE_KEY)
    logger.info(f"Loaded {len(existing_hashes)} existing hashes")

    # Phase 2: Scan filesystem
    logger.info(f"Scanning directory: {source_path}")
    all_files = scan_filesystem(source_path)
    stats.total_files = len(all_files)
    logger.info(f"Found {stats.total_files} files")

    if not all_files:
        stats.end_time = time.time()
        return stats

    # Ensure bucket exists
    client = create_client(settings.SUPABASE_URL, settings.SUPABASE_SERVICE_KEY)
    try:
        buckets = client.storage.list_buckets()
        if not any(b.name == bucket for b in buckets):
            client.storage.create_bucket(bucket, options={"public": False})
            logger.info(f"Created bucket: {bucket}")
    except Exception as e:
        if "already exists" not in str(e).lower():
            logger.error(f"Failed to ensure bucket exists: {e}")
            stats.end_time = time.time()
            return stats

    # Phase 3: Process files with worker pool
    logger.info(f"Processing files with {max_workers} workers...")

    def process_with_worker(filepath: Path) -> tuple[str, bool, str]:
        # Each call creates worker with own client (thread-safe)
        worker = FileWorker(existing_hashes, bucket, source_path)
        return worker.process_file(filepath)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_file = {executor.submit(process_with_worker, f): f for f in all_files}

        for i, future in enumerate(as_completed(future_to_file), 1):
            filepath = future_to_file[future]
            try:
                filename, success, message = future.result()
                
                if success:
                    if message == "unchanged":
                        stats.skipped_unchanged += 1
                        if i % 1000 == 0:  # Log progress every 1000 files
                            logger.debug(f"[{i}/{stats.total_files}] {filename}: {message}")
                    else:
                        stats.uploaded += 1
                        logger.info(f"[{i}/{stats.total_files}] {filename}: {message}")
                else:
                    stats.errors += 1
                    logger.warning(f"[{i}/{stats.total_files}] {filename}: {message}")

            except Exception as e:
                stats.errors += 1
                logger.error(f"[{i}/{stats.total_files}] {filepath.name}: exception: {e}")

            # Progress update every 10%
            if i % max(1, stats.total_files // 10) == 0:
                pct = (i / stats.total_files) * 100
                logger.info(f"Progress: {pct:.0f}% ({i}/{stats.total_files})")

    stats.end_time = time.time()
    return stats


def run_sync_cycle(source_paths: list[str], max_workers: int = DEFAULT_WORKERS) -> tuple[int, int, float]:
    """
    Run sync for all source paths.
    Returns: (total_uploaded, total_errors, duration_seconds)
    """
    total_uploaded = 0
    total_errors = 0
    start_time = time.time()

    for source_path_str in source_paths:
        source_path = Path(source_path_str)
        if not source_path.exists():
            logger.warning(f"Source path does not exist, skipping: {source_path}")
            continue

        logger.info(f"Syncing source: {source_path}")
        stats = sync_source(source_path, max_workers)
        
        total_uploaded += stats.uploaded
        total_errors += stats.errors

        logger.info(
            f"Source {source_path.name} complete: "
            f"{stats.uploaded} uploaded, {stats.skipped_unchanged} unchanged, "
            f"{stats.errors} errors in {stats.duration_human}"
        )

    duration = time.time() - start_time
    return total_uploaded, total_errors, duration
