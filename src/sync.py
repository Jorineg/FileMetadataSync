"""
File sync with CAS (Content-Addressable Storage) architecture.
- Scanner/Watcher only registers files and hashes.
- S3 uploads are handled by a separate Uploader component.
"""
import os
import time
import hashlib
import mimetypes
from pathlib import Path
from dataclasses import dataclass
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from supabase import create_client, ClientOptions

from src import settings
from src.logging_conf import logger

# Timeout for DB/storage operations (seconds)
# Note: supabase-py doesn't support separate connect/read timeouts
CLIENT_TIMEOUT = 120  # 2 minutes - balance between fast failure and allowing operations
HASH_CHUNK_SIZE = 65536
DEFAULT_WORKERS = 4  # Reduced as it's now mostly I/O (hash + DB)
MAX_FILE_SIZE = 1 * 1024 * 1024 * 1024  # 1GB - skip larger files to prevent DoS

@dataclass
class SyncStats:
    """Statistics for a sync run."""
    total_files: int = 0
    skipped_unchanged: int = 0
    skipped_security: int = 0  # Symlinks, oversized files
    registered: int = 0
    updated: int = 0
    soft_deleted: int = 0
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
        return f"{d/3600:.1f}h"


def validate_file_security(filepath: Path, source_base: Path) -> tuple[bool, str]:
    """
    Validate file for security issues before processing.
    Returns (is_safe, reason) - if not safe, reason explains why.
    """
    # Check symlink escape: symlinks must resolve within source_base
    if filepath.is_symlink():
        try:
            real_path = filepath.resolve()
            source_resolved = source_base.resolve()
            if not str(real_path).startswith(str(source_resolved)):
                return False, f"symlink escape: {filepath} -> {real_path}"
        except (OSError, ValueError) as e:
            return False, f"symlink resolution failed: {e}"
    
    # Check file size (use lstat to not follow symlinks)
    try:
        st = os.lstat(filepath)
        if st.st_size > MAX_FILE_SIZE:
            return False, f"file too large: {st.st_size / (1024**3):.2f}GB > 1GB limit"
    except OSError as e:
        return False, f"cannot stat file: {e}"
    
    return True, ""


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
    """Extract metadata using lstat() to not follow symlinks."""
    try:
        st = os.lstat(filepath)  # lstat doesn't follow symlinks
    except Exception as e:
        logger.error(f"Failed to stat {filepath}: {e}")
        return {}

    fs_attributes = {
        "size_bytes": st.st_size,
        "mode_octal": oct(st.st_mode)[-3:],
        "uid": st.st_uid,
        "gid": st.st_gid,
        "is_symlink": filepath.is_symlink(),
    }

    mime_type, _ = mimetypes.guess_type(str(filepath))
    auto_metadata = {
        "mime_type": mime_type,
        "extension": filepath.suffix.lower() if filepath.suffix else None,
        "original_path": str(filepath),
        "source_base": str(source_base),
    }

    return {
        "fs_mtime": datetime.fromtimestamp(st.st_mtime, tz=timezone.utc).isoformat(),
        "fs_ctime": datetime.fromtimestamp(st.st_ctime, tz=timezone.utc).isoformat(),
        "filesystem_inode": st.st_ino,
        "filesystem_attributes": fs_attributes,
        "auto_extracted_metadata": auto_metadata,
        "size_bytes": st.st_size, # Needed for file_contents
        "mime_type": mime_type,   # Needed for file_contents
    }


def get_supabase_client():
    """Create a new Supabase client."""
    return create_client(
        settings.SUPABASE_URL,
        settings.SUPABASE_SERVICE_KEY,
        options=ClientOptions(
            postgrest_client_timeout=CLIENT_TIMEOUT,
            storage_client_timeout=CLIENT_TIMEOUT
        )
    )


def fetch_path_map(client) -> dict[str, str]:
    """
    Fetch all existing file paths from DB.
    Returns: {full_path: content_hash}
    """
    path_map = {}
    page_size = 2000
    offset = 0

    while True:
        result = client.from_("files").select("full_path,content_hash").range(offset, offset + page_size - 1).execute()
        if not result.data:
            break
        for row in result.data:
            path_map[row["full_path"]] = row["content_hash"]
        if len(result.data) < page_size:
            break
        offset += page_size

    return path_map


def normalize_path(path_str: str) -> str:
    """Ensure path starts with / for consistency."""
    return path_str if path_str.startswith('/') else '/' + path_str


def process_single_file(filepath: Path, source_base: Path, path_map: dict, client) -> tuple[str, str, str]:
    """
    Register a file in the DB: hash -> update file_contents -> update files reference.
    """
    full_path = normalize_path(str(filepath))
    now = datetime.now(timezone.utc).isoformat()

    # Security validation
    is_safe, reason = validate_file_security(filepath, source_base)
    if not is_safe:
        logger.warning(f"Security skip: {filepath}: {reason}")
        return full_path, "skipped", reason

    content_hash = compute_hash_streaming(filepath)
    if not content_hash:
        return full_path, "error", "hash failed"

    # Quick check if unchanged in local cache
    if path_map.get(full_path) == content_hash:
        try:
            client.from_("files").update({"last_seen_at": now}).eq("full_path", full_path).execute()
            return full_path, "unchanged", "unchanged"
        except Exception as e:
            logger.warning(f"Failed to update last_seen_at for {full_path}: {e}")
            return full_path, "unchanged", "last_seen_at update failed"

    metadata = extract_file_metadata(filepath, source_base)
    if not metadata:
        return full_path, "error", "metadata extraction failed"

    try:
        # 1. UPSERT into file_contents (Content-Addressable)
        # We only set status to pending if it doesn't exist yet
        content_record = {
            "content_hash": content_hash,
            "size_bytes": metadata.pop("size_bytes"),
            "mime_type": metadata.pop("mime_type"),
            "db_updated_at": now,
        }
        client.from_("file_contents").upsert(content_record, on_conflict="content_hash").execute()

        # 2. UPSERT into files (Path reference)
        # This will also trigger project auto-linking in DB
        file_record = {
            "full_path": full_path,
            "content_hash": content_hash,
            "last_seen_at": now,
            "deleted_at": None, # Resurrect if soft-deleted
            "db_updated_at": now,
            **metadata,
        }
        result = client.from_("files").upsert(file_record, on_conflict="full_path").execute()
        
        if not result.data:
            raise Exception("Upsert returned no data")
            
        # Update local map
        path_map[full_path] = content_hash
        
        return full_path, "registered", f"registered ({content_hash[:8]})"
    except Exception as e:
        return full_path, "error", f"db registration failed: {e}"


def run_full_scan(source_paths: list[str], max_workers: int = DEFAULT_WORKERS) -> SyncStats:
    """Full filesystem reconciliation."""
    stats = SyncStats()
    stats.start_time = time.time()
    client = get_supabase_client()
    scan_start = datetime.now(timezone.utc)

    logger.info("Full scan: Loading file map from database...")
    path_map = fetch_path_map(client)
    logger.info(f"Full scan: Loaded {len(path_map)} existing file paths")

    # Scan all source paths
    all_files: list[tuple[Path, Path]] = []
    for source_path_str in source_paths:
        source_path = Path(source_path_str)
        if not source_path.exists():
            continue
        for root, dirs, filenames in os.walk(source_path):
            # Same skip dirs as before
            dirs[:] = [d for d in dirs if not d.startswith('.') and d not in {'.', '@eaDir', '#recycle'}]
            for filename in filenames:
                if not filename.startswith('.'):
                    all_files.append((Path(root) / filename, source_path))

    stats.total_files = len(all_files)
    logger.info(f"Full scan: Found {stats.total_files} files")

    if all_files:
        def worker_task(args):
            filepath, source_base = args
            worker_client = get_supabase_client()
            return process_single_file(filepath, source_base, path_map, worker_client)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(worker_task, args): args for args in all_files}
            for i, future in enumerate(as_completed(futures), 1):
                try:
                    path, action, message = future.result()
                    if action == "registered":
                        stats.registered += 1
                        logger.info(f"[{i}/{stats.total_files}] REG: {path}: {message}")
                    elif action == "unchanged":
                        stats.skipped_unchanged += 1
                    elif action == "skipped":
                        stats.skipped_security += 1
                        # Warning level ensures it goes to betterstack
                        logger.warning(f"[{i}/{stats.total_files}] SKIP: {path}: {message}")
                    else:
                        stats.errors += 1
                        logger.warning(f"[{i}/{stats.total_files}] ERR: {path}: {message}")
                except Exception as e:
                    stats.errors += 1
                    logger.error(f"[{i}/{stats.total_files}] EXC: {e}")

                if i % max(1, stats.total_files // 10) == 0:
                    logger.info(f"Full scan progress: {i*100//stats.total_files}%")

    # Soft-delete cleanup
    logger.info("Full scan: Marking deleted files...")
    for source_path_str in source_paths:
        try:
            normalized_source = normalize_path(source_path_str)
            result = client.from_("files").update({
                "deleted_at": scan_start.isoformat()
            }).lt("last_seen_at", scan_start.isoformat()) \
              .is_("deleted_at", "null") \
              .ilike("full_path", f"{normalized_source}%") \
              .execute()
            
            if result.data:
                stats.soft_deleted += len(result.data)
        except Exception as e:
            logger.error(f"Failed to soft-delete orphan files for {source_path_str}: {e}")

    stats.end_time = time.time()
    return stats


def process_watcher_events(events: list, source_paths: list[str]) -> tuple[int, int, int, int]:
    """Process real-time events."""
    from src.watcher import PendingEvent
    
    registered, unchanged, errors = 0, 0, 0
    client = get_supabase_client()
    path_map = fetch_path_map(client)
    source_bases = [Path(p) for p in source_paths]

    def get_source_base(filepath: Path) -> Path | None:
        for base in source_bases:
            try:
                filepath.relative_to(base)
                return base
            except ValueError:
                continue
        return None

    for event in events:
        event: PendingEvent
        try:
            path = event.dest_path if event.dest_path else event.path
            if path.exists():
                source_base = get_source_base(path)
                if source_base:
                    _, action, message = process_single_file(path, source_base, path_map, client)
                    if action == "registered":
                        registered += 1
                        logger.info(f"Watcher: {path.name}: {message}")
                    elif action == "unchanged":
                        unchanged += 1
                    else:
                        errors += 1
                        logger.warning(f"Watcher error: {path.name}: {message}")
        except Exception as e:
            errors += 1
            logger.error(f"Error processing event {event}: {e}")

    return registered, 0, unchanged, errors



