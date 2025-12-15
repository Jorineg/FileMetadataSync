"""
File sync with hybrid mode: real-time watcher + daily full scan.

Architecture:
- Watcher handles create/modify/move events in real-time
- Daily full scan reconciles state (soft-delete orphans, update paths)
- Workers process files independently (hash → compare → upload → insert/update)
- UUID-based storage paths, content_hash for deduplication
"""
import os
import time
import uuid
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

UPLOAD_TIMEOUT = 300
HASH_CHUNK_SIZE = 65536
DEFAULT_WORKERS = 6


@dataclass
class SyncStats:
    """Statistics for a sync run."""
    total_files: int = 0
    skipped_unchanged: int = 0
    uploaded: int = 0
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

    try:
        rel_path = filepath.relative_to(source_base)
        folder_path = f"{source_base.name}/{rel_path.parent}" if rel_path.parent != Path(".") else source_base.name
    except ValueError:
        folder_path = str(filepath.parent)

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


def get_supabase_client():
    """Create a new Supabase client."""
    return create_client(
        settings.SUPABASE_URL,
        settings.SUPABASE_SERVICE_KEY,
        options=ClientOptions(postgrest_client_timeout=UPLOAD_TIMEOUT, storage_client_timeout=UPLOAD_TIMEOUT)
    )


def fetch_existing_files(client) -> dict[str, dict]:
    """Fetch all existing files from DB. Returns {content_hash: {id, filename, folder_path}}."""
    all_files = {}
    page_size = 1000
    offset = 0

    while True:
        result = client.from_("files").select("id,content_hash,filename,folder_path").range(offset, offset + page_size - 1).execute()
        if not result.data:
            break
        for row in result.data:
            if row.get("content_hash"):
                all_files[row["content_hash"]] = {
                    "id": row["id"],
                    "filename": row.get("filename"),
                    "folder_path": row.get("folder_path")
                }
        if len(result.data) < page_size:
            break
        offset += page_size

    return all_files


def process_single_file(filepath: Path, source_base: Path, existing_files: dict, bucket: str, client) -> tuple[str, str, str]:
    """
    Process a single file: hash → compare → upload/update.
    Returns: (filename, action, message)
    action: "uploaded", "updated", "unchanged", "error"
    """
    filename = filepath.name

    content_hash = compute_hash_streaming(filepath)
    if not content_hash:
        return filename, "error", "hash failed"

    metadata = extract_file_metadata(filepath, source_base)
    if not metadata:
        return filename, "error", "metadata extraction failed"

    existing = existing_files.get(content_hash)
    now = datetime.now(timezone.utc).isoformat()

    if existing:
        # File exists - check if path changed
        if existing["filename"] != metadata["filename"] or existing["folder_path"] != metadata["folder_path"]:
            try:
                client.from_("files").update({
                    "filename": metadata["filename"],
                    "folder_path": metadata["folder_path"],
                    "auto_extracted_metadata": metadata["auto_extracted_metadata"],
                    "last_seen_at": now,
                    "deleted_at": None,  # Resurrect if was soft-deleted
                    "db_updated_at": now,
                }).eq("id", existing["id"]).execute()
                return filename, "updated", f"path updated → {existing['id']}"
            except Exception as e:
                return filename, "error", f"update failed: {e}"
        else:
            # Just update last_seen_at
            try:
                client.from_("files").update({"last_seen_at": now}).eq("id", existing["id"]).execute()
            except Exception as e:
                logger.warning(f"Failed to update last_seen_at for {existing['id']}: {e}")
            return filename, "unchanged", "unchanged"

    # New file - upload
    storage_uuid = str(uuid.uuid4())
    extension = filepath.suffix.lower() if filepath.suffix else ""
    storage_path = f"{storage_uuid}{extension}"

    try:
        with open(filepath, 'rb') as f:
            file_data = f.read()
        content_type, _ = mimetypes.guess_type(str(filepath))
        client.storage.from_(bucket).upload(
            storage_path, file_data,
            file_options={"content-type": content_type or "application/octet-stream"}
        )
    except Exception as e:
        return filename, "error", f"upload failed: {e}"

    db_record = {
        "storage_path": storage_path,
        "content_hash": content_hash,
        "last_seen_at": now,
        **metadata,
    }

    try:
        result = client.from_("files").insert(db_record).execute()
        if not result.data:
            raise Exception("No data returned")
        file_id = result.data[0].get("id", "unknown")
        existing_files[content_hash] = {"id": file_id, "filename": metadata["filename"], "folder_path": metadata["folder_path"]}
        return filename, "uploaded", f"uploaded → {file_id}"
    except Exception as e:
        # Compensating transaction
        try:
            client.storage.from_(bucket).remove([storage_path])
        except Exception:
            pass
        return filename, "error", f"db insert failed: {e}"


def handle_move_event(src_path: Path, dest_path: Path, source_base: Path, client) -> tuple[str, str]:
    """
    Handle a file move/rename event.
    Returns: (action, message)
    """
    # Compute hash of destination file
    content_hash = compute_hash_streaming(dest_path)
    if not content_hash:
        return "error", "hash failed"

    metadata = extract_file_metadata(dest_path, source_base)
    if not metadata:
        return "error", "metadata extraction failed"

    now = datetime.now(timezone.utc).isoformat()

    # Find existing file by hash
    result = client.from_("files").select("id").eq("content_hash", content_hash).limit(1).execute()
    
    if result.data:
        file_id = result.data[0]["id"]
        try:
            client.from_("files").update({
                "filename": metadata["filename"],
                "folder_path": metadata["folder_path"],
                "auto_extracted_metadata": metadata["auto_extracted_metadata"],
                "last_seen_at": now,
                "deleted_at": None,
                "db_updated_at": now,
            }).eq("id", file_id).execute()
            return "updated", f"moved → {file_id}"
        except Exception as e:
            return "error", f"update failed: {e}"
    else:
        return "not_found", "file not in DB (will be processed as new)"


def scan_filesystem(source_path: Path) -> list[Path]:
    """Scan directory and return list of file paths (excludes hidden and system dirs)."""
    files = []
    skip_dirs = {'.', '@eaDir', '#recycle', '.SynologyWorkingDirectory'}
    for root, dirs, filenames in os.walk(source_path):
        dirs[:] = [d for d in dirs if not d.startswith('.') and d not in skip_dirs]
        for filename in filenames:
            if not filename.startswith('.'):
                files.append(Path(root) / filename)
    return files


def run_full_scan(source_paths: list[str], max_workers: int = DEFAULT_WORKERS) -> SyncStats:
    """
    Run a full filesystem scan with reconciliation.
    - Uploads new files
    - Updates paths for moved files
    - Soft-deletes files no longer on filesystem
    """
    stats = SyncStats()
    stats.start_time = time.time()
    bucket = settings.S3_BUCKET
    client = get_supabase_client()
    scan_start = datetime.now(timezone.utc)

    logger.info("Full scan: Loading existing files from database...")
    existing_files = fetch_existing_files(client)
    logger.info(f"Full scan: Loaded {len(existing_files)} existing files")

    # Ensure bucket exists
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

    # Scan all source paths
    all_files: list[tuple[Path, Path]] = []  # (filepath, source_base)
    for source_path_str in source_paths:
        source_path = Path(source_path_str)
        if not source_path.exists():
            logger.warning(f"Source path does not exist: {source_path}")
            continue
        for filepath in scan_filesystem(source_path):
            all_files.append((filepath, source_path))

    stats.total_files = len(all_files)
    logger.info(f"Full scan: Found {stats.total_files} files")

    if all_files:
        logger.info(f"Full scan: Processing with {max_workers} workers...")

        def process_file(args):
            filepath, source_base = args
            file_client = get_supabase_client()
            return process_single_file(filepath, source_base, existing_files, bucket, file_client)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(process_file, args): args for args in all_files}
            for i, future in enumerate(as_completed(futures), 1):
                try:
                    filename, action, message = future.result()
                    if action == "uploaded":
                        stats.uploaded += 1
                        logger.info(f"[{i}/{stats.total_files}] {filename}: {message}")
                    elif action == "updated":
                        stats.updated += 1
                        logger.info(f"[{i}/{stats.total_files}] {filename}: {message}")
                    elif action == "unchanged":
                        stats.skipped_unchanged += 1
                    else:
                        stats.errors += 1
                        logger.warning(f"[{i}/{stats.total_files}] {filename}: {message}")
                except Exception as e:
                    stats.errors += 1
                    logger.error(f"[{i}/{stats.total_files}] exception: {e}")

                if i % max(1, stats.total_files // 10) == 0:
                    logger.info(f"Full scan progress: {i*100//stats.total_files}%")

    # Soft-delete files not seen during this scan
    logger.info("Full scan: Checking for deleted files...")
    try:
        result = client.from_("files").update({
            "deleted_at": scan_start.isoformat()
        }).lt("last_seen_at", scan_start.isoformat()).is_("deleted_at", "null").execute()
        
        if result.data:
            stats.soft_deleted = len(result.data)
            logger.info(f"Full scan: Soft-deleted {stats.soft_deleted} files no longer on filesystem")
    except Exception as e:
        logger.error(f"Failed to soft-delete orphan files: {e}")

    stats.end_time = time.time()
    return stats


def process_watcher_events(events: list, source_paths: list[str]) -> tuple[int, int, int, int]:
    """
    Process events from the filesystem watcher.
    Returns: (uploaded, updated, unchanged, errors)
    """
    from src.watcher import EventType, PendingEvent
    
    uploaded, updated, unchanged, errors = 0, 0, 0, 0
    bucket = settings.S3_BUCKET
    client = get_supabase_client()
    existing_files = fetch_existing_files(client)
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
            if event.event_type == EventType.MOVED:
                if event.dest_path and event.dest_path.exists():
                    source_base = get_source_base(event.dest_path)
                    if source_base:
                        action, message = handle_move_event(event.path, event.dest_path, source_base, client)
                        if action == "updated":
                            updated += 1
                            logger.info(f"Move: {event.path.name} → {event.dest_path.name}: {message}")
                        elif action == "not_found":
                            # Process as new file
                            filename, action, message = process_single_file(
                                event.dest_path, source_base, existing_files, bucket, client
                            )
                            if action == "uploaded":
                                uploaded += 1
                                logger.info(f"New (from move): {filename}: {message}")
                        else:
                            errors += 1
                            logger.warning(f"Move error: {event.dest_path.name}: {message}")
            else:
                # Created or Modified
                if event.path.exists():
                    source_base = get_source_base(event.path)
                    if source_base:
                        filename, action, message = process_single_file(
                            event.path, source_base, existing_files, bucket, client
                        )
                        if action == "uploaded":
                            uploaded += 1
                            logger.info(f"{event.event_type.value.title()}: {filename}: {message}")
                        elif action == "updated":
                            updated += 1
                            logger.info(f"{event.event_type.value.title()}: {filename}: {message}")
                        elif action == "unchanged":
                            unchanged += 1
                        else:
                            errors += 1
                            logger.warning(f"{event.event_type.value.title()} error: {filename}: {message}")
        except Exception as e:
            errors += 1
            logger.error(f"Error processing event {event}: {e}", exc_info=True)

    return uploaded, updated, unchanged, errors


def upsert_checkpoint(last_event_time: Optional[datetime]) -> None:
    """Upsert checkpoint to DB via RPC."""
    client = get_supabase_client()
    try:
        params = {"p_last_event_time": last_event_time.isoformat() if last_event_time else None}
        client.rpc("upsert_files_checkpoint", params).execute()
        logger.debug("Checkpoint saved")
    except Exception as e:
        logger.warning(f"Failed to save checkpoint: {e}")
