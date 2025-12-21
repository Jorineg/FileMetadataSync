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


def fetch_existing_files(client) -> tuple[dict[str, dict], dict[tuple[str, str], dict]]:
    """
    Fetch all existing files from DB.
    Returns: (hash_map, path_map)
    hash_map: {content_hash: {id, filename, folder_path}}
    path_map: {(folder_path, filename): {id, content_hash}}
    """
    hash_map = {}
    path_map = {}
    page_size = 1000
    offset = 0

    while True:
        result = client.from_("files").select("id,content_hash,filename,folder_path").range(offset, offset + page_size - 1).execute()
        if not result.data:
            break
        for row in result.data:
            f_path = row.get("folder_path")
            f_name = row.get("filename")
            f_hash = row.get("content_hash")
            
            # Map by hash (for moved files)
            if f_hash:
                hash_map[f_hash] = {
                    "id": row["id"],
                    "filename": f_name,
                    "folder_path": f_path
                }
            
            # Map by path (for same-path duplicates/updates)
            if f_name:
                path_map[(f_path, f_name)] = {
                    "id": row["id"],
                    "content_hash": f_hash
                }
                
        if len(result.data) < page_size:
            break
        offset += page_size

    return hash_map, path_map


def process_single_file(filepath: Path, source_base: Path, hash_map: dict, path_map: dict, bucket: str, client) -> tuple[str, str, str]:
    """
    Process a single file: hash → compare → upload/update using UPSERT.
    """
    filename = filepath.name
    now = datetime.now(timezone.utc).isoformat()

    content_hash = compute_hash_streaming(filepath)
    if not content_hash:
        return filename, "error", "hash failed"

    metadata = extract_file_metadata(filepath, source_base)
    if not metadata:
        return filename, "error", "metadata extraction failed"

    folder_path = metadata["folder_path"]
    path_key = (folder_path, filename)

    # 1. Check if the exact PATH exists (Same Path Check)
    existing_path = path_map.get(path_key)
    if existing_path:
        if existing_path["content_hash"] == content_hash:
            # Hash matches path - just update last_seen_at
            try:
                client.from_("files").update({"last_seen_at": now}).eq("id", existing_path["id"]).execute()
                return filename, "unchanged", "unchanged"
            except Exception as e:
                logger.warning(f"Failed to update last_seen_at for {existing_path['id']}: {e}")
                return filename, "unchanged", "last_seen_at update failed"
        else:
            # Path exists but content changed - we need to upload
            pass

    storage_path = None
    
    # NEW or UPDATED file - upload
    if not storage_path:
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
        "deleted_at": None,
        "db_updated_at": now,
        **metadata,
    }

    try:
        # Use upsert with on_conflict on (folder_path, filename)
        result = client.from_("files").upsert(db_record, on_conflict="folder_path,filename").execute()
        if not result.data:
            raise Exception("Upsert returned no data")
        file_id = result.data[0].get("id", "unknown")
        
        # Update local maps
        hash_map[content_hash] = {"id": file_id, "filename": filename, "folder_path": folder_path}
        path_map[path_key] = {"id": file_id, "content_hash": content_hash}
        
        return filename, "uploaded", f"upserted → {file_id}"
    except Exception as e:
        # Cleanup storage if it was a brand new path
        if not existing_path:
            try:
                client.storage.from_(bucket).remove([storage_path])
            except Exception:
                pass
        return filename, "error", f"db upsert failed: {e}"


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
    # Use UPSERT on (folder_path, filename) to move/update record
    try:
        db_record = {
            "filename": metadata["filename"],
            "folder_path": metadata["folder_path"],
            "auto_extracted_metadata": metadata["auto_extracted_metadata"],
            "content_hash": content_hash,
            "last_seen_at": now,
            "deleted_at": None,
            "db_updated_at": now,
        }
        # We don't have storage_path easily available here if we only have the hash.
        # Let's fetch the storage_path from the existing hash record.
        result = client.from_("files").select("storage_path").eq("content_hash", content_hash).limit(1).execute()
        
        if result.data:
            db_record["storage_path"] = result.data[0]["storage_path"]
            client.from_("files").upsert(db_record, on_conflict="folder_path,filename").execute()
            return "updated", f"moved/upserted → {content_hash[:8]}"
        else:
            return "not_found", "file content (hash) not in DB"
    except Exception as e:
        return "error", f"move upsert failed: {e}"


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
    hash_map, path_map = fetch_existing_files(client)
    logger.info(f"Full scan: Loaded {len(hash_map)} hashes and {len(path_map)} paths")

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
            return process_single_file(filepath, source_base, hash_map, path_map, bucket, file_client)

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

    # Soft-delete files not seen during this scan (scoped to this instance's source paths)
    logger.info("Full scan: Checking for deleted files...")
    for source_path_str in source_paths:
        try:
            result = client.from_("files").update({
                "deleted_at": scan_start.isoformat()
            }).lt("last_seen_at", scan_start.isoformat()) \
              .is_("deleted_at", "null") \
              .eq("auto_extracted_metadata->>source_base", source_path_str) \
              .execute()
            
            if result.data:
                stats.soft_deleted += len(result.data)
        except Exception as e:
            logger.error(f"Failed to soft-delete orphan files for {source_path_str}: {e}")
    
    if stats.soft_deleted > 0:
        logger.info(f"Full scan: Soft-deleted {stats.soft_deleted} files no longer on filesystem")

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
    hash_map, path_map = fetch_existing_files(client)
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
                                event.dest_path, source_base, hash_map, path_map, bucket, client
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
                            event.path, source_base, hash_map, path_map, bucket, client
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
