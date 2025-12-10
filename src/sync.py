"""File sync with hash-based skip logic for efficiency at scale."""
import os
from pathlib import Path
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from src import settings
from src.logging_conf import logger
from src.db import Database
from src.metadata import extract_metadata, get_file_hash
from src.storage_upload import StorageUploader, sanitize_path


def scan_filesystem(source_path: Path) -> list[Path]:
    """Scan directory and return list of file paths (excludes hidden)."""
    files = []
    for root, dirs, filenames in os.walk(source_path):
        dirs[:] = [d for d in dirs if not d.startswith('.')]
        for filename in filenames:
            if not filename.startswith('.'):
                files.append(Path(root) / filename)
    return files


def compute_file_hashes(files: list[Path], max_workers: int = 4) -> dict[Path, str]:
    """Compute hashes for all files in parallel. Returns {path: hash}."""
    results = {}
    
    def hash_file(path: Path) -> tuple[Path, Optional[str]]:
        return path, get_file_hash(path)
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(hash_file, f): f for f in files}
        for future in as_completed(futures):
            path, file_hash = future.result()
            if file_hash:
                results[path] = file_hash
    
    return results


def process_single_file(
    uploader: StorageUploader,
    db: Database,
    local_path: Path,
    source_base: Path,
    bucket: str
) -> tuple[bool, str]:
    """Upload file and update metadata. Returns (success, message)."""
    try:
        rel_path = local_path.relative_to(source_base)
        storage_path = f"{source_base.name}/{rel_path}"
        
        # Upload (returns sanitized path)
        sanitized_path = uploader.upload_file(local_path, bucket, storage_path)
        if not sanitized_path:
            return False, f"Upload failed: {local_path.name}"
        
        # Get storage object ID
        storage_object_id = db.get_storage_object_id(bucket, sanitized_path)
        if not storage_object_id:
            return False, f"Storage object not found: {sanitized_path}"
        
        # Extract and save metadata
        metadata = extract_metadata(local_path, source_base)
        metadata['storage_object_id'] = storage_object_id
        
        file_id = db.upsert_file(metadata)
        if file_id:
            db.commit()
            return True, f"{local_path.name} -> {file_id}"
        else:
            db.rollback()
            return False, f"DB insert failed: {local_path.name}"
            
    except Exception as e:
        return False, str(e)


def sync_source(db: Database, source_path: Path) -> tuple[int, int]:
    """
    Efficient sync with hash-based skip logic.
    
    Phase 1: Scan filesystem and compute hashes (parallel)
    Phase 2: Compare with DB hashes (local, instant)
    Phase 3: Upload only new/changed files (parallel)
    """
    bucket = settings.S3_BUCKET
    
    # Phase 1: Get existing hashes from DB (single query)
    logger.info("Loading existing file hashes from database...")
    existing_hashes = db.get_all_file_hashes()
    existing_hash_set = set(existing_hashes.keys())
    
    # Phase 2: Scan filesystem
    logger.info(f"Scanning directory: {source_path}")
    all_files = scan_filesystem(source_path)
    total_files = len(all_files)
    logger.info(f"Found {total_files} files on disk")
    
    # Phase 3: Compute hashes (parallel, CPU-bound)
    logger.info("Computing file hashes...")
    local_hashes = compute_file_hashes(all_files, max_workers=os.cpu_count() or 4)
    logger.info(f"Computed {len(local_hashes)} hashes")
    
    # Phase 4: Find files needing sync (new or changed hash)
    files_to_sync = []
    for path, file_hash in local_hashes.items():
        if file_hash not in existing_hash_set:
            files_to_sync.append(path)
    
    skipped = total_files - len(files_to_sync)
    logger.info(f"Skipping {skipped} unchanged files, syncing {len(files_to_sync)} new/changed files")
    
    if not files_to_sync:
        logger.info("No files to sync - all up to date")
        return 0, 0
    
    # Phase 5: Upload changed files (parallel, I/O-bound)
    uploader = StorageUploader()
    if not uploader.ensure_bucket_exists(bucket):
        logger.error(f"Failed to ensure bucket {bucket} exists")
        return 0, 0
    
    processed = 0
    errors = 0
    max_workers = min(8, os.cpu_count() or 4)
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_file = {
            executor.submit(process_single_file, uploader, db, f, source_path, bucket): f
            for f in files_to_sync
        }
        
        for i, future in enumerate(as_completed(future_to_file), 1):
            file_path = future_to_file[future]
            try:
                success, message = future.result()
                if success:
                    processed += 1
                    logger.info(f"[{i}/{len(files_to_sync)}] {message}")
                else:
                    errors += 1
                    logger.warning(f"[{i}/{len(files_to_sync)}] Error: {message}")
            except Exception as e:
                errors += 1
                logger.error(f"[{i}/{len(files_to_sync)}] Exception: {e}")
    
    uploader.close()
    return processed, errors


def full_scan_metadata(db: Database, source_path: Path) -> tuple[int, int]:
    """Alias for sync_source."""
    return sync_source(db, source_path)
