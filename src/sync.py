"""File sync and metadata processing."""
import subprocess
import os
from pathlib import Path
from typing import Optional

from src import settings
from src.logging_conf import logger
from src.db import Database
from src.metadata import extract_metadata
from src.rclone_parser import parse_stream, get_storage_path


def build_rclone_config() -> str:
    """Generate rclone config content for S3 remote."""
    return f"""[supabase]
type = s3
provider = Other
endpoint = {settings.S3_ENDPOINT}
access_key_id = {settings.S3_ACCESS_KEY}
secret_access_key = {settings.S3_SECRET_KEY}
region = {settings.S3_REGION}
acl = private
"""


def run_rclone_sync(source_path: Path, bucket: str = None) -> subprocess.Popen:
    """
    Run rclone sync with efficient flags for many files.
    Returns the subprocess for log parsing.
    """
    bucket = bucket or settings.S3_BUCKET

    # Create temp rclone config
    config_path = settings.DATA_DIR / "rclone.conf"
    with open(config_path, "w") as f:
        f.write(build_rclone_config())

    # Destination path in S3 - use source folder name
    dest_folder = source_path.name
    dest = f"supabase:{bucket}/{dest_folder}"

    cmd = [
        "rclone", "sync",
        str(source_path), dest,
        "--config", str(config_path),
        # Efficient flags for many files
        "--transfers", "8",
        "--checkers", "16",
        "--buffer-size", "32M",
        "--fast-list",
        "--checksum",  # Use checksum instead of modtime for more reliable sync
        # Progress and logging
        "--progress",
        "--stats", "10s",
        "--stats-one-line",
        "-v",  # Verbose to see file operations
        "--log-format", "date,time",
        # Don't modify times on S3 (not all S3 implementations support it)
        "--no-update-modtime",
    ]

    logger.info(f"Starting rclone sync: {source_path} -> {dest}")
    return subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,  # Combine stderr into stdout
        text=True,
        bufsize=1,  # Line buffered
    )


def process_synced_file(db: Database, local_path: Path, source_base: Path, bucket: str) -> bool:
    """Process a synced file: extract metadata and upsert to DB."""
    if not local_path.exists():
        logger.warning(f"File not found (may have been deleted): {local_path}")
        return False

    if not local_path.is_file():
        return False

    # Get storage object path
    storage_path = get_storage_path(str(local_path), source_base)
    # Prepend source folder name to match rclone destination
    storage_name = f"{source_base.name}/{storage_path}"

    # Look up storage object ID
    storage_object_id = db.get_storage_object_id(bucket, storage_name)
    if not storage_object_id:
        logger.warning(f"Storage object not found for {storage_name} - will retry later")
        return False

    # Extract metadata
    metadata = extract_metadata(local_path, source_base)
    metadata["storage_object_id"] = storage_object_id

    # Upsert to database
    try:
        file_id = db.upsert_file(metadata)
        db.commit()
        logger.info(f"Processed file: {local_path.name} -> {file_id}")
        return True
    except Exception as e:
        db.rollback()
        logger.error(f"Failed to upsert file {local_path}: {e}")
        return False


def sync_source(db: Database, source_path: Path) -> tuple[int, int]:
    """
    Sync a source path and process all files.
    Returns (processed_count, error_count).
    """
    bucket = settings.S3_BUCKET
    processed = 0
    errors = 0

    # Run rclone sync
    proc = run_rclone_sync(source_path, bucket)

    # Parse output and process files
    for line in iter(proc.stdout.readline, ''):
        line = line.strip()
        if not line:
            continue

        # Log all rclone output
        logger.debug(f"rclone: {line}")

        # Parse for copied files
        from src.rclone_parser import parse_line
        result = parse_line(line)
        if result:
            path, operation = result
            if operation == "copied":
                local_path = source_path / path
                if process_synced_file(db, local_path, source_path, bucket):
                    processed += 1
                else:
                    errors += 1

    # Wait for rclone to finish
    proc.wait()
    if proc.returncode != 0:
        logger.error(f"rclone exited with code {proc.returncode}")

    return processed, errors


def full_scan_metadata(db: Database, source_path: Path) -> tuple[int, int]:
    """
    Scan all files in source path and update metadata in DB.
    Use this for initial sync or to refresh all metadata.
    Returns (processed_count, error_count).
    """
    bucket = settings.S3_BUCKET
    processed = 0
    errors = 0

    logger.info(f"Starting full metadata scan: {source_path}")

    for root, dirs, files in os.walk(source_path):
        # Skip hidden directories
        dirs[:] = [d for d in dirs if not d.startswith('.')]

        for filename in files:
            if filename.startswith('.'):
                continue

            local_path = Path(root) / filename
            if process_synced_file(db, local_path, source_path, bucket):
                processed += 1
            else:
                errors += 1

    logger.info(f"Full scan complete: {processed} processed, {errors} errors")
    return processed, errors

