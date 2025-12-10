"""Parse rclone log output to extract synced file information."""
import re
import sys
from pathlib import Path
from typing import Optional, Generator

from src.logging_conf import logger


# rclone log patterns for different operations
# Format: timestamp INFO  : path: Copied (new)
# Format: timestamp INFO  : path: Copied (replaced existing)
# Format: timestamp INFO  : path: Deleted
COPIED_PATTERN = re.compile(r"INFO\s+:\s+(.+?):\s+Copied\s+\((new|replaced existing)\)")
DELETED_PATTERN = re.compile(r"INFO\s+:\s+(.+?):\s+Deleted")


def parse_line(line: str) -> Optional[tuple[str, str]]:
    """
    Parse a single rclone log line.
    Returns (path, operation) tuple or None if not relevant.
    operation is one of: 'copied', 'deleted'
    """
    # Check for copied files
    match = COPIED_PATTERN.search(line)
    if match:
        return (match.group(1), "copied")

    # Check for deleted files
    match = DELETED_PATTERN.search(line)
    if match:
        return (match.group(1), "deleted")

    return None


def parse_stream(stream=None) -> Generator[tuple[str, str], None, None]:
    """
    Parse rclone log from stdin or provided stream.
    Yields (path, operation) tuples for each synced file.
    """
    if stream is None:
        stream = sys.stdin

    for line in stream:
        line = line.strip()
        if not line:
            continue

        result = parse_line(line)
        if result:
            yield result
        else:
            # Pass through other log lines for visibility
            logger.debug(f"rclone: {line}")


def get_storage_path(local_path: str, source_base: Path) -> str:
    """Convert local path to storage object path (relative to source base)."""
    try:
        local = Path(local_path)
        if local.is_absolute():
            return str(local.relative_to(source_base))
        return local_path
    except ValueError:
        return local_path

