"""File metadata extraction."""
import os
import stat
import hashlib
import mimetypes
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional

from src.logging_conf import logger


def get_file_hash(filepath: Path, algorithm: str = "sha256") -> Optional[str]:
    """Calculate file hash. Returns None on error."""
    try:
        hasher = hashlib.new(algorithm)
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                hasher.update(chunk)
        return hasher.hexdigest()
    except Exception as e:
        logger.warning(f"Failed to hash {filepath}: {e}")
        return None


def extract_metadata(filepath: Path, source_base_path: Path) -> dict:
    """Extract all metadata from a file."""
    try:
        st = os.stat(filepath)
    except Exception as e:
        logger.error(f"Failed to stat {filepath}: {e}")
        return {}

    # Parse mode bits
    mode = st.st_mode
    access_rights = {
        "owner_read": bool(mode & stat.S_IRUSR),
        "owner_write": bool(mode & stat.S_IWUSR),
        "owner_execute": bool(mode & stat.S_IXUSR),
        "group_read": bool(mode & stat.S_IRGRP),
        "group_write": bool(mode & stat.S_IWGRP),
        "group_execute": bool(mode & stat.S_IXGRP),
        "other_read": bool(mode & stat.S_IROTH),
        "other_write": bool(mode & stat.S_IWOTH),
        "other_execute": bool(mode & stat.S_IXOTH),
        "mode_octal": oct(mode)[-3:],
    }

    # Filesystem attributes
    fs_attributes = {
        "size_bytes": st.st_size,
        "nlinks": st.st_nlink,
        "uid": st.st_uid,
        "gid": st.st_gid,
        "device": st.st_dev,
        "is_symlink": filepath.is_symlink(),
    }

    # Try to get owner name (may fail on some systems)
    try:
        import pwd
        fs_attributes["owner_name"] = pwd.getpwuid(st.st_uid).pw_name
    except (ImportError, KeyError):
        pass
    try:
        import grp
        fs_attributes["group_name"] = grp.getgrgid(st.st_gid).gr_name
    except (ImportError, KeyError):
        pass

    # Auto-extracted metadata
    mime_type, _ = mimetypes.guess_type(str(filepath))
    auto_metadata = {
        "mime_type": mime_type,
        "extension": filepath.suffix.lower() if filepath.suffix else None,
        "original_filename": filepath.name,  # Preserve before sanitization
        "source_path": str(filepath),
        "source_base": str(source_base_path),
    }

    # Relative path from source base (include source folder name as root)
    try:
        rel_path = filepath.relative_to(source_base_path)
        # folder_path includes root folder: test/subfolder (not just subfolder)
        if rel_path.parent != Path("."):
            folder_path = f"{source_base_path.name}/{rel_path.parent}"
        else:
            folder_path = source_base_path.name
    except ValueError:
        folder_path = str(filepath.parent)

    return {
        "filename": filepath.name,
        "folder_path": folder_path,
        "content_hash": get_file_hash(filepath),
        "file_created_at": datetime.fromtimestamp(st.st_ctime, tz=timezone.utc),
        "file_modified_at": datetime.fromtimestamp(st.st_mtime, tz=timezone.utc),
        "file_created_by": fs_attributes.get("owner_name"),
        "filesystem_inode": st.st_ino,
        "filesystem_access_rights": access_rights,
        "filesystem_attributes": fs_attributes,
        "auto_extracted_metadata": auto_metadata,
    }

