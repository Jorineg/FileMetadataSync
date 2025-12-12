"""Configuration management for FileMetadataSync."""
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
LOGS_DIR = BASE_DIR / "logs"

DATA_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
BETTERSTACK_SOURCE_TOKEN = os.getenv("BETTERSTACK_SOURCE_TOKEN")
BETTERSTACK_INGEST_HOST = os.getenv("BETTERSTACK_INGEST_HOST")

# Supabase (REST API for both DB and Storage)
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

# Storage bucket
S3_BUCKET = os.getenv("S3_BUCKET", "files")

# Sync settings
# Default: scan /data (user mounts whatever they want there)
_default_sync_path = "/data"
_env_paths = os.getenv("SYNC_SOURCE_PATHS", "")
SYNC_SOURCE_PATHS = [p.strip() for p in _env_paths.split(",") if p.strip()] if _env_paths else [_default_sync_path]
SYNC_WORKERS = int(os.getenv("SYNC_WORKERS", "6"))

# Watcher settings
DEBOUNCE_SECONDS = float(os.getenv("DEBOUNCE_SECONDS", "3.0"))
IGNORE_PATTERNS = [p.strip() for p in os.getenv("IGNORE_PATTERNS", "").split(",") if p.strip()] or [
    "*.tmp", "*.temp", ".DS_Store", "Thumbs.db", "*.partial",
    ".syncing", "@eaDir/*", "#recycle/*", ".SynologyWorkingDirectory/*"
]

# Full scan schedule (hour of day in 24h format, e.g. 3 = 3 AM)
FULL_SCAN_HOUR = int(os.getenv("FULL_SCAN_HOUR", "3"))
FULL_SCAN_ON_STARTUP = os.getenv("FULL_SCAN_ON_STARTUP", "true").lower() in ("true", "1", "yes")

# Timezone
TIMEZONE = os.getenv("TIMEZONE", "Europe/Berlin")


def validate_config():
    """Validate required configuration."""
    errors = []

    if not SUPABASE_URL:
        errors.append("SUPABASE_URL is required")
    if not SUPABASE_SERVICE_KEY:
        errors.append("SUPABASE_SERVICE_KEY is required")

    # SYNC_SOURCE_PATHS now has a default, so this check is just for existence
    if not SYNC_SOURCE_PATHS or not any(Path(p).exists() for p in SYNC_SOURCE_PATHS):
        errors.append(f"No valid source paths found. Mount directories to /data or set SYNC_SOURCE_PATHS. Checked: {SYNC_SOURCE_PATHS}")

    if errors:
        raise ValueError("Configuration errors:\n  " + "\n  ".join(errors))


if __name__ == "__main__":
    try:
        validate_config()
        print("✓ Configuration is valid")
        print(f"  SUPABASE_URL: {SUPABASE_URL}")
        print(f"  S3_BUCKET: {S3_BUCKET}")
        print(f"  SYNC_SOURCE_PATHS: {SYNC_SOURCE_PATHS}")
        print(f"  SYNC_WORKERS: {SYNC_WORKERS}")
    except ValueError as e:
        print(f"✗ {e}")
