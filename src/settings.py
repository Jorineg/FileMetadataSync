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
SYNC_SOURCE_PATHS = [p.strip() for p in os.getenv("SYNC_SOURCE_PATHS", "").split(",") if p.strip()]
SYNC_INTERVAL = int(os.getenv("SYNC_INTERVAL", "900"))
SYNC_WORKERS = int(os.getenv("SYNC_WORKERS", "6"))

# Timezone
TIMEZONE = os.getenv("TIMEZONE", "Europe/Berlin")


def validate_config():
    """Validate required configuration."""
    errors = []

    if not SUPABASE_URL:
        errors.append("SUPABASE_URL is required")
    if not SUPABASE_SERVICE_KEY:
        errors.append("SUPABASE_SERVICE_KEY is required")

    if not SYNC_SOURCE_PATHS:
        errors.append("SYNC_SOURCE_PATHS is required (comma-separated paths)")

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
