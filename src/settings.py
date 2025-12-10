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

# Database backend: "rest" (HTTPS/443) or "postgres" (direct/5432)
DB_BACKEND = os.getenv("DB_BACKEND", "rest")

# PostgreSQL direct connection (only for DB_BACKEND=postgres)
PG_DSN = os.getenv("PG_DSN")
DB_CONNECT_TIMEOUT = int(os.getenv("DB_CONNECT_TIMEOUT", "10"))
DB_RECONNECT_DELAY = int(os.getenv("DB_RECONNECT_DELAY", "5"))
DB_MAX_RECONNECT_DELAY = int(os.getenv("DB_MAX_RECONNECT_DELAY", "60"))
DB_OPERATION_RETRIES = int(os.getenv("DB_OPERATION_RETRIES", "3"))

# Supabase REST API (only for DB_BACKEND=rest)
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

# S3/Supabase Storage
S3_ENDPOINT = os.getenv("S3_ENDPOINT")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")
S3_BUCKET = os.getenv("S3_BUCKET", "files")
S3_REGION = os.getenv("S3_REGION", "eu-central-1")

# Sync settings
SYNC_SOURCE_PATHS = [p.strip() for p in os.getenv("SYNC_SOURCE_PATHS", "").split(",") if p.strip()]
SYNC_INTERVAL = int(os.getenv("SYNC_INTERVAL", "900"))

# Timezone
TIMEZONE = os.getenv("TIMEZONE", "Europe/Berlin")


def validate_config():
    """Validate required configuration."""
    errors = []

    # Validate DB backend config
    if DB_BACKEND == "rest":
        if not SUPABASE_URL:
            errors.append("SUPABASE_URL is required for DB_BACKEND=rest")
        if not SUPABASE_SERVICE_KEY:
            errors.append("SUPABASE_SERVICE_KEY is required for DB_BACKEND=rest")
    elif DB_BACKEND == "postgres":
        if not PG_DSN:
            errors.append("PG_DSN is required for DB_BACKEND=postgres")
    else:
        errors.append(f"Invalid DB_BACKEND: {DB_BACKEND}. Use 'rest' or 'postgres'")

    # S3 config
    if not S3_ENDPOINT:
        errors.append("S3_ENDPOINT is required")
    if not S3_ACCESS_KEY:
        errors.append("S3_ACCESS_KEY is required")
    if not S3_SECRET_KEY:
        errors.append("S3_SECRET_KEY is required")

    # Sync paths
    if not SYNC_SOURCE_PATHS:
        errors.append("SYNC_SOURCE_PATHS is required (comma-separated paths)")
    for path in SYNC_SOURCE_PATHS:
        if not Path(path).exists():
            errors.append(f"Source path does not exist: {path}")

    if errors:
        raise ValueError("Configuration errors:\n  " + "\n  ".join(errors))


if __name__ == "__main__":
    try:
        validate_config()
        print("✓ Configuration is valid")
    except ValueError as e:
        print(f"✗ {e}")
