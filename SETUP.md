# FileMetadataSync Setup

Syncs files from NAS to Supabase S3 storage and extracts metadata to database.

## Prerequisites

- Docker (Docker Desktop on Mac, or Synology Container Manager)
- Supabase instance (self-hosted or cloud)
- Supabase S3 Storage credentials

## Database Backend Options

| Backend | Protocol | Port | Use Case |
|---------|----------|------|----------|
| `rest` | HTTPS | 443 | Firewall-friendly, recommended |
| `postgres` | PostgreSQL | 5432 | Faster for bulk ops, needs port open |

Set via `DB_BACKEND=rest` or `DB_BACKEND=postgres` in `.env`.

## Quick Start

### 1. Run SQL Setup (required for REST backend)

The REST API can't query `storage.objects` directly. Run this SQL once:

```sql
-- sql/get_storage_object_id.sql
CREATE OR REPLACE FUNCTION get_storage_object_id(p_bucket_id TEXT, p_object_name TEXT)
RETURNS UUID
LANGUAGE sql
SECURITY DEFINER
AS $$
    SELECT id FROM storage.objects
    WHERE bucket_id = p_bucket_id AND name = p_object_name
    LIMIT 1;
$$;

GRANT EXECUTE ON FUNCTION get_storage_object_id TO service_role;
```

### 2. Get Credentials

**For REST backend (recommended):**
- `SUPABASE_URL`: Your Supabase URL (e.g., `https://api.ibhelm.de`)
- `SUPABASE_SERVICE_KEY`: Service role key (Settings → API → service_role key)

**For S3 Storage:**
1. Go to **Settings** → **Storage**
2. Enable S3 Protocol
3. Create S3 access keys

### 3. Create Environment File

```bash
cp env.example .env
```

Edit `.env`:

```env
# REST backend (works over HTTPS/443)
DB_BACKEND=rest
SUPABASE_URL=https://api.ibhelm.de
SUPABASE_SERVICE_KEY=your-service-role-key

# S3 Storage
S3_ENDPOINT=https://api.ibhelm.de/storage/v1/s3
S3_ACCESS_KEY=your-s3-access-key
S3_SECRET_KEY=your-s3-secret-key
S3_BUCKET=files

# Source paths (container-internal, must match volume mount targets)
SYNC_SOURCE_PATHS=/data/test

# Logging
LOG_LEVEL=DEBUG
BETTERSTACK_SOURCE_TOKEN=your-token
```

### 4. Configure Volume Mounts

In `docker-compose.local.yml` (local) or `docker-compose.yml` (NAS):

```yaml
volumes:
  # External path : Container path
  - ./test_data:/data/test:ro      # local
  # - /volume1/projects:/data/projects:ro  # NAS
```

**Important**: Container paths must match `SYNC_SOURCE_PATHS`.

## Local Testing (Mac)

```bash
cd FileMetadataSync

# 1. Create .env (see above)
cp env.example .env

# 2. Create test data
chmod +x scripts/local_test.sh
./scripts/local_test.sh

# 3. Build and run
docker-compose -f docker-compose.local.yml up --build

# Or single sync cycle:
docker-compose -f docker-compose.local.yml run --rm file-metadata-sync python scripts/sync_once.py
```

## NAS Deployment

### Using Container Manager UI

1. Copy files to NAS
2. Open Container Manager → **Project** → **Create**
3. Set path to FileMetadataSync folder
4. Select `docker-compose.yml`
5. Create `.env` file with production values
6. Click **Build** then **Run**

### Using SSH

```bash
cd /path/to/FileMetadataSync
docker-compose up -d --build
```

### Verify

```bash
docker logs file-metadata-sync -f
```

## How It Works

1. **rclone sync**: Efficiently syncs files to S3 using checksums
2. **Metadata extraction**: For each synced file:
   - Calculates SHA256 content hash
   - Reads filesystem metadata (timestamps, permissions, inode)
   - Looks up corresponding `storage.objects` entry
   - Upserts to `public.files` table
3. **Periodic**: Repeats every `SYNC_INTERVAL` seconds

## Database Schema

Files are stored in `public.files`:

| Column | Description |
|--------|-------------|
| `storage_object_id` | Link to `storage.objects` |
| `filename` | File name |
| `folder_path` | Relative path from source |
| `content_hash` | SHA256 hash |
| `file_created_at` | Filesystem ctime |
| `file_modified_at` | Filesystem mtime |
| `file_created_by` | File owner username |
| `filesystem_inode` | Inode number |
| `filesystem_access_rights` | Permission bits (JSON) |
| `filesystem_attributes` | Size, uid, gid, etc. (JSON) |
| `auto_extracted_metadata` | MIME type, extension, paths (JSON) |

## rclone Flags

- `--transfers 8`: 8 parallel file transfers
- `--checkers 16`: 16 parallel file checkers
- `--buffer-size 32M`: Larger buffer for big files
- `--fast-list`: Use fewer API calls for listing
- `--checksum`: Sync based on content hash

## Scripts

```bash
# Full metadata scan (no rclone sync)
docker exec file-metadata-sync python scripts/full_scan.py

# Single sync cycle
docker exec file-metadata-sync python scripts/sync_once.py
```

## Troubleshooting

### "Storage object not found"
File synced but `storage.objects` not updated yet. Will retry next cycle.

### Connection errors
- REST backend: Check `SUPABASE_URL` and `SUPABASE_SERVICE_KEY`
- Postgres backend: Check `PG_DSN` and port 5432 access

### Permission errors
Ensure source folders are readable (`:ro` mount should work).

## Environment Variables

| Variable | Backend | Required | Default | Description |
|----------|---------|----------|---------|-------------|
| `DB_BACKEND` | - | No | `rest` | `rest` or `postgres` |
| `SUPABASE_URL` | rest | Yes | - | Supabase URL |
| `SUPABASE_SERVICE_KEY` | rest | Yes | - | Service role key |
| `PG_DSN` | postgres | Yes | - | PostgreSQL connection string |
| `S3_ENDPOINT` | - | Yes | - | S3 endpoint URL |
| `S3_ACCESS_KEY` | - | Yes | - | S3 access key |
| `S3_SECRET_KEY` | - | Yes | - | S3 secret key |
| `S3_BUCKET` | - | No | `files` | S3 bucket name |
| `S3_REGION` | - | No | `eu-central-1` | S3 region |
| `SYNC_SOURCE_PATHS` | - | Yes | - | Comma-separated container paths |
| `SYNC_INTERVAL` | - | No | `900` | Seconds between syncs |
| `LOG_LEVEL` | - | No | `INFO` | Logging level |
| `BETTERSTACK_SOURCE_TOKEN` | - | No | - | BetterStack token |
| `BETTERSTACK_INGEST_HOST` | - | No | - | BetterStack custom host |
| `TIMEZONE` | - | No | `Europe/Berlin` | Timezone |
