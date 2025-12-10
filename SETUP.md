# FileMetadataSync Setup

Syncs files from NAS to Supabase S3 storage and extracts metadata to database.

## Prerequisites

- Docker (Docker Desktop on Mac, or Synology Container Manager)
- PostgreSQL database (Supabase)
- Supabase S3 Storage credentials

## Local Testing (Mac)

Before deploying to NAS, test locally:

```bash
cd FileMetadataSync

# 1. Create .env file (copy from env.example and fill in values)
cp env.example .env

# 2. Create test data
chmod +x scripts/local_test.sh
./scripts/local_test.sh

# 3. Run with Docker Desktop
docker-compose -f docker-compose.local.yml up --build

# Or run single sync cycle:
docker-compose -f docker-compose.local.yml run --rm file-metadata-sync python scripts/sync_once.py
```

## NAS Deployment

## Quick Start

### 1. Get Supabase S3 Credentials

In Supabase Dashboard:
1. Go to **Settings** → **Storage**
2. Enable S3 Protocol (if not already)
3. Create S3 access keys
4. Note the endpoint URL, access key, and secret key

### 2. Create Environment File

```bash
cp env.example .env
```

Edit `.env`:

```env
# Database - use your Supabase connection string
PG_DSN=postgresql://postgres:PASSWORD@HOST:5432/postgres

# S3 Storage
S3_ENDPOINT=https://YOUR-PROJECT.supabase.co/storage/v1/s3
S3_ACCESS_KEY=your-access-key
S3_SECRET_KEY=your-secret-key
S3_BUCKET=files
S3_REGION=eu-central-1

# Source paths (container paths, see volume mounts)
SYNC_SOURCE_PATHS=/data/projects,/data/documents

# Sync every 15 minutes
SYNC_INTERVAL=900

# BetterStack (optional)
BETTERSTACK_SOURCE_TOKEN=your-token
BETTERSTACK_INGEST_HOST=

LOG_LEVEL=INFO
```

### 3. Configure Volume Mounts

Edit `docker-compose.yml` volumes section to mount your NAS folders:

```yaml
volumes:
  # NAS path : Container path
  - /volume1/projects:/data/projects:ro
  - /volume1/documents:/data/documents:ro
```

**Important**: The container paths (`/data/projects`, etc.) must match `SYNC_SOURCE_PATHS`.

### 4. Deploy on Synology

#### Option A: Using Container Manager UI

1. Open Container Manager
2. Go to **Project** → **Create**
3. Set path to this folder
4. Select `docker-compose.yml`
5. Click **Build** then **Run**

#### Option B: Using SSH

```bash
cd /path/to/FileMetadataSync
docker-compose up -d --build
```

### 5. Verify

Check logs:
```bash
docker logs file-metadata-sync -f
```

Or in Container Manager → Containers → file-metadata-sync → Logs

## How It Works

1. **rclone sync**: Efficiently syncs files to S3 using checksums (only changed files)
2. **Metadata extraction**: For each synced file:
   - Calculates SHA256 content hash
   - Reads filesystem metadata (timestamps, permissions, inode)
   - Looks up corresponding `storage.objects` entry
   - Upserts to `public.files` table
3. **Periodic**: Repeats every `SYNC_INTERVAL` seconds

## Database Schema

Files are stored in `public.files` with these metadata fields:

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

The sync uses these efficient flags:
- `--transfers 8`: 8 parallel file transfers
- `--checkers 16`: 16 parallel file checkers
- `--buffer-size 32M`: Larger buffer for big files
- `--fast-list`: Use fewer API calls for listing
- `--checksum`: Sync based on content hash (more reliable)

## Scripts

### Run full metadata scan (no sync)

Useful to populate metadata for existing files:

```bash
docker exec file-metadata-sync python scripts/full_scan.py
```

### Run single sync cycle

```bash
docker exec file-metadata-sync python scripts/sync_once.py
```

## Troubleshooting

### "Storage object not found"

The file was synced but storage.objects wasn't updated yet. Will retry on next cycle.

### Permission errors

Ensure the source folders are readable by the container (`:ro` mount should work).

### Connection errors

Check PG_DSN and S3 credentials. The app will retry connections automatically.

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `PG_DSN` | Yes | - | PostgreSQL connection string |
| `S3_ENDPOINT` | Yes | - | S3 endpoint URL |
| `S3_ACCESS_KEY` | Yes | - | S3 access key |
| `S3_SECRET_KEY` | Yes | - | S3 secret key |
| `S3_BUCKET` | No | `files` | S3 bucket name |
| `S3_REGION` | No | `eu-central-1` | S3 region |
| `SYNC_SOURCE_PATHS` | Yes | - | Comma-separated paths to sync |
| `SYNC_INTERVAL` | No | `900` | Seconds between syncs |
| `LOG_LEVEL` | No | `INFO` | Logging level |
| `BETTERSTACK_SOURCE_TOKEN` | No | - | BetterStack token |
| `BETTERSTACK_INGEST_HOST` | No | - | BetterStack custom host |
| `TIMEZONE` | No | `Europe/Berlin` | Timezone |

