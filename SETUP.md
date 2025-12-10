# FileMetadataSync Setup

Syncs files from local directories to Supabase Storage with metadata tracking.

## Architecture

- **UUID-based storage paths**: Files are stored with UUID names, original filenames preserved in metadata
- **Single-phase workers**: Each worker handles hash → compare → upload → insert (no waiting)
- **Thread-safe**: Each worker has its own Supabase client
- **Self-healing**: Failed files retry automatically on next sync cycle
- **Compensating transactions**: If DB insert fails, file is deleted from storage (no orphans)

## Requirements

- Docker
- Supabase project with:
  - `files` table (see schema below)
  - Storage bucket named `files` (or configure via `S3_BUCKET`)

## Database Schema

Run this SQL in your Supabase SQL editor:

```sql
CREATE TABLE IF NOT EXISTS public.files (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    storage_path TEXT UNIQUE NOT NULL,  -- UUID-based path in storage
    content_hash TEXT NOT NULL,         -- SHA256 for change detection
    filename TEXT NOT NULL,             -- Original filename
    folder_path TEXT,                   -- Original folder path
    file_created_at TIMESTAMPTZ,
    file_modified_at TIMESTAMPTZ,
    filesystem_attributes JSONB,
    auto_extracted_metadata JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Index for hash lookups (critical for performance)
CREATE INDEX IF NOT EXISTS idx_files_content_hash ON public.files(content_hash);

-- Index for folder queries
CREATE INDEX IF NOT EXISTS idx_files_folder_path ON public.files(folder_path);
```

## Configuration

Create `.env` file:

```env
# Supabase (required)
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_SERVICE_KEY=your-service-role-key

# Storage bucket (default: files)
S3_BUCKET=files

# Source paths (container-internal paths, comma-separated)
SYNC_SOURCE_PATHS=/data/documents,/data/images

# Sync interval in seconds (default: 900 = 15 minutes)
SYNC_INTERVAL=900

# Worker threads (default: 6, adjust for your hardware)
# Synology DS923+: 4-6 recommended
SYNC_WORKERS=6

# Logging (optional)
LOG_LEVEL=INFO
BETTERSTACK_SOURCE_TOKEN=
BETTERSTACK_INGEST_HOST=
```

## Local Testing (Mac/Linux)

```bash
# Create test data
mkdir -p test_data
echo "test content" > test_data/test.txt

# Create .env from example
cp env.example .env
# Edit .env with your Supabase credentials

# Run
docker compose -f docker-compose.local.yml up --build
```

## Synology NAS Deployment

1. Copy project to NAS
2. Create `.env` with your config
3. Edit `docker-compose.yml` to map your data directories:

```yaml
volumes:
  - /volume1/documents:/data/documents:ro
  - /volume1/images:/data/images:ro
```

4. Deploy:

```bash
docker compose up -d
```

## Performance Tuning

| Hardware | Workers | Expected Performance |
|----------|---------|---------------------|
| DS923+ (4 threads) | 4-6 | ~2-3 files/sec |
| DS1621+ (6 threads) | 6-8 | ~4-5 files/sec |
| DS1821+ (8 threads) | 8-12 | ~6-8 files/sec |

### Sync Time Estimates (100k files, 100GB)

| Scenario | 1GbE | 10GbE |
|----------|------|-------|
| Initial sync | 15-20 min | 8-12 min |
| Typical sync (1% changed) | 2-3 min | 1-2 min |
| Typical sync (0.1% changed) | 30-60 sec | 30 sec |

## Monitoring

Logs are written to:
- Console (stdout)
- `./logs/app.log`
- Betterstack (if configured)

Log format:
```
2024-01-15 10:30:45 [INFO] file_metadata_sync: Sync complete: 150 uploaded, 0 errors in 2.5m
```

## Troubleshooting

### "SUPABASE_URL is required"
Ensure `.env` file exists and contains valid credentials.

### "Failed to upload"
Check Supabase Storage bucket exists and service key has write permissions.

### High memory usage
Reduce `SYNC_WORKERS` to limit concurrent file processing.

### Slow sync
Increase `SYNC_WORKERS` if CPU/network are not saturated.
