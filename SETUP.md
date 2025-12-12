# FileMetadataSync Setup

Syncs files from local directories to Supabase Storage with metadata tracking.

## Architecture

**Hybrid mode: real-time watcher + daily full scan**

```
┌─────────────────────────────────────────────────────────────┐
│                    FileMetadataSync                         │
├─────────────────────────────────────────────────────────────┤
│  ┌──────────────────┐         ┌──────────────────────────┐  │
│  │  Watcher Thread  │         │    Daily Full Scan       │  │
│  │                  │         │    (scheduled hour)      │  │
│  │  - create        │         │                          │  │
│  │  - modify        │         │  - Catch missed events   │  │
│  │  - move/rename   │         │  - Soft-delete orphans   │  │
│  │                  │         │  - Update moved paths    │  │
│  └────────┬─────────┘         └───────────┬──────────────┘  │
│           └───────────┬───────────────────┘                 │
│                       ▼                                     │
│           ┌───────────────────────┐                         │
│           │   In-memory Queue     │                         │
│           │   + Debounce          │                         │
│           └───────────┬───────────┘                         │
│                       ▼                                     │
│           ┌───────────────────────┐                         │
│           │   Worker Pool         │                         │
│           │   hash → upload → DB  │                         │
│           └───────────────────────┘                         │
└─────────────────────────────────────────────────────────────┘
```

**Key features:**
- **Real-time**: Watcher detects file changes instantly (using inotify on Linux, FSEvents on macOS)
- **Self-contained**: No external cron jobs needed - scheduler runs inside the container
- **Resilient**: Daily full scan catches anything missed by the watcher
- **Soft-delete**: Files removed from filesystem are marked `deleted_at` (not hard-deleted)
- **Thread-safe**: Each worker has its own Supabase client
- **Compensating transactions**: If DB insert fails, file is deleted from storage (no orphans)

## Requirements

- Docker
- Supabase project with:
  - `files` table (managed by ibhelmDB schema)
  - Storage bucket named `files` (or configure via `S3_BUCKET`)

## Configuration

Create `.env` file from example:

```bash
cp env.example .env
```

### Required Settings

| Variable | Description |
|----------|-------------|
| `SUPABASE_URL` | Your Supabase project URL |
| `SUPABASE_SERVICE_KEY` | Service role key (has write access) |

**That's it!** Mount your directories to `/data` in docker-compose and they're automatically scanned.

### Optional Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `S3_BUCKET` | `files` | Storage bucket name |
| `SYNC_WORKERS` | `6` | Number of worker threads |
| `DEBOUNCE_SECONDS` | `3.0` | Wait time before processing events |
| `FULL_SCAN_HOUR` | `3` | Hour (0-23) for daily full scan |
| `FULL_SCAN_ON_STARTUP` | `true` | Run full scan when container starts |
| `IGNORE_PATTERNS` | (see below) | Patterns to ignore |
| `LOG_LEVEL` | `INFO` | Logging level |
| `TIMEZONE` | `Europe/Berlin` | Timezone for scheduling |
| `SYNC_SOURCE_PATHS` | `/data` | Override scan paths (advanced) |

### Default Ignore Patterns

```
*.tmp, *.temp, .DS_Store, Thumbs.db, *.partial,
.syncing, @eaDir/*, #recycle/*, .SynologyWorkingDirectory/*
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

**Test the watcher:**
```bash
# In another terminal, create/modify files
echo "new file" > test_data/newfile.txt
echo "modified" >> test_data/test.txt
mv test_data/test.txt test_data/renamed.txt
```

Watch the logs - you should see events processed within a few seconds.

## Synology NAS Deployment

1. Copy project to NAS (e.g., via SSH or Synology Drive)

2. Create `.env` with your credentials:
```env
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_SERVICE_KEY=your-service-key
```

3. Edit `docker-compose.yml` - just mount your directories to `/data`:
```yaml
volumes:
  # Everything under /data is automatically scanned
  - /volume1/documents:/data/documents:ro
  - /volume1/images:/data/images:ro
  # Or mount a single parent folder:
  # - /volume1/files:/data:ro
```

4. (Optional) Increase inotify watch limit if you have many directories:
```bash
# Check current limit
cat /proc/sys/fs/inotify/max_user_watches

# Increase if needed (default 8192 may not be enough)
echo 524288 | sudo tee /proc/sys/fs/inotify/max_user_watches
```

5. Deploy:
```bash
docker compose up -d
```

## Performance Tuning

| Hardware | Workers | Expected Performance |
|----------|---------|---------------------|
| DS923+ (4 threads) | 4-6 | ~2-3 files/sec |
| DS1621+ (6 threads) | 6-8 | ~4-5 files/sec |
| DS1821+ (8 threads) | 8-12 | ~6-8 files/sec |

### Disk I/O Comparison

| Mode | Daily Disk Reads (100GB dataset) |
|------|----------------------------------|
| Old (polling every 15 min) | ~4.8 TB/day |
| New (watcher + 1 daily scan) | ~100 GB/day |

**~50x reduction in disk I/O** - much gentler on NAS HDDs.

## Monitoring

Logs are written to:
- Console (stdout)
- `./logs/app.log`
- Betterstack (if configured)

### Log Examples

```
FileMetadataSync starting (hybrid mode)
Source paths: ['/data/documents', '/data/images']
Full scan hour: 3:00
Full scan on startup: True
Watching: /data/documents
Watching: /data/images
Watcher started with 2 paths, debounce=3.0s

# Real-time events
Event: created /data/documents/report.pdf
Watcher batch: 1 uploaded, 0 updated, 0 unchanged, 0 errors

# Daily scan
Starting full scan (scheduled)...
Full scan complete: 5 uploaded, 12 updated, 9983 unchanged, 3 soft-deleted, 0 errors in 2.5m
```

## Troubleshooting

### "SUPABASE_URL is required"
Ensure `.env` file exists and contains valid credentials.

### "Failed to upload"
Check Supabase Storage bucket exists and service key has write permissions.

### Watcher not detecting changes
- Check `IGNORE_PATTERNS` isn't excluding your files
- Verify inotify watch limit: `cat /proc/sys/fs/inotify/max_user_watches`
- Check logs for "Watching:" messages

### High memory usage
Reduce `SYNC_WORKERS` to limit concurrent file processing.

### Files not syncing on NAS
- Ensure volumes are mounted with `:ro` (read-only is fine for sync)
- Check Synology Drive isn't creating temp files that match ignore patterns
