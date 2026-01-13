# FileMetadataSync - CAS Hybrid Sync Engine

A high-performance file synchronization service that bridges local NAS storage (Synology) with the IBHelm cloud database and storage. It implements a **Content-Addressable Storage (CAS)** architecture and a **Hybrid Processing** model to maximize efficiency and reliability.

## 🏗 Architecture: CAS Hybrid Mode

The system is split into three decoupled components that communicate through the PostgreSQL database:

### 1. The Watcher (Real-time)
- Uses a `PollingObserver` to monitor filesystem events (`CREATED`, `MODIFIED`, `MOVED`).
- **Debouncing**: Events are held in a queue for 3 seconds (configurable) to prevent duplicate processing of temporary files.
- **Docker-Ready**: The polling mechanism ensures reliability even when mounting network shares or using Docker volumes where standard inotify might fail.

### 2. The Scanner (Full Reconciliation)
- Performs a deep-sweep of configured directories.
- **Reconciliation**: Compares local files against the `files` database map.
- **Self-Healing**: Marks files as `deleted_at` if they are no longer present on the disk.
- **CAS Registration**: Computes SHA256 hashes and registers them in the `file_contents` table.

### 3. The Uploader (Background)
- A dedicated thread that processes the "Upload Queue" stored in the database.
- **Atomic Dequeue**: Scalable batch processing via PostgreSQL RPC (`dequeue_upload_batch`).
- **Content-Addressable Storage**: Files are stored in S3 at paths derived from their hash: `files/{hash}{extension}`.
- **Deduplication**: Multiple paths pointing to the identical content upload only once.

## 🔄 The Data Pipeline

```
[ Local Filesystem ]
        │
  (Watcher/Scanner)
        │
        ▼
[ PostgreSQL Table: files ] <─── Registers metadata (Path, Size, Inode)
        │
        ▼
[ PostgreSQL Table: file_contents ] <─── Registers content (Hash)
        │
 [ RPC: Dequeue Batch ]
        │
        ▼
[ S3 / Supabase Storage ] <─── Uploads physical data to files/{hash}.ext
```

## ✨ New Features in this Version

- **Hybrid Mode**: Combines real-time responsiveness with an optional daily reconciliation scan (scheduled hour).
- **Streaming Hash**: SHA256 calculation happens in chunks, avoiding high memory usage for large files.
- **Soft-Deletion**: Files are never hard-deleted from the database during a sync; they are marked with a timestamp for audit trails.
- **Unified Logic**: Both the Watcher and Scanner use the same `process_single_file` registration pipeline.
- **Auto-Linking**: Database triggers automatically link new file registrations to projects, buildings, and cost groups based on their paths and metadata.

## 🚀 Quick Start

1. **Configure Environment**:
   ```bash
   cp env.example .env
   # Set SUPABASE_URL and SUPABASE_SERVICE_KEY
   ```

2. **Mount Volumes**:
   In `docker-compose.yml`, mount your NAS folders to `/data`:
   ```yaml
   volumes:
     - /volume1/documents:/data/documents:ro
   ```

3. **Deploy**:
   ```bash
   docker compose up -d
   ```

## 🛠 Advanced Features

- **Polling Observer**: Specifically chosen for Synology NAS stability.
- **Custom Ignore Patterns**: Flexible fnmatch support to skip @eaDir, #recycle, and temp browser files.
- **Threaded Scanner**: Parallelizes hashing and DB registration for high-throughput initial indexing.
