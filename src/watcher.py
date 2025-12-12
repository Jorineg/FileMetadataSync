"""
Filesystem watcher with debouncing for real-time file sync.
Uses watchdog (cross-platform: inotify on Linux, FSEvents on macOS).
"""
import fnmatch
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Callable

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileSystemEvent

from src import settings
from src.logging_conf import logger


class EventType(Enum):
    CREATED = "created"
    MODIFIED = "modified"
    MOVED = "moved"


@dataclass
class PendingEvent:
    """A file event waiting to be processed after debounce window."""
    path: Path
    event_type: EventType
    timestamp: float
    dest_path: Path | None = None  # For move events


@dataclass
class EventQueue:
    """Thread-safe in-memory queue with debouncing."""
    debounce_seconds: float
    _events: dict = field(default_factory=dict)  # path -> PendingEvent
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def add(self, event: PendingEvent) -> None:
        """Add or update an event (later events replace earlier ones for same path)."""
        with self._lock:
            self._events[str(event.path)] = event

    def get_ready(self) -> list[PendingEvent]:
        """Get events that have passed the debounce window."""
        now = time.time()
        cutoff = now - self.debounce_seconds
        ready = []
        with self._lock:
            to_remove = []
            for path, event in self._events.items():
                if event.timestamp < cutoff:
                    ready.append(event)
                    to_remove.append(path)
            for path in to_remove:
                del self._events[path]
        return ready

    def pending_count(self) -> int:
        with self._lock:
            return len(self._events)


class SyncEventHandler(FileSystemEventHandler):
    """Handles filesystem events and adds them to the queue."""

    def __init__(self, queue: EventQueue, source_bases: list[Path], ignore_patterns: list[str]):
        super().__init__()
        self.queue = queue
        self.source_bases = source_bases
        self.ignore_patterns = ignore_patterns

    def _should_ignore(self, path: str) -> bool:
        """Check if path matches any ignore pattern."""
        path_obj = Path(path)
        # Check filename and full path against patterns
        for pattern in self.ignore_patterns:
            if fnmatch.fnmatch(path_obj.name, pattern):
                return True
            if fnmatch.fnmatch(str(path_obj), f"*{pattern}"):
                return True
        # Ignore hidden files/dirs
        if any(part.startswith('.') for part in path_obj.parts):
            return True
        return False

    def _is_file(self, path: str) -> bool:
        """Check if path is a file (not directory)."""
        return Path(path).is_file()

    def on_created(self, event: FileSystemEvent) -> None:
        if event.is_directory or self._should_ignore(event.src_path):
            return
        # Wait a moment for file to be fully written
        if not self._is_file(event.src_path):
            return
        self.queue.add(PendingEvent(
            path=Path(event.src_path),
            event_type=EventType.CREATED,
            timestamp=time.time()
        ))
        logger.debug(f"Event: created {event.src_path}")

    def on_modified(self, event: FileSystemEvent) -> None:
        if event.is_directory or self._should_ignore(event.src_path):
            return
        if not self._is_file(event.src_path):
            return
        self.queue.add(PendingEvent(
            path=Path(event.src_path),
            event_type=EventType.MODIFIED,
            timestamp=time.time()
        ))
        logger.debug(f"Event: modified {event.src_path}")

    def on_moved(self, event: FileSystemEvent) -> None:
        if event.is_directory:
            return
        # Handle source being ignored (file moved into watched area)
        src_ignored = self._should_ignore(event.src_path)
        dest_ignored = self._should_ignore(event.dest_path)
        
        if dest_ignored:
            return  # Moved to ignored location, ignore
        
        if src_ignored:
            # Moved from ignored to watched = treat as created
            self.queue.add(PendingEvent(
                path=Path(event.dest_path),
                event_type=EventType.CREATED,
                timestamp=time.time()
            ))
        else:
            # Normal move within watched area
            self.queue.add(PendingEvent(
                path=Path(event.src_path),
                event_type=EventType.MOVED,
                timestamp=time.time(),
                dest_path=Path(event.dest_path)
            ))
        logger.debug(f"Event: moved {event.src_path} → {event.dest_path}")


class FileWatcher:
    """Manages filesystem watching across multiple source paths."""

    def __init__(
        self,
        source_paths: list[str],
        on_events_ready: Callable[[list[PendingEvent]], None],
        debounce_seconds: float = 3.0,
        ignore_patterns: list[str] | None = None
    ):
        self.source_paths = [Path(p) for p in source_paths]
        self.on_events_ready = on_events_ready
        self.queue = EventQueue(debounce_seconds=debounce_seconds)
        self.ignore_patterns = ignore_patterns or settings.IGNORE_PATTERNS
        self.observer = Observer()
        self._processor_thread: threading.Thread | None = None
        self._running = False

    def start(self) -> None:
        """Start watching all source paths."""
        handler = SyncEventHandler(self.queue, self.source_paths, self.ignore_patterns)
        
        for source_path in self.source_paths:
            if not source_path.exists():
                logger.warning(f"Watch path does not exist: {source_path}")
                continue
            self.observer.schedule(handler, str(source_path), recursive=True)
            logger.info(f"Watching: {source_path}")

        self._running = True
        self.observer.start()
        
        # Start queue processor thread
        self._processor_thread = threading.Thread(target=self._process_queue, daemon=True)
        self._processor_thread.start()
        
        logger.info(f"Watcher started with {len(self.source_paths)} paths, debounce={self.queue.debounce_seconds}s")

    def stop(self) -> None:
        """Stop watching."""
        self._running = False
        self.observer.stop()
        self.observer.join(timeout=5)
        if self._processor_thread:
            self._processor_thread.join(timeout=5)
        logger.info("Watcher stopped")

    def _process_queue(self) -> None:
        """Continuously process ready events from the queue."""
        while self._running:
            try:
                ready_events = self.queue.get_ready()
                if ready_events:
                    logger.info(f"Processing {len(ready_events)} events (pending: {self.queue.pending_count()})")
                    self.on_events_ready(ready_events)
            except Exception as e:
                logger.error(f"Error processing events: {e}", exc_info=True)
            time.sleep(0.5)  # Check every 500ms

    def get_source_base(self, filepath: Path) -> Path | None:
        """Find which source base a file belongs to."""
        for base in self.source_paths:
            try:
                filepath.relative_to(base)
                return base
            except ValueError:
                continue
        return None

