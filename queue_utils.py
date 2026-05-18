"""Atomic queue file helpers shared by app/wrapper trigger handoff.

Writer and consumer coordinate via a per-queue lock file plus rename-based
dequeue, so an append can't be lost to a concurrent consumer's wipe.

POSIX: fcntl.flock. Windows: msvcrt.locking on a 1-byte lock file.
The lock is held only across queue-file mutations (append, replace,
recovery append-back). Reading + JSON parsing happen outside the lock.
"""

from __future__ import annotations

import os
import uuid
from contextlib import contextmanager
from pathlib import Path

if os.name == "nt":
    import msvcrt
else:
    import fcntl


@contextmanager
def queue_lock(queue_file: Path):
    """Hold an exclusive lock on the per-queue lock file."""
    queue_file.parent.mkdir(parents=True, exist_ok=True)
    lock_file = queue_file.with_suffix(queue_file.suffix + ".lock")
    if os.name == "nt":
        with open(lock_file, "a+b") as f:
            if f.tell() == 0:
                f.write(b"\0")
                f.flush()
            f.seek(0)
            msvcrt.locking(f.fileno(), msvcrt.LK_LOCK, 1)
            try:
                yield
            finally:
                f.seek(0)
                msvcrt.locking(f.fileno(), msvcrt.LK_UNLCK, 1)
    else:
        with open(lock_file, "a", encoding="utf-8") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(f, fcntl.LOCK_UN)


def append_queue_line(queue_file: Path, line: str) -> None:
    """Append one JSONL line under the queue lock, with fsync."""
    with queue_lock(queue_file):
        with open(queue_file, "a", encoding="utf-8") as f:
            f.write(line)
            if not line.endswith("\n"):
                f.write("\n")
            f.flush()
            os.fsync(f.fileno())


def claim_queue_file(queue_file: Path) -> Path | None:
    """Atomically move the queue file aside for processing.

    Returns the renamed `.processing.<uuid>` path, or None if there's
    nothing to claim. Caller reads + unlinks the returned file.
    """
    with queue_lock(queue_file):
        if not queue_file.exists() or queue_file.stat().st_size == 0:
            return None
        processing = queue_file.with_name(
            f"{queue_file.name}.processing.{uuid.uuid4().hex}"
        )
        os.replace(queue_file, processing)
        return processing


def recover_processing_files(queue_file: Path) -> None:
    """Append back any orphaned `.processing.<id>` files into the queue.

    Run only at controlled lifecycle points (wrapper startup, watcher
    start). Running this every poll tick would race against an actively
    held processing file and manufacture duplicate wakeups.
    """
    pattern = f"{queue_file.name}.processing.*"
    for processing in sorted(queue_file.parent.glob(pattern)):
        try:
            data = processing.read_text(encoding="utf-8")
        except FileNotFoundError:
            continue
        if data.strip():
            with queue_lock(queue_file):
                with open(queue_file, "a", encoding="utf-8") as q:
                    q.write(data)
                    if not data.endswith("\n"):
                        q.write("\n")
                    q.flush()
                    os.fsync(q.fileno())
        try:
            processing.unlink()
        except FileNotFoundError:
            pass
