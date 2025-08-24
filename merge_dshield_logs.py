#!/usr/bin/env python3
"""
Merge rotated gz logs into 50MB chunks without duplication.

- Input patterns (default): /var/log/dshield.log.[1-9].gz
- Output dir: /var/log/dshield_merged/
- State file: /var/lib/dshield_merge/state.json
"""

import os
import sys
import json
import glob
import gzip
import errno
from pathlib import Path
from typing import Dict, Tuple, List

# ----------------- CONFIG -----------------
LOG_DIRS = ["/var/log"]
INPUT_PATTERNS = [
    "dshield.log.[1-9].gz",  
]
OUTPUT_DIR = "/var/log/dshield_merged"
OUTPUT_PREFIX = "dshield-merged-"
CHUNK_BYTES = 50 * 1024 * 1024  # 50MB
STATE_FILE = "/var/lib/dshield_merge/state.json"
# ------------------------------------------

def ensure_dir(path: str, mode: int = 0o755):
    Path(path).mkdir(parents=True, exist_ok=True)
    os.chmod(path, mode)

def load_state() -> Dict:
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {"processed": {}, "last_processed_mtime": 0}
    except json.JSONDecodeError:
        # Corrupted state—start fresh but keep a backup
        backup = STATE_FILE + ".bak"
        try:
            os.rename(STATE_FILE, backup)
        except OSError:
            pass
        return {"processed": {}, "last_processed_mtime": 0}

def save_state(state: Dict):
    ensure_dir(os.path.dirname(STATE_FILE) or "/")
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, sort_keys=True)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, STATE_FILE)

def file_signature(path: str) -> Tuple[int, int, int]:
    """Return a tuple (inode, size, mtime_int) for idempotence."""
    st = os.stat(path)
    return (st.st_ino, st.st_size, int(st.st_mtime))

def discover_inputs() -> List[str]:
    files = []
    for d in LOG_DIRS:
        for pat in INPUT_PATTERNS:
            files.extend(glob.glob(os.path.join(d, pat)))
    # Sort by mtime ascending to keep chronological order
    files.sort(key=lambda p: os.stat(p).st_mtime)
    return files

def next_output_index() -> int:
    ensure_dir(OUTPUT_DIR)
    existing = sorted(glob.glob(os.path.join(OUTPUT_DIR, f"{OUTPUT_PREFIX}*.log")))
    if not existing:
        return 1
    # Parse the highest N from ...-merged-00012.log
    last = existing[-1]
    try:
        base = os.path.basename(last)
        idx = int(base.split(OUTPUT_PREFIX, 1)[1].split(".log", 1)[0])
        return max(1, idx)
    except Exception:
        # Fallback if names are unexpected
        return 1

class ChunkWriter:
    def __init__(self, out_dir: str, prefix: str, start_index: int, chunk_bytes: int):
        self.out_dir = out_dir
        self.prefix = prefix
        self.chunk_bytes = chunk_bytes
        self.index = start_index
        self.fh = None
        self.bytes_in_chunk = 0
        self._open_existing_or_new()

    def _open_existing_or_new(self):
        """Open the current chunk file, continuing if it exists."""
        ensure_dir(self.out_dir)
        path = self._path(self.index)
        mode = "ab" if os.path.exists(path) else "wb"
        self.fh = open(path, mode)
        self.bytes_in_chunk = os.path.getsize(path) if os.path.exists(path) else 0

    def _path(self, idx: int) -> str:
        return os.path.join(self.out_dir, f"{self.prefix}{idx:05d}.log")

    def write_line(self, line: str):
        data = line.encode("utf-8", errors="replace")
        if self.bytes_in_chunk + len(data) > self.chunk_bytes and self.bytes_in_chunk > 0:
            self.rotate()
        self.fh.write(data)
        self.bytes_in_chunk += len(data)

    def rotate(self):
        self.fh.flush()
        os.fsync(self.fh.fileno())
        self.fh.close()
        self.index += 1
        self.fh = open(self._path(self.index), "wb")
        self.bytes_in_chunk = 0

    def close(self):
        if self.fh:
            self.fh.flush()
            os.fsync(self.fh.fileno())
            self.fh.close()
            self.fh = None

def process():
    ensure_dir(OUTPUT_DIR)
    state = load_state()
    processed: Dict[str, Dict] = state.get("processed", {})
    last_mtime_seen = int(state.get("last_processed_mtime", 0))

    inputs = discover_inputs()
    if not inputs:
        print("No matching gz logs found. (Patterns: {})".format(", ".join(INPUT_PATTERNS)))
        return 0

    out_index = next_output_index()
    writer = ChunkWriter(OUTPUT_DIR, OUTPUT_PREFIX, out_index, CHUNK_BYTES)

    processed_count = 0
    for path in inputs:
        try:
            ino, size, mtime = file_signature(path)
        except FileNotFoundError:
            continue  # rotated between scan and stat
        sig = {"ino": ino, "size": size, "mtime": mtime}

        # Skip if we've already processed this exact file (by signature)
        prev = processed.get(path)
        if prev and prev.get("ino") == ino and prev.get("size") == size and prev.get("mtime") == mtime:
            # Already merged this file in a previous run
            last_mtime_seen = max(last_mtime_seen, mtime)
            continue

        # Read & append
        try:
            with gzip.open(path, "rt", encoding="utf-8", errors="replace") as gz:
                for line in gz:
                    # ensure each line ends with a single newline in output
                    if not line.endswith("\n"):
                        line = line + "\n"
                    writer.write_line(line)
        except OSError as e:
            print(f"Warning: failed to read {path}: {e}", file=sys.stderr)
            continue

        # Mark processed
        processed[path] = sig
        last_mtime_seen = max(last_mtime_seen, mtime)
        processed_count += 1
        print(f"Merged: {path}")

    writer.close()

    # Persist state
    state["processed"] = processed
    state["last_processed_mtime"] = last_mtime_seen
    save_state(state)

    print(f"Done. Processed {processed_count} new file(s). Output at {OUTPUT_DIR}")
    return 0

if __name__ == "__main__":
    try:
        sys.exit(process())
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        sys.exit(130)
