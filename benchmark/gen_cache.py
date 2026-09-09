#!/usr/bin/env python3
"""Generate a synthetic bazel-remote CAS cache for local benchmarking.

Mimics the file distribution observed on oke-rnd's production bazel_fs:
  75% of files are sub-512 bytes
  15% are 512B - 4KB
  10% are 4KB - 1MB

Files are truncated (sparse) so generation is bounded by inode/dirent
creation, not by data copy — matches what we're benchmarking anyway
(walk/stat performance, not throughput).

Naming follows the fork's compressed-CAS convention (v18+):
  <dir>/cas.v2/<hash[:2]>/<hash>-<size>-<random>

Only ONE level of sharding (256 shards, not 256x256) — files sit directly
under the two-hex-char shard directory. This matches the production layout
on oke-rnd's WekaFS and what scanDir/scanFilesWithAtime expect.

Deterministic: uses SHA-256 of the index as hash, so re-running with
the same numFiles produces the exact same tree.

Usage:
    python3 gen_cache.py <target_dir> <num_files>

Example (for a 100K-file benchmark cache):
    python3 gen_cache.py ./cache-data 100000

Reference sizes:
     10K files: ~5MB total disk (sparse), ~2s gen
    100K files: ~50MB total disk (sparse), ~15s gen
      1M files: ~500MB total disk (sparse), ~2-5min gen
     10M files: ~5GB total disk (sparse), ~30-60min gen
"""

import hashlib
import os
import struct
import sys
import time


def gen(target_dir: str, num_files: int) -> None:
    cas_root = os.path.join(target_dir, "cas.v2")
    os.makedirs(cas_root, exist_ok=True)

    # Pre-create the 256 shard directories once. mkdir-per-file is far slower.
    print(f"pre-creating 256 shard directories under {cas_root}...", file=sys.stderr)
    for a in range(256):
        os.makedirs(os.path.join(cas_root, f"{a:02x}"), exist_ok=True)

    # Also create ac.v2 and raw.v2 with 256 shard dirs each (empty) — scanDir
    # walks them harmlessly. scanDir errors on unexpected top-level dirs.
    for kind_dir in ("ac.v2", "raw.v2"):
        for a in range(256):
            os.makedirs(os.path.join(target_dir, kind_dir, f"{a:02x}"), exist_ok=True)

    print(f"generating {num_files} files...", file=sys.stderr)
    t0 = time.monotonic()
    last_report = t0

    for i in range(num_files):
        # Deterministic hash from index.
        h = hashlib.sha256(struct.pack("<Q", i)).digest()
        hash_str = h.hex()

        # Size distribution — bucket by (i mod 100).
        bucket = i % 100
        if bucket < 75:  # < 512B
            size = (h[0] * 2 + 1) & 0x1FF or 1
        elif bucket < 90:  # 512B - 4KB
            size = 512 + (struct.unpack_from("<H", h, 0)[0] % 3584)
        else:  # 4KB - 1MB
            size = 4096 + (struct.unpack_from("<I", h, 0)[0] % (1024 * 1024 - 4096))

        # Random suffix (8 hex chars — deterministic from hash).
        rand_suffix = h[28:32].hex()
        fname = f"{hash_str}-{size}-{rand_suffix}"
        full_path = os.path.join(cas_root, hash_str[:2], fname)

        with open(full_path, "wb") as f:
            f.truncate(size)

        # Progress every ~5s or every 100K files.
        now = time.monotonic()
        if i > 0 and (i % 100_000 == 0 or now - last_report > 5):
            rate = i / (now - t0)
            print(
                f"  {i}/{num_files} files ({rate:.0f}/s)",
                file=sys.stderr,
            )
            last_report = now

    elapsed = time.monotonic() - t0
    print(
        f"done: {num_files} files under {target_dir} in {elapsed:.1f}s "
        f"({num_files / elapsed:.0f} files/s)",
        file=sys.stderr,
    )


def main() -> None:
    if len(sys.argv) != 3:
        print(__doc__, file=sys.stderr)
        sys.exit(2)
    target_dir = sys.argv[1]
    try:
        num_files = int(sys.argv[2])
    except ValueError:
        print(f"error: num_files must be an integer, got {sys.argv[2]!r}", file=sys.stderr)
        sys.exit(2)
    if num_files < 1:
        print(f"error: num_files must be >= 1", file=sys.stderr)
        sys.exit(2)
    gen(target_dir, num_files)


if __name__ == "__main__":
    main()
