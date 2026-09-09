# Benchmark harness

Local performance measurement for the WEKA fork of bazel-remote. Two layers:

- **Go micro-benchmarks** (`cache/disk/scan_bench_test.go`) — direct measurement of `scanDir` and `scanFilesWithAtime`. Fastest iteration. Use for tight PR loops.
- **Docker-compose end-to-end** (this directory) — full stack with leader + 2 followers on shared bind-mount, exercises the whole boot + GC-cycle path. Use before pushing to dev/rnd.

Both use the same synthetic-cache model matching production file distribution on `oke-rnd`:
- 75% of files sub-512 bytes
- 15% between 512B and 4KB
- 10% between 4KB and 1MB
- Naming: `cas.v2/XX/XX/<hash>-<size>-<random>` (fork's v2 deterministic-with-legacy-random format)

## Layer 1 — Go benchmarks

```bash
cd /path/to/bazel-remote

# Baseline (current unmodified code):
go test -run='^$' -bench=BenchmarkScan -benchtime=1x -count=3 -timeout=60m ./cache/disk/ | tee baseline.txt

# After a code change:
go test -run='^$' -bench=BenchmarkScan -benchtime=1x -count=3 -timeout=60m ./cache/disk/ | tee modified.txt

# Compare (install once: go install golang.org/x/perf/cmd/benchstat@latest)
benchstat baseline.txt modified.txt
```

Individual size targets (pick based on how long you can wait for cache generation):

```bash
# 10K files — ~seconds; sanity check that the bench even runs
go test -run='^$' -bench=BenchmarkScanFilesWithAtime_10K -benchtime=1x ./cache/disk/

# 100K files — ~15-30s prep + <1s scan; fast iteration
go test -run='^$' -bench=BenchmarkScanFilesWithAtime_100K -benchtime=1x ./cache/disk/

# 1M files — ~2-5min prep + seconds scan; realistic perf comparison
go test -run='^$' -bench=BenchmarkScanFilesWithAtime_1M -benchtime=1x ./cache/disk/
```

`-benchtime=1x` is required — cache generation is expensive; we don't want the bench harness re-running it. Use `-count=N` to get variance across multiple runs.

### With CPU/memory profiling

```bash
go test -run='^$' -bench=BenchmarkScanFilesWithAtime_1M -benchtime=1x \
  -cpuprofile=cpu.prof -memprofile=mem.prof ./cache/disk/

go tool pprof -http=:9099 cpu.prof
go tool pprof -http=:9099 mem.prof
```

## Layer 2 — Docker-compose harness

The harness references whatever image tag you pass. **Local builds work fine — no registry / OCIR push needed for benchmarking**, since docker-compose just needs the tag to exist in the local docker daemon.

### Recommended workflow — compose builds the image automatically

The compose file uses `build: ..` + `image: ${BAZEL_REMOTE_IMAGE}` on the leader service, and the followers reference the same tag. `run_bench.sh` triggers `docker compose build` before boot — so a single script call builds from the current working-tree source, tags the image, boots the stack, and measures. No separate `docker build` step needed.

```bash
cd /path/to/bazel-remote/benchmark

# Baseline — checkout the code state you want as the reference point (usually current master
# with no scan changes)
git checkout master
./run_bench.sh bazel-remote:baseline 100000

# ... make code changes on your working branch ...

# Modified — same script call, different tag; compose rebuilds and re-runs
./run_bench.sh bazel-remote:scan-parallel 100000

# Compare the reported boot_time / first_gc_scan_completion / eviction_phase_duration
```

If you want to skip the rebuild (e.g., re-running the same image against a fresh cache), set `NO_BUILD=1`:
```bash
NO_BUILD=1 ./run_bench.sh bazel-remote:baseline 100000
```

Recommended tag conventions (avoid using `v19` — that's just the branch name and would be confusing since baseline == v18 tip == current v19 HEAD):
- `bazel-remote:baseline` — current unchanged code
- `bazel-remote:skip-stat` — after dropping `de.Info()` in `scanDir`
- `bazel-remote:parallel-gc-scan` — after parallelizing `scanFilesWithAtime`
- `bazel-remote:skip-stat+parallel-gc-scan` — combined

### Scale up

```bash
NUM_FILES=1000000 ./run_bench.sh bazel-remote:scan-parallel
```

### What the harness measures

Boots 1 leader + 2 followers on the same bind-mounted `./cache-data` dir (populated by `gen_cache.py`), and measures:

- **`boot_time`** — leader HTTP `/status` answers (proxy for LRU load complete)
- **`first_gc_scan_completion`** — first `Shared storage GC: current size ... exceeds limit` log line appears (proxy for `scanFilesWithAtime` cycle completing)
- **`eviction_phase_duration`** — first `freed` log appears (actual disk-file deletion complete)

Logs are saved to `./bench-logs/<timestamp>-<image-tag-safe>.*.log` for post-hoc analysis.

### Debugging / interactive use

`KEEP_UP=1` skips teardown so you can poke at the running stack:
```bash
KEEP_UP=1 ./run_bench.sh bazel-remote:baseline 100000
# then explore:
curl http://localhost:8080/status
docker compose logs -f leader
# and manually tear down when done:
docker compose down --volumes
```

## Scale reference

Local laptop, warm filesystem, single SSD, single-user:

| Files | Cache prep | scanDir (Go bench) | scanFilesWithAtime (Go bench) |
|---|---|---|---|
| 10K | ~2s | ~50ms | ~30ms |
| 100K | ~15-30s | ~500ms | ~300ms |
| 1M | ~2-5min | ~5-10s | ~5-30s |
| 10M | ~30-60min | minutes | minutes |

Production reference (rnd, WekaFS, ~30M entries, ~85M inodes):
- Boot LRU load: **~77-100 min** (this is the scanDir path)
- First GC cycle: **~17 hours** (this is the scanFilesWithAtime path)

Local numbers won't match production because:
- Different filesystem (local ext4/APFS vs WekaFS network FS)
- Different scale (85M inodes vs 1M local)
- No cross-pod contention
- Warm local metadata cache vs cold WekaFS backend metadata

But **relative improvements** from code changes are directly transferable. If parallelizing `scanFilesWithAtime` gives a 10× speedup locally, it'll give a comparable improvement in production.

## What to expect from the planned changes

Targets for the v19 work — measure these to validate:

1. **Skip `de.Info()` in `scanDir`** (`load.go:456`) — should eliminate ~30M syscalls per boot on rnd. Locally: `BenchmarkScanDir_1M` should drop measurably (30-70% depending on cache-hit vs cold-stat ratio).
2. **Parallelize `scanFilesWithAtime`** (`load.go:920`) — should scale ~N× with worker count. Locally: `BenchmarkScanFilesWithAtime_1M` should drop by ~4-8× on a laptop.
3. **Skip stat in `scanFilesWithAtime`** — combined with parallelization, could reach 20-50× speedup vs baseline.
