// Package disk benchmarks for the scan functions.
//
// These benchmarks target the two performance-critical scan paths on shared_storage_mode:
//   - scanDir:            invoked once per pod boot to rebuild the in-memory LRU from disk.
//                         On rnd this takes ~77-100 min for the current ~30M-entry cache.
//   - scanFilesWithAtime: invoked every 5m by the leader's GC ticker.
//                         On rnd this takes many hours per cycle (serial filepath.Walk +
//                         per-file stat), causing GC cycles to run back-to-back forever.
//
// Usage:
//
//   # Baseline (before code changes):
//   go test -run='^$' -bench=BenchmarkScan -benchtime=1x -count=3 -timeout=60m ./cache/disk/ > baseline.txt
//
//   # After code changes:
//   go test -run='^$' -bench=BenchmarkScan -benchtime=1x -count=3 -timeout=60m ./cache/disk/ > modified.txt
//
//   # Compare:
//   go install golang.org/x/perf/cmd/benchstat@latest
//   benchstat baseline.txt modified.txt
//
// -benchtime=1x is important: cache generation is expensive (minutes for large sizes),
// so we don't want the bench harness auto-multiplying b.N.
//
// The synthetic cache generator uses a deterministic (seeded) size distribution and
// hash sequence so that two runs of the same benchmark inspect identically-sized
// filesystem layouts.
//
// Scale reference (local laptop, warm FS):
//    10K files:  ~1s prep,  scanDir ~50ms, scanFilesWithAtime ~30ms
//   100K files:  ~10s prep, scanDir ~500ms, scanFilesWithAtime ~300ms
//     1M files:  ~2-5min prep, scanDir ~5-10s, scanFilesWithAtime ~5-30s
//    10M files:  ~30-60min prep, scanDir minutes, scanFilesWithAtime minutes
//
// Production reference (rnd, WekaFS, ~30M entries, ~85M inodes):
//   scanDir ~77-100 min, scanFilesWithAtime ~hours per cycle.

package disk

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// generateSyntheticCache creates a bazel-remote CAS cache under dir with numFiles
// synthetic blobs matching production file distribution observed on oke-rnd:
//   - 75% of files are sub-512 bytes
//   - 15% are 512B - 4KB
//   - 10% are 4KB - 1MB
//
// Naming follows the fork's compressed-CAS convention (matches production layout
// on oke-rnd's WekaFS — see cache/disk/load.go:530-585 for the walker):
//
//	<dir>/cas.v2/<hash[:2]>/<hash>-<size>-<random>
//
// Only ONE level of sharding (256 shards, not 256×256) — files sit directly
// under the two-hex-char shard directory. scanDir walks two levels deep
// (cas.v2 → shard → files) and errors on unexpected nesting.
//
// Files are truncated (sparse) so the generator itself is fast — we're benchmarking
// walk/stat performance, not file content.
//
// Deterministic: uses SHA-256 of the index as the hash and derived-bytes for size,
// so two invocations with the same numFiles produce identical trees.
func generateSyntheticCache(tb testing.TB, dir string, numFiles int) {
	tb.Helper()

	// Ensure the base cas.v2 dir exists — mkdir per-file is expensive, so we
	// pre-create the 256 shard directories in one pass instead of letting
	// MkdirAll fight itself on every file.
	casRoot := filepath.Join(dir, "cas.v2")
	for a := 0; a < 256; a++ {
		p := filepath.Join(casRoot, fmt.Sprintf("%02x", a))
		if err := os.MkdirAll(p, 0755); err != nil {
			tb.Fatalf("mkdir shard: %v", err)
		}
	}

	// Also create ac.v2 and raw.v2 with 256 shard dirs each so scanDir walks them
	// harmlessly (empty dirs). scanDir errors on unexpected top-level dirs.
	for _, kindDir := range []string{"ac.v2", "raw.v2"} {
		for a := 0; a < 256; a++ {
			p := filepath.Join(dir, kindDir, fmt.Sprintf("%02x", a))
			if err := os.MkdirAll(p, 0755); err != nil {
				tb.Fatalf("mkdir %s shard: %v", kindDir, err)
			}
		}
	}

	for i := 0; i < numFiles; i++ {
		// Deterministic hash from index.
		var idxBuf [8]byte
		binary.LittleEndian.PutUint64(idxBuf[:], uint64(i))
		sum := sha256.Sum256(idxBuf[:])
		hashStr := hex.EncodeToString(sum[:])

		// Size distribution — deterministic bucket by (i mod 100).
		bucket := i % 100
		var size int64
		switch {
		case bucket < 75: // < 512B
			size = int64(sum[0])<<1 + 1 // 1..511
			if size > 511 {
				size = 511
			}
		case bucket < 90: // 512B - 4KB
			size = 512 + int64(binary.LittleEndian.Uint16(sum[0:2]))%3584
		default: // 4KB - 1MB
			size = 4096 + int64(binary.LittleEndian.Uint32(sum[0:4]))%(1024*1024-4096)
		}

		// Random suffix (8 hex chars — deterministic from hash).
		randSuffix := hex.EncodeToString(sum[28:32])
		fname := fmt.Sprintf("%s-%d-%s", hashStr, size, randSuffix)
		fullPath := filepath.Join(casRoot, hashStr[:2], fname)

		f, err := os.Create(fullPath)
		if err != nil {
			tb.Fatalf("create %s: %v", fullPath, err)
		}
		if err := f.Truncate(size); err != nil {
			tb.Fatalf("truncate %s: %v", fullPath, err)
		}
		if err := f.Close(); err != nil {
			tb.Fatalf("close %s: %v", fullPath, err)
		}

		if i > 0 && i%100_000 == 0 {
			tb.Logf("  generated %d/%d files", i, numFiles)
		}
	}
	tb.Logf("cache generation complete: %d files under %s", numFiles, dir)
}

// -----------------------------------------------------------------------------
// scanDir — boot-time LRU load path
// -----------------------------------------------------------------------------

func BenchmarkScanDir_10K(b *testing.B)  { benchmarkScanDir(b, 10_000) }
func BenchmarkScanDir_100K(b *testing.B) { benchmarkScanDir(b, 100_000) }
func BenchmarkScanDir_1M(b *testing.B)   { benchmarkScanDir(b, 1_000_000) }

// Uncomment for near-production-scale runs. Expect long generation time (30-60min)
// and ~2GB+ of disk usage under b.TempDir().
// func BenchmarkScanDir_10M(b *testing.B) { benchmarkScanDir(b, 10_000_000) }

func benchmarkScanDir(b *testing.B, numFiles int) {
	dir := b.TempDir()
	generateSyntheticCache(b, dir, numFiles)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c := &diskCache{dir: dir}
		result, err := c.scanDir()
		if err != nil {
			b.Fatalf("scanDir: %v", err)
		}
		if len(result.item) == 0 {
			b.Fatalf("scanDir returned zero items (expected %d)", numFiles)
		}
		if len(result.item) != numFiles {
			b.Logf("scanDir returned %d items, expected %d (may indicate filename-format filtering)", len(result.item), numFiles)
		}
	}
}

// -----------------------------------------------------------------------------
// scanFilesWithAtime — leader GC-cycle scan
// -----------------------------------------------------------------------------

func BenchmarkScanFilesWithAtime_10K(b *testing.B)  { benchmarkScanFilesWithAtime(b, 10_000) }
func BenchmarkScanFilesWithAtime_100K(b *testing.B) { benchmarkScanFilesWithAtime(b, 100_000) }
func BenchmarkScanFilesWithAtime_1M(b *testing.B)   { benchmarkScanFilesWithAtime(b, 1_000_000) }

// func BenchmarkScanFilesWithAtime_10M(b *testing.B) { benchmarkScanFilesWithAtime(b, 10_000_000) }

func benchmarkScanFilesWithAtime(b *testing.B, numFiles int) {
	dir := b.TempDir()
	generateSyntheticCache(b, dir, numFiles)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c := &diskCache{dir: dir}
		files, totalSize, err := c.scanFilesWithAtime()
		if err != nil {
			b.Fatalf("scanFilesWithAtime: %v", err)
		}
		if totalSize == 0 {
			b.Fatalf("scanFilesWithAtime returned zero totalSize")
		}
		if len(files) != numFiles {
			b.Logf("scanFilesWithAtime returned %d files, expected %d", len(files), numFiles)
		}
	}
}
