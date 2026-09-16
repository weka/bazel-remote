// Tests for the leader-side GC instrumentation.
//
// Two things here are regression guards for mistakes that were nearly shipped:
//
//   - The inventory age distribution must be a SNAPSHOT. The obvious
//     implementation (a prometheus.Histogram, Observe()d once per file per
//     cycle) is cumulative across cycles, so re-observing the whole inventory
//     every few minutes yields "file-observations since process start" rather
//     than "what is on disk now". TestInventoryHistogramIsIdempotent is what
//     catches that.
//
//   - Protection totals must cover the entire inventory. They previously
//     stopped accumulating as soon as the free target was met, so the reported
//     "protected" figures were really "protected bytes encountered before we
//     had queued enough deletions" -- understated by a margin that varied with
//     where the loop happened to break.
package disk

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/buchgr/bazel-remote/v2/cache"
	testutils "github.com/buchgr/bazel-remote/v2/utils"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// putAged writes a CAS blob and backdates its atime by the given duration.
// Returns the blob size on disk as the GC scan will see it.
func putAged(t *testing.T, c *diskCache, seed int, age time.Duration) int64 {
	t.Helper()
	data, hash := testutils.RandomDataAndHash(int64(1024 + seed))
	if err := c.Put(context.Background(), cache.CAS, hash, int64(len(data)),
		bytes.NewReader(data)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	key := cache.LookupKey(cache.CAS, hash)
	c.mu.Lock()
	item, _ := c.lru.Get(key)
	c.mu.Unlock()
	p := filepath.Join(c.dir, c.FileLocation(cache.CAS, item.legacy, hash, item.size, item.random))

	ts := time.Now().Add(-age)
	if err := os.Chtimes(p, ts, ts); err != nil {
		t.Fatalf("Chtimes: %v", err)
	}
	fi, err := os.Stat(p)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	return fi.Size()
}

// gaugeVal reads a single labelled gauge.
func gaugeVal(t *testing.T, g *prometheus.GaugeVec, labels ...string) float64 {
	t.Helper()
	return testutil.ToFloat64(g.WithLabelValues(labels...))
}

// TestInventoryHistogramIsIdempotent runs two scans over an unchanged cache and
// asserts the exported distribution is identical rather than doubled. A plain
// cumulative Histogram would double here.
func TestInventoryHistogramIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	c := newGCCache(t, dir, 1024*1024*1024, time.Hour)

	for i := 0; i < 5; i++ {
		putAged(t, c, i, 48*time.Hour)
	}

	collect := func() (files, bytesTotal uint64) {
		t.Helper()
		reg := prometheus.NewPedanticRegistry()
		if err := reg.Register(c.gcMetrics.inventory); err != nil {
			t.Fatalf("register: %v", err)
		}
		mfs, err := reg.Gather()
		if err != nil {
			t.Fatalf("gather: %v", err)
		}
		for _, mf := range mfs {
			for _, m := range mf.GetMetric() {
				h := m.GetHistogram()
				if h == nil {
					continue
				}
				switch mf.GetName() {
				case "bazel_remote_disk_cache_inventory_age_files":
					files += h.GetSampleCount()
				case "bazel_remote_disk_cache_inventory_age_bytes":
					bytesTotal += h.GetSampleCount()
				}
			}
		}
		return
	}

	scanned, _, err := c.scanFilesWithAtime()
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	c.publishInventoryAges(scanned)
	files1, bytes1 := collect()

	if files1 != 5 {
		t.Fatalf("first scan reported %d files, want 5", files1)
	}

	// Second scan over the same unchanged cache.
	scanned, _, err = c.scanFilesWithAtime()
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	c.publishInventoryAges(scanned)
	files2, bytes2 := collect()

	if files2 != files1 {
		t.Errorf("file count changed across identical scans: %d then %d. "+
			"The distribution must be a snapshot, not accumulated across cycles.",
			files1, files2)
	}
	if bytes2 != bytes1 {
		t.Errorf("byte total changed across identical scans: %d then %d", bytes1, bytes2)
	}
}

// TestInventoryHistogramBucketsByAge asserts files land in the right buckets,
// which is what makes the series usable for choosing a retention value.
func TestInventoryHistogramBucketsByAge(t *testing.T) {
	dir := t.TempDir()
	c := newGCCache(t, dir, 1024*1024*1024, time.Hour)

	// One blob in each of several distinct age bands.
	putAged(t, c, 1, 30*time.Minute)   // <= 1h
	putAged(t, c, 2, 2*time.Hour)      // <= 6h
	putAged(t, c, 3, 25*time.Hour)     // <= 72h
	putAged(t, c, 4, 40*24*time.Hour)  // <= 1440h
	putAged(t, c, 5, 100*24*time.Hour) // > 2160h (only in _count)

	scanned, _, err := c.scanFilesWithAtime()
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	c.publishInventoryAges(scanned)

	reg := prometheus.NewPedanticRegistry()
	if err := reg.Register(c.gcMetrics.inventory); err != nil {
		t.Fatalf("register: %v", err)
	}
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}

	var got map[float64]uint64
	var total uint64
	for _, mf := range mfs {
		if mf.GetName() != "bazel_remote_disk_cache_inventory_age_files" {
			continue
		}
		for _, m := range mf.GetMetric() {
			h := m.GetHistogram()
			total = h.GetSampleCount()
			got = make(map[float64]uint64, len(h.GetBucket()))
			for _, b := range h.GetBucket() {
				got[b.GetUpperBound()] = b.GetCumulativeCount()
			}
		}
	}

	if total != 5 {
		t.Fatalf("sample count = %d, want 5", total)
	}
	// Cumulative counts at selected bounds (bounds are exported in seconds).
	for _, tc := range []struct {
		boundHours float64
		want       uint64
	}{
		{1, 1},    // the 30m blob
		{6, 2},    // + the 2h blob
		{72, 3},   // + the 25h blob
		{720, 3},  // nothing new between 72h and 720h
		{1440, 4}, // + the 40d blob
		{2160, 4}, // the 100d blob is above the last bound
	} {
		if got[tc.boundHours*3600] != tc.want {
			t.Errorf("cumulative count at %.0fh = %d, want %d",
				tc.boundHours, got[tc.boundHours*3600], tc.want)
		}
	}
}

// TestGCProtectionAccountingCoversWholeInventory is the regression test for the
// truncated accounting. Several blobs are protected by min-age; the free target
// is tiny, so the old code would stop counting almost immediately.
func TestGCProtectionAccountingCoversWholeInventory(t *testing.T) {
	dir := t.TempDir()
	// min_age 1h: everything written now is protected.
	c := newGCCache(t, dir, 1024*1024*1024, time.Hour)

	var protectedBytes int64
	const n = 8
	for i := 0; i < n; i++ {
		protectedBytes += putAged(t, c, i, time.Minute)
	}

	// One old blob, so the cycle has something to delete and stops feeding
	// almost immediately.
	putAged(t, c, 99, 72*time.Hour)

	// Target of 1 byte forces "over cap" with a minimal free target.
	c.runGCCycle(1)

	gotBytes := int64(gaugeVal(t, c.gcMetrics.gaugeProtectedBytes, gcReasonMinAge))
	gotFiles := int(gaugeVal(t, c.gcMetrics.gaugeProtectedFiles, gcReasonMinAge))

	if gotFiles != n {
		t.Errorf("protected files = %d, want %d (the whole inventory, not just "+
			"the prefix examined before the free target was met)", gotFiles, n)
	}
	if gotBytes != protectedBytes {
		t.Errorf("protected bytes = %d, want %d", gotBytes, protectedBytes)
	}
}

// TestGCMetricsEmittedOnUnderCapCycle asserts the scan-result series are
// published even when the cache is healthy. If they were only set on the
// over-cap path, they would go stale exactly when nothing is wrong, and
// "GC idle" would be indistinguishable from "GC stopped".
func TestGCMetricsEmittedOnUnderCapCycle(t *testing.T) {
	dir := t.TempDir()
	c := newGCCache(t, dir, 1024*1024*1024, time.Hour)

	for i := 0; i < 3; i++ {
		putAged(t, c, i, time.Hour)
	}

	before := testutil.ToFloat64(c.gcMetrics.counterCycles.WithLabelValues("under_cap"))
	c.runGCCycle(1024 * 1024 * 1024) // far above actual usage
	after := testutil.ToFloat64(c.gcMetrics.counterCycles.WithLabelValues("under_cap"))

	if after != before+1 {
		t.Errorf("under_cap cycle counter = %v, want %v", after, before+1)
	}
	if got := testutil.ToFloat64(c.gcMetrics.gaugeScannedFiles); got != 3 {
		t.Errorf("scanned files = %v, want 3 (must be set on the under-cap path)", got)
	}
	if got := testutil.ToFloat64(c.gcMetrics.gaugeScannedBytes); got <= 0 {
		t.Errorf("scanned bytes = %v, want > 0", got)
	}
	if got := testutil.ToFloat64(c.gcMetrics.gaugeEvictableBytes); got != 0 {
		t.Errorf("evictable bytes = %v, want 0 on an under-cap cycle", got)
	}
}

// TestGCMetricsScanErrorCounted asserts a failed scan is distinguishable from a
// quiet one. Both previously just returned.
func TestGCMetricsScanErrorCounted(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root: permission bits do not deny access")
	}
	dir := t.TempDir()
	c := newGCCache(t, dir, 1024*1024*1024, time.Hour)
	putAged(t, c, 1, time.Hour)

	// Make a shard unreadable so the scan fails.
	casDir := filepath.Join(dir, "cas.v2")
	entries, err := os.ReadDir(casDir)
	if err != nil || len(entries) == 0 {
		t.Fatalf("expected a cas shard: %v", err)
	}
	shard := filepath.Join(casDir, entries[0].Name())
	if err := os.Chmod(shard, 0o000); err != nil {
		t.Fatalf("Chmod: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(shard, 0o755) })

	before := testutil.ToFloat64(c.gcMetrics.counterCycles.WithLabelValues("scan_error"))
	c.runGCCycle(1)
	after := testutil.ToFloat64(c.gcMetrics.counterCycles.WithLabelValues("scan_error"))

	if after != before+1 {
		t.Errorf("scan_error counter = %v, want %v: a GC that cannot read the "+
			"filesystem must not look like a GC with nothing to do", after, before+1)
	}
}

// TestGCDeleteDoesNotInflateEvictionQueue is the regression guard for the
// eviction-storm hazard.
//
// The GC unlinks files itself, then drops the corresponding index entries. If
// that drop goes through RemoveKey -> removeElement, each one also queues an
// eviction and adds the entry's bytes to counterEvictedBytes and
// queuedEvictionsSize. queuedEvictionsSize feeds
// calcTotalDiskSizeAndUpdatePeak, so at production scale (a single cycle has
// deleted 7.28M files / 9.75 TB) it inflates bazel_remote_disk_cache_size_bytes
// by the entire deleted volume -- and with max_size_hard_limit configured,
// Reserve would reject writes for the duration of every GC cycle.
//
// This is cheap to assert and does not need a large fixture, which is the whole
// point: the hazard is not observable on a dev cluster, because the leader is
// deliberately excluded from the client-facing Service.
func TestGCDeleteDoesNotInflateEvictionQueue(t *testing.T) {
	dir := t.TempDir()
	c := newGCCache(t, dir, 1024*1024*1024, time.Hour)

	// Old enough to be evictable.
	const n = 12
	for i := 0; i < n; i++ {
		putAged(t, c, i, 72*time.Hour)
	}

	evictedBefore := testutil.ToFloat64(c.lru.counterEvictedBytes)

	// Force a cycle that must delete everything.
	c.runGCCycle(1)

	if q := c.lru.queuedEvictionsSize.Load(); q != 0 {
		t.Errorf("queuedEvictionsSize = %d after a GC delete phase, want 0. "+
			"GC-deleted entries must not be queued for eviction: the files are "+
			"already unlinked, and the queued bytes inflate the cache-size gauge "+
			"and can trip max_size_hard_limit.", q)
	}

	evictedAfter := testutil.ToFloat64(c.lru.counterEvictedBytes)
	if evictedAfter != evictedBefore {
		t.Errorf("counterEvictedBytes moved by %v during GC deletion, want 0: "+
			"those bytes are reported by the GC's own freed counter, and "+
			"double-counting them here misrepresents LRU eviction pressure.",
			evictedAfter-evictedBefore)
	}
}

// TestGCDeleteRemovesIndexEntriesAcrossBatches asserts that index entries for
// deleted files actually leave the index, both when a batch fills and when the
// remainder is flushed on worker exit.
//
// GC delete workers accumulate keys and drop them under a single lock every
// gcLRURemovalBatch entries, instead of taking the global cache mutex once per
// file. A production cycle deletes millions of files, so per-file locking means
// millions of acquisitions contending with live request traffic. The risk in
// batching is the remainder: if the tail of a partial batch is not flushed on
// worker exit, the index keeps entries for files that no longer exist.
func TestGCDeleteRemovesIndexEntriesAcrossBatches(t *testing.T) {
	dir := t.TempDir()
	c := newGCCache(t, dir, 1024*1024*1024, time.Hour)

	// Deliberately not a multiple of the batch size, so the run ends with a
	// partial batch that only the deferred flush can clear.
	const n = 7
	if n >= gcLRURemovalBatch {
		t.Fatalf("test assumes n < gcLRURemovalBatch (%d)", gcLRURemovalBatch)
	}
	for i := 0; i < n; i++ {
		putAged(t, c, 700+i, 72*time.Hour)
	}

	c.mu.Lock()
	before := c.lru.Len()
	c.mu.Unlock()
	if before != n {
		t.Fatalf("indexed %d entries, want %d", before, n)
	}

	c.runGCCycle(1) // force deletion of everything evictable

	c.mu.Lock()
	after := c.lru.Len()
	c.mu.Unlock()

	if after != 0 {
		t.Errorf("index holds %d entries after GC deleted every file, want 0. "+
			"A partial final batch was not flushed on worker exit, leaving the "+
			"index naming files that no longer exist.", after)
	}
}
