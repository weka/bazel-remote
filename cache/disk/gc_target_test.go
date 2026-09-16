// Tests for decoupling the GC target from the LRU capacity.
//
// Background: --max_size did two unrelated jobs. It bounded the in-memory index
// AND it was the size the leader's GC reclaimed down to. Those cannot be the
// same number on a shared filesystem, because GC only has work to do when the
// filesystem exceeds the target -- which is exactly the condition under which a
// single value forces the index to be smaller than the filesystem. The oldest
// entries then fall out of the index while their files remain on disk, and this
// replica reports NOT FOUND for blobs another replica serves happily.
//
// That is not hypothetical: it took down builds on the production cluster,
// where the leader indexed ~10.95M of ~40M files on disk and 404'd a blob all
// three followers could serve.
package disk

import (
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/buchgr/bazel-remote/v2/cache"
	testutils "github.com/buchgr/bazel-remote/v2/utils"
)

// TestGCTargetDefaultsToMaxSize asserts the flag is inert when unset, so
// existing deployments behave exactly as before the split.
func TestGCTargetDefaultsToMaxSize(t *testing.T) {
	dir := t.TempDir()
	const maxSize = 4 * 1024 * 1024

	c, err := New(dir, maxSize,
		WithAccessLogger(testutils.NewSilentLogger()),
		WithSharedStorageMode(true),
		WithSharedStorageLeader(true))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	dc := c.(*diskCache)

	if got := dc.gcTargetBytes(); got != maxSize {
		t.Errorf("gcTargetBytes() = %d with the option unset, want %d (max_size). "+
			"An unset flag must not change behaviour.", got, maxSize)
	}
}

// TestGCTargetOverridesMaxSize asserts the override takes effect.
func TestGCTargetOverridesMaxSize(t *testing.T) {
	dir := t.TempDir()
	const maxSize = 8 * 1024 * 1024
	const gcTarget = 2 * 1024 * 1024

	c, err := New(dir, maxSize,
		WithAccessLogger(testutils.NewSilentLogger()),
		WithSharedStorageMode(true),
		WithSharedStorageLeader(true),
		WithSharedStorageGCMaxSize(gcTarget))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	dc := c.(*diskCache)

	if got := dc.gcTargetBytes(); got != gcTarget {
		t.Errorf("gcTargetBytes() = %d, want %d", got, gcTarget)
	}
	if got := dc.lru.MaxSize(); got != maxSize {
		t.Errorf("lru.MaxSize() = %d, want %d: the GC target must not move the "+
			"index capacity", got, maxSize)
	}
}

// TestGCTargetRejectsNegative guards the option itself.
func TestGCTargetRejectsNegative(t *testing.T) {
	dir := t.TempDir()
	_, err := New(dir, 1024*1024,
		WithAccessLogger(testutils.NewSilentLogger()),
		WithSharedStorageGCMaxSize(-1))
	if err == nil {
		t.Error("a negative GC target was accepted, want an error")
	}
}

// TestIndexStaysCompleteWhileGCReclaims is the direct regression test for the
// production outage.
//
// The index capacity is set well above what the cache will hold, while the GC
// target is set low. Every blob must remain indexed -- and therefore servable --
// even as GC reclaims the filesystem down to the smaller target.
//
// Before the split this was impossible to express: one value meant reclaiming
// to a small size necessarily shrank the index to that same size.
func TestIndexStaysCompleteWhileGCReclaims(t *testing.T) {
	dir := t.TempDir()

	// Index capacity deliberately far larger than the content.
	const indexCapacity = 512 * 1024 * 1024

	c, err := New(dir, indexCapacity,
		WithAccessLogger(testutils.NewSilentLogger()),
		WithSharedStorageMode(true),
		WithSharedStorageLeader(true),
		WithSharedStorageGCMinAge(time.Hour))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	dc := c.(*diskCache)

	// A mix of ages: the old ones are evictable, the fresh ones are not.
	const nOld, nFresh = 10, 6
	oldHashes := make([]string, 0, nOld)
	for i := 0; i < nOld; i++ {
		h, p := putCASBlobAt(t, dc, 2000+i, 72*time.Hour)
		oldHashes = append(oldHashes, h)
		_ = p
	}
	freshHashes := make([]string, 0, nFresh)
	for i := 0; i < nFresh; i++ {
		h, _ := putCASBlobAt(t, dc, 3000+i, time.Minute)
		freshHashes = append(freshHashes, h)
	}

	dc.mu.Lock()
	indexedBefore := dc.lru.Len()
	dc.mu.Unlock()
	if indexedBefore != nOld+nFresh {
		t.Fatalf("indexed %d entries before GC, want %d: the index capacity "+
			"should be large enough to hold everything", indexedBefore, nOld+nFresh)
	}

	// Reclaim to a target far below what is on disk. The fresh blobs are
	// protected by min-age, so GC cannot reach the target -- which is precisely
	// the production situation, and must not cause the index to be culled.
	dc.runGCCycle(1)

	dc.mu.Lock()
	indexedAfter := dc.lru.Len()
	dc.mu.Unlock()

	// Every fresh blob must still be both on disk and in the index.
	for _, h := range freshHashes {
		dc.mu.Lock()
		_, present := dc.lru.Get(cache.LookupKey(cache.CAS, h))
		dc.mu.Unlock()
		if present == nil {
			t.Errorf("fresh blob %s fell out of the index; a replica that has "+
				"the file on disk but not in its index reports NOT FOUND for it", h[:12])
		}
	}

	// The index should have shrunk by exactly the blobs GC deleted, no more.
	deleted := 0
	for _, h := range oldHashes {
		dc.mu.Lock()
		_, present := dc.lru.Get(cache.LookupKey(cache.CAS, h))
		dc.mu.Unlock()
		if present == nil {
			deleted++
		}
	}
	if indexedAfter != indexedBefore-deleted {
		t.Errorf("index holds %d entries, want %d (started %d, GC deleted %d). "+
			"Entries disappeared for a reason other than their file being deleted.",
			indexedAfter, indexedBefore-deleted, indexedBefore, deleted)
	}
}

// putCASBlobAt writes a blob and backdates both its atime and mtime.
func putCASBlobAt(t *testing.T, c *diskCache, seed int, age time.Duration) (hash, path string) {
	t.Helper()
	h, p := putCASBlob(t, c, seed)
	ts := time.Now().Add(-age)
	if err := os.Chtimes(p, ts, ts); err != nil {
		t.Fatalf("Chtimes: %v", err)
	}
	return h, p
}

// TestOversizedBlobDoesNotBlockStartup covers a blob larger than the entire
// cache being present on disk at boot.
//
// SizedLRU.Add refuses such an entry. The old handling then tried to delete it
// using filepath.Join(c.dir, lookupKey) -- but a lookup key is "cas/<hash>",
// not a path, so the remove always failed with ENOENT and that error was
// returned from New(). A single oversized blob on disk would stop the server
// from starting at all, with an error naming a file that never existed.
func TestOversizedBlobDoesNotBlockStartup(t *testing.T) {
	dir := t.TempDir()

	// Write a blob through a generously-sized cache first.
	big, err := New(dir, 64*1024*1024, WithAccessLogger(testutils.NewSilentLogger()))
	if err != nil {
		t.Fatalf("New (large): %v", err)
	}
	putCASBlob(t, big.(*diskCache), 1)

	// Now reopen the same directory with a cache far too small to hold it.
	// Boot must succeed regardless.
	small, err := New(dir, 4096, WithAccessLogger(testutils.NewSilentLogger()))
	if err != nil {
		t.Fatalf("New() failed on a cache containing an oversized blob: %v\n"+
			"An unindexable blob must not prevent startup.", err)
	}
	if small == nil {
		t.Fatal("New returned a nil cache")
	}
}

// TestWorkerConcurrencyDefaultsUnchanged asserts that adding the concurrency
// flags is a behavioural no-op until they are set. An upgrade must not silently
// change how hard the scan and GC pools hit the filesystem.
func TestWorkerConcurrencyDefaultsUnchanged(t *testing.T) {
	want := runtime.NumCPU()
	if want < 4 {
		want = 4
	} else if want > defaultWorkerCap {
		want = defaultWorkerCap
	}

	dir := t.TempDir()
	c, err := New(dir, 1024*1024, WithAccessLogger(testutils.NewSilentLogger()))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	dc := c.(*diskCache)

	if got := dc.bootScanWorkers(); got != want {
		t.Errorf("bootScanWorkers() = %d with no override, want %d (the "+
			"historical min(NumCPU, %d) bound)", got, want, defaultWorkerCap)
	}
	if got := dc.gcWorkers(); got != want {
		t.Errorf("gcWorkers() = %d with no override, want %d", got, want)
	}
}

// TestWorkerConcurrencyOverrides asserts the two knobs are independent, which
// is the point of splitting them: the boot scan runs before readiness and can
// be raised freely, while the GC pools contend with live client traffic.
func TestWorkerConcurrencyOverrides(t *testing.T) {
	dir := t.TempDir()
	c, err := New(dir, 1024*1024,
		WithAccessLogger(testutils.NewSilentLogger()),
		WithBootScanConcurrency(64),
		WithGCConcurrency(8))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	dc := c.(*diskCache)

	if got := dc.bootScanWorkers(); got != 64 {
		t.Errorf("bootScanWorkers() = %d, want 64", got)
	}
	if got := dc.gcWorkers(); got != 8 {
		t.Errorf("gcWorkers() = %d, want 8 (must be independent of the boot "+
			"scan setting)", got)
	}
}

// TestWorkerConcurrencyFloor keeps the historical floor: a pool of one or two
// would make a large scan pathologically slow.
func TestWorkerConcurrencyFloor(t *testing.T) {
	dir := t.TempDir()
	c, err := New(dir, 1024*1024,
		WithAccessLogger(testutils.NewSilentLogger()),
		WithGCConcurrency(1))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got := c.(*diskCache).gcWorkers(); got != 4 {
		t.Errorf("gcWorkers() = %d for a configured 1, want the floor of 4", got)
	}
}

// TestWorkerConcurrencyRejectsNegative guards the options.
func TestWorkerConcurrencyRejectsNegative(t *testing.T) {
	dir := t.TempDir()
	if _, err := New(dir, 1024*1024,
		WithAccessLogger(testutils.NewSilentLogger()),
		WithBootScanConcurrency(-1)); err == nil {
		t.Error("negative boot scan concurrency was accepted")
	}
	if _, err := New(t.TempDir(), 1024*1024,
		WithAccessLogger(testutils.NewSilentLogger()),
		WithGCConcurrency(-1)); err == nil {
		t.Error("negative GC concurrency was accepted")
	}
}
