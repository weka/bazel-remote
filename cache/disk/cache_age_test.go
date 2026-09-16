// Tests for updateCacheAgeMetric.
//
// In shared storage mode the leader's GC unlinks files from the shared
// filesystem without telling the other replicas, so a follower's LRU tail
// routinely points at a file that is already gone. Before the fix this wedged
// the metric permanently: the stat failed, the gauge was left untouched, the
// stale entry stayed at the tail, and the next tick retried the same dead entry
// a minute later. Observed in production as a frozen dashboard value plus one
// ERROR per minute per pod, indefinitely.
//
// The distinction that matters here is ENOENT vs everything else. A missing
// file means the entry is genuinely dead and must be shed. Any other stat
// error (EIO, EACCES, ESTALE -- all plausible on a network filesystem under
// load) may be transient, and evicting on those would silently shrink the
// index. That is not harmless: an index entry for a legacy random-suffixed
// blob cannot be rebuilt, because discoverAndIndex only knows the
// deterministic path.
package disk

import (
	"bytes"
	"context"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/buchgr/bazel-remote/v2/cache"
	testutils "github.com/buchgr/bazel-remote/v2/utils"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

// newAgeCache builds a disk cache without starting any background goroutine,
// so tests can call updateCacheAgeMetric directly and deterministically.
func newAgeCache(t *testing.T, dir string) *diskCache {
	t.Helper()
	c, err := New(dir, 1024*1024*1024,
		WithAccessLogger(testutils.NewSilentLogger()))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c.(*diskCache)
}

// putCASBlob writes a blob through the cache and returns its on-disk path.
func putCASBlob(t *testing.T, c *diskCache, seed int) (hash string, path string) {
	t.Helper()
	data, h := testutils.RandomDataAndHash(64 + int64(seed))
	if err := c.Put(context.Background(), cache.CAS, h, int64(len(data)),
		bytes.NewReader(data)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	key := cache.LookupKey(cache.CAS, h)
	c.mu.Lock()
	item, _ := c.lru.Get(key)
	c.mu.Unlock()
	return h, filepath.Join(c.dir, c.FileLocation(cache.CAS, item.legacy, h, item.size, item.random))
}

// ageOf reads the current value of the idle-time gauge.
func ageOf(c *diskCache) float64 { return testutil.ToFloat64(c.gaugeCacheAge) }

// validOf reads the companion validity gauge.
func validOf(c *diskCache) float64 { return testutil.ToFloat64(c.gaugeCacheAgeValid) }

// TestCacheAgeMetric_SelfHealsOnDeletedTail is the direct regression test for
// the production wedge: delete the tail's backing file but leave the index
// entry, then assert the metric advances to the next-oldest live item and the
// dead key is gone.
func TestCacheAgeMetric_SelfHealsOnDeletedTail(t *testing.T) {
	dir := t.TempDir()
	c := newAgeCache(t, dir)

	deadHash, deadPath := putCASBlob(t, c, 1)
	time.Sleep(10 * time.Millisecond)
	liveHash, livePath := putCASBlob(t, c, 2)

	// Make the first blob the LRU tail (oldest atime) and the second clearly
	// newer, so the expected surviving value is unambiguous.
	old := time.Now().Add(-48 * time.Hour)
	if err := os.Chtimes(deadPath, old, old); err != nil {
		t.Fatalf("Chtimes: %v", err)
	}
	recent := time.Now().Add(-1 * time.Hour)
	if err := os.Chtimes(livePath, recent, recent); err != nil {
		t.Fatalf("Chtimes: %v", err)
	}

	// Simulate the leader's GC unlinking the file out from under this replica.
	if err := os.Remove(deadPath); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	c.updateCacheAgeMetric()

	deadKey := cache.LookupKey(cache.CAS, deadHash)
	c.mu.Lock()
	_, stillIndexed := c.lru.Get(deadKey)
	liveIndexed := func() bool {
		_, ok := c.lru.Get(cache.LookupKey(cache.CAS, liveHash))
		return ok != nil
	}()
	c.mu.Unlock()

	if stillIndexed != nil {
		t.Error("stale tail entry was not evicted; the metric would stay wedged")
	}
	if !liveIndexed {
		t.Error("the live entry was evicted; only the missing file should be shed")
	}
	if validOf(c) != 1 {
		t.Errorf("validity gauge = %v, want 1 (a live entry was reachable)", validOf(c))
	}
	// ~1h, allowing generous slack for slow CI.
	if got := ageOf(c); got < 1800 || got > 7200 {
		t.Errorf("idle time = %vs, want ~3600s from the surviving entry", got)
	}
}

// TestCacheAgeMetric_DoesNotEvictOnNonNotExistError guards the ENOENT-only
// rule. A stat failure that is not "missing" must leave the index untouched,
// because shedding an entry for a legacy random-suffixed blob is irreversible.
func TestCacheAgeMetric_DoesNotEvictOnNonNotExistError(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root: permission bits do not deny access")
	}

	dir := t.TempDir()
	c := newAgeCache(t, dir)

	hash, path := putCASBlob(t, c, 1)

	// Deny traversal of the containing shard so stat fails with EACCES rather
	// than ENOENT. The file itself still exists and the entry is still valid.
	shard := filepath.Dir(path)
	if err := os.Chmod(shard, 0o000); err != nil {
		t.Fatalf("Chmod: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(shard, 0o755) })

	c.updateCacheAgeMetric()

	c.mu.Lock()
	_, stillIndexed := c.lru.Get(cache.LookupKey(cache.CAS, hash))
	c.mu.Unlock()

	if stillIndexed == nil {
		t.Fatal("entry was evicted on a non-ENOENT stat error; " +
			"a transient filesystem fault must not shrink the index")
	}
	if validOf(c) != 0 {
		t.Errorf("validity gauge = %v, want 0 when the age is unknown", validOf(c))
	}
	if got := ageOf(c); !math.IsNaN(got) {
		t.Errorf("idle time = %v, want NaN so the dashboard gaps instead of "+
			"showing a stale value as current", got)
	}
}

// TestCacheAgeMetric_RetryBounded asserts one pass sheds at most
// maxCacheAgeEvictions entries. A GC cycle can delete millions of consecutive
// tail entries; an unbounded loop would hold the global cache mutex against
// live request traffic.
func TestCacheAgeMetric_RetryBounded(t *testing.T) {
	dir := t.TempDir()
	c := newAgeCache(t, dir)

	// One more dead entry than a single pass is allowed to shed.
	n := maxCacheAgeEvictions + 1
	for i := 0; i < n; i++ {
		_, p := putCASBlob(t, c, i)
		ts := time.Now().Add(-time.Duration(n-i) * time.Hour)
		if err := os.Chtimes(p, ts, ts); err != nil {
			t.Fatalf("Chtimes: %v", err)
		}
		if err := os.Remove(p); err != nil {
			t.Fatalf("Remove: %v", err)
		}
	}

	c.mu.Lock()
	before := c.lru.Len()
	c.mu.Unlock()

	c.updateCacheAgeMetric()

	c.mu.Lock()
	after := c.lru.Len()
	c.mu.Unlock()

	shed := before - after
	if shed != maxCacheAgeEvictions {
		t.Errorf("shed %d entries in one pass, want exactly %d (the bound)",
			shed, maxCacheAgeEvictions)
	}
	// Nothing resolvable was found, so the age must read as unknown rather
	// than retaining whatever was last published.
	if validOf(c) != 0 {
		t.Errorf("validity gauge = %v, want 0", validOf(c))
	}
	if got := ageOf(c); !math.IsNaN(got) {
		t.Errorf("idle time = %v, want NaN", got)
	}

	// The next pass must make progress rather than spin on the same entries.
	c.updateCacheAgeMetric()
	c.mu.Lock()
	final := c.lru.Len()
	c.mu.Unlock()
	if final != 0 {
		t.Errorf("after a second pass %d entries remain, want 0: "+
			"the backlog must drain across ticks", final)
	}
}
