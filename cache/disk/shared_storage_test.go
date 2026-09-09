// Tests for fork-specific shared_storage_mode behavior in the disk cache.
//
// These tests guard the correctness invariants that make a multi-replica
// shared-filesystem deployment coherent, and that must not regress when the
// scan/GC/discovery code paths are modified. Perf coverage lives separately
// in scan_bench_test.go.
//
// The fork is used exclusively in shared_storage_mode, so tests here always
// exercise shared-mode behavior instead of gating on the flag.
package disk

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/buchgr/bazel-remote/v2/cache"
	testutils "github.com/buchgr/bazel-remote/v2/utils"
)

// newSharedCache constructs a diskCache pinned into shared_storage_mode with
// tunable GC knobs. The returned cache is the concrete *diskCache (not the
// metrics decorator), because most tests here need to reach into internal
// fields (lru, sharedStorageLeader, gcMinAge, etc.).
func newSharedCache(t *testing.T, dir string, sizeBytes int64, opts ...Option) *diskCache {
	t.Helper()
	baseOpts := []Option{
		WithAccessLogger(testutils.NewSilentLogger()),
		WithSharedStorageMode(true),
		// Default min-age long enough that tests never accidentally trip the
		// grace-window protection in runGCCycle unless they set atime old
		// themselves. Individual tests can override via opts.
		WithSharedStorageGCMinAge(time.Hour),
	}
	c, err := New(dir, sizeBytes, append(baseOpts, opts...)...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c.(*diskCache)
}

// putCAS writes a CAS blob through the cache's normal Put path and returns
// its data + hash. Uses the fork's deterministic-naming commit path.
func putCAS(t *testing.T, c *diskCache, size int64) ([]byte, string) {
	t.Helper()
	data, hash := testutils.RandomDataAndHash(size)
	err := c.Put(context.Background(), cache.CAS, hash, size, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Put CAS: %v", err)
	}
	return data, hash
}

// -----------------------------------------------------------------------------
// onEvict is a no-op in shared_storage_mode
// -----------------------------------------------------------------------------

// TestSharedMode_LRUEvictLeavesFileOnDisk verifies that when the in-memory LRU
// sheds an entry, the underlying file remains on the shared FS. This is the
// core invariant that keeps a pod's local index size independent from the
// on-disk cache size; the leader's GC is the only thing that may delete.
func TestSharedMode_LRUEvictLeavesFileOnDisk(t *testing.T) {
	dir := testutils.TempDir(t)
	defer func() { _ = os.RemoveAll(dir) }()

	blobSize := int64(1024)
	// Small cap so the second Put forces the first blob out of the LRU. Two
	// blocks + a slack block; roundUp4k(compressed sizeOnDisk) is <= 4k for a
	// 1KB blob so two of them fit at capacity == 2*BlockSize.
	cacheSize := int64(2 * BlockSize)

	c := newSharedCache(t, dir, cacheSize)

	// First blob.
	_, hash1 := putCAS(t, c, blobSize)
	path1 := filepath.Join(dir, c.FileLocationBase(cache.CAS, false, hash1, blobSize))
	if _, err := os.Stat(path1); err != nil {
		t.Fatalf("expected first blob on disk: %v", err)
	}

	// Fill cache to force eviction of the first entry from the LRU. We push
	// several more blobs to make sure the tail item is evicted regardless of
	// exact rounding.
	for i := 0; i < 4; i++ {
		_, _ = putCAS(t, c, blobSize)
	}

	// Wait briefly for the async eviction queue to drain (see
	// performQueuedEvictionsContinuously). onEvict is a no-op in shared mode,
	// so this is really about waiting for the queue to observe entries.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && c.lru.queuedEvictionsSize.Load() > 0 {
		time.Sleep(10 * time.Millisecond)
	}

	// The first blob's LRU entry should be gone.
	c.mu.Lock()
	_, present := c.lru.Get(cache.LookupKey(cache.CAS, hash1))
	c.mu.Unlock()
	if present != nil {
		t.Skip("first blob was not evicted from LRU under this rounding; adjust cacheSize if this recurs")
	}

	// The file must still exist on disk in shared mode.
	if _, err := os.Stat(path1); err != nil {
		t.Fatalf("shared mode: file should survive LRU eviction, got stat err: %v", err)
	}
}

// -----------------------------------------------------------------------------
// statAndIndexCAS: cross-pod discovery via FindMissingBlobs
// -----------------------------------------------------------------------------

// TestSharedMode_StatAndIndexCAS_HitAndIndex simulates a blob written by
// another pod (present on the shared FS at its deterministic path, absent
// from this pod's LRU) and verifies statAndIndexCAS returns true and
// populates the LRU.
func TestSharedMode_StatAndIndexCAS_HitAndIndex(t *testing.T) {
	dir := testutils.TempDir(t)
	defer func() { _ = os.RemoveAll(dir) }()

	c := newSharedCache(t, dir, 10*1024*1024)

	// Put a blob through the normal path, then drop it from the LRU only
	// (leaves the file on disk — same shape as "another pod wrote it").
	blobSize := int64(2048)
	_, hash := putCAS(t, c, blobSize)
	key := cache.LookupKey(cache.CAS, hash)

	c.mu.Lock()
	c.lru.RemoveKey(key)
	_, before := c.lru.Get(key)
	c.mu.Unlock()
	if before != nil {
		t.Fatalf("precondition: expected LRU miss after RemoveKey")
	}

	if !c.statAndIndexCAS(hash, blobSize) {
		t.Fatalf("statAndIndexCAS returned false for a blob present on disk")
	}

	c.mu.Lock()
	_, after := c.lru.Get(key)
	c.mu.Unlock()
	if after == nil {
		t.Fatalf("statAndIndexCAS did not populate the LRU")
	}
}

// TestSharedMode_StatAndIndexCAS_MissingReturnsFalse verifies that a blob
// truly absent from the shared FS produces false and does NOT insert into
// the LRU.
func TestSharedMode_StatAndIndexCAS_MissingReturnsFalse(t *testing.T) {
	dir := testutils.TempDir(t)
	defer func() { _ = os.RemoveAll(dir) }()

	c := newSharedCache(t, dir, 10*1024*1024)

	_, hash := testutils.RandomDataAndHash(1024)
	if c.statAndIndexCAS(hash, 1024) {
		t.Fatalf("statAndIndexCAS returned true for a blob that is not on disk")
	}
	c.mu.Lock()
	_, present := c.lru.Get(cache.LookupKey(cache.CAS, hash))
	c.mu.Unlock()
	if present != nil {
		t.Fatalf("statAndIndexCAS populated LRU despite missing blob")
	}
}

// -----------------------------------------------------------------------------
// discoverAndIndex: cross-pod discovery for Get / Contains
// -----------------------------------------------------------------------------

// TestSharedMode_ContainsDiscoversCrossPodBlob mirrors the read-side path:
// a blob on the shared FS but missing from the local LRU should be found by
// Contains via discoverAndIndex and end up in the LRU afterwards.
func TestSharedMode_ContainsDiscoversCrossPodBlob(t *testing.T) {
	ctx := context.Background()
	dir := testutils.TempDir(t)
	defer func() { _ = os.RemoveAll(dir) }()

	c := newSharedCache(t, dir, 10*1024*1024)

	blobSize := int64(1024)
	_, hash := putCAS(t, c, blobSize)
	key := cache.LookupKey(cache.CAS, hash)

	// Drop only the LRU entry.
	c.mu.Lock()
	c.lru.RemoveKey(key)
	c.mu.Unlock()

	found, size := c.Contains(ctx, cache.CAS, hash, blobSize)
	if !found {
		t.Fatalf("Contains: expected cross-pod discovery to find the blob on disk")
	}
	if size != blobSize {
		t.Fatalf("Contains: expected size %d, got %d", blobSize, size)
	}

	c.mu.Lock()
	_, present := c.lru.Get(key)
	c.mu.Unlock()
	if present == nil {
		t.Fatalf("Contains: discoverAndIndex should have populated the LRU")
	}
}

// TestSharedMode_GetDiscoversCrossPodBlob is the Get-path analog: same setup
// (blob on disk, absent from local LRU) and the returned ReadCloser must
// yield the original bytes.
func TestSharedMode_GetDiscoversCrossPodBlob(t *testing.T) {
	ctx := context.Background()
	dir := testutils.TempDir(t)
	defer func() { _ = os.RemoveAll(dir) }()

	c := newSharedCache(t, dir, 10*1024*1024)

	blobSize := int64(4096)
	data, hash := putCAS(t, c, blobSize)
	key := cache.LookupKey(cache.CAS, hash)

	c.mu.Lock()
	c.lru.RemoveKey(key)
	c.mu.Unlock()

	rc, size, err := c.Get(ctx, cache.CAS, hash, blobSize, 0)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if rc == nil {
		t.Fatalf("Get: expected non-nil ReadCloser after cross-pod discovery")
	}
	defer func() { _ = rc.Close() }()
	if size != blobSize {
		t.Fatalf("Get: expected size %d, got %d", blobSize, size)
	}
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("Get: content mismatch after cross-pod discovery")
	}
}

// -----------------------------------------------------------------------------
// Deterministic naming survives restart
// -----------------------------------------------------------------------------

// TestSharedMode_DeterministicNamePersistsAcrossRestart writes a blob via the
// normal Put path (which uses deterministic naming on commit), instantiates a
// fresh diskCache over the same directory, and verifies the blob is
// discoverable via Contains + Get with the same bytes.
func TestSharedMode_DeterministicNamePersistsAcrossRestart(t *testing.T) {
	ctx := context.Background()
	dir := testutils.TempDir(t)
	defer func() { _ = os.RemoveAll(dir) }()

	blobSize := int64(2048)
	var data []byte
	var hash string

	{
		c := newSharedCache(t, dir, 10*1024*1024)
		data, hash = putCAS(t, c, blobSize)
	}

	// Fresh diskCache instance over the same dir — mimics a pod restart.
	c2 := newSharedCache(t, dir, 10*1024*1024)

	found, size := c2.Contains(ctx, cache.CAS, hash, blobSize)
	if !found {
		t.Fatalf("restart: Contains failed for deterministically-named blob")
	}
	if size != blobSize {
		t.Fatalf("restart: expected size %d, got %d", blobSize, size)
	}

	rc, _, err := c2.Get(ctx, cache.CAS, hash, blobSize, 0)
	if err != nil || rc == nil {
		t.Fatalf("restart: Get failed: rc=%v err=%v", rc, err)
	}
	defer func() { _ = rc.Close() }()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("restart: ReadAll: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("restart: content mismatch")
	}
}

// -----------------------------------------------------------------------------
// scanDir: .tmp files are skipped; legacy random-suffix names still work
// -----------------------------------------------------------------------------

// TestScanDir_SkipsTmpFiles ensures an in-progress temp write is never
// promoted into the LRU by scanDir. This is critical: a scanner that
// indexed .tmp files would serve half-written blobs to clients.
func TestScanDir_SkipsTmpFiles(t *testing.T) {
	dir := testutils.TempDir(t)
	defer func() { _ = os.RemoveAll(dir) }()

	c := newSharedCache(t, dir, 10*1024*1024)

	// Place a bogus .tmp file directly in a CAS shard. Content doesn't need
	// to be a real casblob because scanDir only inspects the filename.
	_, hash := testutils.RandomDataAndHash(64)
	shard := filepath.Join(dir, "cas.v2", hash[:2])
	if err := os.MkdirAll(shard, 0755); err != nil {
		t.Fatal(err)
	}
	tmpPath := filepath.Join(shard, hash+"-123-999.tmp")
	if err := os.WriteFile(tmpPath, []byte("half-written"), 0644); err != nil {
		t.Fatal(err)
	}

	result, err := c.scanDir()
	if err != nil {
		t.Fatalf("scanDir: %v", err)
	}
	for i, m := range result.metadata {
		if m.lookupKey == cache.LookupKey(cache.CAS, hash) {
			t.Fatalf("scanDir indexed a .tmp file at position %d: %+v", i, result.item[i])
		}
	}
}

// TestScanDir_HandlesLegacyRandomSuffixNames verifies that a CAS blob with a
// legacy random-suffix filename (<hash>-<size>-<random>) is still discovered
// by scanDir and reported with correct size/random metadata.
func TestScanDir_HandlesLegacyRandomSuffixNames(t *testing.T) {
	dir := testutils.TempDir(t)
	defer func() { _ = os.RemoveAll(dir) }()

	c := newSharedCache(t, dir, 10*1024*1024)

	_, hash := testutils.RandomDataAndHash(128)
	shard := filepath.Join(dir, "cas.v2", hash[:2])
	if err := os.MkdirAll(shard, 0755); err != nil {
		t.Fatal(err)
	}
	// Legacy compressed-CAS name: <hash>-<size>-<random>. Content is
	// deliberately opaque; scanDir extracts size from the filename.
	legacyName := hash + "-128-777888"
	if err := os.WriteFile(filepath.Join(shard, legacyName), make([]byte, 128), 0644); err != nil {
		t.Fatal(err)
	}

	result, err := c.scanDir()
	if err != nil {
		t.Fatalf("scanDir: %v", err)
	}
	found := false
	for i, m := range result.metadata {
		if m.lookupKey == cache.LookupKey(cache.CAS, hash) {
			found = true
			it := result.item[i]
			if it.size != 128 {
				t.Fatalf("legacy CAS: expected size 128, got %d", it.size)
			}
			if it.random != "777888" {
				t.Fatalf("legacy CAS: expected random=777888, got %q", it.random)
			}
			break
		}
	}
	if !found {
		t.Fatalf("scanDir did not index legacy random-suffix CAS file")
	}
}

// TestScanDir_HandlesLegacyACRandomSuffixNames covers the AC/RAW legacy shape
// (<hash>-<random>), which uses a different filename parser branch.
func TestScanDir_HandlesLegacyACRandomSuffixNames(t *testing.T) {
	dir := testutils.TempDir(t)
	defer func() { _ = os.RemoveAll(dir) }()

	c := newSharedCache(t, dir, 10*1024*1024)

	_, hash := testutils.RandomDataAndHash(64)
	shard := filepath.Join(dir, "ac.v2", hash[:2])
	if err := os.MkdirAll(shard, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(shard, hash+"-112233"), make([]byte, 64), 0644); err != nil {
		t.Fatal(err)
	}

	result, err := c.scanDir()
	if err != nil {
		t.Fatalf("scanDir: %v", err)
	}
	found := false
	for i, m := range result.metadata {
		if m.lookupKey == cache.LookupKey(cache.AC, hash) {
			found = true
			if result.item[i].random != "112233" {
				t.Fatalf("legacy AC: expected random=112233, got %q", result.item[i].random)
			}
			break
		}
	}
	if !found {
		t.Fatalf("scanDir did not index legacy random-suffix AC file")
	}
}
