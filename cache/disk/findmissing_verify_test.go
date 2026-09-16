// Tests for FindMissingBlobs existence verification in shared storage mode.
//
// The bug these guard against: findMissingLocalCAS used to report a blob
// present purely on an index hit. In shared storage mode the leader's GC
// unlinks files from the shared filesystem without notifying the other
// replicas, so an index entry can outlive its file. A false "present" from
// FindMissingBlobs is worse than a miss -- the client concludes it has nothing
// to upload, and the build fails later when a worker tries to fetch the blob.
//
// Contains() has always verified and self-healed; this path did not, so the two
// sibling lookups disagreed about the same blob. That is the production
// "lost-inputs (GC)" / "remote-exec-fail (GC)" failure signature.
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

	pb "github.com/buchgr/bazel-remote/v2/genproto/build/bazel/remote/execution/v2"
)

// newVerifyCache builds a shared-storage leader cache with FindMissingBlobs
// existence checking ENABLED. The feature is off by default, so every test that
// exercises it must opt in explicitly -- which is itself the guarantee that the
// default path is unchanged.
func newVerifyCache(t *testing.T, dir string) *diskCache {
	t.Helper()
	c, err := New(dir, 1024*1024*1024,
		WithAccessLogger(testutils.NewSilentLogger()),
		WithSharedStorageMode(true),
		WithSharedStorageLeader(true),
		WithSharedStorageGCMinAge(time.Hour),
		WithSharedStorageVerifyInterval(time.Minute))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c.(*diskCache)
}

// putForFMB writes a CAS blob and returns its digest and on-disk path.
func putForFMB(t *testing.T, c *diskCache, size int64) (*pb.Digest, string) {
	t.Helper()
	data, hash := testutils.RandomDataAndHash(size)
	if err := c.Put(context.Background(), cache.CAS, hash, int64(len(data)),
		bytes.NewReader(data)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	c.mu.Lock()
	item, _ := c.lru.Get(cache.LookupKey(cache.CAS, hash))
	c.mu.Unlock()
	p := filepath.Join(c.dir, c.FileLocation(cache.CAS, item.legacy, hash, item.size, item.random))
	return &pb.Digest{Hash: hash, SizeBytes: int64(len(data))}, p
}

// expireVerification backdates an entry's lastVerified so the next lookup is
// forced to re-check the filesystem, standing in for the passage of time.
func expireVerification(c *diskCache, hash string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, hit := c.lru.cache[cache.LookupKey(cache.CAS, hash)]; hit {
		elem.Value.(*entry).value.lastVerified =
			time.Now().Add(-2 * time.Minute)
	}
}

// TestFindMissingReportsDeletedBlobAsMissing is the core regression test: a
// blob the leader's GC removed must not be reported present.
func TestFindMissingReportsDeletedBlobAsMissing(t *testing.T) {
	dir := t.TempDir()
	c := newVerifyCache(t, dir)

	digest, path := putForFMB(t, c, 256)

	// Simulate the leader GC unlinking the file out from under this replica,
	// leaving the index entry behind.
	if err := os.Remove(path); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	expireVerification(c, digest.Hash)

	blobs := []*pb.Digest{{Hash: digest.Hash, SizeBytes: digest.SizeBytes}}
	missing := c.findMissingLocalCAS(blobs)

	if missing != 1 {
		t.Errorf("missing count = %d, want 1: a blob deleted by the leader GC "+
			"must not be reported present", missing)
	}
	if blobs[0] == nil {
		t.Error("blob was reported present but its file is gone; the client " +
			"would skip the upload and the build would fail on fetch")
	}

	// The dead entry should also have been shed, so the next call is cheap.
	c.mu.Lock()
	_, stillIndexed := c.lru.Get(cache.LookupKey(cache.CAS, digest.Hash))
	c.mu.Unlock()
	if stillIndexed != nil {
		t.Error("stale index entry survived verification")
	}
}

// TestFindMissingReportsPresentBlobAsPresent guards the other direction: a
// blob that really is on disk must still be reported present, and verification
// must not evict it.
func TestFindMissingReportsPresentBlobAsPresent(t *testing.T) {
	dir := t.TempDir()
	c := newVerifyCache(t, dir)

	digest, _ := putForFMB(t, c, 256)
	expireVerification(c, digest.Hash)

	blobs := []*pb.Digest{{Hash: digest.Hash, SizeBytes: digest.SizeBytes}}
	if missing := c.findMissingLocalCAS(blobs); missing != 0 {
		t.Errorf("missing count = %d, want 0", missing)
	}
	if blobs[0] != nil {
		t.Error("a blob present on disk was reported missing")
	}

	c.mu.Lock()
	_, stillIndexed := c.lru.Get(cache.LookupKey(cache.CAS, digest.Hash))
	c.mu.Unlock()
	if stillIndexed == nil {
		t.Error("verification evicted an entry whose file exists")
	}
}

// TestFindMissingVerificationIsThrottled asserts the check is rate-limited per
// entry. Without throttling this becomes a stat per digest per call, which on a
// network filesystem would dominate FindMissingBlobs latency.
func TestFindMissingVerificationIsThrottled(t *testing.T) {
	dir := t.TempDir()
	c := newVerifyCache(t, dir)

	digest, path := putForFMB(t, c, 256)

	// First lookup verifies (lastVerified is zero for a freshly Put entry) and
	// stamps the entry.
	blobs := []*pb.Digest{{Hash: digest.Hash, SizeBytes: digest.SizeBytes}}
	if missing := c.findMissingLocalCAS(blobs); missing != 0 {
		t.Fatalf("first lookup: missing = %d, want 0", missing)
	}

	// Now delete the file WITHOUT expiring the verification stamp. Within the
	// throttle window the entry is trusted, so the blob is still reported
	// present. This is the deliberate, bounded staleness the design accepts.
	if err := os.Remove(path); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	blobs = []*pb.Digest{{Hash: digest.Hash, SizeBytes: digest.SizeBytes}}
	if missing := c.findMissingLocalCAS(blobs); missing != 0 {
		t.Errorf("within the throttle window the entry should still be trusted; "+
			"missing = %d, want 0. If this fails the check is not throttled and "+
			"every digest costs a stat.", missing)
	}

	// Once the window lapses the deletion must be noticed.
	expireVerification(c, digest.Hash)
	blobs = []*pb.Digest{{Hash: digest.Hash, SizeBytes: digest.SizeBytes}}
	if missing := c.findMissingLocalCAS(blobs); missing != 1 {
		t.Errorf("after the throttle window expired, missing = %d, want 1", missing)
	}
}

// TestFindMissingNoVerificationOutsideSharedMode asserts the stat is skipped
// entirely when nothing else can delete our files, so upstream behaviour and
// cost are unchanged.
func TestFindMissingNoVerificationOutsideSharedMode(t *testing.T) {
	dir := t.TempDir()
	c, err := New(dir, 1024*1024*1024,
		WithAccessLogger(testutils.NewSilentLogger()))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	dc := c.(*diskCache)

	if dc.sharedStorageMode {
		t.Fatal("expected a non-shared-storage cache")
	}

	digest, _ := putForFMB(t, dc, 256)
	dc.mu.Lock()
	item, _ := dc.lru.Get(cache.LookupKey(cache.CAS, digest.Hash))
	dc.mu.Unlock()

	if dc.verifyDue(item, time.Now().Add(24*time.Hour)) {
		t.Error("verification must never be due outside shared storage mode: " +
			"no other process deletes our files, so the stat is pure overhead")
	}
}

// TestFindMissingMixedBatch exercises the two-pass structure with a batch that
// mixes present, deleted and unknown blobs -- the realistic shape of a
// FindMissingBlobs call.
func TestFindMissingMixedBatch(t *testing.T) {
	dir := t.TempDir()
	c := newVerifyCache(t, dir)

	presentDigest, _ := putForFMB(t, c, 128)
	deletedDigest, deletedPath := putForFMB(t, c, 256)
	if err := os.Remove(deletedPath); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	expireVerification(c, presentDigest.Hash)
	expireVerification(c, deletedDigest.Hash)

	_, unknownHash := testutils.RandomDataAndHash(64)

	blobs := []*pb.Digest{
		{Hash: presentDigest.Hash, SizeBytes: presentDigest.SizeBytes},
		{Hash: deletedDigest.Hash, SizeBytes: deletedDigest.SizeBytes},
		{Hash: unknownHash, SizeBytes: 64},
	}

	missing := c.findMissingLocalCAS(blobs)

	if missing != 2 {
		t.Errorf("missing = %d, want 2 (the deleted blob and the unknown one)", missing)
	}
	if blobs[0] != nil {
		t.Error("the blob that exists on disk was reported missing")
	}
	if blobs[1] == nil {
		t.Error("the deleted blob was reported present")
	}
	if blobs[2] == nil {
		t.Error("a blob never written was reported present")
	}
}

// TestFindMissingVerificationDisabledByDefault is the guard that shipping iter2
// does not change FindMissingBlobs behaviour or cost unless explicitly asked.
//
// The check adds a stat per blob per interval on the hot path, and that cost
// scales with the active working set -- which cannot be measured on an idle
// test cluster. So it ships off, and this test fails if anyone changes the
// default without deciding to.
func TestFindMissingVerificationDisabledByDefault(t *testing.T) {
	dir := t.TempDir()
	c := newGCCache(t, dir, 1024*1024*1024, time.Hour) // no verify interval set

	if c.sharedStorageVerifyInterval != 0 {
		t.Fatalf("verify interval = %v by default, want 0 (disabled)",
			c.sharedStorageVerifyInterval)
	}

	digest, path := putForFMB(t, c, 256)

	// Delete the file behind the cache's back and age the entry well past any
	// plausible interval. With verification off the blob is still reported
	// present -- the pre-existing behaviour we are deliberately preserving.
	if err := os.Remove(path); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	c.mu.Lock()
	if elem, hit := c.lru.cache[cache.LookupKey(cache.CAS, digest.Hash)]; hit {
		elem.Value.(*entry).value.lastVerified = time.Now().Add(-24 * time.Hour)
	}
	c.mu.Unlock()

	blobs := []*pb.Digest{{Hash: digest.Hash, SizeBytes: digest.SizeBytes}}
	if missing := c.findMissingLocalCAS(blobs); missing != 0 {
		t.Errorf("missing = %d with verification disabled, want 0: the default "+
			"path must not stat, so a deleted blob is still reported present", missing)
	}
	if !c.verifyDue(lruItem{}, time.Now().Add(365*24*time.Hour)) {
		return // expected: never due when disabled
	}
	t.Error("verifyDue returned true with the interval disabled")
}
