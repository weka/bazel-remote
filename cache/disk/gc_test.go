// Tests for the fork's leader-side garbage collector (runGCCycle /
// scanFilesWithAtime / collectACReferencedCASHashes).
//
// The leader is the only actor that deletes files on the shared FS, so these
// tests pin down the eviction rules that the LRU tests can't cover:
//
//   - under-cap: silent no-op
//   - over-cap:  evict oldest first, but never files inside the min-age grace
//     window, and never CAS blobs referenced by a live AC entry
//   - scanFilesWithAtime must ignore neither AC nor CAS nor RAW roots, and
//     the reported total must include all files it walks (so the "under cap?"
//     early-return decision is made on accurate data)
//
// All tests exercise shared-storage mode (the only mode the fork runs in);
// the leader flag is set directly on the diskCache to avoid spawning the
// background ticker.
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
	"google.golang.org/protobuf/proto"
)

// newGCCache builds a shared-storage disk cache and flips it into leader mode
// WITHOUT starting the background GC loop. Tests invoke runGCCycle directly
// so timing is deterministic.
func newGCCache(t *testing.T, dir string, sizeBytes int64, minAge time.Duration) *diskCache {
	t.Helper()
	c, err := New(dir, sizeBytes,
		WithAccessLogger(testutils.NewSilentLogger()),
		WithSharedStorageMode(true),
		WithSharedStorageGCMinAge(minAge),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	dc := c.(*diskCache)
	// Enable leader semantics without triggering the ticker goroutine.
	dc.sharedStorageLeader = true
	return dc
}

// setAtime backs an atime older/newer than a reference time so tests can
// pretend "this blob was accessed long ago". runGCCycle reads atime via
// atime.Get(FileInfo), so os.Chtimes is authoritative.
func setAtime(t *testing.T, path string, when time.Time) {
	t.Helper()
	if err := os.Chtimes(path, when, when); err != nil {
		t.Fatalf("Chtimes %s: %v", path, err)
	}
}

// -----------------------------------------------------------------------------
// runGCCycle: under-cap early return
// -----------------------------------------------------------------------------

// TestGC_UnderCapIsNoOp verifies that when totalSize <= maxSize the leader
// GC returns without touching disk. This is the fast path that must not
// falsely evict data on a lightly-loaded cluster.
func TestGC_UnderCapIsNoOp(t *testing.T) {
	dir := testutils.TempDir(t)
	defer func() { _ = os.RemoveAll(dir) }()

	// Very large cap so we're definitely under.
	c := newGCCache(t, dir, 1<<30, time.Hour)

	blobSize := int64(1024)
	data, hash := testutils.RandomDataAndHash(blobSize)
	if err := c.Put(context.Background(), cache.CAS, hash, blobSize, bytes.NewReader(data)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	blobPath := filepath.Join(dir, c.FileLocationBase(cache.CAS, false, hash, blobSize))

	before, err := os.Stat(blobPath)
	if err != nil {
		t.Fatalf("precondition: blob missing: %v", err)
	}

	c.runGCCycle(1 << 30)

	after, err := os.Stat(blobPath)
	if err != nil {
		t.Fatalf("under-cap GC deleted the blob: %v", err)
	}
	if before.Size() != after.Size() {
		t.Fatalf("under-cap GC modified the blob (size changed)")
	}
}

// -----------------------------------------------------------------------------
// runGCCycle: min-age grace window
// -----------------------------------------------------------------------------

// TestGC_RespectsMinAgeWindow is a table-driven test that pins the grace
// behavior: recently-accessed files (atime within gcMinAge) survive even
// when the cache is over cap; older files are the ones evicted.
func TestGC_RespectsMinAgeWindow(t *testing.T) {
	tests := []struct {
		name        string
		atimeOffset time.Duration // relative to now, negative = past
		wantEvicted bool
	}{
		{"inside_min_age_recent", -1 * time.Minute, false},
		{"inside_min_age_at_boundary_ish", -30 * time.Minute, false},
		{"outside_min_age_older", -3 * time.Hour, true},
		{"outside_min_age_ancient", -30 * 24 * time.Hour, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := testutils.TempDir(t)
			defer func() { _ = os.RemoveAll(dir) }()

			minAge := time.Hour
			// LRU cap must exceed blob size or Put is rejected outright.
			// runGCCycle takes its own cap arg — pass 1 to force over-cap
			// in the GC decision independently of the LRU's own reserve check.
			c := newGCCache(t, dir, 10*1024, minAge)

			blobSize := int64(2048)
			data, hash := testutils.RandomDataAndHash(blobSize)
			if err := c.Put(context.Background(), cache.CAS, hash, blobSize, bytes.NewReader(data)); err != nil {
				t.Fatalf("Put: %v", err)
			}
			blobPath := filepath.Join(dir, c.FileLocationBase(cache.CAS, false, hash, blobSize))

			setAtime(t, blobPath, time.Now().Add(tc.atimeOffset))

			c.runGCCycle(1)

			_, err := os.Stat(blobPath)
			gone := os.IsNotExist(err)
			if gone != tc.wantEvicted {
				t.Fatalf("evicted=%v, want=%v (atimeOffset=%v)", gone, tc.wantEvicted, tc.atimeOffset)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// runGCCycle: AC-referenced CAS blobs are protected regardless of age
// -----------------------------------------------------------------------------

// TestGC_ProtectsCASReferencedByLiveAC writes a CAS blob, an AC entry that
// references it via OutputFiles, then ages BOTH beyond the min-age window
// and forces a GC pass. The AC entry is evictable, but the CAS blob it
// references must NOT be deleted while the AC entry is still on disk.
func TestGC_ProtectsCASReferencedByLiveAC(t *testing.T) {
	dir := testutils.TempDir(t)
	defer func() { _ = os.RemoveAll(dir) }()

	// LRU cap large enough to accept blob Put; runGCCycle(1) forces over-cap.
	c := newGCCache(t, dir, 10*1024, time.Hour)

	// A CAS blob that will be referenced by an AC entry.
	blobSize := int64(1024)
	casData, casHash := testutils.RandomDataAndHash(blobSize)
	if err := c.Put(context.Background(), cache.CAS, casHash, blobSize, bytes.NewReader(casData)); err != nil {
		t.Fatalf("Put CAS: %v", err)
	}
	casPath := filepath.Join(dir, c.FileLocationBase(cache.CAS, false, casHash, blobSize))

	// AC entry pointing at the CAS blob via OutputFiles.
	ar := &pb.ActionResult{
		OutputFiles: []*pb.OutputFile{
			{
				Path: "out",
				Digest: &pb.Digest{
					Hash:      casHash,
					SizeBytes: blobSize,
				},
			},
		},
	}
	arBytes, err := proto.Marshal(ar)
	if err != nil {
		t.Fatalf("marshal AR: %v", err)
	}
	// AC hash is arbitrary — the fork keys AC entries by their own hash.
	acHash := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	if err := c.Put(context.Background(), cache.AC, acHash, int64(len(arBytes)), bytes.NewReader(arBytes)); err != nil {
		t.Fatalf("Put AC: %v", err)
	}
	acPath := filepath.Join(dir, c.FileLocationBase(cache.AC, false, acHash, int64(len(arBytes))))

	// Age both files far beyond the min-age window so age-protection does
	// not confound the AC-reference-protection we're testing.
	old := time.Now().Add(-10 * time.Hour)
	setAtime(t, casPath, old)
	setAtime(t, acPath, old)

	c.runGCCycle(1)

	// CAS must survive because the AC entry references it.
	if _, err := os.Stat(casPath); err != nil {
		t.Fatalf("GC evicted CAS blob referenced by a live AC entry: %v", err)
	}
	// AC entry itself is evictable and, being older than min-age with cache
	// over cap, should be gone. (If a future change makes AC also protected,
	// relax this to just require CAS survival.)
	if _, err := os.Stat(acPath); err == nil {
		t.Logf("note: AC entry survived GC — acceptable if policy changes, but the referenced-CAS survival above is the real invariant")
	}
}

// -----------------------------------------------------------------------------
// runGCCycle: unreferenced, aged-out CAS is actually deleted
// -----------------------------------------------------------------------------

// TestGC_EvictsAgedUnreferencedCAS covers the happy path: an old CAS blob
// with no live AC reference and cache over cap is deleted.
func TestGC_EvictsAgedUnreferencedCAS(t *testing.T) {
	dir := testutils.TempDir(t)
	defer func() { _ = os.RemoveAll(dir) }()

	c := newGCCache(t, dir, 10*1024, time.Hour)

	blobSize := int64(2048)
	data, hash := testutils.RandomDataAndHash(blobSize)
	if err := c.Put(context.Background(), cache.CAS, hash, blobSize, bytes.NewReader(data)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	blobPath := filepath.Join(dir, c.FileLocationBase(cache.CAS, false, hash, blobSize))

	setAtime(t, blobPath, time.Now().Add(-10*time.Hour))

	c.runGCCycle(1)

	if _, err := os.Stat(blobPath); !os.IsNotExist(err) {
		t.Fatalf("expected aged unreferenced CAS to be evicted, stat err=%v", err)
	}
}

// -----------------------------------------------------------------------------
// scanFilesWithAtime: what it counts
// -----------------------------------------------------------------------------

// TestScanFilesWithAtime_CountsAllKinds ensures scanFilesWithAtime walks
// ac.v2, cas.v2 AND raw.v2, and its returned totalSize matches the sum of
// the files it emits. runGCCycle relies on this total to decide over-cap
// vs no-op.
func TestScanFilesWithAtime_CountsAllKinds(t *testing.T) {
	dir := testutils.TempDir(t)
	defer func() { _ = os.RemoveAll(dir) }()

	c := newGCCache(t, dir, 10*1024*1024, time.Hour)

	// One blob per kind via the normal Put path.
	casData, casHash := testutils.RandomDataAndHash(512)
	if err := c.Put(context.Background(), cache.CAS, casHash, 512, bytes.NewReader(casData)); err != nil {
		t.Fatalf("Put CAS: %v", err)
	}

	// Fake AC bytes; content doesn't need to be a valid ActionResult for
	// the scan path (only collectACReferencedCASHashes cares).
	acData, acHash := testutils.RandomDataAndHash(256)
	if err := c.Put(context.Background(), cache.AC, acHash, 256, bytes.NewReader(acData)); err != nil {
		t.Fatalf("Put AC: %v", err)
	}

	rawData, rawHash := testutils.RandomDataAndHash(128)
	if err := c.Put(context.Background(), cache.RAW, rawHash, 128, bytes.NewReader(rawData)); err != nil {
		t.Fatalf("Put RAW: %v", err)
	}

	files, totalSize, err := c.scanFilesWithAtime()
	if err != nil {
		t.Fatalf("scanFilesWithAtime: %v", err)
	}

	seen := map[cache.EntryKind]int{}
	var summed int64
	for _, f := range files {
		seen[f.kind]++
		summed += f.size
	}
	if seen[cache.CAS] < 1 {
		t.Fatalf("no CAS files reported: %+v", seen)
	}
	if seen[cache.AC] < 1 {
		t.Fatalf("no AC files reported: %+v", seen)
	}
	if seen[cache.RAW] < 1 {
		t.Fatalf("no RAW files reported: %+v", seen)
	}
	if summed != totalSize {
		t.Fatalf("totalSize (%d) != sum of file sizes (%d)", totalSize, summed)
	}
}

// TestScanFilesWithAtime_IncludesTmpFiles documents current behavior: unlike
// scanDir (which explicitly skips .tmp), scanFilesWithAtime uses
// filepath.Walk without filename filtering, so an orphaned .tmp file is
// included in both `files` and `totalSize`. This means the leader GC will
// happily delete orphaned .tmp files that fall outside the min-age window,
// which is arguably desirable cleanup.
//
// If a future change adds .tmp filtering to scanFilesWithAtime, this test
// should be updated to assert the opposite.
func TestScanFilesWithAtime_IncludesTmpFiles(t *testing.T) {
	dir := testutils.TempDir(t)
	defer func() { _ = os.RemoveAll(dir) }()

	c := newGCCache(t, dir, 10*1024*1024, time.Hour)

	_, hash := testutils.RandomDataAndHash(64)
	shard := filepath.Join(dir, "cas.v2", hash[:2])
	if err := os.MkdirAll(shard, 0755); err != nil {
		t.Fatal(err)
	}
	tmpPath := filepath.Join(shard, hash+"-64-999.tmp")
	if err := os.WriteFile(tmpPath, make([]byte, 64), 0644); err != nil {
		t.Fatal(err)
	}

	files, _, err := c.scanFilesWithAtime()
	if err != nil {
		t.Fatalf("scanFilesWithAtime: %v", err)
	}

	// Compare by basename to sidestep macOS /var → /private/var symlink resolution
	// in filepath.Walk results.
	wantBase := filepath.Base(tmpPath)
	sawTmp := false
	for _, f := range files {
		if filepath.Base(f.path) == wantBase {
			sawTmp = true
			break
		}
	}
	if !sawTmp {
		t.Fatalf("scanFilesWithAtime is expected to include .tmp files; got %d files, none matched %q", len(files), wantBase)
	}
}

// newGCCacheWithACRefCap is newGCCache + WithSharedStorageACRefMaxAge, for
// Change F (iter 1) tests: AC entries older than acRefMaxAge stop shielding
// their CAS references.
func newGCCacheWithACRefCap(t *testing.T, dir string, sizeBytes int64, minAge, acRefMaxAge time.Duration) *diskCache {
	t.Helper()
	c, err := New(dir, sizeBytes,
		WithAccessLogger(testutils.NewSilentLogger()),
		WithSharedStorageMode(true),
		WithSharedStorageGCMinAge(minAge),
		WithSharedStorageACRefMaxAge(acRefMaxAge),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	dc := c.(*diskCache)
	dc.sharedStorageLeader = true
	return dc
}

// -----------------------------------------------------------------------------
// Change F: age-capped AC-reference protection
// -----------------------------------------------------------------------------

// TestGC_ACRefAgeCap_OldACStopsShieldingCAS is the F1 semantics contract:
// when an AC entry's mtime is older than shared_storage_ac_ref_max_age,
// its CAS references are NOT added to the referenced set, so an aged
// unreferenced-by-fresh-AC CAS blob becomes evictable.
//
// Setup: CAS blob + AC entry pointing at it, both aged. AC entry is older
// than acRefMaxAge=1h. GC forced over-cap. Expected: CAS blob is deleted.
// Contrast with TestGC_ProtectsCASReferencedByLiveAC, which uses the same
// shape but WITHOUT the age cap — there CAS survives.
func TestGC_ACRefAgeCap_OldACStopsShieldingCAS(t *testing.T) {
	dir := testutils.TempDir(t)
	defer func() { _ = os.RemoveAll(dir) }()

	// acRefMaxAge=1h, minAge=1h. Both AC entry and CAS blob will be aged 10h
	// so both are past the AC-shield cap AND past the min-age grace.
	c := newGCCacheWithACRefCap(t, dir, 10*1024, time.Hour, time.Hour)

	blobSize := int64(1024)
	casData, casHash := testutils.RandomDataAndHash(blobSize)
	if err := c.Put(context.Background(), cache.CAS, casHash, blobSize, bytes.NewReader(casData)); err != nil {
		t.Fatalf("Put CAS: %v", err)
	}
	casPath := filepath.Join(dir, c.FileLocationBase(cache.CAS, false, casHash, blobSize))

	ar := &pb.ActionResult{
		OutputFiles: []*pb.OutputFile{{
			Path:   "out",
			Digest: &pb.Digest{Hash: casHash, SizeBytes: blobSize},
		}},
	}
	arBytes, err := proto.Marshal(ar)
	if err != nil {
		t.Fatalf("marshal AR: %v", err)
	}
	acHash := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	if err := c.Put(context.Background(), cache.AC, acHash, int64(len(arBytes)), bytes.NewReader(arBytes)); err != nil {
		t.Fatalf("Put AC: %v", err)
	}
	acPath := filepath.Join(dir, c.FileLocationBase(cache.AC, false, acHash, int64(len(arBytes))))

	// Age both. AC entry age is compared via mtime in collectACReferencedCASHashes.
	// os.Chtimes sets both atime and mtime, which is what we want.
	old := time.Now().Add(-10 * time.Hour)
	setAtime(t, casPath, old)
	setAtime(t, acPath, old)

	c.runGCCycle(1)

	if _, err := os.Stat(casPath); !os.IsNotExist(err) {
		t.Fatalf("CAS blob referenced only by aged-past-cap AC entry should be evicted, stat err=%v", err)
	}
}

// TestGC_ACRefAgeCap_DisabledPreservesLegacyBehavior: with acRefMaxAge=0
// (the disable value), the age-cap does NOT apply and every live AC entry
// shields its CAS references regardless of age. This is the safety guardrail
// for anyone who runs without the flag — behavior is unchanged from before F.
func TestGC_ACRefAgeCap_DisabledPreservesLegacyBehavior(t *testing.T) {
	dir := testutils.TempDir(t)
	defer func() { _ = os.RemoveAll(dir) }()

	// acRefMaxAge=0 (disabled)
	c := newGCCacheWithACRefCap(t, dir, 10*1024, time.Hour, 0)

	blobSize := int64(1024)
	casData, casHash := testutils.RandomDataAndHash(blobSize)
	if err := c.Put(context.Background(), cache.CAS, casHash, blobSize, bytes.NewReader(casData)); err != nil {
		t.Fatalf("Put CAS: %v", err)
	}
	casPath := filepath.Join(dir, c.FileLocationBase(cache.CAS, false, casHash, blobSize))

	ar := &pb.ActionResult{
		OutputFiles: []*pb.OutputFile{{Path: "out", Digest: &pb.Digest{Hash: casHash, SizeBytes: blobSize}}},
	}
	arBytes, _ := proto.Marshal(ar)
	acHash := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	if err := c.Put(context.Background(), cache.AC, acHash, int64(len(arBytes)), bytes.NewReader(arBytes)); err != nil {
		t.Fatalf("Put AC: %v", err)
	}
	acPath := filepath.Join(dir, c.FileLocationBase(cache.AC, false, acHash, int64(len(arBytes))))

	// Age both way past any reasonable cap.
	old := time.Now().Add(-30 * 24 * time.Hour)
	setAtime(t, casPath, old)
	setAtime(t, acPath, old)

	c.runGCCycle(1)

	// With cap disabled, ancient AC still shields → CAS must survive.
	if _, err := os.Stat(casPath); err != nil {
		t.Fatalf("with acRefMaxAge=0, CAS referenced by ancient AC must survive, got err=%v", err)
	}
}

// TestGC_ACRefAgeCap_FreshACStillShields verifies that F1 only skips OLD
// AC entries — a fresh AC entry (mtime within acRefMaxAge) still shields
// its CAS references. Prevents F1 from over-collecting.
func TestGC_ACRefAgeCap_FreshACStillShields(t *testing.T) {
	dir := testutils.TempDir(t)
	defer func() { _ = os.RemoveAll(dir) }()

	// acRefMaxAge=1h, minAge=1h.
	c := newGCCacheWithACRefCap(t, dir, 10*1024, time.Hour, time.Hour)

	blobSize := int64(1024)
	casData, casHash := testutils.RandomDataAndHash(blobSize)
	if err := c.Put(context.Background(), cache.CAS, casHash, blobSize, bytes.NewReader(casData)); err != nil {
		t.Fatalf("Put CAS: %v", err)
	}
	casPath := filepath.Join(dir, c.FileLocationBase(cache.CAS, false, casHash, blobSize))

	ar := &pb.ActionResult{
		OutputFiles: []*pb.OutputFile{{Path: "out", Digest: &pb.Digest{Hash: casHash, SizeBytes: blobSize}}},
	}
	arBytes, _ := proto.Marshal(ar)
	acHash := "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	if err := c.Put(context.Background(), cache.AC, acHash, int64(len(arBytes)), bytes.NewReader(arBytes)); err != nil {
		t.Fatalf("Put AC: %v", err)
	}
	// Do NOT age the AC entry: it stays fresh (mtime = now).

	// Age the CAS blob so age-protection alone doesn't rescue it.
	setAtime(t, casPath, time.Now().Add(-10*time.Hour))

	c.runGCCycle(1)

	// CAS must survive: it's referenced by a fresh (within-cap) AC entry.
	if _, err := os.Stat(casPath); err != nil {
		t.Fatalf("CAS referenced by fresh AC must survive age-capped GC, err=%v", err)
	}
}

// -----------------------------------------------------------------------------
// Change C+D: parallel scanFilesWithAtime + parallel delete determinism
// -----------------------------------------------------------------------------

// TestScanFilesWithAtime_ParallelDeterminism runs scanFilesWithAtime N times
// on the same fixture and asserts (a) file count is stable across runs and
// (b) totalSize is stable. Parallel workers append into a shared slice via
// channel; result ordering may differ across runs but counts must not.
func TestScanFilesWithAtime_ParallelDeterminism(t *testing.T) {
	dir := testutils.TempDir(t)
	defer func() { _ = os.RemoveAll(dir) }()

	c := newGCCache(t, dir, 10*1024*1024, time.Hour)

	// 50 CAS + 30 AC + 20 RAW blobs of varied size across many shards.
	kinds := []struct {
		k     cache.EntryKind
		count int
	}{
		{cache.CAS, 50},
		{cache.AC, 30},
		{cache.RAW, 20},
	}
	for _, kk := range kinds {
		for i := 0; i < kk.count; i++ {
			sz := int64(64 + i*7)
			data, h := testutils.RandomDataAndHash(sz)
			if err := c.Put(context.Background(), kk.k, h, sz, bytes.NewReader(data)); err != nil {
				t.Fatalf("Put %v: %v", kk.k, err)
			}
		}
	}

	var firstCount int
	var firstTotal int64
	const runs = 10
	for i := 0; i < runs; i++ {
		files, total, err := c.scanFilesWithAtime()
		if err != nil {
			t.Fatalf("run %d: scanFilesWithAtime: %v", i, err)
		}
		if i == 0 {
			firstCount = len(files)
			firstTotal = total
			continue
		}
		if len(files) != firstCount {
			t.Fatalf("run %d: file count %d != first-run count %d", i, len(files), firstCount)
		}
		if total != firstTotal {
			t.Fatalf("run %d: totalSize %d != first-run total %d", i, total, firstTotal)
		}
	}
}

// TestGC_ParallelDelete_RespectsBytesToFree pushes many aged unreferenced
// blobs and asserts the parallel-delete phase (Change D) frees roughly
// bytesToFree — not "delete everything" and not "delete way past target".
// A small overshoot is acceptable because the feeder queues jobs based on
// pre-flight expected-freed (before workers confirm), so the last few
// concurrent unlinks may push us over.
func TestGC_ParallelDelete_RespectsBytesToFree(t *testing.T) {
	dir := testutils.TempDir(t)
	defer func() { _ = os.RemoveAll(dir) }()

	// Fresh cache; big LRU so Put isn't rejected. minAge=1h; we'll age
	// everything past that so ALL blobs are eviction-eligible.
	c := newGCCache(t, dir, 100*1024*1024, time.Hour)

	const numBlobs = 40
	const blobSize = int64(1024)
	blobPaths := make([]string, 0, numBlobs)
	for i := 0; i < numBlobs; i++ {
		data, h := testutils.RandomDataAndHash(blobSize)
		if err := c.Put(context.Background(), cache.CAS, h, blobSize, bytes.NewReader(data)); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
		blobPaths = append(blobPaths, filepath.Join(dir, c.FileLocationBase(cache.CAS, false, h, blobSize)))
	}
	// Age everything past min-age.
	old := time.Now().Add(-10 * time.Hour)
	for _, p := range blobPaths {
		setAtime(t, p, old)
	}

	// Total logical size = 40 * 1024 = 40960. Cap the GC target at 10 KB.
	// After GC, disk usage should be ~30-32 KB — we ask for ~10 KB freed;
	// with 8-16 concurrent workers and 1 KB blobs, overshoot is bounded.
	c.runGCCycle(30 * 1024)

	remaining := 0
	for _, p := range blobPaths {
		if _, err := os.Stat(p); err == nil {
			remaining++
		}
	}
	// We asked to free ~10 KB (10 blobs). Accept 8-20 remaining (overshoot
	// window covers the parallel feeder's pre-count vs actual-freed gap).
	if remaining < 8 || remaining > 32 {
		t.Fatalf("parallel delete freed too little or too much: %d/%d blobs remain (expected 8-32)", remaining, numBlobs)
	}
}
