package disk

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/buchgr/bazel-remote/v2/cache"
	"github.com/buchgr/bazel-remote/v2/cache/disk/casblob"
	"github.com/buchgr/bazel-remote/v2/cache/disk/zstdimpl"
	"github.com/buchgr/bazel-remote/v2/utils/annotate"
	"github.com/buchgr/bazel-remote/v2/utils/sha256verifier"
	"github.com/buchgr/bazel-remote/v2/utils/tempfile"
	"github.com/buchgr/bazel-remote/v2/utils/validate"

	"github.com/djherbis/atime"

	pb "github.com/buchgr/bazel-remote/v2/genproto/build/bazel/remote/execution/v2"
	"google.golang.org/protobuf/proto"

	"github.com/prometheus/client_golang/prometheus"

	"golang.org/x/sync/semaphore"
)

var tfc = tempfile.NewCreator()

var emptyZstdBlob = []byte{40, 181, 47, 253, 32, 0, 1, 0, 0}

type Cache interface {
	Get(ctx context.Context, kind cache.EntryKind, hash string, size int64, offset int64) (io.ReadCloser, int64, error)
	GetValidatedActionResult(ctx context.Context, hash string) (*pb.ActionResult, []byte, error)
	GetZstd(ctx context.Context, hash string, size int64, offset int64) (io.ReadCloser, int64, error)
	Put(ctx context.Context, kind cache.EntryKind, hash string, size int64, r io.Reader) error
	Contains(ctx context.Context, kind cache.EntryKind, hash string, size int64) (bool, int64)
	FindMissingCasBlobs(ctx context.Context, blobs []*pb.Digest) ([]*pb.Digest, error)

	MaxSize() int64
	Stats() (totalSize int64, reservedSize int64, numItems int, uncompressedSize int64)
	RegisterMetrics()
}

// lruItem is the type of the values stored in SizedLRU to keep track of items.
type lruItem struct {
	// Size of the blob in uncompressed form.
	size int64

	// Size of the blob on disk (possibly with header + compression).
	sizeOnDisk int64

	// A random string (of digits, for now) that is included in the filename.
	random string

	// If true, the blob is a raw CAS file (no header, uncompressed)
	// with a ".v1" filename suffix.
	legacy bool
}

// diskCache is a filesystem-based LRU cache, with an optional backend proxy.
// It is safe for concurrent use.
type diskCache struct {
	dir              string
	proxy            cache.Proxy
	storageMode      casblob.CompressionType
	zstd             zstdimpl.ZstdImpl
	maxBlobSize      int64
	maxProxyBlobSize int64
	accessLogger     *log.Logger
	containsQueue    chan proxyCheck

	// Limit the number of simultaneous file removals and filesystem write
	// operations (apart from atime updates, which we hope are fast).
	// When acquiring both the "diskWaitSem" semaphore and the "mu" mutex,
	// the "diskWaitSem" must be acquired before "mu" in order to avoid
	// potential deadlocks.
	diskWaitSem *semaphore.Weighted

	mu  sync.Mutex
	lru SizedLRU

	gaugeCacheAge prometheus.Gauge

	// Shared storage mode settings for multiple replicas on shared filesystem
	sharedStorageMode       bool
	sharedStorageLeader     bool
	sharedStorageGCInterval time.Duration
	sharedStorageGCMinAge   time.Duration
}

const sha256HashStrSize = sha256.Size * 2 // Two hex characters per byte.
const emptySha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

func internalErr(err error) *cache.Error {
	return &cache.Error{
		Code: http.StatusInternalServerError,
		Text: err.Error(),
	}
}

func badReqErr(format string, a ...interface{}) *cache.Error {
	return &cache.Error{
		Code: http.StatusBadRequest,
		Text: fmt.Sprintf(format, a...),
	}
}

// Non-test users must call this to expose metrics.
func (c *diskCache) RegisterMetrics() {
	c.lru.RegisterMetrics()

	prometheus.MustRegister(c.gaugeCacheAge)

	// Update the cache age metric on a static interval
	// Note: this could be modeled as a GuageFunc that updates as needed
	// but since the updater func must lock the cache mu, it was deemed
	// necessary to have greater control of when to get the cache age
	go c.pollCacheAge()

	go c.shiftMetricPeriodContinuously()
}

// Shift to new period for metrics every 30 seconds. A period of
// 30 seconds should give margin to catch all peaks (with for example
// a 10 second scrape interval) even in cases of delayed or missed
// scrapes from prometheus.
func (c *diskCache) shiftMetricPeriodContinuously() {
	ticker := time.NewTicker(30 * time.Second)
	for range ticker.C {
		c.mu.Lock()
		c.lru.shiftToNextMetricPeriod()
		c.mu.Unlock()
	}
}

// Update metric every minute with the idle time of the least recently used item in the cache
func (c *diskCache) pollCacheAge() {
	ticker := time.NewTicker(60 * time.Second)
	for ; true; <-ticker.C {
		c.updateCacheAgeMetric()
	}
}

// Get the idle time of the least-recently used item in the cache, and store the value in a metric
func (c *diskCache) updateCacheAgeMetric() {
	c.mu.Lock()

	key, value, ok := c.lru.getTailItem()
	if !ok {
		// No items in the cache.
		c.mu.Unlock()
		return
	}

	age := 0.0
	validAge := true

	f := c.getElementPath(key, value)
	ts, err := atime.Stat(f)

	if err != nil {
		log.Printf("ERROR: failed to determine time of least recently used cache item: %v, unable to stat %s", err, f)
		validAge = false
	} else {
		age = time.Since(ts).Seconds()
	}

	c.mu.Unlock()

	if validAge {
		c.gaugeCacheAge.Set(age)
	}
}

func (c *diskCache) getElementPath(key string, value lruItem) string {
	ks := key
	hash := ks[len(ks)-sha256.Size*2:]
	var kind = cache.AC
	if strings.HasPrefix(ks, "cas") {
		kind = cache.CAS
	} else if strings.HasPrefix(ks, "ac") {
		kind = cache.AC
	} else if strings.HasPrefix(ks, "raw") {
		kind = cache.RAW
	}

	return filepath.Join(c.dir, c.FileLocation(kind, value.legacy, hash, value.size, value.random))
}

func (c *diskCache) removeFile(f string) {
	err := os.Remove(f)
	if err != nil {
		log.Printf("ERROR: failed to remove evicted cache file: %s", f)
	}
}

func (c *diskCache) FileLocationBase(kind cache.EntryKind, legacy bool, hash string, size int64) string {
	if kind == cache.RAW {
		return path.Join("raw.v2", hash[:2], hash)
	}

	if kind == cache.AC {
		return path.Join("ac.v2", hash[:2], hash)
	}

	if legacy {
		return path.Join("cas.v2", hash[:2], hash)
	}

	return fmt.Sprintf("cas.v2/%s/%s-%d", hash[:2], hash, size)
}

func (c *diskCache) FileLocation(kind cache.EntryKind, legacy bool, hash string, size int64, random string) string {
	// Deterministic naming (random == ""): the final path is computed
	// identically by every replica, so a blob written by one instance can be
	// located by another via a direct stat -- no need to know the writer's
	// random suffix. Legacy random-suffixed files (written before this scheme,
	// or discovered on disk) still resolve via their stored random suffix.
	if random == "" {
		return c.FileLocationBase(kind, legacy, hash, size)
	}

	if kind == cache.RAW {
		return path.Join("raw.v2", hash[:2], hash+"-"+random)
	}

	if kind == cache.AC {
		return path.Join("ac.v2", hash[:2], hash+"-"+random)
	}

	if legacy {
		return fmt.Sprintf("cas.v2/%s/%s-%s.v1", hash[:2], hash, random)
	}

	return fmt.Sprintf("cas.v2/%s/%s-%d-%s", hash[:2], hash, size, random)
}

// Put stores a stream of `size` bytes from `r` into the cache.
// If `hash` is not the empty string, and the contents don't match it,
// a non-nil error is returned. All data will be read from `r` before
// this function returns.
func (c *diskCache) Put(ctx context.Context, kind cache.EntryKind, hash string, size int64, r io.Reader) (rErr error) {
	defer func() {
		if r != nil {
			_, _ = io.Copy(io.Discard, r)
		}
	}()

	if size < 0 {
		return badReqErr("Invalid (negative) size: %d", size)
	}

	if size > c.maxBlobSize {
		return badReqErr("Blob size %d too large, max blob size is %d", size, c.maxBlobSize)
	}

	// The hash format is checked properly in the http/grpc code.
	// Just perform a simple/fast check here, to catch bad tests.
	if len(hash) != sha256HashStrSize {
		return badReqErr("Invalid hash size: %d, expected: %d", len(hash), sha256.Size)
	}

	if kind == cache.CAS && size == 0 && hash == emptySha256 {
		return nil
	}

	// Put requests are processed using blocking file syscalls, which
	// consume one operating system thread per request. We throttle
	// these requests with a semaphore to avoid creating too many
	// operating system threads.
	if err := c.diskWaitSem.Acquire(context.Background(), 1); err != nil {
		log.Printf("ERROR: failed to acquire semaphore: %v", err)
		return internalErr(err)
	}
	defer c.diskWaitSem.Release(1)

	key := cache.LookupKey(kind, hash)

	var tf *os.File // Tempfile.
	var blobFile string

	// Cleanup intermediate state if something went wrong and we
	// did not successfully commit.
	unreserve := false
	removeTempfile := false
	defer func() {
		// No lock required to remove stray tempfiles.
		if removeTempfile {
			err := os.Remove(blobFile)
			if err != nil {
				log.Printf("warning: failed to remove temp file: %q", blobFile)
			}
		}

		if unreserve {
			c.mu.Lock()
			err := c.lru.Unreserve(size)
			if err != nil {
				// Set named return value.
				rErr = internalErr(err)
				log.Println(rErr.Error())
			}
			c.mu.Unlock()
		}
	}()

	if size > 0 {
		c.mu.Lock()
		err := c.lru.Reserve(size)
		if err != nil {
			c.mu.Unlock()
			return err
		}
		c.mu.Unlock()
		unreserve = true
	}

	legacy := kind == cache.CAS && c.storageMode == casblob.Identity

	// Final destination, if all goes well.
	filePath := path.Join(c.dir, c.FileLocationBase(kind, legacy, hash, size))

	// We will download to this temporary file.
	tf, random, err := tfc.Create(filePath, legacy)
	if err != nil {
		return internalErr(err)
	}
	if tf == nil {
		return &cache.Error{
			Code: http.StatusInternalServerError,
			Text: fmt.Sprintf("Failed to create tempfile for %q", filePath),
		}
	}
	blobFile = tf.Name()
	removeTempfile = true

	var sizeOnDisk int64
	sizeOnDisk, err = c.writeAndCloseFile(ctx, r, kind, hash, size, tf)
	if err != nil {
		return internalErr(err)
	}

	r = nil // We read all the data from r.

	if c.proxy != nil {
		rc, err := os.Open(blobFile)
		if err != nil {
			log.Println("Failed to proxy Put:", err)
		} else {
			// Doesn't block, should be fast.
			c.proxy.Put(ctx, kind, hash, size, sizeOnDisk, rc)
		}
	}

	unreserve, removeTempfile, err = c.commit(key, legacy, blobFile, size, size, sizeOnDisk, random)
	if err != nil {
		return internalErr(err)
	}

	return nil
}

func (c *diskCache) writeAndCloseFile(ctx context.Context, r io.Reader, kind cache.EntryKind, hash string, size int64, f *os.File) (int64, error) {
	closeFile := true
	defer func() {
		if closeFile {
			_ = f.Close()
		}
	}()

	var err error
	var sizeOnDisk int64

	if kind == cache.CAS && c.storageMode != casblob.Identity {
		sizeOnDisk, err = casblob.WriteAndClose(c.zstd, r, f, c.storageMode, hash, size)
		if err != nil {
			return -1, annotate.Err(ctx, "Failed to write compressed CAS blob to disk", err)
		}
		closeFile = false
		return sizeOnDisk, nil
	}

	var writeCloser io.WriteCloser = f
	if kind == cache.CAS { // c.storageMode == casblob.Identity
		writeCloser = sha256verifier.New(hash, size, f)
	}

	sizeOnDisk, err = io.Copy(writeCloser, r)
	if err != nil {
		return -1, annotate.Err(ctx, "Failed to copy data to disk", err)
	}

	if isSizeMismatch(sizeOnDisk, size) {
		return -1, fmt.Errorf(
			"sizes don't match. Expected %d, found %d", size, sizeOnDisk)
	}

	err = f.Sync()
	if err != nil {
		return -1, fmt.Errorf("failed to sync file to disk: %w", err)
	}

	err = writeCloser.Close()
	if err != nil {
		return -1, fmt.Errorf("failed to verify hash: %w", err)
	}

	closeFile = false

	return sizeOnDisk, nil
}

// flockShard takes an advisory lock on the shard directory (e.g. cas.v2/<hh>)
// that holds the blob for `hash`, returning a release func. Writers use
// exclusive=true (LOCK_EX) around the create/rename; readers use false
// (LOCK_SH) around their existence stat. On a shared WekaFS this serves two
// purposes that plain stat can't: a reader's LOCK_SH blocks until an in-flight
// writer in that shard releases (so we don't observe a half-done write), and
// the lock handoff acts as a cross-node coherency point so the writer's freshly
// created directory entry is flushed/visible to the reader's node -- the
// writecache/forcedirect modes otherwise lag many seconds under load.
//
// Shard dirs are pre-existing (created at startup) so both sides can open them
// without creating anything, and there are only 256 per kind so lock files
// don't proliferate. Best-effort: on any error we return a no-op release rather
// than failing the cache operation.
func (c *diskCache) flockShard(kind cache.EntryKind, hash string, exclusive bool) func() {
	dirPath := filepath.Join(c.dir, kind.DirName(), hash[:2])
	f, err := os.Open(dirPath)
	if err != nil {
		return func() {}
	}
	how := syscall.LOCK_SH
	if exclusive {
		how = syscall.LOCK_EX
	}
	if err := syscall.Flock(int(f.Fd()), how); err != nil {
		_ = f.Close()
		return func() {}
	}
	return func() {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		_ = f.Close()
	}
}

// keyToKindHash splits an LRU key (e.g. "cas/<hash>") into its kind and hash.
func keyToKindHash(key string) (cache.EntryKind, string) {
	hash := key[len(key)-sha256HashStrSize:]
	switch {
	case strings.HasPrefix(key, "cas"):
		return cache.CAS, hash
	case strings.HasPrefix(key, "raw"):
		return cache.RAW, hash
	default:
		return cache.AC, hash
	}
}

// This must be called when the lock is not held.
//
// The freshly-written temp file (which has a unique random suffix only to avoid
// write collisions) is renamed to its deterministic final name so that any
// replica sharing this filesystem can locate it by a direct stat. The `random`
// parameter is therefore unused for the final name.
func (c *diskCache) commit(key string, legacy bool, tempfile string, reservedSize int64, logicalSize int64, sizeOnDisk int64, random string) (unreserve bool, removeTempfile bool, err error) {
	unreserve = reservedSize > 0
	removeTempfile = true

	kind, hash := keyToKindHash(key)
	finalPath := filepath.Join(c.dir, c.FileLocationBase(kind, legacy, hash, logicalSize))

	// Hold an exclusive lock on the shard dir across the rename. Releasing it
	// acts as a cross-node coherency point so the new entry is promptly visible
	// to other replicas (writecache/forcedirect otherwise lag under load), and
	// it makes concurrent readers (LOCK_SH) wait rather than observe a partial
	// state. Gated to shared-storage mode; no-op/best-effort otherwise.
	releaseShard := func() {}
	if c.sharedStorageMode {
		releaseShard = c.flockShard(kind, hash, true)
	}

	if err = os.Rename(tempfile, finalPath); err != nil {
		releaseShard()
		log.Println(err.Error())
		return unreserve, removeTempfile, err
	}
	// Flush the parent directory so the new entry is pushed to the shared
	// backend, then release the shard lock (the release is the coherency point).
	if dirFile, derr := os.Open(filepath.Dir(finalPath)); derr == nil {
		_ = dirFile.Sync()
		_ = dirFile.Close()
	}
	releaseShard()
	// The temp file no longer exists under its original name; if indexing fails
	// below, the final file is the one to clean up (removeTempfile is for the
	// caller's stray-temp cleanup, which no longer applies).
	removeTempfile = false
	removeFinal := true
	defer func() {
		if removeFinal {
			_ = os.Remove(finalPath)
		}
	}()

	c.mu.Lock()
	defer c.mu.Unlock()

	if unreserve {
		err = c.lru.Unreserve(reservedSize)
		if err != nil {
			log.Println(err.Error())
			return true, false, err
		}
	}
	unreserve = false

	newItem := lruItem{
		size:       logicalSize,
		sizeOnDisk: sizeOnDisk,
		legacy:     legacy,
		random:     "", // deterministic name (see FileLocation)
	}

	if !c.lru.Add(key, newItem) {
		err = fmt.Errorf("INTERNAL ERROR: failed to add: %s, size %d (on disk: %d)",
			key, logicalSize, sizeOnDisk)
		log.Println(err.Error())
		return unreserve, false, err
	}

	removeFinal = false

	// Commit successful if we made it this far! \o/
	return unreserve, false, nil
}

// Return a non-nil io.ReadCloser and non-negative size if the item is available
// locally, and a boolean that indicates if the item is not available locally
// but that we can try the proxy backend.
//
// This function assumes that only CAS blobs are requested in zstd form.

// discoverAndIndex looks for a blob on the (shared) filesystem that is not yet
// in this instance's in-memory index -- typically one written by another
// instance. It first checks the deterministic path (which every instance
// computes identically, so a direct stat reliably finds another replica's
// write), then falls back to the legacy random-suffixed naming for files
// written before the deterministic scheme. If found, it is indexed. Returns
// true if an entry for the hash is present in the index afterwards.
//
// This is what makes a multi-instance shared_storage_mode deployment coherent
// without depending on a proxy backend: a read miss in the local index falls
// back to the actual directory contents, by a name every replica agrees on.
func (c *diskCache) discoverAndIndex(kind cache.EntryKind, hash string, size int64) bool {
	legacy := kind == cache.CAS && c.storageMode == casblob.Identity
	key := cache.LookupKey(kind, hash)

	addItem := func(item lruItem) bool {
		c.mu.Lock()
		if _, present := c.lru.Get(key); present == nil {
			c.lru.Add(key, item)
		}
		c.mu.Unlock()
		return true
	}

	// Deterministic name only. This version never commits a random-suffixed
	// file (those are always in-progress .tmp writes, renamed to the base name
	// on commit), so a direct stat of the base path is authoritative. For CAS
	// we need the size to build the path; without it we can't locate the blob.
	if kind != cache.CAS || size > 0 {
		// Take a shared lock on the shard dir for the stat: if a writer is
		// mid-write in this shard we wait for it to release, and the lock
		// acquire is a cross-node coherency point so we see the writer's fresh
		// entry rather than a stale "missing". Gated to shared-storage mode.
		release := func() {}
		if c.sharedStorageMode {
			release = c.flockShard(kind, hash, false)
		}
		det := filepath.Join(c.dir, c.FileLocationBase(kind, legacy, hash, size))
		info, statErr := os.Stat(det)
		release()
		if statErr == nil && !info.IsDir() {
			item := lruItem{sizeOnDisk: info.Size(), size: size, legacy: legacy}
			if kind != cache.CAS {
				item.size = info.Size()
			}
			return addItem(item)
		}
	}

	return false
}

// statAndIndexCAS checks for a CAS blob at its deterministic path with a plain
// stat (no shard flock) and indexes it on hit. Used by FindMissingBlobs, where
// dropping the flock is deliberate: unlike the read path, a stale cross-node
// miss here is harmless -- the client just re-uploads an already-present blob
// (idempotent) -- so the flock's per-blob backend round-trip isn't worth it.
// Returns true if the blob is present on the shared filesystem.
func (c *diskCache) statAndIndexCAS(hash string, size int64) bool {
	legacy := c.storageMode == casblob.Identity
	det := filepath.Join(c.dir, c.FileLocationBase(cache.CAS, legacy, hash, size))
	info, err := os.Stat(det)
	if err != nil || info.IsDir() {
		return false
	}

	key := cache.LookupKey(cache.CAS, hash)
	item := lruItem{sizeOnDisk: info.Size(), size: size, legacy: legacy}
	c.mu.Lock()
	if _, present := c.lru.Get(key); present == nil {
		c.lru.Add(key, item)
	}
	c.mu.Unlock()
	return true
}

func (c *diskCache) availableOrTryProxy(kind cache.EntryKind, hash string, size int64, offset int64, zstd bool) (io.ReadCloser, int64, bool, error) {
	key := cache.LookupKey(kind, hash)

	// Shared filesystem: another instance may have written this blob under a
	// random suffix that is not in our in-memory index. Discover it on disk
	// and index it so the lookup below can serve it. (A configured proxy is
	// only a best-effort race-breaker here and need not reflect the filesystem.)
	if c.sharedStorageMode {
		c.mu.Lock()
		_, present := c.lru.Get(key)
		c.mu.Unlock()
		if present == nil {
			c.discoverAndIndex(kind, hash, size)
		}
	}

	locked := true
	var err error
	c.mu.Lock()

	item, listElem := c.lru.Get(key)
	if listElem != nil {
		c.mu.Unlock() // We expect a cache hit below.
		locked = false

		blobPath := path.Join(c.dir, c.FileLocation(kind, item.legacy, hash, item.size, item.random))

		if !isSizeMismatch(size, item.size) {
			var f *os.File
			fastPath := true
			f, err = os.Open(blobPath)
			if err != nil && os.IsNotExist(err) {
				// Another request replaced the file before we could open it?
				// Enter slow path.
				fastPath = false

				c.mu.Lock()
				item, listElem = c.lru.Get(key)
				if listElem != nil {
					blobPath = path.Join(c.dir, c.FileLocation(kind, item.legacy, hash, item.size, item.random))
					f, err = os.Open(blobPath)
					if err != nil {
						// We will log the error below, while not holding the lock.
						c.lru.RemoveElement(listElem)
					}
				}
				c.mu.Unlock()
			}

			if err != nil {
				// Race condition, was the item purged after we released the lock?
				log.Printf("Warning: expected %q to exist on disk (fast path: %t), undersized cache? Last reported error: %v", blobPath, fastPath, err)
			} else if kind == cache.CAS {
				var rc io.ReadCloser
				if item.legacy {
					// The file is uncompressed, without a casblob header.
					_, err = f.Seek(offset, io.SeekStart)
					if zstd && err == nil {
						rc, err = casblob.GetLegacyZstdReadCloser(c.zstd, f)
					} else if err == nil {
						rc = f
					}
				} else {
					// The file is compressed.
					if zstd {
						rc, err = casblob.GetZstdReadCloser(c.zstd, f, size, offset)
					} else {
						rc, err = casblob.GetUncompressedReadCloser(c.zstd, f, size, offset)
					}
				}

				if err != nil {
					log.Printf("Warning: expected item to be on disk, but something happened when retrieving %s (compressed: %v, legacy: %v): %v",
						blobPath, zstd, item.legacy, err)
					_ = f.Close()

					c.mu.Lock()
					c.lru.RemoveElement(listElem)
					c.mu.Unlock()
				} else {
					c.touchAtime(blobPath)
					return rc, item.size, false, nil
				}
			} else {
				var fileInfo os.FileInfo
				fileInfo, err = f.Stat()
				if err != nil {
					_ = f.Close()
					return nil, -1, true, err
				}
				foundSize := fileInfo.Size()
				if isSizeMismatch(size, foundSize) {
					// Race condition, was the item replaced after we released the lock?
					log.Printf("Warning: expected %s to on disk to have size %d, found %d",
						blobPath, size, foundSize)
				} else {
					c.touchAtime(blobPath)
					_, err = f.Seek(offset, io.SeekStart)
					return f, foundSize, false, err
				}
			}
		}
	}
	err = nil

	var tryProxy bool

	if c.proxy != nil && size <= c.maxProxyBlobSize {
		if size > 0 {
			// If we know the size, attempt to reserve that much space.
			if !locked {
				c.mu.Lock()
			}
			err = c.lru.Reserve(size)
			if err == nil {
				tryProxy = true
			}
			c.mu.Unlock()
			locked = false
		} else {
			// If the size is unknown, take a risk and hope it's not
			// too large.
			tryProxy = true
		}
	}

	if locked {
		c.mu.Unlock()
	}

	return nil, -1, tryProxy, err
}

// touchAtime advances the on-disk access time of a served blob so the
// shared-storage leader GC (which ranks eviction candidates by atime) sees
// real, cross-pod access recency. WEKA's default relatime only bumps atime on
// the first read after a write, so without this an actively-referenced blob
// keeps its creation-time atime and looks cold. Best-effort and asynchronous:
// it never blocks or fails a cache hit. Passing a zero mtime leaves mtime
// unchanged (Go >=1.24).
func (c *diskCache) touchAtime(blobPath string) {
	if !c.sharedStorageMode {
		return
	}
	now := time.Now()
	go func() {
		if err := os.Chtimes(blobPath, now, time.Time{}); err != nil && !os.IsNotExist(err) {
			log.Printf("Warning: failed to update atime for %q: %v", blobPath, err)
		}
	}()
}

var errOnlyCompressedCAS = &cache.Error{
	Code: http.StatusBadRequest,
	Text: "Only CAS blobs are available in compressed form",
}

// Get returns an io.ReadCloser with the content of the cache item stored
// under `hash` and the number of bytes that can be read from it. If the
// item is not found, the io.ReadCloser will be nil. If some error occurred
// when processing the request, then it is returned. Callers should provide
// the `size` of the item to be retrieved, or -1 if unknown.
func (c *diskCache) Get(ctx context.Context, kind cache.EntryKind, hash string, size int64, offset int64) (rc io.ReadCloser, s int64, rErr error) {
	return c.get(ctx, kind, hash, size, offset, false)
}

// GetZstd is just like Get, except the data available from rc is zstandard
// compressed. Note that the returned `s` value still refers to the amount
// of data once it has been decompressed.
func (c *diskCache) GetZstd(ctx context.Context, hash string, size int64, offset int64) (rc io.ReadCloser, s int64, rErr error) {
	return c.get(ctx, cache.CAS, hash, size, offset, true)
}

func (c *diskCache) get(ctx context.Context, kind cache.EntryKind, hash string, size int64, offset int64, zstd bool) (rc io.ReadCloser, s int64, rErr error) {
	// The hash format is checked properly in the http/grpc code.
	// Just perform a simple/fast check here, to catch bad tests.
	if len(hash) != sha256HashStrSize {
		return nil, -1, badReqErr("Invalid hash size: %d, expected: %d", len(hash), sha256.Size)
	}

	if kind == cache.CAS && size <= 0 && hash == emptySha256 {
		if zstd {
			return io.NopCloser(bytes.NewReader(emptyZstdBlob)), 0, nil
		}

		return io.NopCloser(bytes.NewReader([]byte{})), 0, nil
	}

	if kind != cache.CAS && zstd {
		return nil, -1, errOnlyCompressedCAS
	}

	if offset < 0 {
		return nil, -1, badReqErr("Invalid offset: %d", offset)
	}
	if size > 0 && offset >= size {
		return nil, -1, badReqErr("Invalid offset: %d for size %d", offset, size)
	}

	var err error
	key := cache.LookupKey(kind, hash)

	var tf *os.File // Tempfile we will write to.
	var blobFile string

	// Cleanup intermediate state if something went wrong and we
	// did not successfully commit.
	unreserve := false
	removeTempfile := false
	defer func() {
		// No lock required to remove stray tempfiles.
		if removeTempfile {
			err := os.Remove(blobFile)
			if err != nil {
				log.Printf("warning: failed to remove temp file: %q", blobFile)
			}
		}

		if unreserve {
			c.mu.Lock()
			err := c.lru.Unreserve(size)
			if err != nil {
				// Set named return value.
				rErr = internalErr(err)
				log.Println(rErr.Error())
			}
			c.mu.Unlock()
		}
	}()

	f, foundSize, tryProxy, err := c.availableOrTryProxy(kind, hash, size, offset, zstd)
	if err != nil {
		return nil, -1, err
	}
	if tryProxy && size > 0 {
		unreserve = true
	}
	if f != nil {
		return f, foundSize, nil
	}

	if !tryProxy {
		return nil, -1, nil
	}

	// Non-proxied Get requests do not seem to consume any significant amount of OS threads,
	// and are therefore not throttled. However, it is assumed that proxied Get requests might,
	// at least when storing the result from the proxy to disk, and perhaps also when
	// waiting for the proxy. Proxied Get requests are therefore throttled by a semaphore.
	// Unfortunately, this proxy-specific throttling does not limit the size reservation
	// performed inside availableOrTryProxy. It should still be effective in limiting the number
	// of OS threads, but it does not help reduce the risk of http.StatusInsufficientStorage.
	err = c.diskWaitSem.Acquire(context.Background(), 1)
	if err != nil {
		log.Printf("ERROR: failed to acquire semaphore: %v", err)
		return nil, -1, internalErr(err)
	}
	defer c.diskWaitSem.Release(1)

	r, foundSize, err := c.proxy.Get(ctx, kind, hash, size)
	if r != nil {
		defer func() { _ = r.Close() }()
	}
	if err != nil {
		return nil, -1, internalErr(err)
	}
	if r == nil {
		return nil, -1, nil
	}
	if foundSize > c.maxProxyBlobSize {
		_ = r.Close()
		return nil, -1, nil
	}

	if isSizeMismatch(size, foundSize) || foundSize < 0 {
		return nil, -1, nil
	}

	legacy := kind == cache.CAS && c.storageMode == casblob.Identity

	blobPathBase := path.Join(c.dir, c.FileLocationBase(kind, legacy, hash, foundSize))
	tf, random, err := tfc.Create(blobPathBase, legacy)
	if err != nil {
		return nil, -1, internalErr(err)
	}
	removeTempfile = true

	blobFile = tf.Name()

	var sizeOnDisk int64
	sizeOnDisk, err = io.Copy(tf, r)
	_ = tf.Close()
	if err != nil {
		return nil, -1, internalErr(err)
	}

	rcf, err := os.Open(blobFile)
	if err != nil {
		return nil, -1, internalErr(err)
	}

	uncompressedOnDisk := (kind != cache.CAS) || (c.storageMode == casblob.Identity)
	if uncompressedOnDisk {
		if offset > 0 {
			_, err = rcf.Seek(offset, io.SeekStart)
			if err != nil {
				return nil, -1, internalErr(err)
			}
		}

		if zstd {
			rc, err = casblob.GetLegacyZstdReadCloser(c.zstd, rcf)
		} else {
			rc = rcf
		}
	} else { // Compressed CAS blob.
		if zstd {
			rc, err = casblob.GetZstdReadCloser(c.zstd, rcf, foundSize, offset)
		} else {
			rc, err = casblob.GetUncompressedReadCloser(c.zstd, rcf, foundSize, offset)
		}
	}
	if err != nil {
		return nil, -1, internalErr(err)
	}

	unreserve, removeTempfile, err = c.commit(key, legacy, blobFile, size, foundSize, sizeOnDisk, random)
	if err != nil {
		_ = rc.Close()
		return nil, -1, internalErr(err)
	}

	return rc, foundSize, nil
}

// Contains returns true if the `hash` key exists in the cache, and
// the size if known (or -1 if unknown).
//
// If there is a local cache miss, the proxy backend (if there is
// one) will be checked.
//
// Callers should provide the `size` of the item, or -1 if unknown.
func (c *diskCache) Contains(ctx context.Context, kind cache.EntryKind, hash string, size int64) (bool, int64) {
	// The hash format is checked properly in the http/grpc code.
	// Just perform a simple/fast check here, to catch bad tests.
	if len(hash) != sha256HashStrSize {
		return false, -1
	}

	if kind == cache.CAS && size <= 0 && hash == emptySha256 {
		return true, 0
	}

	foundSize := int64(-1)
	key := cache.LookupKey(kind, hash)

	// Shared filesystem: discover and index a blob written by another instance
	// (different random suffix, absent from our index) before checking.
	if c.sharedStorageMode {
		c.mu.Lock()
		_, present := c.lru.Get(key)
		c.mu.Unlock()
		if present == nil {
			c.discoverAndIndex(kind, hash, size)
		}
	}

	c.mu.Lock()
	item, listElem := c.lru.Get(key)
	exists := listElem != nil
	if exists {
		foundSize = item.size
	}
	c.mu.Unlock()

	if exists && !isSizeMismatch(size, foundSize) {
		// In shared storage mode, verify file still exists (may have been deleted by leader GC)
		if c.sharedStorageMode {
			blobPath := filepath.Join(c.dir, c.FileLocation(kind, item.legacy, hash, item.size, item.random))
			if _, err := os.Stat(blobPath); os.IsNotExist(err) {
				// File was deleted by leader GC, remove from LRU
				c.mu.Lock()
				c.lru.RemoveKey(key)
				c.mu.Unlock()
				exists = false
			} else {
				c.touchAtime(blobPath)
				return true, foundSize
			}
		} else {
			return true, foundSize
		}
	}

	if c.proxy != nil && size <= c.maxProxyBlobSize {
		exists, foundSize = c.proxy.Contains(ctx, kind, hash, size)
		if exists && foundSize <= c.maxProxyBlobSize && !isSizeMismatch(size, foundSize) {
			return true, foundSize
		}
	}

	return false, -1
}

// MaxSize returns the maximum cache size in bytes.
func (c *diskCache) MaxSize() int64 {
	// The underlying value is never modified, no need to lock.
	return c.lru.MaxSize()
}

// Stats returns the current size of the cache in bytes, and the number of
// items stored in the cache.
func (c *diskCache) Stats() (totalSize int64, reservedSize int64, numItems int, uncompressedSize int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.lru.TotalSize(), c.lru.ReservedSize(), c.lru.Len(), c.lru.UncompressedSize()
}

func isSizeMismatch(requestedSize int64, foundSize int64) bool {
	return requestedSize > -1 && foundSize > -1 && requestedSize != foundSize
}

// GetValidatedActionResult returns a valid ActionResult and its serialized
// value from the CAS if it and all its dependencies are also available. If
// not, nil values are returned. If something unexpected went wrong, return
// an error.
func (c *diskCache) GetValidatedActionResult(ctx context.Context, hash string) (*pb.ActionResult, []byte, error) {
	rc, sizeBytes, err := c.Get(ctx, cache.AC, hash, -1, 0)
	if rc != nil {
		defer func() { _ = rc.Close() }()
	}
	if err != nil {
		return nil, nil, err
	}

	if rc == nil || sizeBytes <= 0 {
		return nil, nil, nil // aka "not found"
	}

	acdata, err := io.ReadAll(rc)
	if err != nil {
		return nil, nil, err
	}

	result := &pb.ActionResult{}
	err = proto.Unmarshal(acdata, result)
	if err != nil {
		return nil, nil, err
	}

	// Validate the ActionResult's immediate fields, but don't check for dependent blobs.
	err = validate.ActionResult(result)
	if err != nil {
		return nil, nil, err // Should we return "not found" instead of an error?
	}

	pendingValidations := []*pb.Digest{}

	for _, f := range result.OutputFiles {
		// f was validated in validate.ActionResult but blobs were not checked for existence
		if len(f.Contents) == 0 {
			pendingValidations = append(pendingValidations, f.Digest)
		}
	}

	for _, d := range result.OutputDirectories {
		// d was validated in validate.ActionResult but blobs were not checked for existence
		r, size, err := c.Get(ctx, cache.CAS, d.TreeDigest.Hash, d.TreeDigest.SizeBytes, 0)
		if r == nil {
			return nil, nil, err // aka "not found", or an err if non-nil
		}
		if err != nil {
			_ = r.Close()
			return nil, nil, err
		}
		if size != d.TreeDigest.SizeBytes {
			_ = r.Close()
			return nil, nil, fmt.Errorf("expected %d bytes, found %d",
				d.TreeDigest.SizeBytes, size)
		}

		var oddata []byte
		oddata, err = io.ReadAll(r)
		_ = r.Close()
		if err != nil {
			return nil, nil, err
		}

		tree := pb.Tree{}
		err = proto.Unmarshal(oddata, &tree)
		if err != nil {
			return nil, nil, err
		}

		for _, f := range tree.Root.GetFiles() {
			if f.Digest != nil {
				pendingValidations = append(pendingValidations, f.Digest)
			}
		}

		for _, child := range tree.GetChildren() {
			for _, f := range child.GetFiles() {
				if f.Digest != nil {
					pendingValidations = append(pendingValidations, f.Digest)
				}
			}
		}
	}

	if result.StdoutDigest != nil {
		pendingValidations = append(pendingValidations, result.StdoutDigest)
	}

	if result.StderrDigest != nil {
		pendingValidations = append(pendingValidations, result.StderrDigest)
	}

	err = c.findMissingCasBlobsInternal(ctx, pendingValidations, true)
	if errors.Is(err, errMissingBlob) {
		return nil, nil, nil // aka "not found"
	}
	if err != nil {
		return nil, nil, err
	}

	return result, acdata, nil
}
