package disk

import (
	"context"
	"errors"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/buchgr/bazel-remote/v2/cache"

	pb "github.com/buchgr/bazel-remote/v2/genproto/build/bazel/remote/execution/v2"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type proxyCheck struct {
	wg          *sync.WaitGroup
	digest      **pb.Digest
	ctx         context.Context
	onProxyMiss func()
}

var errMissingBlob = errors.New("a blob could not be found")

var errRequestCancelled = status.Error(codes.Canceled, "Request was cancelled")

// Optimised implementation of FindMissingBlobs, which batches local index
// lookups and performs concurrent proxy lookups for local cache misses.
// Returns a slice with the blobs that are missing from the cache.
//
// Note that this modifies the input slice and returns a subset of it.
func (c *diskCache) FindMissingCasBlobs(ctx context.Context, blobs []*pb.Digest) ([]*pb.Digest, error) {
	err := c.findMissingCasBlobsInternal(ctx, blobs, false)
	if err != nil {
		return nil, err
	}
	return filterNonNil(blobs), nil
}

// Identifies local and proxy cache misses for blobs. Modifies the input `blobs` slice such that found
// blobs are replaced with nil, while the missing digests remain unchanged.
//
// When failFast is true and a blob could not be found in the local cache nor in the
// proxy back end, the search will immediately terminate and errMissingBlob will be returned. Given that the
// search is terminated early, the contents of blobs will only have partially been updated.
func (c *diskCache) findMissingCasBlobsInternal(ctx context.Context, blobs []*pb.Digest, failFast bool) error {
	// batchSize moderates how long the cache lock is held by findMissingLocalCAS.
	const batchSize = 20

	var cancelContextForFailFast context.CancelFunc = nil
	cancelledDueToFailFast := false

	if failFast && c.proxy != nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(ctx)
		defer cancel()

		cancelContextForFailFast = func() {
			// Indicate that we were canceled so that we can fail fast.
			cancelledDueToFailFast = true
			cancel()
		}
	}

	var wg sync.WaitGroup

	var chunk []*pb.Digest
	remaining := blobs

	for len(remaining) > 0 {
		select {
		case <-ctx.Done():
			if cancelledDueToFailFast {
				return errMissingBlob
			}
			return errRequestCancelled
		default:
		}

		if len(remaining) <= batchSize {
			chunk = remaining
			remaining = nil
		} else {
			chunk = remaining[:batchSize]
			remaining = remaining[batchSize:]
		}

		numMissing := c.findMissingLocalCAS(chunk)
		if numMissing == 0 {
			continue
		}

		// Shared storage: a blob written by another replica lives on the shared
		// filesystem but may be absent from our in-memory index. Resolve misses
		// with a direct stat of the deterministic path (same FS, cheap) and index
		// it, rather than falling to the S3 proxy -- that does a ListObjects per
		// blob against the gateway, which is orders of magnitude slower and was
		// the cause of multi-minute FindMissingBlobs. The stat is authoritative
		// on a shared FS, so we never consult the proxy in this mode.
		if c.sharedStorageMode {
			for i := range chunk {
				if chunk[i] == nil {
					continue
				}
				if chunk[i].SizeBytes > 0 && c.statAndIndexCAS(chunk[i].Hash, chunk[i].SizeBytes) {
					c.accessLogger.Printf("GRPC CAS HEAD %s OK", chunk[i].Hash)
					chunk[i] = nil
					continue
				}
				// Still missing on the shared FS.
				if failFast {
					return errMissingBlob
				}
			}
			continue
		}

		if c.proxy == nil && failFast {
			// There's no proxy, there are missing blobs from the local cache, and we are failing fast.
			return errMissingBlob
		}

		if c.proxy != nil {
			for i := range chunk {
				if chunk[i] == nil {
					continue
				}

				if chunk[i].SizeBytes > c.maxProxyBlobSize {
					// The blob would exceed the limit, so skip it.
					if failFast {
						return errMissingBlob
					}
					continue
				}

				// Adding to the containsQueue channel may have blocked on a previous iteration,
				// so check to see if the context has cancelled.
				select {
				case <-ctx.Done():
					if cancelledDueToFailFast {
						return errMissingBlob
					}
					return errRequestCancelled
				default:
				}

				wg.Add(1)
				c.containsQueue <- proxyCheck{
					wg:     &wg,
					digest: &chunk[i],
					ctx:    ctx,
					// When failFast is true, onProxyMiss will have been set to a function that
					// will cancel the context, causing the remaining proxyChecks to short-circuit.
					onProxyMiss: cancelContextForFailFast,
				}
			}
		}
	}

	if c.proxy != nil {
		// Adapt the waitgroup for select
		waitCh := make(chan struct{})
		go func() {
			wg.Wait()
			close(waitCh)
		}()

		// Wait for all proxyChecks to finish or a context cancellation.
		select {
		case <-ctx.Done():
			if cancelledDueToFailFast {
				return errMissingBlob
			}
			return errRequestCancelled
		case <-waitCh: // Everything in the waitgroup has finished.
		}
	}

	return nil
}

// Move all the non-nil items in the input slice to the
// start, and return the non-nil sub-slice.
func filterNonNil(blobs []*pb.Digest) []*pb.Digest {
	count := 0
	for i := 0; i < len(blobs); i++ {
		if blobs[i] != nil {
			blobs[count] = blobs[i]
			count++
		}
	}

	return blobs[:count]
}

// pendingVerify is an index hit whose backing file still needs an existence
// check before it can be reported present.
type pendingVerify struct {
	idx  int
	hash string
	item lruItem
}

// Set blobs that exist in the disk cache to nil, and return the number
// of missing blobs.
//
// In shared storage mode an index hit is NOT sufficient to report a blob
// present. The leader's GC unlinks files from the shared filesystem without
// notifying the other replicas, so this replica's index can name blobs that no
// longer exist. Reporting one of those as present is worse than a cache miss:
// the client concludes it does not need to upload, and the build fails later
// when a worker tries to fetch it ("BLOB NOT FOUND"). Contains() has always
// guarded against this; this path did not, which made the two sibling lookups
// disagree about the same blob.
//
// Verification is throttled per entry rather than performed on every request,
// because FindMissingBlobs carries thousands of digests per call and a stat per
// digest against a network filesystem would dominate its latency. The cost is a
// bounded staleness window: a deleted blob can be reported present for at most
// sharedStorageVerifyInterval.
//
// The stats are deliberately issued with c.mu released. This function holds the
// global cache mutex across the whole batch, so filesystem I/O inside that loop
// would block every other request for the duration.
func (c *diskCache) findMissingLocalCAS(blobs []*pb.Digest) int {
	var key string
	missing := 0
	var toVerify []pendingVerify

	now := time.Now()

	c.mu.Lock()

	for i := range blobs {
		if blobs[i].SizeBytes == 0 && blobs[i].Hash == emptySha256 {
			c.accessLogger.Printf("GRPC CAS HEAD %s OK", blobs[i].Hash)
			blobs[i] = nil
			continue
		}

		foundSize := int64(-1)
		key = cache.LookupKey(cache.CAS, blobs[i].Hash)
		item, listElem, atimeDue := c.lru.GetWithAtimeRefresh(key)
		if listElem != nil {
			foundSize = item.size
		}

		if listElem != nil && !isSizeMismatch(blobs[i].SizeBytes, foundSize) {
			// LRU hit means "present and about to be used". Refresh the on-disk
			// atime (throttled) so an actively-referenced blob stays out of the
			// leader GC's eviction set.
			c.maybeRefreshAtime(atimeDue, cache.CAS, blobs[i].Hash, item)

			if c.verifyDue(item, now) {
				// Defer the decision until the file has been checked.
				toVerify = append(toVerify, pendingVerify{
					idx: i, hash: blobs[i].Hash, item: item,
				})
				continue
			}

			c.accessLogger.Printf("GRPC CAS HEAD %s OK", blobs[i].Hash)
			blobs[i] = nil
		} else {
			missing++
		}
	}

	c.mu.Unlock()

	if len(toVerify) == 0 {
		return missing
	}

	return missing + c.verifyPending(blobs, toVerify)
}

// verifyPending stats each deferred entry with the cache mutex released, then
// applies the outcome in a single locked pass. Returns how many turned out to
// be missing.
func (c *diskCache) verifyPending(blobs []*pb.Digest, toVerify []pendingVerify) int {
	present := make([]int, 0, len(toVerify))
	gone := make([]int, 0)
	missing := 0

	for n, p := range toVerify {
		blobPath := filepath.Join(c.dir,
			c.FileLocation(cache.CAS, p.item.legacy, p.hash, p.item.size, p.item.random))

		_, err := os.Stat(blobPath)
		switch {
		case err == nil:
			present = append(present, n)
		case errors.Is(err, fs.ErrNotExist):
			gone = append(gone, n)
			missing++
		default:
			// Could not confirm either way (EIO, EACCES, ESTALE ...). Report
			// missing, which costs at worst a redundant re-upload, rather than
			// risking a build failure on a blob we could not check. Do not
			// evict: the entry may be perfectly valid and the filesystem merely
			// unhappy, and for a legacy random-suffixed blob an eviction is
			// unrecoverable.
			log.Printf("Warning: could not verify %s during FindMissingBlobs: %v",
				blobPath, err)
			missing++
		}
	}

	now := time.Now()

	c.mu.Lock()
	for _, n := range present {
		c.lru.markVerified(cache.LookupKey(cache.CAS, toVerify[n].hash), now)
	}
	for _, n := range gone {
		// The file is already unlinked, so there is nothing to evict and
		// nothing to charge to the eviction counters.
		c.lru.RemoveKeyNoEvict(cache.LookupKey(cache.CAS, toVerify[n].hash))
		counterStaleIndexEntries.WithLabelValues(stalePathFindMissing).Inc()
	}
	c.mu.Unlock()

	for _, n := range present {
		p := toVerify[n]
		c.accessLogger.Printf("GRPC CAS HEAD %s OK", p.hash)
		blobs[p.idx] = nil
	}

	return missing
}

// verifyDue reports whether an index hit needs its backing file checked before
// it can be treated as present.
//
// Disabled by default (interval 0). Enabling it closes a real correctness hole
// -- the leader's GC deletes files without telling other replicas, so an index
// hit is not proof the blob exists, and a false "present" makes the client skip
// its upload and the build fail later on fetch -- but it costs one stat per
// blob per interval against the shared filesystem, on the FindMissingBlobs hot
// path. That cost scales with the active working set, not with the interval
// alone, so it must be measured on a real workload before being switched on.
//
// Always false outside shared storage mode, where nothing else can delete our
// files behind our back.
func (c *diskCache) verifyDue(item lruItem, now time.Time) bool {
	if !c.sharedStorageMode {
		return false
	}
	iv := c.sharedStorageVerifyInterval
	if iv <= 0 {
		return false
	}
	return now.Sub(item.lastVerified) > iv
}

func (c *diskCache) containsWorker() {
	var ok bool
	for req := range c.containsQueue {
		if req.ctx != nil {
			select {
			case <-req.ctx.Done():
				// Fast-fail if the context has already been cancelled.
				c.accessLogger.Printf("GRPC CAS HEAD %s CANCELLED", (*req.digest).Hash)
				req.wg.Done()
				continue
			default:
			}
		}

		ok, _ = c.proxy.Contains(req.ctx, cache.CAS, (*req.digest).Hash, (*req.digest).SizeBytes)
		if ok {
			c.accessLogger.Printf("GRPC CAS HEAD %s OK", (*req.digest).Hash)
			// The blob exists on the proxy, remove it from the
			// list of missing blobs.
			*(req.digest) = nil
		} else {
			c.accessLogger.Printf("GRPC CAS HEAD %s NOT FOUND", (*req.digest).Hash)
			if req.onProxyMiss != nil {
				req.onProxyMiss()
			}
		}
		req.wg.Done()
	}
}

func (c *diskCache) spawnContainsQueueWorkers() {
	// TODO: make these configurable?
	const queueSize = 2048
	const numWorkers = 512

	c.containsQueue = make(chan proxyCheck, queueSize)
	for i := 0; i < numWorkers; i++ {
		go c.containsWorker()
	}
}
