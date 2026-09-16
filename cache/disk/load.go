package disk

import (
	"context"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/buchgr/bazel-remote/v2/cache"
	"github.com/buchgr/bazel-remote/v2/cache/disk/casblob"
	"github.com/buchgr/bazel-remote/v2/cache/disk/zstdimpl"
	"github.com/buchgr/bazel-remote/v2/utils/validate"

	pb "github.com/buchgr/bazel-remote/v2/genproto/build/bazel/remote/execution/v2"
	"google.golang.org/protobuf/proto"

	"github.com/djherbis/atime"

	"github.com/prometheus/client_golang/prometheus"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

const lowercaseDSStoreFile = ".ds_store"

// New returns a new instance of a filesystem-based cache rooted at `dir`,
// with a maximum size of `maxSizeBytes` bytes and `opts` Options set.
func New(dir string, maxSizeBytes int64, opts ...Option) (Cache, error) {

	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return nil, err
	}

	dir, err = filepath.EvalSymlinks(dir)
	if err != nil {
		return nil, err
	}

	// Go defaults to a limit of 10,000 operating system threads. Going
	// over that limit would result in a crash and therefore we use a
	// semaphore to throttle amount of concurrently running blocking
	// file syscalls. A semaphore weight of 5,000 should give plenty of
	// margin. The weight should not be set too low because the
	// average latency could increase if a few slow clients could block
	// all other clients.
	semaphoreWeight := int64(5000)

	if runtime.GOOS == "darwin" {
		// Mac seems to fail to create os threads when removing
		// lots of files, so allow fewer than linux.
		//
		// NOTE: 3000 still exceeds the default `ulimit -u` on some macOS
		// setups (2666 observed), which is why running the whole
		// cache/disk package in one process aborts there with
		// "pthread_create failed: Resource temporarily unavailable"
		// before any test reports. Individual suites pass; use -run
		// filters locally, or run the suite on Linux. A real fix would
		// clamp this to RLIMIT_NPROC, which needs golang.org/x/sys/unix
		// since darwin's stdlib syscall package lacks the constant.
		semaphoreWeight = 3000
	}

	log.Printf("Limiting concurrent disk waiting requests to %d\n", semaphoreWeight)

	zi, err := zstdimpl.Get("go")
	if err != nil {
		return nil, err
	}

	c := diskCache{
		dir: dir,

		// Not using config here, to avoid test import cycles.
		storageMode:      casblob.Zstandard,
		zstd:             zi,
		maxBlobSize:      math.MaxInt64,
		maxProxyBlobSize: math.MaxInt64,

		// Acquire 1 of these before starting filesystem writes/deletes, or
		// reject filesystem writes upon failure (since this will create a
		// new OS thread and we don't want to hit Go's default 10,000 OS
		// thread limit.
		diskWaitSem: semaphore.NewWeighted(semaphoreWeight),

		gcMetrics: newGCMetrics(),

		gaugeCacheAge: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "bazel_remote_disk_cache_longest_item_idle_time_seconds",
			Help: "The idle time (now - atime) of the last item in the LRU cache, updated once per minute. Depending on filesystem mount options (e.g. relatime), the resolution may be measured in 'days' and not accurate to the second. If using noatime this will be 0. NaN means the value could not be determined; check bazel_remote_disk_cache_longest_item_idle_time_valid.",
		}),

		gaugeCacheAgeValid: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "bazel_remote_disk_cache_longest_item_idle_time_valid",
			Help: "1 if bazel_remote_disk_cache_longest_item_idle_time_seconds holds a real measurement, 0 if the least-recently-used item could not be stat'ed. Without this a stale reading is indistinguishable from a current one.",
		}),
	}

	cc := CacheConfig{diskCache: &c}

	// Apply options.
	for _, o := range opts {
		err = o(&cc)
		if err != nil {
			return nil, err
		}
	}

	// Create the directory structure.
	hexLetters := []byte("0123456789abcdef")
	for _, c1 := range hexLetters {
		for _, c2 := range hexLetters {
			subDir := string(c1) + string(c2)
			err := os.MkdirAll(filepath.Join(dir, cache.CAS.DirName(), subDir), os.ModePerm)
			if err != nil {
				return nil, err
			}
			err = os.MkdirAll(filepath.Join(dir, cache.AC.DirName(), subDir), os.ModePerm)
			if err != nil {
				return nil, err
			}
			err = os.MkdirAll(filepath.Join(dir, cache.RAW.DirName(), subDir), os.ModePerm)
			if err != nil {
				return nil, err
			}
		}
	}

	err = c.migrateDirectories()
	if err != nil {
		return nil, fmt.Errorf("attempting to migrate the old directory structure failed: %w", err)
	}
	err = c.loadExistingFiles(maxSizeBytes, cc)
	if err != nil {
		return nil, fmt.Errorf("loading of existing cache entries failed due to error: %w", err)
	}

	if cc.metrics == nil {
		return &c, nil
	}

	cc.metrics.diskCache = &c

	return cc.metrics, nil
}

func (c *diskCache) migrateDirectories() error {
	err := migrateDirectory(c.dir, cache.AC)
	if err != nil {
		return err
	}
	err = migrateDirectory(c.dir, cache.CAS)
	if err != nil {
		return err
	}
	err = migrateDirectory(c.dir, cache.RAW)
	if err != nil {
		return err
	}
	return nil
}

func migrateDirectory(baseDir string, kind cache.EntryKind) error {
	sourceDir := path.Join(baseDir, kind.String())

	_, err := os.Stat(sourceDir)
	if os.IsNotExist(err) {
		return nil
	}

	log.Println("Migrating files (if any) to new directory structure:", sourceDir)

	listing, err := os.ReadDir(sourceDir)
	if err != nil {
		return err
	}

	// The v0 directory structure was lowercase sha256 hash filenames
	// stored directly in the ac/ and cas/ directories.

	// The v1 directory structure has subdirs for each two lowercase
	// hex character pairs.
	v1DirRegex := regexp.MustCompile("^[a-f0-9]{2}$")

	targetDir := path.Join(baseDir, kind.DirName())

	itemChan := make(chan os.DirEntry)
	errChan := make(chan error)

	var wg sync.WaitGroup
	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for item := range itemChan {

				oldName := item.Name()
				oldNamePath := filepath.Join(sourceDir, oldName)

				if item.IsDir() {
					if !v1DirRegex.MatchString(oldName) {
						// Warn about non-v1 subdirectories.
						log.Println("Warning: unexpected directory", oldNamePath)
					}

					destDir := filepath.Join(targetDir, oldName[:2])
					err := migrateV1Subdir(oldNamePath, destDir, kind)
					if err != nil {
						log.Printf("Warning: failed to read subdir %q: %s",
							oldNamePath, err)
						continue
					}

					continue
				}

				if !item.Type().IsRegular() {
					log.Println("Warning: skipping non-regular file:", oldNamePath)
					continue
				}

				if !validate.HashKeyRegex.MatchString(oldName) {
					log.Println("Warning: skipping unexpected file:", oldNamePath)
					continue
				}

				src := filepath.Join(sourceDir, oldName)

				// Add a not-so-random "random" string. This should be OK
				// since there is probably only be one file for this hash.
				dest := filepath.Join(targetDir, oldName[:2], oldName+"-222444666")
				if kind == cache.CAS {
					dest += ".v1"
				}

				// TODO: make this work across filesystems?
				err := os.Rename(src, dest)
				if err != nil {
					errChan <- err
					return
				}
			}
		}()
	}

	err = nil
	numItems := len(listing)
	i := 1
	for _, item := range listing {
		select {
		case itemChan <- item:
			log.Printf("Migrating %s item(s) %d/%d, %s\n", sourceDir, i, numItems, item.Name())
			i++
		case err = <-errChan:
			log.Println("Encountered error while migrating files:", err)
			close(itemChan)
		}
	}
	close(itemChan)
	wg.Wait()

	if err != nil {
		return err
	}

	// Remove the empty directories.
	return os.RemoveAll(sourceDir)
}

func migrateV1Subdir(oldDir string, destDir string, kind cache.EntryKind) error {
	listing, err := os.ReadDir(oldDir)
	if err != nil {
		return err
	}

	if kind == cache.CAS {
		for _, item := range listing {

			name := item.Name()

			oldPath := path.Join(oldDir, name)

			if !validate.HashKeyRegex.MatchString(name) {
				if strings.ToLower(name) == lowercaseDSStoreFile {
					_ = os.Remove(oldPath)
					continue
				}

				return fmt.Errorf("unexpected file: %s", oldPath)
			}

			destPath := path.Join(destDir, name) + "-556677.v1"
			err = os.Rename(oldPath, destPath)
			if err != nil {
				return fmt.Errorf("failed to migrate CAS blob %s: %w",
					oldPath, err)
			}
		}

		return os.Remove(oldDir)
	}

	for _, item := range listing {
		name := item.Name()

		oldPath := path.Join(oldDir, name)

		if !validate.HashKeyRegex.MatchString(name) {
			if strings.ToLower(name) == lowercaseDSStoreFile {
				_ = os.Remove(oldPath)
				continue
			}

			return fmt.Errorf("unexpected file: %s %s", oldPath, name)
		}

		destPath := path.Join(destDir, name) + "-112233"

		// TODO: support cross-filesystem migration.
		err = os.Rename(oldPath, destPath)
		if err != nil {
			return fmt.Errorf("failed to migrate blob %s: %w", oldPath, err)
		}
	}

	return nil
}

// Metadata for an lruItem.
type keyAndAtime struct {
	lookupKey string

	// atime of the file.
	ts time.Time
}

// The result of scanning a directory for cache items. This uses slices
// of pointers because it seems to perform better than slices of values.
// I suspect this might be due to a reduction of the amount of data to append.
type scanResult struct {
	// These will eventually populate the LRU index.
	item []*lruItem

	// Metadata for each item above. Both slices must be the same length.
	metadata []*keyAndAtime
}

// Implement the sort.Sort interface.
func (r scanResult) Len() int {
	return len(r.item)
}

func (r scanResult) Less(i, j int) bool {
	return r.metadata[i].ts.Before(r.metadata[j].ts)
}

func (r scanResult) Swap(i, j int) {
	r.item[i], r.item[j] = r.item[j], r.item[i]
	r.metadata[i], r.metadata[j] = r.metadata[j], r.metadata[i]
}

// defaultWorkerCap is the historical bound on every scan/GC worker pool:
// min(NumCPU, 16), floored at 4. Preserved as the default so adding the
// concurrency flags changes nothing until they are set.
//
// Note runtime.NumCPU() reports the HOST's CPUs inside a container, not the
// cgroup quota, and this binary does not set GOMAXPROCS from the quota either.
// So on a large node this has always clamped to 16 regardless of the pod's CPU
// limit, and raising it can oversubscribe that limit into CFS throttling rather
// than speedup. Measure before raising.
const defaultWorkerCap = 16

// resolveWorkers applies a configured override, falling back to the historical
// min(NumCPU, defaultWorkerCap) bound. The floor of 4 is kept in both paths so
// a tiny machine still makes progress.
func resolveWorkers(configured int) int {
	if configured > 0 {
		if configured < 4 {
			return 4
		}
		return configured
	}
	n := runtime.NumCPU()
	if n < 4 {
		return 4
	}
	if n > defaultWorkerCap {
		return defaultWorkerCap
	}
	return n
}

// bootScanWorkers is the concurrency of the startup index scan. This pool runs
// before the server reports ready, so it never competes with client requests --
// it can be raised more aggressively than the GC pools.
func (c *diskCache) bootScanWorkers() int {
	return resolveWorkers(c.bootScanConcurrency)
}

// gcWorkers is the concurrency of every pool that runs DURING a GC cycle: the
// filesystem scan, the AC reference collection, and the unlink pool. These run
// concurrently with client traffic against the same filesystem metadata
// service, so raising them trades request latency for GC throughput. Kept as
// one knob because they are sequential phases of a single cycle and there is no
// evidence yet that they want different values.
func (c *diskCache) gcWorkers() int {
	return resolveWorkers(c.gcConcurrency)
}

func (c *diskCache) scanDir() (scanResult, error) {

	numWorkers := c.bootScanWorkers()
	log.Println("Scanning cache directory with", numWorkers, "goroutines")

	dc := make(chan string, numWorkers) // Feed directory names to workers.
	dcClosed := false
	defer func() {
		if !dcClosed {
			close(dc)
		}
	}()

	scanResults := make(chan scanResult, numWorkers) // Received from workers.
	scanResultsClosed := false
	defer func() {
		if !scanResultsClosed {
			close(scanResults)
		}
	}()

	finalScanResult := scanResult{
		item:     []*lruItem{},
		metadata: []*keyAndAtime{},
	}

	received := make(chan struct{})

	go func() {
		for sr := range scanResults {
			finalScanResult.item = append(finalScanResult.item, sr.item...)
			finalScanResult.metadata = append(finalScanResult.metadata, sr.metadata...)
		}
		received <- struct{}{}
	}()

	dirListers := new(errgroup.Group)

	// Blob filenames. Deterministic naming (no random suffix) plus the legacy
	// random-suffixed naming that may still be on disk:
	//   compressed CAS:   <hash>-<logical size>     (legacy: <hash>-<logical size>-<random>)
	//   uncompressed CAS: <hash>                    (legacy: <hash>-<random>.v1)
	//   AC and RAW:       <hash>                    (legacy: <hash>-<random>)
	hashRe := regexp.MustCompile(`^[a-f0-9]{64}$`)

	// Ignore lost+found dirs, which are automatically created in the
	// root dir of some unix style filesystems.
	const lostAndFound = "lost+found"

	for i := 0; i < numWorkers; i++ {
		dirListers.Go(func() error {
			for d := range dc {
				dirName := path.Join(c.dir, d)

				var lookupKeyPrefix string
				if strings.HasPrefix(d, "cas.v2/") {
					lookupKeyPrefix = "cas/"
				} else if strings.HasPrefix(d, "ac.v2/") {
					lookupKeyPrefix = "ac/"
				} else if strings.HasPrefix(d, "raw.v2/") {
					lookupKeyPrefix = "raw/"
				} else {
					return fmt.Errorf("unrecognised directory in cache dir: %q", dirName)
				}

				des, err := os.ReadDir(dirName)
				if err != nil {
					return err
				}

				item := make([]*lruItem, len(des))
				item_values := make([]lruItem, len(des))
				metadata := make([]*keyAndAtime, len(des))
				metadata_values := make([]keyAndAtime, len(des))

				n := 0 // The number of items to return for this dir.
				for _, de := range des {
					name := de.Name()

					if de.IsDir() {
						if de.Name() == lostAndFound {
							continue
						}

						return fmt.Errorf("unexpected directory: %q", path.Join(dirName, name))
					}

					// In-progress or crash-orphaned temp writes; never index them.
					if strings.HasSuffix(name, ".tmp") {
						continue
					}

					fields := strings.Split(name, "/")
					file := fields[len(fields)-1]

					// Strip the legacy ".v1" (uncompressed CAS) suffix first.
					legacy := strings.HasSuffix(file, ".v1")
					base := strings.TrimSuffix(file, ".v1")

					parts := strings.Split(base, "-")
					hash := parts[0]
					if !hashRe.MatchString(hash) {
						return fmt.Errorf("unrecognized file: %q", path.Join(dirName, name))
					}

					// Change A: skip the per-file de.Info() stat when the compressed-CAS
					// filename encodes the size (<hash>-<size> or <hash>-<size>-<random>)
					// AND we're in shared_storage_mode (per-pod LRU atime ordering is
					// irrelevant because the leader is the sole GC authority). On WekaFS
					// this eliminates ~30M stat syscalls per boot on production caches.
					// sizeOnDisk here becomes the logical (uncompressed) size — an
					// approximation vs. actual on-disk bytes, acceptable in shared mode
					// where per-pod byte accounting is not authoritative for GC.
					skipStat := c.sharedStorageMode &&
						lookupKeyPrefix == "cas/" &&
						!legacy &&
						(len(parts) == 2 || len(parts) == 3)

					item[n] = &item_values[n]
					metadata[n] = &metadata_values[n]
					metadata[n].lookupKey = lookupKeyPrefix + hash
					item[n].random = ""
					item[n].legacy = legacy

					if skipStat {
						parsedSize, perr := strconv.ParseInt(parts[1], 10, 64)
						if perr != nil {
							return fmt.Errorf("failed to parse size from %q: %w",
								path.Join(dirName, name), perr)
						}
						item[n].size = parsedSize
						item[n].sizeOnDisk = parsedSize
						if len(parts) == 3 {
							item[n].random = parts[2]
						}
						// metadata[n].ts stays at zero value — unused in shared mode.
					} else {
						info, err := de.Info()
						if err != nil {
							return fmt.Errorf("failed to get file info for %q: %w", path.Join(dirName, name), err)
						}

						item[n].sizeOnDisk = info.Size()
						item[n].size = info.Size()

						if lookupKeyPrefix == "cas/" && !legacy {
							// compressed CAS: <hash>-<size> or <hash>-<size>-<random>;
							// a bare <hash> is a deterministic uncompressed (legacy) blob.
							switch len(parts) {
							case 1:
								item[n].legacy = true
							case 2:
								item[n].size, err = strconv.ParseInt(parts[1], 10, 64)
							case 3:
								item[n].size, err = strconv.ParseInt(parts[1], 10, 64)
								item[n].random = parts[2]
							default:
								return fmt.Errorf("unrecognized file: %q", path.Join(dirName, name))
							}
							if err != nil {
								return fmt.Errorf("failed to parse size from %q: %w",
									path.Join(dirName, name), err)
							}
						} else {
							// AC, RAW, or legacy uncompressed CAS:
							// <hash> (deterministic) or <hash>-<random>.
							switch len(parts) {
							case 1:
								// deterministic; random stays "".
							case 2:
								item[n].random = parts[1]
							default:
								return fmt.Errorf("unrecognized file: %q", path.Join(dirName, name))
							}
						}

						metadata[n].ts = atime.Get(info)
					}

					n++
				}

				scanResults <- scanResult{
					item:     item[:n],
					metadata: metadata[:n],
				}
			}

			return nil
		})
	}

	des, err := os.ReadDir(c.dir)
	if err != nil {
		return scanResult{}, fmt.Errorf("failed to read cache dir %q: %w", c.dir, err)
	}

	dre := regexp.MustCompile(`^[a-f0-9]{2}$`)

	for _, de := range des {
		name := de.Name()

		if !de.IsDir() {
			if strings.ToLower(name) == lowercaseDSStoreFile {
				continue
			}

			return scanResult{}, fmt.Errorf("unexpected file: %s", name)
		}

		if name == lostAndFound {
			continue
		}

		if name != "ac.v2" && name != "cas.v2" && name != "raw.v2" {
			return scanResult{}, fmt.Errorf("unexpected dir: %s", name)
		}

		dir := path.Join(c.dir, name)
		des2, err := os.ReadDir(dir)
		if err != nil {
			return scanResult{}, err
		}

		for _, de2 := range des2 {
			name2 := de2.Name()

			dirPath := path.Join(name, name2)

			if !de2.IsDir() {
				if strings.ToLower(name2) == lowercaseDSStoreFile {
					continue
				}

				return scanResult{}, fmt.Errorf("unexpected file: %s", dirPath)
			}

			if name2 == lostAndFound {
				continue
			}

			if !dre.MatchString(name2) {
				return scanResult{}, fmt.Errorf("unexpected dir: %s", dirPath)
			}

			dc <- dirPath
		}
	}

	close(dc) // Ensure that the workers exit their range loop.
	dcClosed = true

	err = dirListers.Wait()
	if err != nil {
		return scanResult{}, err
	}
	close(scanResults)
	scanResultsClosed = true

	<-received

	return finalScanResult, nil
}

// loadExistingFiles lists all files in the cache directory, and adds them to the
// LRU index so that they can be served. Files are sorted by access time first,
// so that the eviction behavior is preserved across server restarts.
func (c *diskCache) loadExistingFiles(maxSizeBytes int64, cc CacheConfig) error {
	log.Printf("Loading existing files in %s.\n", c.dir)

	result, err := c.scanDir()
	if err != nil {
		log.Printf("Failed to scan cache dir: %s", err.Error())
		return err
	}

	log.Println("Sorting cache files by atime.")
	sort.Sort(result)

	// The eviction callback deletes the file from disk.
	// This function is only called while the lock is not held
	// by the current goroutine.
	onEvict := func(key string, value lruItem) {
		// In shared storage mode, don't delete files during LRU eviction.
		// This includes both normal eviction AND overwrite eviction.
		// The leader's periodic GC (atime-based) is the only thing that
		// should delete files, to avoid race conditions where one pod
		// deletes a file that another pod has in its LRU.
		if c.sharedStorageMode {
			return
		}
		f := c.getElementPath(key, value)
		c.removeFile(f)
	}

	log.Println("Building LRU index.")

	c.lru = NewSizedLRU(maxSizeBytes, onEvict, len(result.item))

	if c.sharedStorageMode {
		// Refresh a blob's atime at most once per quarter of the GC grace, so an
		// actively-accessed blob's atime never ages into the eviction set (worst
		// case ~grace/2) while avoiding a setattr on every access.
		c.lru.atimeRefreshInterval = c.gcMinAge() / 4
	}

	log.Printf("Will evict at max size: %.2f GB", bytesToGigaBytes(maxSizeBytes))

	if cc.maxSizeHardLimit > 0 {
		// Only set and print if optional limit is enabled.
		c.lru.maxSizeHardLimit = cc.maxSizeHardLimit
		log.Printf("Will reject requests at hard limit: %.2f GB",
			bytesToGigaBytes(c.lru.maxSizeHardLimit))
	}

	// Start one single goroutine running in background, continuously
	// waiting for files to be removed and removing them. Benchmarks on
	// Linux with the XFS file system have surprisingly shown that removal
	// sequentially with a single goroutine is much faster than starting
	// separate go routines for each file and removing them in parallel
	// despite SSDs with high IOPS performance. Benchmarks have also shown
	// that the single background goroutine is still slightly faster even
	// if the parallel goroutines would be serialized with a semaphore.
	// Sequentially evicting all files helps ensure that Go's default
	// limit of 10,000 operating system threads is not reached. Otherwise,
	// the number of concurrent removals could explode when a large new
	// file suddenly evicts thousands of old small files.
	go c.lru.performQueuedEvictionsContinuously()

	for i := 0; i < len(result.item); i++ {
		ok := c.lru.Add(result.metadata[i].lookupKey, *result.item[i])
		if !ok {
			// Add only refuses an entry larger than the whole cache. The
			// previous code tried to delete it by joining the cache dir with
			// the LOOKUP KEY ("cas/<hash>"), which is not a filesystem path --
			// so the remove always failed with ENOENT, the error propagated,
			// and New() returned it, meaning the server refused to start over a
			// single oversized blob.
			//
			// Log and skip instead: the blob simply stays on disk unindexed,
			// and the leader's GC will reclaim it on age like anything else.
			log.Printf("Warning: cache item %q does not fit in the cache (size %d, max %d); leaving it on disk unindexed",
				result.metadata[i].lookupKey, result.item[i].sizeOnDisk, c.lru.MaxSize())
			continue
		}
	}

	if c.lru.queuedEvictionsSize.Load() > 0 {
		// We were either restarted with a lower cache size, or there is still
		// a backlog of blobs to be removed. Wait for them to be removed before
		// accepting connections, to avoid confusing error logs.

		log.Println("Waiting for blob eviction backlog to complete.")

		for c.lru.queuedEvictionsSize.Load() > 0 {
			time.Sleep(200 * time.Millisecond)
		}

		log.Println("Finished waiting for blob evictions.")
	}

	log.Println("Finished loading disk cache files.")

	// Start the shared storage leader GC loop if enabled
	if c.sharedStorageLeader {
		go c.runSharedStorageLeaderGC(maxSizeBytes)
	}

	return nil
}

func bytesToGigaBytes(bytes int64) float64 {
	return float64(bytes) / (1024.0 * 1024.0 * 1024.0)
}

// fileWithAtime holds file path and access time for sorting during GC
type fileWithAtime struct {
	path  string
	size  int64
	atime time.Time
	kind  cache.EntryKind
	hash  string
}

// runSharedStorageLeaderGC runs the garbage collection loop for shared storage leader mode.
// It periodically scans the filesystem and evicts files based on atime when over the size limit.
func (c *diskCache) runSharedStorageLeaderGC(maxSizeBytes int64) {
	if !c.sharedStorageLeader {
		return
	}

	interval := c.gcInterval()

	// The GC target is resolved per cycle rather than captured here, so it
	// always reflects the configured value. maxSizeBytes (the LRU capacity) is
	// logged alongside it because the relationship between the two is the thing
	// operators need to see: an index smaller than the filesystem cannot serve
	// everything the filesystem holds.
	target := c.gcTargetBytes()
	log.Printf("Starting shared storage leader GC loop (interval: %v, min age: %v, gc target: %.2f GB, index capacity: %.2f GB)",
		interval, c.gcMinAge(), bytesToGigaBytes(target), bytesToGigaBytes(maxSizeBytes))
	if target >= maxSizeBytes {
		log.Printf("Note: GC target (%.2f GB) is not below the index capacity (%.2f GB), so whenever GC has work to do this replica's index cannot cover the whole filesystem. Set shared_storage_gc_max_size below max_size to separate them.",
			bytesToGigaBytes(target), bytesToGigaBytes(maxSizeBytes))
	}

	ticker := time.NewTicker(interval)
	for range ticker.C {
		c.runGCCycle(c.gcTargetBytes())
	}
}

// gcMinAge returns the minimum blob age that is protected from eviction.
// Blobs whose access time is younger than this are never deleted, so a blob
// still referenced by an in-flight build (whose reference is not visible to
// the leader) is not evicted out from under it. Floored at 1h to safely
// exceed both the longest build and WEKA's atime propagation lag.
func (c *diskCache) gcMinAge() time.Duration {
	minAge := c.sharedStorageGCMinAge
	if minAge < time.Hour {
		minAge = time.Hour
	}
	return minAge
}

// gcACRefMaxAge returns the max age for AC entries to shield their CAS
// references from GC (Change F, iter 1). Zero disables the age cap (legacy
// behavior: every live AC entry shields its CAS references indefinitely).
// A non-zero value is floored at 1h to avoid pathological configurations.
// The intent is to prevent very old AC entries from pinning CAS blobs
// forever, so aged content can actually drain from the cache.
func (c *diskCache) gcACRefMaxAge() time.Duration {
	d := c.sharedStorageACRefMaxAge
	if d <= 0 {
		return 0
	}
	if d < time.Hour {
		return time.Hour
	}
	return d
}

// publishInventoryAges converts a completed GC scan into the exported age
// distribution. The scan already holds each file's atime, so this costs no
// extra syscalls -- only arithmetic over a slice that is already resident.
func (c *diskCache) publishInventoryAges(files []fileWithAtime) {
	now := time.Now()
	acc := newInventoryAccumulator()
	for _, f := range files {
		age := now.Sub(f.atime).Seconds()
		if age < 0 {
			// Clock skew between replicas on the shared filesystem, or a blob
			// whose atime was just refreshed. Treat as brand new.
			age = 0
		}
		acc[kindLabel(f.kind)].observe(age, f.size)
	}
	c.gcMetrics.inventory.publish(acc, now)
}

// recordGCOverrun flags cycles that ran longer than the configured interval.
// time.Ticker drops ticks silently in that case, so without this a GC that can
// no longer keep up looks identical to one running on schedule.
func (c *diskCache) recordGCOverrun(d time.Duration) {
	if interval := c.gcInterval(); interval > 0 && d > interval {
		c.gcMetrics.counterOverruns.Inc()
	}
}

// runGCCycle performs one garbage collection cycle
func (c *diskCache) runGCCycle(maxSizeBytes int64) {
	cycleStart := time.Now()

	// Scan filesystem to get all files with their sizes and atimes
	scanStart := time.Now()
	files, totalSize, err := c.scanFilesWithAtime()
	if err != nil {
		log.Printf("Warning: failed to scan cache directory for GC: %v", err)
		c.gcMetrics.counterCycles.WithLabelValues("scan_error").Inc()
		return
	}
	scanSeconds := time.Since(scanStart).Seconds()

	// Publish scan results before any early return. These describe the shared
	// filesystem rather than this replica's index, and they must be emitted on
	// every cycle -- including healthy under-target ones -- or the series goes
	// stale precisely when the cache is fine, and "GC is idle because there is
	// nothing to do" becomes indistinguishable from "GC stopped running".
	c.gcMetrics.gaugeScannedBytes.Set(float64(totalSize))
	c.gcMetrics.gaugeScannedFiles.Set(float64(len(files)))
	c.gcMetrics.gaugeTargetBytes.Set(float64(maxSizeBytes))
	c.gcMetrics.gaugePhaseSeconds.WithLabelValues(gcPhaseScan).Set(scanSeconds)
	c.publishInventoryAges(files)

	if totalSize <= maxSizeBytes {
		// Under limit, nothing to do
		c.gcMetrics.counterCycles.WithLabelValues("under_cap").Inc()
		c.gcMetrics.gaugeEvictableBytes.Set(0)
		c.gcMetrics.gaugeEvictableFiles.Set(0)
		c.gcMetrics.gaugePhaseSeconds.WithLabelValues(gcPhaseTotal).Set(time.Since(cycleStart).Seconds())
		c.recordGCOverrun(time.Since(cycleStart))
		return
	}

	bytesToFree := totalSize - maxSizeBytes
	acRefMaxAge := c.gcACRefMaxAge()
	log.Printf("Shared storage GC: current size %.2f GB exceeds limit %.2f GB, need to free %.2f GB (ac_ref_max_age=%v, min_age=%v)",
		bytesToGigaBytes(totalSize), bytesToGigaBytes(maxSizeBytes), bytesToGigaBytes(bytesToFree),
		acRefMaxAge, c.gcMinAge())

	// Build the set of CAS blobs referenced by live Action Cache entries.
	// These must never be evicted: as long as an action result is cached, its
	// outputs must remain, or a client that gets an AC hit will fail to fetch
	// the outputs mid-build. AC entries themselves are evictable (a missing AC
	// entry is just a cache miss that re-runs the action), so orphaned outputs
	// become collectable on a later cycle once their AC entry is gone.
	// Change F: AC entries older than acRefMaxAge do not shield their CAS.
	collectStart := time.Now()
	referenced := c.collectACReferencedCASHashes()
	c.gcMetrics.gaugePhaseSeconds.WithLabelValues(gcPhaseCollectAC).Set(time.Since(collectStart).Seconds())
	c.gcMetrics.gaugeACRefHashes.Set(float64(len(referenced)))
	log.Printf("Shared storage GC: AC-referenced CAS set contains %d hashes", len(referenced))

	// Sort files by atime (oldest first)
	sort.Slice(files, func(i, j int) bool {
		return files[i].atime.Before(files[j].atime)
	})

	// Never evict blobs accessed more recently than the min-age grace window.
	// This protects blobs still referenced by in-flight builds, whose input
	// references are not visible to the leader.
	cutoff := time.Now().Add(-c.gcMinAge())

	// Change D: parallelize os.Remove via a bounded worker pool. Serial unlink
	// on WekaFS at ~1-3 ms/op means multi-hour delete phases for TB-scale
	// eviction; parallel pool at N=16 keeps instantaneous metadata pressure
	// bounded while cutting wall-clock ~10×.
	numDeleters := c.gcWorkers()

	deleteStart := time.Now()

	type deleteJob struct {
		path string
		size int64
	}
	jobs := make(chan deleteJob, numDeleters*2)
	var actualFreed atomic.Int64
	var actualDeleted atomic.Int64
	var deleteErrs atomic.Int64

	var deleterWG sync.WaitGroup
	for i := 0; i < numDeleters; i++ {
		deleterWG.Add(1)
		go func() {
			defer deleterWG.Done()
			pending := make([]string, 0, gcLRURemovalBatch)
			defer func() { c.flushLRURemovals(pending) }()

			for j := range jobs {
				if key, ok := c.lruKeyForPath(j.path); ok {
					pending = append(pending, key)
					if len(pending) >= gcLRURemovalBatch {
						c.flushLRURemovals(pending)
						pending = pending[:0]
					}
				}
				if err := os.Remove(j.path); err != nil {
					if !os.IsNotExist(err) {
						log.Printf("Warning: failed to remove file during GC: %s: %v", j.path, err)
						deleteErrs.Add(1)
					}
					continue
				}
				actualFreed.Add(j.size)
				actualDeleted.Add(1)
			}
		}()
	}

	// Feeder: walks pre-sorted files in-order and queues eligible ones for
	// deletion until the running expected-freed counter meets bytesToFree.
	// Running the feeder here (not in a worker) keeps protection accounting
	// serial and avoids over-deletion beyond bytesToFree + a small in-flight
	// margin of ~numDeleters * avgFileSize.
	var expectedFreed int64
	var ageProtBytes int64
	var ageProtCount int
	var refProtBytes int64
	var refProtCount int
	var evictableBytes int64
	var evictableCount int
	// Classify every file, but stop queueing deletions once the target is met.
	//
	// The classification used to stop at the same point as the feeding, which
	// made the protection totals "whatever was seen before we had freed
	// enough" rather than actual totals -- so the shortfall warning below (and
	// any metric derived from it) understated how much was protected, by an
	// amount that varied with how early the loop happened to break. The extra
	// work is an in-memory pass over a slice already resident, and it yields
	// the evictable totals, which are what reveal a GC that cannot make
	// progress because retention is protecting everything.
	feeding := true
	for _, f := range files {
		// Blobs younger than the grace window are protected from eviction.
		if f.atime.After(cutoff) {
			ageProtBytes += f.size
			ageProtCount++
			continue
		}

		// CAS blobs referenced by a (fresh-enough, per Change F) AC entry
		// are protected regardless of age.
		if f.kind == cache.CAS && f.hash != "" {
			if _, ok := referenced[f.hash]; ok {
				refProtBytes += f.size
				refProtCount++
				continue
			}
		}

		evictableBytes += f.size
		evictableCount++

		if feeding {
			jobs <- deleteJob{path: f.path, size: f.size}
			expectedFreed += f.size
			if expectedFreed >= bytesToFree {
				feeding = false
			}
		}
	}
	close(jobs)
	deleterWG.Wait()

	freed := actualFreed.Load()
	deletedCount := actualDeleted.Load()

	c.gcMetrics.gaugePhaseSeconds.WithLabelValues(gcPhaseDelete).Set(time.Since(deleteStart).Seconds())
	c.gcMetrics.counterBytesFreed.Add(float64(freed))
	c.gcMetrics.counterFilesDelete.Add(float64(deletedCount))
	c.gcMetrics.counterDeleteErrs.Add(float64(deleteErrs.Load()))
	c.gcMetrics.counterCycles.WithLabelValues("freed").Inc()
	c.gcMetrics.gaugeEvictableBytes.Set(float64(evictableBytes))
	c.gcMetrics.gaugeEvictableFiles.Set(float64(evictableCount))
	c.gcMetrics.gaugeProtectedBytes.WithLabelValues(gcReasonMinAge).Set(float64(ageProtBytes))
	c.gcMetrics.gaugeProtectedFiles.WithLabelValues(gcReasonMinAge).Set(float64(ageProtCount))
	c.gcMetrics.gaugeProtectedBytes.WithLabelValues(gcReasonACRef).Set(float64(refProtBytes))
	c.gcMetrics.gaugeProtectedFiles.WithLabelValues(gcReasonACRef).Set(float64(refProtCount))
	c.gcMetrics.gaugePhaseSeconds.WithLabelValues(gcPhaseTotal).Set(time.Since(cycleStart).Seconds())
	c.recordGCOverrun(time.Since(cycleStart))

	log.Printf("Shared storage GC: freed %.2f GB by deleting %d files (protected: %d AC-referenced, %d within min-age)",
		bytesToGigaBytes(freed), deletedCount, refProtCount, ageProtCount)

	if freed < bytesToFree {
		log.Printf("Warning: shared storage GC could not free enough space: %.2f GB still over limit "+
			"(%.2f GB AC-referenced, %.2f GB within %v min-age are protected). Consider raising max size, lowering shared_storage_ac_ref_max_age (currently %v), or lowering shared_storage_gc_min_age.",
			bytesToGigaBytes(bytesToFree-freed), bytesToGigaBytes(refProtBytes), bytesToGigaBytes(ageProtBytes), c.gcMinAge(), acRefMaxAge)
	}
}

// collectACReferencedCASHashes scans the Action Cache and returns the set of
// CAS blob hashes that are reachable from a cached ActionResult: its output
// files, the tree blob (and the files within it) for each output directory,
// and the stdout/stderr blobs. GC must not evict any of these while the
// referencing AC entry exists.
//
// Change E (iter 1): worker pool over ac.v2 shard directories parallelizes
// the AC read+unmarshal phase and a second bounded pool parallelizes the
// tree-Get phase. Serial filepath.Walk on production-scale AC (~10M entries
// on rnd) was multi-hour; parallel workers cut it to minutes.
//
// Change F (iter 1): if sharedStorageACRefMaxAge > 0, AC entries whose file
// mtime is older than that threshold are skipped entirely — their CAS
// references are NOT added to the returned set. This unpins ancient CAS
// blobs that were being shielded by long-cold AC entries.
func (c *diskCache) collectACReferencedCASHashes() map[string]struct{} {
	referenced := make(map[string]struct{})
	acDir := filepath.Join(c.dir, "ac.v2")

	numWorkers := c.gcWorkers()

	// Change F: cutoff for age-cap. Zero-value time means "no cap".
	var acCutoff time.Time
	if maxAge := c.gcACRefMaxAge(); maxAge > 0 {
		acCutoff = time.Now().Add(-maxAge)
	}

	type shardResult struct {
		refs         map[string]struct{}
		treeDigests  []*pb.Digest
		skippedByAge int
		unparseable  int
	}

	shardJobs := make(chan string, numWorkers)
	results := make(chan shardResult, numWorkers)

	var allTreeDigests []*pb.Digest
	var skippedByAgeTotal int
	var unparseableTotal int
	receiverDone := make(chan struct{})
	go func() {
		for sr := range results {
			for h := range sr.refs {
				referenced[h] = struct{}{}
			}
			allTreeDigests = append(allTreeDigests, sr.treeDigests...)
			skippedByAgeTotal += sr.skippedByAge
			unparseableTotal += sr.unparseable
		}
		close(receiverDone)
	}()

	var acWorkers sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		acWorkers.Add(1)
		go func() {
			defer acWorkers.Done()
			for shardDir := range shardJobs {
				sr := shardResult{refs: make(map[string]struct{})}
				des, err := os.ReadDir(shardDir)
				if err != nil {
					log.Printf("Warning: collectACReferencedCASHashes: read shard %q: %v", shardDir, err)
					results <- sr
					continue
				}
				for _, de := range des {
					if de.IsDir() {
						continue
					}
					p := filepath.Join(shardDir, de.Name())

					// Change F: skip AC entries older than the cap.
					if !acCutoff.IsZero() {
						info, ierr := de.Info()
						if ierr != nil {
							continue // deleted between ReadDir and Info
						}
						if info.ModTime().Before(acCutoff) {
							sr.skippedByAge++
							continue
						}
					}

					data, err := os.ReadFile(p)
					if err != nil {
						continue
					}
					ar := &pb.ActionResult{}
					if err := proto.Unmarshal(data, ar); err != nil {
						sr.unparseable++
						continue
					}
					for _, of := range ar.OutputFiles {
						if of.Digest != nil {
							sr.refs[of.Digest.Hash] = struct{}{}
						}
					}
					for _, d := range ar.OutputDirectories {
						if d.TreeDigest != nil {
							sr.refs[d.TreeDigest.Hash] = struct{}{}
							sr.treeDigests = append(sr.treeDigests, d.TreeDigest)
						}
					}
					if ar.StdoutDigest != nil {
						sr.refs[ar.StdoutDigest.Hash] = struct{}{}
					}
					if ar.StderrDigest != nil {
						sr.refs[ar.StderrDigest.Hash] = struct{}{}
					}
				}
				results <- sr
			}
		}()
	}

	// Feed shard directories.
	shards, err := os.ReadDir(acDir)
	if err != nil {
		log.Printf("Warning: collectACReferencedCASHashes: read %q: %v", acDir, err)
	} else {
		for _, shard := range shards {
			if !shard.IsDir() {
				continue
			}
			shardJobs <- filepath.Join(acDir, shard.Name())
		}
	}
	close(shardJobs)

	acWorkers.Wait()
	close(results)
	<-receiverDone

	if skippedByAgeTotal > 0 || unparseableTotal > 0 {
		log.Printf("collectACReferencedCASHashes: %d AC entries skipped by age cap, %d unparseable",
			skippedByAgeTotal, unparseableTotal)
	}

	// Deduplicate tree digests before the tree-walk phase — many AC entries
	// share the same OutputDirectory Tree, and c.Get + decompress per tree is
	// expensive.
	seenTrees := make(map[string]struct{}, len(allTreeDigests))
	dedupedTrees := allTreeDigests[:0]
	for _, td := range allTreeDigests {
		if td == nil {
			continue
		}
		if _, ok := seenTrees[td.Hash]; ok {
			continue
		}
		seenTrees[td.Hash] = struct{}{}
		dedupedTrees = append(dedupedTrees, td)
	}

	if len(dedupedTrees) == 0 {
		return referenced
	}

	treeJobs := make(chan *pb.Digest, numWorkers)
	var treeMu sync.Mutex
	var treeWorkers sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		treeWorkers.Add(1)
		go func() {
			defer treeWorkers.Done()
			localRefs := make(map[string]struct{})
			for td := range treeJobs {
				c.markTreeFiles(td, localRefs)
			}
			if len(localRefs) == 0 {
				return
			}
			treeMu.Lock()
			for h := range localRefs {
				referenced[h] = struct{}{}
			}
			treeMu.Unlock()
		}()
	}
	for _, td := range dedupedTrees {
		treeJobs <- td
	}
	close(treeJobs)
	treeWorkers.Wait()

	return referenced
}

// markTreeFiles reads the CAS tree blob identified by treeDigest and adds the
// hashes of all files it contains to the referenced set. Best effort: if the
// tree blob is unavailable or unparseable, its file references are skipped.
func (c *diskCache) markTreeFiles(treeDigest *pb.Digest, referenced map[string]struct{}) {
	r, _, err := c.Get(context.Background(), cache.CAS, treeDigest.Hash, treeDigest.SizeBytes, 0)
	if r == nil || err != nil {
		if r != nil {
			_ = r.Close()
		}
		return
	}
	defer func() { _ = r.Close() }()

	data, err := io.ReadAll(r)
	if err != nil {
		return
	}

	tree := &pb.Tree{}
	if err := proto.Unmarshal(data, tree); err != nil {
		return
	}

	for _, f := range tree.Root.GetFiles() {
		if f.Digest != nil {
			referenced[f.Digest.Hash] = struct{}{}
		}
	}
	for _, child := range tree.GetChildren() {
		for _, f := range child.GetFiles() {
			if f.Digest != nil {
				referenced[f.Digest.Hash] = struct{}{}
			}
		}
	}
}

// scanFilesWithAtime scans the cache directory and returns all files with their atimes.
//
// Change C (iter 1): worker pool over <kind>.v2/<xx> shard directories, mirroring
// scanDir's parallelism pattern. Each worker os.ReadDir's a shard and stats each
// entry to capture size + atime (both required by runGCCycle — atime is the sort
// key and the min-age cutoff input; size is the eviction accounting). Result
// ordering doesn't matter — runGCCycle does its own sort.Slice.
//
// Note: unlike scanDir's Change A, we cannot skip the per-file stat here — atime
// is load-bearing for GC eviction ordering and the grace-window cutoff.
func (c *diskCache) scanFilesWithAtime() ([]fileWithAtime, int64, error) {
	numWorkers := c.gcWorkers()

	type shardJob struct {
		dir  string
		kind cache.EntryKind
	}

	jobs := make(chan shardJob, numWorkers)
	results := make(chan []fileWithAtime, numWorkers)

	var totalSize atomic.Int64

	var finalFiles []fileWithAtime
	receiverDone := make(chan struct{})
	go func() {
		for batch := range results {
			finalFiles = append(finalFiles, batch...)
		}
		close(receiverDone)
	}()

	workers := new(errgroup.Group)
	for i := 0; i < numWorkers; i++ {
		workers.Go(func() error {
			for job := range jobs {
				des, err := os.ReadDir(job.dir)
				if err != nil {
					return fmt.Errorf("scanFilesWithAtime: read shard %q: %w", job.dir, err)
				}
				batch := make([]fileWithAtime, 0, len(des))
				var batchSize int64
				for _, de := range des {
					if de.IsDir() {
						continue
					}
					info, err := de.Info()
					if err != nil {
						// File may have been unlinked between ReadDir and Info
						// (e.g., by a concurrent request Put's overwrite path).
						// Skip silently — matches the prior filepath.Walk
						// behavior which returned nil on walk errors.
						if os.IsNotExist(err) {
							continue
						}
						return fmt.Errorf("scanFilesWithAtime: stat %q: %w", filepath.Join(job.dir, de.Name()), err)
					}
					name := de.Name()
					full := filepath.Join(job.dir, name)

					// Slice the hash out of `full`, not out of `name`.
					//
					// A Go string slice shares its backing array with the
					// original, so `name[:64]` keeps the whole de.Name()
					// allocation alive for as long as the entry is retained.
					// The scan holds every entry until the cycle ends, so at
					// production scale (tens of millions of files) that pins an
					// extra ~75 bytes per file -- gigabytes of otherwise dead
					// string data, on a process whose peak RSS is already the
					// binding constraint. `full` is retained regardless, so
					// slicing from it costs nothing extra.
					hash := ""
					if len(name) >= sha256HashStrSize {
						start := len(full) - len(name)
						hash = full[start : start+sha256HashStrSize]
					}
					batch = append(batch, fileWithAtime{
						path:  full,
						size:  info.Size(),
						atime: atime.Get(info),
						kind:  job.kind,
						hash:  hash,
					})
					batchSize += info.Size()
				}
				totalSize.Add(batchSize)
				results <- batch
			}
			return nil
		})
	}

	// Feeder: enumerate <kind>.v2/<xx> shard directories and enqueue. Baseline
	// shard-level topology matches scanDir's expectations (2-level layout);
	// unexpected nesting under a shard is not walked here — see scanDir for
	// strict validation of the on-disk layout at boot.
	var feedErr error
	for _, kindDir := range []string{"ac.v2", "cas.v2", "raw.v2"} {
		var kind cache.EntryKind
		switch kindDir {
		case "ac.v2":
			kind = cache.AC
		case "cas.v2":
			kind = cache.CAS
		default:
			kind = cache.RAW
		}
		baseDir := filepath.Join(c.dir, kindDir)
		shards, err := os.ReadDir(baseDir)
		if err != nil {
			feedErr = fmt.Errorf("scanFilesWithAtime: read %q: %w", baseDir, err)
			break
		}
		for _, shard := range shards {
			if !shard.IsDir() {
				continue
			}
			jobs <- shardJob{dir: filepath.Join(baseDir, shard.Name()), kind: kind}
		}
	}
	close(jobs)

	workerErr := workers.Wait()
	close(results)
	<-receiverDone

	if feedErr != nil {
		return nil, 0, feedErr
	}
	if workerErr != nil {
		return nil, 0, workerErr
	}
	return finalFiles, totalSize.Load(), nil
}

// removeFromLRUByPath attempts to remove a file from the LRU by its path
// lruKeyForPath derives the index key for a file the GC is deleting. Pure
// parsing, no locking, so callers can batch the locked part.
//
// Path format: <dir>/<kind>.v2/<prefix>/<hash>[-<size>][-<random>]
func (c *diskCache) lruKeyForPath(filePath string) (string, bool) {
	rel, err := filepath.Rel(c.dir, filePath)
	if err != nil {
		return "", false
	}

	parts := strings.Split(rel, string(filepath.Separator))
	if len(parts) < 3 {
		return "", false
	}

	kindDir := parts[0]
	filename := parts[2]

	var kind cache.EntryKind
	switch kindDir {
	case "ac.v2":
		kind = cache.AC
	case "cas.v2":
		kind = cache.CAS
	case "raw.v2":
		kind = cache.RAW
	default:
		return "", false
	}

	// Extract hash from filename (first 64 hex chars)
	if len(filename) < sha256HashStrSize {
		return "", false
	}

	return cache.LookupKey(kind, filename[:sha256HashStrSize]), true
}

// gcLRURemovalBatch is how many index keys a GC delete worker accumulates
// before taking the cache mutex to drop them.
//
// c.mu is the global cache lock, contended by every request. Removing entries
// one at a time meant one acquisition per deleted file: a production cycle has
// deleted 7.28M files, so that is 7.28M acquisitions by 16 workers, serialised
// against live traffic. Batching trades a bounded amount of index staleness
// (at most this many entries still naming files already unlinked, briefly) for
// three orders of magnitude fewer acquisitions. The stale window is safe --
// Contains and FindMissingBlobs both verify against the filesystem before
// trusting an index hit.
const gcLRURemovalBatch = 1000

// flushLRURemovals drops a batch of index entries under a single lock.
//
// NoEvict throughout: the GC unlinks these files itself, so queueing an
// eviction would ask onEvict to remove a file that is already gone, and would
// add their bytes to counterEvictedBytes and queuedEvictionsSize.
// queuedEvictionsSize feeds calcTotalDiskSizeAndUpdatePeak, so at GC scale that
// inflates bazel_remote_disk_cache_size_bytes by the entire deleted volume and,
// with max_size_hard_limit configured, would make Reserve reject writes for the
// duration of every GC cycle.
func (c *diskCache) flushLRURemovals(keys []string) {
	if len(keys) == 0 {
		return
	}
	c.mu.Lock()
	for _, k := range keys {
		c.lru.RemoveKeyNoEvict(k)
	}
	c.mu.Unlock()
}
