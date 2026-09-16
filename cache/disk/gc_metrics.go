package disk

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/buchgr/bazel-remote/v2/cache"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics for the fork's leader-side shared-storage GC.
//
// These exist because the GC's own numbers were previously available only as
// log lines, which meant no alerting and no history. The specific condition
// that motivated them: on 2026-09-14 the leader's GC had been freeing ~0 bytes
// per cycle for days because every remaining file was inside the min-age
// window, leaving the cache permanently over its target. Nothing distinguished
// that from a healthy idle cache -- both simply logged and returned. With
// gaugeGCScannedBytes and counterGCBytesFreed the condition is directly
// alertable:
//
//	shared_gc_scanned_bytes > shared_gc_target_bytes
//	  AND rate(shared_gc_bytes_freed_total[1h]) == 0
//
// All of these are leader-only. Followers never run a GC cycle, so registering
// them everywhere would export permanent zeros and poison any sum() across
// pods. Dashboards should aggregate with max(), not sum().

// inventoryAgeBucketsHours are the upper bounds, in hours, of the cache
// inventory age histogram.
//
// Boundaries are placed to answer the retention question directly: "if min_age
// were R, how many bytes would become evictable?" 336h and 720h are included
// because they are the values actually under consideration, so the answer can
// be read straight off the chart rather than interpolated.
//
// Nothing finer than an hour is useful. The fork refreshes atime explicitly via
// Chtimes for hot blobs, but cold blobs on a relatime mount only advance once
// per 24h, so sub-hour precision would be noise.
var inventoryAgeBucketsHours = []float64{1, 6, 24, 72, 168, 336, 720, 1440, 2160}

// inventoryKindStats is the age distribution for one entry kind, expressed as
// cumulative-at-or-below counts per bucket bound (Prometheus histogram
// convention).
type inventoryKindStats struct {
	// files[i] / bytes[i] count everything with age <= inventoryAgeBucketsHours[i].
	files []uint64
	bytes []uint64

	totalFiles uint64
	totalBytes uint64

	// Sum of ages in seconds, and of age*bytes, for the histogram _sum fields.
	sumAgeSeconds      float64
	sumAgeSecondsBytes float64
}

// inventorySnapshot is the result of one GC scan, published atomically so that
// scrapes always observe a complete and self-consistent distribution.
type inventorySnapshot struct {
	at    time.Time
	kinds map[string]*inventoryKindStats
}

// newInventoryAccumulator returns a zeroed per-kind accumulator.
func newInventoryAccumulator() map[string]*inventoryKindStats {
	m := make(map[string]*inventoryKindStats, 3)
	for _, k := range []string{casKind, acKind, rawKind} {
		m[k] = &inventoryKindStats{
			files: make([]uint64, len(inventoryAgeBucketsHours)),
			bytes: make([]uint64, len(inventoryAgeBucketsHours)),
		}
	}
	return m
}

// observe records one file into the accumulator.
//
// This is deliberately plain arithmetic on an int slice rather than
// prometheus Observe(). A production scan is ~35M files per cycle; going
// through a Histogram would take a mutex 35M times, and -- more importantly --
// a Histogram is cumulative across cycles, so re-observing the whole inventory
// every cycle would produce "file-observations since process start" rather than
// a snapshot of what is currently on disk.
func (s *inventoryKindStats) observe(ageSeconds float64, size int64) {
	s.totalFiles++
	s.totalBytes += uint64(size)
	s.sumAgeSeconds += ageSeconds
	s.sumAgeSecondsBytes += ageSeconds * float64(size)

	ageHours := ageSeconds / 3600.0
	for i, bound := range inventoryAgeBucketsHours {
		if ageHours <= bound {
			s.files[i]++
			s.bytes[i] += uint64(size)
		}
	}
}

// bucketMap converts the cumulative slice into the map form MustNewConstHistogram wants.
func bucketMap(cumulative []uint64) map[float64]uint64 {
	m := make(map[float64]uint64, len(cumulative))
	for i, bound := range inventoryAgeBucketsHours {
		// Bounds are published in seconds so the metric is consistent with
		// every other duration in this exporter.
		m[bound*3600.0] = cumulative[i]
	}
	return m
}

// inventoryCollector publishes the most recent inventory snapshot.
//
// It is a custom Collector rather than a plain Histogram so the exported values
// are a point-in-time snapshot that can go down as well as up, without the
// scrape race that Reset()-then-refill would introduce.
type inventoryCollector struct {
	snapshot atomic.Pointer[inventorySnapshot]

	filesDesc *prometheus.Desc
	bytesDesc *prometheus.Desc
	ageDesc   *prometheus.Desc
}

func newInventoryCollector() *inventoryCollector {
	return &inventoryCollector{
		filesDesc: prometheus.NewDesc(
			"bazel_remote_disk_cache_inventory_age_files",
			"Distribution of cache entries on disk by idle age (now - atime), from the most recent leader GC scan. Leader-only; aggregate with max(), not sum().",
			[]string{"kind"}, nil),
		bytesDesc: prometheus.NewDesc(
			"bazel_remote_disk_cache_inventory_age_bytes",
			"Distribution of cache bytes on disk by idle age (now - atime), from the most recent leader GC scan. Observations are weighted by file size, so bucket values are cumulative bytes at or below each age bound. This is the series that answers 'how much would a given min_age free'. Leader-only; aggregate with max(), not sum().",
			[]string{"kind"}, nil),
		ageDesc: prometheus.NewDesc(
			"bazel_remote_disk_cache_inventory_scan_timestamp_seconds",
			"Unix timestamp of the GC scan that produced the current inventory age distribution. Use to detect a stale snapshot (e.g. GC wedged or not running).",
			nil, nil),
	}
}

func (i *inventoryCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- i.filesDesc
	ch <- i.bytesDesc
	ch <- i.ageDesc
}

func (i *inventoryCollector) Collect(ch chan<- prometheus.Metric) {
	snap := i.snapshot.Load()
	if snap == nil {
		// No scan has completed yet. Export nothing rather than zeros, so
		// "not measured" is distinguishable from "measured as empty".
		return
	}

	ch <- prometheus.MustNewConstMetric(i.ageDesc, prometheus.GaugeValue,
		float64(snap.at.Unix()))

	for kind, s := range snap.kinds {
		if s.totalFiles == 0 {
			continue
		}
		ch <- prometheus.MustNewConstHistogram(i.filesDesc,
			s.totalFiles, s.sumAgeSeconds, bucketMap(s.files), kind)
		ch <- prometheus.MustNewConstHistogram(i.bytesDesc,
			s.totalBytes, s.sumAgeSecondsBytes, bucketMap(s.bytes), kind)
	}
}

func (i *inventoryCollector) publish(kinds map[string]*inventoryKindStats, at time.Time) {
	i.snapshot.Store(&inventorySnapshot{at: at, kinds: kinds})
}

// gcMetrics holds the leader-side GC instrumentation.
type gcMetrics struct {
	registerOnce sync.Once

	// Cumulative work.
	counterCycles      *prometheus.CounterVec // result=freed|under_cap|scan_error
	counterBytesFreed  prometheus.Counter
	counterFilesDelete prometheus.Counter
	counterDeleteErrs  prometheus.Counter
	counterOverruns    prometheus.Counter

	// Last-cycle snapshots. Gauges rather than histograms: at one cycle every
	// few minutes a histogram is far too thin to alert on, and the question is
	// always "what did the last cycle do", not "what is the p99 cycle".
	gaugeScannedBytes   prometheus.Gauge
	gaugeScannedFiles   prometheus.Gauge
	gaugeTargetBytes    prometheus.Gauge
	gaugeEvictableBytes prometheus.Gauge
	gaugeEvictableFiles prometheus.Gauge
	gaugeProtectedBytes *prometheus.GaugeVec // reason=min_age|ac_referenced
	gaugeProtectedFiles *prometheus.GaugeVec
	gaugeACRefHashes    prometheus.Gauge
	gaugePhaseSeconds   *prometheus.GaugeVec // phase=scan|collect_ac|delete|total

	inventory *inventoryCollector
}

func newGCMetrics() *gcMetrics {
	return &gcMetrics{
		counterCycles: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "bazel_remote_shared_gc_cycles_total",
			Help: "Leader GC cycles by outcome. 'under_cap' means the scan found the cache within its target and nothing was attempted; 'scan_error' means the filesystem scan failed and the cycle did nothing. Distinguishing these was previously impossible -- both simply returned.",
		}, []string{"result"}),
		counterBytesFreed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "bazel_remote_shared_gc_bytes_freed_total",
			Help: "Total bytes actually unlinked by the leader GC, measured from the files it successfully removed.",
		}),
		counterFilesDelete: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "bazel_remote_shared_gc_files_deleted_total",
			Help: "Total files actually unlinked by the leader GC.",
		}),
		counterDeleteErrs: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "bazel_remote_shared_gc_delete_errors_total",
			Help: "Unlink failures during GC. Without this a GC that cannot delete looks identical to a GC with nothing to delete: both report zero bytes freed.",
		}),
		counterOverruns: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "bazel_remote_shared_gc_cycle_overruns_total",
			Help: "Cycles whose duration exceeded the configured GC interval. The ticker silently drops ticks in that case, so back-to-back cycles are otherwise invisible.",
		}),

		gaugeScannedBytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "bazel_remote_shared_gc_scanned_bytes",
			Help: "Total bytes on the shared filesystem as measured by the most recent GC scan. This is filesystem truth, unlike bazel_remote_disk_cache_size_bytes which reflects only this replica's in-memory index.",
		}),
		gaugeScannedFiles: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "bazel_remote_shared_gc_scanned_files",
			Help: "Total files on the shared filesystem as measured by the most recent GC scan.",
		}),
		gaugeTargetBytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "bazel_remote_shared_gc_target_bytes",
			Help: "Size the leader GC is currently trying to bring the shared filesystem down to.",
		}),
		gaugeEvictableBytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "bazel_remote_shared_gc_evictable_bytes",
			Help: "Bytes the most recent cycle found eligible for deletion: neither inside the min-age window nor shielded by a live AC entry. Zero while over target is the 'GC has run dry' signal -- retention is protecting everything and the cache cannot shrink.",
		}),
		gaugeEvictableFiles: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "bazel_remote_shared_gc_evictable_files",
			Help: "Files the most recent cycle found eligible for deletion.",
		}),
		gaugeProtectedBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "bazel_remote_shared_gc_protected_bytes",
			Help: "Bytes protected from deletion in the most recent cycle, by reason. Counted over the entire inventory, not merely the prefix examined before the free target was met.",
		}, []string{"reason"}),
		gaugeProtectedFiles: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "bazel_remote_shared_gc_protected_files",
			Help: "Files protected from deletion in the most recent cycle, by reason.",
		}, []string{"reason"}),
		gaugeACRefHashes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "bazel_remote_shared_gc_ac_referenced_hashes",
			Help: "Size of the CAS hash set reachable from live Action Cache entries in the most recent cycle.",
		}),
		gaugePhaseSeconds: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "bazel_remote_shared_gc_last_cycle_duration_seconds",
			Help: "Duration of each phase of the most recent GC cycle.",
		}, []string{"phase"}),

		inventory: newInventoryCollector(),
	}
}

// register exposes the GC metrics. Safe to call more than once; only the first
// call registers, so tests constructing many caches in one process do not panic
// on duplicate registration.
func (g *gcMetrics) register() {
	g.registerOnce.Do(func() {
		prometheus.MustRegister(
			g.counterCycles,
			g.counterBytesFreed,
			g.counterFilesDelete,
			g.counterDeleteErrs,
			g.counterOverruns,
			g.gaugeScannedBytes,
			g.gaugeScannedFiles,
			g.gaugeTargetBytes,
			g.gaugeEvictableBytes,
			g.gaugeEvictableFiles,
			g.gaugeProtectedBytes,
			g.gaugeProtectedFiles,
			g.gaugeACRefHashes,
			g.gaugePhaseSeconds,
			g.inventory,
		)
	})
}

// counterStaleIndexEntries counts index entries found to name a file that no
// longer exists on the shared filesystem.
//
// This measures how far this replica's index has drifted from disk, which is
// the root phenomenon behind both known failure modes: a stale entry on the
// find_missing path would (before verification was added) be reported to the
// client as present, causing it to skip an upload and fail later on fetch --
// the "lost-inputs (GC)" signature. Nobody previously knew how often that
// occurred, because the self-healing paths fixed it silently and the
// non-healing path failed the build somewhere else entirely.
//
// A rising rate here means the leader GC is deleting blobs this replica still
// believes in. Expect a spike after any large drain, e.g. following a
// shared_storage_gc_min_age reduction.
var registerStaleCounterOnce sync.Once

var counterStaleIndexEntries = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "bazel_remote_disk_cache_stale_index_entries_total",
	Help: "Index entries discovered to reference a file that no longer exists on disk, by the lookup path that found them. Non-zero means this replica's index has drifted from the shared filesystem, which in shared storage mode happens when the leader's GC deletes blobs this replica has indexed.",
}, []string{"path"})

const (
	stalePathFindMissing = "find_missing"
	stalePathContains    = "contains"
	stalePathCacheAge    = "cache_age_metric"

	gcReasonMinAge = "min_age"
	gcReasonACRef  = "ac_referenced"

	gcPhaseScan      = "scan"
	gcPhaseCollectAC = "collect_ac"
	gcPhaseDelete    = "delete"
	gcPhaseTotal     = "total"
)

// kindLabel maps an EntryKind to the label value used by the inventory metrics.
func kindLabel(k cache.EntryKind) string {
	switch k {
	case cache.CAS:
		return casKind
	case cache.AC:
		return acKind
	default:
		return rawKind
	}
}
