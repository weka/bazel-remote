package disk

import (
	"fmt"
	"log"
	"time"

	"github.com/buchgr/bazel-remote/v2/cache"
	"github.com/buchgr/bazel-remote/v2/cache/disk/casblob"
	"github.com/buchgr/bazel-remote/v2/cache/disk/zstdimpl"

	"github.com/prometheus/client_golang/prometheus"
)

type Option func(*CacheConfig) error

type CacheConfig struct {
	diskCache        *diskCache        // Assumed to be non-nil.
	metrics          *metricsDecorator // May be nil.
	maxSizeHardLimit int64
}

func WithStorageMode(mode string) Option {
	return func(c *CacheConfig) error {
		switch mode {
		case "zstd":
			c.diskCache.storageMode = casblob.Zstandard
			return nil
		case "uncompressed":
			c.diskCache.storageMode = casblob.Identity
			return nil
		default:
			return fmt.Errorf("unsupported storage mode: %s", mode)
		}
	}
}

func WithZstdImplementation(impl string) Option {
	return func(c *CacheConfig) error {
		var err error
		c.diskCache.zstd, err = zstdimpl.Get(impl)
		return err
	}
}

func WithMaxBlobSize(size int64) Option {
	return func(c *CacheConfig) error {
		if size <= 0 {
			return fmt.Errorf("invalid MaxBlobSize: %d", size)
		}

		c.diskCache.maxBlobSize = size
		return nil
	}
}

func WithProxyBackend(proxy cache.Proxy) Option {
	return func(c *CacheConfig) error {
		if c.diskCache.proxy != nil && proxy != nil {
			return fmt.Errorf("proxy backends may be set only once")
		}

		if proxy != nil {
			c.diskCache.proxy = proxy
			c.diskCache.spawnContainsQueueWorkers()
		}

		return nil
	}
}

func WithProxyMaxBlobSize(maxProxyBlobSize int64) Option {
	return func(c *CacheConfig) error {
		if maxProxyBlobSize <= 0 {
			return fmt.Errorf("invalid MaxProxyBlobSize: %d", maxProxyBlobSize)
		}

		c.diskCache.maxProxyBlobSize = maxProxyBlobSize
		return nil
	}
}

func WithAccessLogger(logger *log.Logger) Option {
	return func(c *CacheConfig) error {
		c.diskCache.accessLogger = logger
		return nil
	}
}

func WithEndpointMetrics() Option {
	return func(c *CacheConfig) error {
		if c.metrics != nil {
			return fmt.Errorf("WithEndpointMetrics specified multiple times")
		}

		c.metrics = &metricsDecorator{
			counter: prometheus.NewCounterVec(prometheus.CounterOpts{
				Name: "bazel_remote_incoming_requests_total",
				Help: "The number of incoming cache requests",
			},
				[]string{"method", "kind", "status"}),
		}

		c.metrics.counter.WithLabelValues("get", "cas", "hit").Add(0)
		c.metrics.counter.WithLabelValues("get", "cas", "miss").Add(0)
		c.metrics.counter.WithLabelValues("contains", "cas", "hit").Add(0)
		c.metrics.counter.WithLabelValues("contains", "cas", "miss").Add(0)
		c.metrics.counter.WithLabelValues("get", "ac", "hit").Add(0)
		c.metrics.counter.WithLabelValues("get", "ac", "miss").Add(0)

		return nil
	}
}

func WithMaxSizeHardLimit(maxSizeHardLimit int64) Option {
	return func(cc *CacheConfig) error {
		cc.maxSizeHardLimit = maxSizeHardLimit
		return nil
	}
}

func WithSharedStorageMode(enabled bool) Option {
	return func(cc *CacheConfig) error {
		cc.diskCache.sharedStorageMode = enabled
		return nil
	}
}

func WithSharedStorageLeader(enabled bool) Option {
	return func(cc *CacheConfig) error {
		cc.diskCache.sharedStorageLeader = enabled
		return nil
	}
}

func WithSharedStorageGCInterval(interval time.Duration) Option {
	return func(cc *CacheConfig) error {
		cc.diskCache.sharedStorageGCInterval = interval
		return nil
	}
}

func WithSharedStorageGCMinAge(minAge time.Duration) Option {
	return func(cc *CacheConfig) error {
		cc.diskCache.sharedStorageGCMinAge = minAge
		return nil
	}
}

// WithSharedStorageGCMaxSize sets the size in bytes that the leader's GC
// targets on the shared filesystem. Zero means "use the LRU's max size", which
// preserves the previous behaviour where one value did both jobs.
func WithSharedStorageGCMaxSize(sizeBytes int64) Option {
	return func(cc *CacheConfig) error {
		if sizeBytes < 0 {
			return fmt.Errorf("shared storage GC max size must not be negative: %d", sizeBytes)
		}
		cc.diskCache.sharedStorageGCMaxSize = sizeBytes
		return nil
	}
}

// WithSharedStorageVerifyInterval sets how often FindMissingBlobs re-checks
// that an indexed blob still exists on disk. Zero disables the check.
func WithSharedStorageVerifyInterval(d time.Duration) Option {
	return func(cc *CacheConfig) error {
		if d < 0 {
			return fmt.Errorf("shared storage verify interval must not be negative: %v", d)
		}
		cc.diskCache.sharedStorageVerifyInterval = d
		return nil
	}
}

// WithBootScanConcurrency sets the worker count for the startup index scan.
// Zero keeps the historical min(NumCPU, 16).
func WithBootScanConcurrency(n int) Option {
	return func(cc *CacheConfig) error {
		if n < 0 {
			return fmt.Errorf("boot scan concurrency must not be negative: %d", n)
		}
		cc.diskCache.bootScanConcurrency = n
		return nil
	}
}

// WithGCConcurrency sets the worker count for the pools that run during a GC
// cycle (filesystem scan, AC reference collection, unlink). Zero keeps the
// historical min(NumCPU, 16). These run alongside client traffic, so raising
// this trades request latency for GC throughput.
func WithGCConcurrency(n int) Option {
	return func(cc *CacheConfig) error {
		if n < 0 {
			return fmt.Errorf("GC concurrency must not be negative: %d", n)
		}
		cc.diskCache.gcConcurrency = n
		return nil
	}
}

func WithSharedStorageACRefMaxAge(maxAge time.Duration) Option {
	return func(cc *CacheConfig) error {
		cc.diskCache.sharedStorageACRefMaxAge = maxAge
		return nil
	}
}
