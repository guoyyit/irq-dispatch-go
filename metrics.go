package irq

import (
	"sync"
	"sync/atomic"
	"time"
)

// MetricsSnapshot 是某一时刻运行指标的不可变快照。
type MetricsSnapshot struct {
	Raised         int64
	Delivered      int64
	Claimed        int64
	Refused        int64
	Faults         int64
	StormMerged    int64
	Deferred       int64
	Backlog        int
	Throughput     float64
	LatencyMin     time.Duration
	LatencyMax     time.Duration
	LatencyMean    time.Duration
	LatencyBuckets map[time.Duration]int64
}

// Metrics 以无锁/原子方式采集热路径指标，查询不得阻塞分发路径。
type Metrics struct {
	raised      atomic.Int64
	delivered   atomic.Int64
	claimed     atomic.Int64
	refused     atomic.Int64
	faults      atomic.Int64
	stormMerged atomic.Int64
	deferred    atomic.Int64
	backlog     atomic.Int64

	latencyMinNS atomic.Int64
	latencyMaxNS atomic.Int64
	latencySumNS atomic.Int64
	latencyCount atomic.Int64

	startNano atomic.Int64

	mu      sync.RWMutex
	buckets map[time.Duration]*atomic.Int64
}

// DefaultLatencyBuckets 为延迟分布的默认分桶上界。
var DefaultLatencyBuckets = []time.Duration{
	100 * time.Microsecond,
	time.Millisecond,
	5 * time.Millisecond,
	20 * time.Millisecond,
	100 * time.Millisecond,
	time.Second,
}

func newMetrics(buckets []time.Duration) *Metrics {
	if len(buckets) == 0 {
		buckets = DefaultLatencyBuckets
	}
	m := &Metrics{
		buckets: make(map[time.Duration]*atomic.Int64, len(buckets)),
	}
	for _, bound := range buckets {
		m.buckets[bound] = &atomic.Int64{}
	}
	m.latencyMinNS.Store(1<<63 - 1)
	return m
}

func (m *Metrics) initStart(now time.Time) {
	m.startNano.CompareAndSwap(0, now.UnixNano())
}

func (m *Metrics) recordRaised()     { m.raised.Add(1) }
func (m *Metrics) recordDelivered()  { m.delivered.Add(1) }
func (m *Metrics) recordClaim()      { m.claimed.Add(1) }
func (m *Metrics) recordRefuse()     { m.refused.Add(1) }
func (m *Metrics) recordFault()      { m.faults.Add(1) }
func (m *Metrics) recordStormMerge() { m.stormMerged.Add(1) }
func (m *Metrics) recordDeferred()   { m.deferred.Add(1) }

func (m *Metrics) setBacklog(depth int) { m.backlog.Store(int64(depth)) }

func (m *Metrics) recordLatency(latency time.Duration) {
	value := latency.Nanoseconds()
	for {
		current := m.latencyMinNS.Load()
		if value >= current || m.latencyMinNS.CompareAndSwap(current, value) {
			break
		}
	}
	for {
		current := m.latencyMaxNS.Load()
		if value <= current || m.latencyMaxNS.CompareAndSwap(current, value) {
			break
		}
	}
	m.latencySumNS.Add(value)
	m.latencyCount.Add(1)

	m.mu.RLock()
	for bound, counter := range m.buckets {
		if value <= bound.Nanoseconds() {
			counter.Add(1)
		}
	}
	m.mu.RUnlock()
}

// Snapshot 返回当前指标快照。
func (m *Metrics) Snapshot() MetricsSnapshot {
	snapshot := MetricsSnapshot{
		Raised:         m.raised.Load(),
		Delivered:      m.delivered.Load(),
		Claimed:        m.claimed.Load(),
		Refused:        m.refused.Load(),
		Faults:         m.faults.Load(),
		StormMerged:    m.stormMerged.Load(),
		Deferred:       m.deferred.Load(),
		Backlog:        int(m.backlog.Load()),
		Throughput:     m.throughputPerSecond(time.Now()),
		LatencyBuckets: make(map[time.Duration]int64, len(m.buckets)),
	}
	count := m.latencyCount.Load()
	if count > 0 {
		snapshot.LatencyMin = time.Duration(m.latencyMinNS.Load())
		snapshot.LatencyMax = time.Duration(m.latencyMaxNS.Load())
		snapshot.LatencyMean = time.Duration(m.latencySumNS.Load() / count)
	}
	m.mu.RLock()
	for bound, counter := range m.buckets {
		snapshot.LatencyBuckets[bound] = counter.Load()
	}
	m.mu.RUnlock()
	return snapshot
}

// Reset 清零全部指标。
func (m *Metrics) Reset() {
	m.raised.Store(0)
	m.delivered.Store(0)
	m.claimed.Store(0)
	m.refused.Store(0)
	m.faults.Store(0)
	m.stormMerged.Store(0)
	m.deferred.Store(0)
	m.backlog.Store(0)
	m.latencyMinNS.Store(1<<63 - 1)
	m.latencyMaxNS.Store(0)
	m.latencySumNS.Store(0)
	m.latencyCount.Store(0)
	m.mu.RLock()
	for _, counter := range m.buckets {
		counter.Store(0)
	}
	m.mu.RUnlock()
}

// snapshotAt 与 Snapshot 相同，但吞吐以指定时钟的当前时间计算。
func (m *Metrics) snapshotAt(now time.Time) MetricsSnapshot {
	snapshot := m.Snapshot()
	snapshot.Throughput = m.throughputPerSecond(now)
	return snapshot
}

func (m *Metrics) throughputPerSecond(now time.Time) float64 {
	start := m.startNano.Load()
	if start == 0 {
		return 0
	}
	elapsed := float64(now.UnixNano()-start) / float64(time.Second)
	if elapsed <= 0 {
		return 0
	}
	return float64(m.delivered.Load()) / elapsed
}
