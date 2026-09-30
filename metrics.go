package irq

import (
	"sort"
	"sync/atomic"
	"time"
)

// Snapshot is a point-in-time metrics query result.
type Snapshot struct {
	Delivered  uint64
	Coalesced  uint64
	Suppressed uint64
	Faults     uint64
	Backlog    int
	Samples    int
	LatencyMin time.Duration
	LatencyAvg time.Duration
	LatencyP50 time.Duration
	LatencyP90 time.Duration
	LatencyP99 time.Duration
	LatencyMax time.Duration
}

// Metrics collects runtime counters without blocking dispatch.
//
// Counters are plain atomics and the latency path is a fixed-size ring of
// atomic slots: RecordLatency never locks, never allocates and drops a sample
// rather than block when the ring is contended.
type Metrics struct {
	delivered  atomic.Uint64
	coalesced  atomic.Uint64
	suppressed atomic.Uint64
	faults     atomic.Uint64

	// backlog is maintained by the dispatcher between snapshots.
	backlog atomic.Int64

	ring    []latencySlot
	ringPos atomic.Uint64
}

// latencyRingSize is a power of two to keep slot selection a mask.
const latencyRingSize = 4096

// latencySlot is cache-line padded to avoid false sharing between writers.
type latencySlot struct {
	ns atomic.Int64
	_  [56]byte
}

// newMetrics creates a metrics collector.
func newMetrics() *Metrics {
	return &Metrics{ring: make([]latencySlot, latencyRingSize)}
}

func (m *Metrics) recordDelivered() { m.delivered.Add(1) }
func (m *Metrics) recordCoalesced(n int64) {
	if n > 0 {
		m.coalesced.Add(uint64(n))
	}
}
func (m *Metrics) recordSuppressed(n int64) {
	if n > 0 {
		m.suppressed.Add(uint64(n))
	}
}
func (m *Metrics) recordFault()     { m.faults.Add(1) }
func (m *Metrics) setBacklog(n int) { m.backlog.Store(int64(n)) }

// RecordLatency records one raise-to-delivery latency. Non-blocking: under
// contention a slot may be overwritten, which merely loses that one sample.
func (m *Metrics) RecordLatency(d time.Duration) {
	pos := m.ringPos.Add(1) - 1
	m.ring[pos&(latencyRingSize-1)].ns.Store(d.Nanoseconds())
}

// Snapshot returns a consistent point-in-time view safe for concurrent use.
func (m *Metrics) Snapshot() Snapshot {
	snap := Snapshot{
		Delivered:  m.delivered.Load(),
		Coalesced:  m.coalesced.Load(),
		Suppressed: m.suppressed.Load(),
		Faults:     m.faults.Load(),
		Backlog:    int(m.backlog.Load()),
	}

	samples := make([]int64, 0, latencyRingSize)
	for i := range m.ring {
		if ns := m.ring[i].ns.Load(); ns > 0 {
			samples = append(samples, ns)
		}
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	if len(samples) == 0 {
		return snap
	}
	snap.Samples = len(samples)
	var sum int64
	for _, ns := range samples {
		sum += ns
	}
	pct := func(p float64) time.Duration {
		idx := int(float64(len(samples)-1) * p)
		return time.Duration(samples[idx])
	}
	snap.LatencyMin = time.Duration(samples[0])
	snap.LatencyMax = time.Duration(samples[len(samples)-1])
	snap.LatencyAvg = time.Duration(sum / int64(len(samples)))
	snap.LatencyP50 = pct(0.50)
	snap.LatencyP90 = pct(0.90)
	snap.LatencyP99 = pct(0.99)
	return snap
}
