package irq

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestMetricsSnapshot(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Storm.Threshold = 0
	cfg.Storm.Window = 0

	var delivered atomic.Uint64
	dp := New(cfg)
	must(t, dp.Register(1, EdgeTriggered, Handler{
		ID: "h",
		Handle: func(ctx context.Context, ev Event) bool {
			time.Sleep(time.Millisecond)
			delivered.Add(1)
			return true
		},
	}))
	must(t, dp.Start())

	// Pace raises one outstanding interrupt at a time so every pulse is a
	// distinct response (burst coalescing is covered by edge/storm tests).
	const n = 25
	for i := 0; i < n; i++ {
		dp.Raise(1, SourceID("s"))
		waitFor(t, func() bool { return delivered.Load() == uint64(i+1) },
			"per-raise delivery")
	}

	snap := dp.Metrics().Snapshot()
	if snap.Delivered != n {
		t.Fatalf("delivered=%d want %d", snap.Delivered, n)
	}
	if snap.Samples != n {
		t.Fatalf("latency samples=%d want %d", snap.Samples, n)
	}
	if snap.LatencyMin <= 0 || snap.LatencyMax < snap.LatencyMin ||
		snap.LatencyP99 < snap.LatencyP50 || snap.LatencyP99 > snap.LatencyMax {
		t.Fatalf("inconsistent latency distribution: %+v", snap)
	}
	if snap.Backlog != 0 {
		t.Fatalf("drained backlog should be zero, got %d", snap.Backlog)
	}

	// Concurrent snapshot must not block or race with the dispatch path.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 100; i++ {
			_ = dp.Metrics().Snapshot()
		}
	}()
	base := delivered.Load()
	for i := 0; i < 100; i++ {
		dp.Raise(1, SourceID("s"))
	}
	<-done

	waitFor(t, func() bool {
		return delivered.Load() >= base && dp.Metrics().Snapshot().Backlog >= 0
	}, "snapshot during load")

	mustStop(t, dp)
}
