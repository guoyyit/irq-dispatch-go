package irq

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestStormAggregationAndFinalDelivery(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Storm.Window = 100 * time.Millisecond
	cfg.Storm.Threshold = 10
	cfg.Storm.MaxBacklog = 1000
	dp := New(cfg)

	var deliveries atomic.Int64
	var maxCoalesced atomic.Int64
	must(t, dp.Register(1, EdgeTriggered, Handler{
		ID: "h",
		Handle: func(ctx context.Context, ev Event) bool {
			deliveries.Add(1)
			for {
				cur := maxCoalesced.Load()
				if int64(ev.Coalesced) <= cur ||
					maxCoalesced.CompareAndSwap(cur, int64(ev.Coalesced)) {
					break
				}
			}
			return true
		},
	}))
	must(t, dp.Start())
	defer dp.Stop(context.Background())

	// First 10 raise events are delivered normally; the 11th within the window
	// declares a storm and subsequent pulses are coalesced/rate-limited.
	for i := 0; i < 500; i++ {
		dp.Raise(1, "stormy")
	}

	// During the storm, delivery count must stay far below the raise count and
	// the backlog must remain bounded.
	time.Sleep(40 * time.Millisecond)
	snap := dp.Metrics().Snapshot()
	if snap.Delivered > 20 {
		t.Fatalf("storm not rate-limited: %d deliveries", snap.Delivered)
	}

	// After the quiet window, exactly one final aggregated event must flush so
	// final state is not lost.
	waitFor(t, func() bool {
		s := dp.Metrics().Snapshot()
		return s.Coalesced > 0 && maxCoalesced.Load() > 0
	}, "coalesced final delivery observed")

	totalRaises := uint64(500)
	snap = dp.Metrics().Snapshot()
	if snap.Delivered+snap.Coalesced+snap.Suppressed < totalRaises {
		t.Fatalf("accounted=%d delivered=%d coalesced=%d suppressed=%d, must not lose raises",
			snap.Delivered+snap.Coalesced+snap.Suppressed,
			snap.Delivered, snap.Coalesced, snap.Suppressed)
	}
}

func TestStormBacklogBounded(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Storm.Window = 200 * time.Millisecond
	cfg.Storm.Threshold = 2
	cfg.Storm.MaxBacklog = 5
	dp := New(cfg)

	block := make(chan struct{})
	started := make(chan struct{})
	must(t, dp.Register(1, EdgeTriggered, Handler{
		ID: "h",
		Handle: func(ctx context.Context, ev Event) bool {
			select {
			case <-started:
			default:
				close(started)
			}
			<-block
			return true
		},
	}))
	must(t, dp.Start())
	defer dp.Stop(context.Background())

	// Keep one delivery in flight so storm pulses accumulate in backlog.
	dp.Raise(1, "s")
	<-started

	for i := 0; i < 50; i++ {
		dp.Raise(1, "s")
	}

	// Poll metrics during the storm: suppressed pulses prove the cap held.
	deadline := time.Now().Add(100 * time.Millisecond)
	for time.Now().Before(deadline) {
		snap := dp.Metrics().Snapshot()
		if snap.Backlog > 5+2 { // +2 tolerance for in-flight/queue bookkeeping
			t.Fatalf("backlog %d exceeded cap", snap.Backlog)
		}
		time.Sleep(2 * time.Millisecond)
	}

	if dp.Metrics().Snapshot().Suppressed == 0 {
		t.Fatal("expected suppression once the backlog cap was reached")
	}

	close(block)

	// Final state still flushes after the storm quiets.
	waitFor(t, func() bool {
		s := dp.Metrics().Snapshot()
		return s.Coalesced > 0
	}, "final aggregated event delivered after storm")
}
