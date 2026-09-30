package irq

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestEdgeCoalesceDuringBusy(t *testing.T) {
	dp := New(DefaultConfig())

	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int64
	var sawCoalesced atomic.Int64

	must(t, dp.Register(3, EdgeTriggered, Handler{
		ID:       "slow",
		Priority: 1,
		Handle: func(ctx context.Context, ev Event) bool {
			n := calls.Add(1)
			if n == 1 {
				close(started)
				select {
				case <-release:
				case <-ctx.Done():
				}
			}
			if ev.Coalesced > 0 {
				sawCoalesced.Store(int64(ev.Coalesced))
			}
			return true
		},
	}))
	must(t, dp.Start())
	defer dp.Stop(context.Background())

	dev := NewDevice("devE", 3, EdgeTriggered)
	dev.attach(dp)

	dev.Raise()
	<-started

	// Fire many more pulses while the first delivery is in flight.
	for i := 0; i < 50; i++ {
		dev.Raise()
	}
	time.Sleep(20 * time.Millisecond)

	// Only the first delivery should be running so far.
	if got := calls.Load(); got != 1 {
		t.Fatalf("expected no second delivery while busy, got %d", got)
	}

	close(release)

	// Exactly one follow-up event must carry the merged existence.
	waitFor(t, func() bool { return calls.Load() == 2 }, "one coalesced follow-up delivery")
	time.Sleep(20 * time.Millisecond)
	if got := calls.Load(); got != 2 {
		t.Fatalf("expected exactly 2 deliveries, got %d", got)
	}
	if got := sawCoalesced.Load(); got == 0 {
		t.Fatal("follow-up event must carry coalesced pulse count")
	}
}
