package irq

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestLevelHoldAndRelease(t *testing.T) {
	dp := New(DefaultConfig())
	var calls atomic.Int64
	must(t, dp.Register(7, LevelTriggered, Handler{
		ID:       "lvl",
		Priority: 1,
		Handle: func(ctx context.Context, ev Event) bool {
			calls.Add(1)
			return true
		},
	}))
	must(t, dp.Start())
	defer dp.Stop(context.Background())

	dev := NewDevice("devL", 7, LevelTriggered)
	dev.attach(dp)

	dev.Raise()
	dev.Raise()
	dev.Raise()

	waitFor(t, func() bool { return calls.Load() == 1 }, "held level reports exactly once")

	// Still asserted: no further reports even after the walk completes.
	time.Sleep(20 * time.Millisecond)
	if got := calls.Load(); got != 1 {
		t.Fatalf("expected 1 report while held, got %d", got)
	}

	// Deassert, then re-assert: a fresh report must be delivered.
	dev.Release()
	time.Sleep(20 * time.Millisecond)
	dev.Raise()
	waitFor(t, func() bool { return calls.Load() == 2 }, "re-asserted level reports again")
}
