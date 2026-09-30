package irq

import (
	"context"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

func TestPanicIsolation(t *testing.T) {
	dp := New(DefaultConfig())
	var good atomic.Int64

	must(t, dp.Register(1, EdgeTriggered, Handler{
		ID:       "crasher",
		Priority: 9,
		Handle: func(ctx context.Context, ev Event) bool {
			panic("boom")
		},
	}))
	must(t, dp.Register(1, EdgeTriggered, Handler{
		ID:       "good",
		Priority: 5,
		Handle: func(ctx context.Context, ev Event) bool {
			good.Add(1)
			return true
		},
	}))
	must(t, dp.Start())
	defer dp.Stop(context.Background())

	dp.Raise(1, "src")
	waitFor(t, func() bool { return good.Load() == 1 },
		"handler after crasher still gets asked")

	waitFor(t, func() bool { return dp.Metrics().Snapshot().Faults >= 1 },
		"fault metric recorded")
}

func TestTimeoutAutoDisableAndRecover(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Fault.Timeout = 30 * time.Millisecond
	cfg.Fault.DisableAfter = 2
	cfg.Fault.RecoverAfter = 200 * time.Millisecond
	dp := New(cfg)

	var invocations atomic.Int64
	must(t, dp.Register(1, EdgeTriggered, Handler{
		ID: "slow",
		Handle: func(ctx context.Context, ev Event) bool {
			invocations.Add(1)
			<-ctx.Done() // hangs until the isolation timeout cancels it
			return false
		},
	}))
	must(t, dp.Start())
	defer dp.Stop(context.Background())

	find := func() *HandlerStatus {
		for i := range dp.HandlerStatuses() {
			if dp.HandlerStatuses()[i].ID == "slow" {
				return &dp.HandlerStatuses()[i]
			}
		}
		return nil
	}

	dp.Raise(1, "a")
	waitFor(t, func() bool { return dp.Metrics().Snapshot().Faults >= 1 }, "first timeout fault")
	dp.Raise(1, "a2")
	waitFor(t, func() bool {
		st := find()
		return st != nil && !st.Enabled
	}, "slow handler auto-disabled after threshold faults")

	// While disabled it must not be invoked again even with new interrupts.
	before := invocations.Load()
	dp.Raise(1, "b")
	time.Sleep(100 * time.Millisecond)
	if got := invocations.Load(); got != before {
		t.Fatalf("disabled handler was invoked: %d vs %d", got, before)
	}

	// After the recovery window it is re-enabled and invoked once more.
	waitFor(t, func() bool {
		st := find()
		return st != nil && st.Enabled
	}, "handler recovered after recovery window")

	dp.Raise(1, "c")
	waitFor(t, func() bool { return invocations.Load() >= before+1 },
		"recovered handler invoked again")

	// Stopping cancels in-flight handler contexts; no goroutine may leak.
	mustStop(t, dp)
	deadline := time.Now().Add(500 * time.Millisecond)
	baseline := runtime.NumGoroutine()
	for time.Now().Before(deadline) {
		runtime.GC()
		if runtime.NumGoroutine() <= baseline {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := runtime.NumGoroutine(); got > baseline {
		t.Fatalf("possible goroutine leak: %d goroutines after stop", got)
	}
}
