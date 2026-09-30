package irq

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func waitFor(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timeout waiting for: %s", msg)
}

func TestEdgeSharedLineClaim(t *testing.T) {
	dp := New(DefaultConfig())
	var declined, claimed atomic.Int64
	must(t, dp.Register(1, EdgeTriggered, Handler{
		ID:       "h1",
		Priority: 5,
		Handle: func(ctx context.Context, ev Event) bool {
			declined.Add(1)
			return false
		},
	}))
	must(t, dp.Register(1, EdgeTriggered, Handler{
		ID:       "h2",
		Priority: 5,
		Handle: func(ctx context.Context, ev Event) bool {
			claimed.Add(1)
			return true
		},
	}))
	must(t, dp.Register(1, EdgeTriggered, Handler{
		ID:       "h3",
		Priority: 1,
		Handle: func(ctx context.Context, ev Event) bool {
			t.Fatal("h3 must not be asked after h2 claimed")
			return true
		},
	}))
	must(t, dp.Start())
	defer dp.Stop(context.Background())

	dev := NewDevice("dev1", 1, EdgeTriggered)
	dev.attach(dp)
	dev.Raise()

	waitFor(t, func() bool { return claimed.Load() == 1 && declined.Load() == 1 },
		"h1 declines then h2 claims")
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
