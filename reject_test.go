package irq

import (
	"context"
	"sync/atomic"
	"testing"
)

func TestAllHandlersDecline(t *testing.T) {
	dp := New(DefaultConfig())
	var asked atomic.Int64
	must(t, dp.Register(1, EdgeTriggered, Handler{
		ID: "a", Priority: 3,
		Handle: func(ctx context.Context, ev Event) bool { asked.Add(1); return false },
	}))
	must(t, dp.Register(1, EdgeTriggered, Handler{
		ID: "b", Priority: 2,
		Handle: func(ctx context.Context, ev Event) bool { asked.Add(1); return false },
	}))
	must(t, dp.Start())
	defer dp.Stop(context.Background())

	dp.Raise(1, "nobody")
	waitFor(t, func() bool { return asked.Load() == 2 }, "both handlers asked")

	recs := dp.Journal().Records()
	rejections := 0
	for _, r := range recs {
		if r.Type == RecordRejected {
			rejections++
		}
	}
	// Two per-handler declines plus one line-level rejection.
	if rejections < 3 {
		t.Fatalf("expected per-handler and line rejection, got %d\n%s",
			rejections, dp.Journal().Trace())
	}
}
