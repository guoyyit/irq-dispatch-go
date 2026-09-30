package irq

import (
	"context"
	"testing"
)

func TestMetricsQueryable(t *testing.T) {
	d := newTestDispatcher(t, Config{})
	d.AddLine(LineConfig{ID: "L", Mode: EdgeTriggered, Priority: 1})
	d.Register("L", Handler{
		ID: "h",
		Fn: func(ctx context.Context, env *Env) (Verdict, error) {
			return Claimed, nil
		},
	})

	for i := 0; i < 50; i++ {
		if err := d.Raise("L", SourceID("dev")); err != nil {
			t.Fatal(err)
		}
	}
	if !d.WaitQuiet(timeout2s()) {
		t.Fatal("did not settle")
	}

	snapshot := d.Metrics()
	if snapshot.Raised != 50 {
		t.Fatalf("raised = %d, want 50", snapshot.Raised)
	}
	if snapshot.Claimed < 1 {
		t.Fatalf("claimed = %d, want >= 1", snapshot.Claimed)
	}
	if snapshot.Claimed > 50 {
		t.Fatalf("claimed = %d exceeds raises", snapshot.Claimed)
	}
	if snapshot.Backlog != 0 {
		t.Fatalf("backlog = %d, want 0", snapshot.Backlog)
	}
	if snapshot.LatencyMin > snapshot.LatencyMax {
		t.Fatalf("latency range invalid: min=%v max=%v", snapshot.LatencyMin, snapshot.LatencyMax)
	}
	if len(snapshot.LatencyBuckets) == 0 {
		t.Fatal("latency buckets empty")
	}
}
