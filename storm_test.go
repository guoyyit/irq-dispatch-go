package irq

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestStormMergedDeliveryAndFinalStateNotLost(t *testing.T) {
	d := newTestDispatcher(t, Config{
		Storm: StormConfig{
			Window:     time.Second,
			Threshold:  5,
			Merge:      true,
			MaxBacklog: 4,
		},
	})
	d.AddLine(LineConfig{ID: "L", Mode: EdgeTriggered, Priority: 1})

	var claims atomic.Int64
	d.Register("L", Handler{
		ID: "h",
		Fn: func(ctx context.Context, env *Env) (Verdict, error) {
			claims.Add(1)
			return Claimed, nil
		},
	})

	dev := NewVirtualDevice(d, "noisy")
	// 突发 20 次，远超阈值。
	for i := 0; i < 20; i++ {
		if err := dev.Raise("L"); err != nil {
			t.Fatal(err)
		}
	}

	if !d.WaitQuiet(2 * time.Second) {
		t.Fatal("did not settle after storm")
	}

	// 边缘合并：突发期间收敛为有限次投递，而不是 20 次；
	// 但最终状态（至少一次）绝不丢失。
	if claims.Load() == 0 {
		t.Fatal("final state lost: no delivery after storm")
	}
	if claims.Load() >= 20 {
		t.Fatalf("storm not coalesced: claims = %d for 20 raises", claims.Load())
	}

	metrics := d.Metrics()
	if metrics.StormMerged == 0 {
		t.Fatalf("storm merges not recorded: %+v", metrics)
	}

	// 积压深度任何时刻都必须有界。
	if metrics.Backlog > 4 {
		t.Fatalf("backlog unbounded: %d", metrics.Backlog)
	}
}

func TestStormRateLimitedBacklogDrained(t *testing.T) {
	d := newTestDispatcher(t, Config{
		Storm: StormConfig{
			Window:        500 * time.Millisecond,
			Threshold:     3,
			Merge:         false,
			CoalesceDelay: 20 * time.Millisecond,
			MaxBacklog:    3,
		},
	})
	d.AddLine(LineConfig{ID: "L", Mode: EdgeTriggered, Priority: 1})

	var claims atomic.Int64
	d.Register("L", Handler{
		ID: "h",
		Fn: func(ctx context.Context, env *Env) (Verdict, error) {
			claims.Add(1)
			return Claimed, nil
		},
	})

	dev := NewVirtualDevice(d, "noisy")
	for i := 0; i < 10; i++ {
		dev.Raise("L")
	}

	if !d.WaitQuiet(3 * time.Second) {
		t.Fatal("rate-limited backlog did not drain")
	}
	if claims.Load() == 0 {
		t.Fatal("no delivery after rate-limited storm")
	}
	metrics := d.Metrics()
	if metrics.Backlog != 0 {
		t.Fatalf("backlog after storm = %d, want 0", metrics.Backlog)
	}
}
