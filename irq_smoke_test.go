package irq

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func timeout2s() time.Duration { return 2 * time.Second }

func newTestDispatcher(t *testing.T, cfg Config) *Dispatcher {
	t.Helper()
	d := New(cfg)
	d.Start(context.Background())
	t.Cleanup(d.Stop)
	return d
}

func TestSmokeEdgeClaim(t *testing.T) {
	d := newTestDispatcher(t, Config{})
	d.AddLine(LineConfig{ID: "L1", Mode: EdgeTriggered, Priority: 1})

	var claims atomic.Int64
	err := d.Register("L1", Handler{
		ID:       "h1",
		Priority: 1,
		Fn: func(ctx context.Context, env *Env) (Verdict, error) {
			claims.Add(1)
			return Claimed, nil
		},
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := d.Raise("L1", "dev1"); err != nil {
		t.Fatalf("raise: %v", err)
	}
	if !d.WaitQuiet(timeout2s()) {
		t.Fatal("dispatcher did not settle")
	}
	if claims.Load() != 1 {
		t.Fatalf("claims = %d, want 1", claims.Load())
	}
}
