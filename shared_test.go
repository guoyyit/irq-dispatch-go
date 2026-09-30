package irq

import (
	"context"
	"sync"
	"testing"
)

func TestSharedLineAsksUntilClaim(t *testing.T) {
	d := newTestDispatcher(t, Config{})
	d.AddLine(LineConfig{ID: "sh", Mode: EdgeTriggered, Priority: 1})

	var mu sync.Mutex
	var asked []SourceID

	mk := func(id SourceID, verdict Verdict) Handler {
		return Handler{
			ID: id,
			Fn: func(ctx context.Context, env *Env) (Verdict, error) {
				mu.Lock()
				asked = append(asked, id)
				mu.Unlock()
				return verdict, nil
			},
		}
	}

	if err := d.Register("sh", mk("a", NotMine)); err != nil {
		t.Fatal(err)
	}
	if err := d.Register("sh", mk("b", NotMine)); err != nil {
		t.Fatal(err)
	}
	if err := d.Register("sh", mk("c", Claimed)); err != nil {
		t.Fatal(err)
	}

	if err := d.Raise("sh", "dev"); err != nil {
		t.Fatal(err)
	}
	if !d.WaitQuiet(timeout2s()) {
		t.Fatal("did not settle")
	}

	want := []SourceID{"a", "b", "c"}
	if len(asked) != len(want) {
		t.Fatalf("asked = %v, want %v", asked, want)
	}
	for i := range want {
		if asked[i] != want[i] {
			t.Fatalf("asked = %v, want %v", asked, want)
		}
	}
}

func TestSharedLineAllRefuseCompletes(t *testing.T) {
	d := newTestDispatcher(t, Config{})
	d.AddLine(LineConfig{ID: "sh2", Mode: EdgeTriggered, Priority: 1})

	var asks int
	mk := func(id SourceID) Handler {
		return Handler{
			ID: id,
			Fn: func(ctx context.Context, env *Env) (Verdict, error) {
				asks++
				return NotMine, nil
			},
		}
	}
	d.Register("sh2", mk("a"))
	d.Register("sh2", mk("b"))

	if err := d.Raise("sh2", "dev"); err != nil {
		t.Fatal(err)
	}
	if !d.WaitQuiet(timeout2s()) {
		t.Fatal("all-refuse interrupt must complete")
	}
	if asks != 2 {
		t.Fatalf("asks = %d, want 2", asks)
	}

	metrics := d.Metrics()
	if metrics.Claimed != 0 || metrics.Delivered != 0 {
		t.Fatalf("unexpected metrics: %+v", metrics)
	}
}
