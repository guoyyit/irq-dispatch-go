package irq

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestFaultyHandlerDoesNotBlockOthers(t *testing.T) {
	d := newTestDispatcher(t, Config{
		Fault: FaultPolicy{Timeout: 50 * time.Millisecond},
	})
	d.AddLine(LineConfig{ID: "L", Mode: EdgeTriggered, Priority: 1})

	var goodCalls atomic.Int64

	d.Register("L", Handler{
		ID: "panicker",
		Fn: func(ctx context.Context, env *Env) (Verdict, error) {
			panic("boom")
		},
	})
	d.Register("L", Handler{
		ID: "good",
		Fn: func(ctx context.Context, env *Env) (Verdict, error) {
			goodCalls.Add(1)
			return Claimed, nil
		},
	})

	d.Raise("L", "dev")
	if !d.WaitQuiet(2 * time.Second) {
		t.Fatal("faulty handler blocked dispatch loop")
	}
	if goodCalls.Load() != 1 {
		t.Fatalf("good handler calls = %d, want 1", goodCalls.Load())
	}
	faults := d.Faults()
	if len(faults) != 1 || faults[0].Handler != "panicker" {
		t.Fatalf("faults = %+v, want one panic for panicker", faults)
	}
}

func TestTimeoutHandlerIsolated(t *testing.T) {
	d := newTestDispatcher(t, Config{
		Fault: FaultPolicy{Timeout: 40 * time.Millisecond},
	})
	d.AddLine(LineConfig{ID: "L", Mode: EdgeTriggered, Priority: 1})

	var fallback atomic.Int64
	d.Register("L", Handler{
		ID: "slow",
		Fn: func(ctx context.Context, env *Env) (Verdict, error) {
			time.Sleep(2 * time.Second)
			return Claimed, nil
		},
	})
	d.Register("L", Handler{
		ID: "fallback",
		Fn: func(ctx context.Context, env *Env) (Verdict, error) {
			fallback.Add(1)
			return Claimed, nil
		},
	})

	start := time.Now()
	d.Raise("L", "dev")
	if !d.WaitQuiet(2 * time.Second) {
		t.Fatal("timeout did not isolate slow handler")
	}
	if time.Since(start) > time.Second {
		t.Fatal("dispatch waited for slow handler beyond timeout")
	}
	if fallback.Load() != 1 {
		t.Fatalf("fallback calls = %d, want 1", fallback.Load())
	}
}

func TestAutoDisableAndCooldownRecovery(t *testing.T) {
	d := newTestDispatcher(t, Config{
		Fault: FaultPolicy{
			Threshold:   2,
			AutoDisable: true,
			Cooldown:    80 * time.Millisecond,
			MaxRestarts: 5,
		},
	})
	d.AddLine(LineConfig{ID: "L", Mode: EdgeTriggered, Priority: 1})

	var calls atomic.Int64
	d.Register("L", Handler{
		ID: "flaky",
		Fn: func(ctx context.Context, env *Env) (Verdict, error) {
			calls.Add(1)
			return NotMine, errors.New("nope")
		},
	})
	d.Register("L", Handler{
		ID: "sink",
		Fn: func(ctx context.Context, env *Env) (Verdict, error) {
			return Claimed, nil
		},
	})

	d.Raise("L", "dev1")
	d.Raise("L", "dev2")
	if !d.WaitQuiet(2 * time.Second) {
		t.Fatal("did not settle after faults")
	}
	if calls.Load() != 2 {
		t.Fatalf("flaky calls = %d, want 2 before auto-disable", calls.Load())
	}

	d.Raise("L", "dev3")
	if !d.WaitQuiet(2 * time.Second) {
		t.Fatal("did not settle while disabled")
	}
	if calls.Load() != 2 {
		t.Fatalf("flaky was asked while disabled: calls = %d", calls.Load())
	}

	foundDisabled := false
	for _, event := range d.Journal().Trace() {
		if event.Kind == TraceDisabled && event.Handler == "flaky" {
			foundDisabled = true
		}
	}
	if !foundDisabled {
		t.Fatal("auto-disable event not observed in trace")
	}

	time.Sleep(150 * time.Millisecond)
	d.Raise("L", "dev4")
	if !d.WaitQuiet(2 * time.Second) {
		t.Fatal("did not settle after cooldown")
	}
	if calls.Load() != 3 {
		t.Fatalf("flaky calls after recovery = %d, want 3", calls.Load())
	}

	foundRecovered := false
	for _, event := range d.Journal().Trace() {
		if event.Kind == TraceRecovered && event.Handler == "flaky" {
			foundRecovered = true
		}
	}
	if !foundRecovered {
		t.Fatal("recovery event not observed in trace")
	}
}
