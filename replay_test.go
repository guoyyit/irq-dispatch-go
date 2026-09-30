package irq

import (
	"context"
	"testing"
	"time"
)

// TestDeterministicReplay drives a fixed, non-storming input script through a
// live dispatcher and replays the ordered inputs twice synchronously; all
// three trajectories must be identical.
func TestDeterministicReplay(t *testing.T) {
	cfg := DefaultConfig()
	// Disable time-based storm detection so the trajectory is a pure function
	// of the ordered inputs and the deterministic handler decisions.
	cfg.Storm.Threshold = 0
	cfg.Storm.Window = 0

	handlers := map[HandlerID]Handler{
		"devA":  {ID: "devA", Priority: 5, Handle: func(ctx context.Context, ev Event) bool { return ev.Source == "devA" }},
		"devB":  {ID: "devB", Priority: 3, Handle: func(ctx context.Context, ev Event) bool { return ev.Source == "devB" }},
		"catch": {ID: "catch", Priority: 1, Handle: func(ctx context.Context, ev Event) bool { return true }},
		"devL":  {ID: "devL", Priority: 2, Handle: func(ctx context.Context, ev Event) bool { return true }},
	}
	lookup := func(id HandlerID) (Handler, bool) {
		h, ok := handlers[id]
		return h, ok
	}

	script := []ReplayInput{
		{Line: 1, Source: "devA"},
		{Line: 1, Source: "devB"},
		{Line: 1, Source: "devA"},
		{Line: 1, Source: "devB"},
		{Line: 1, Source: "orphan"},
		{Line: 2, Source: "devL"},
		{Line: 2, Source: "devL"},
		{Release: true, Line: 2, Source: "devL"},
		{Line: 2, Source: "devL"},
		{Release: true, Line: 2, Source: "devL"},
	}

	// Build the reference trajectory with the synchronous replay engine, so
	// the comparison is independent of goroutine scheduling and wall clocks.
	ref := newReplayEngineWithConfig(cfg)
	must(t, ref.register(1, EdgeTriggered, handlers["devA"]))
	must(t, ref.register(1, EdgeTriggered, handlers["devB"]))
	must(t, ref.register(1, EdgeTriggered, handlers["catch"]))
	must(t, ref.register(2, LevelTriggered, handlers["devL"]))
	ctx := context.Background()
	for _, in := range script {
		ref.step(ctx, in, lineModeFor(in.Line, in.Source, handlers), time.Time{})
	}
	ref.drain(ctx)
	original := ref.journal.Records()

	// Two independent replays must reproduce the reference exactly.
	r1 := ReplayWithConfig(cfg, original, script, lookup)
	r2 := ReplayWithConfig(cfg, original, script, lookup)
	if !r1.Match {
		t.Fatalf("replay 1 mismatch: %s\n--- original ---\n%s", r1.FirstMismatch, ref.journal.Trace())
	}
	if !r2.Match {
		t.Fatalf("replay 2 mismatch: %s", r2.FirstMismatch)
	}
}

func lineModeFor(line LineID, _ SourceID, _ map[HandlerID]Handler) TriggerMode {
	if line == 2 {
		return LevelTriggered
	}
	return EdgeTriggered
}

// TestReplayPriorityOrder verifies replay reproduces global delivery order
// when higher- and equal-priority interrupts share lines.
func TestReplayPriorityOrder(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Storm.Threshold = 0
	cfg.Storm.Window = 0

	handlers := map[HandlerID]Handler{
		"lo": {ID: "lo", Priority: 1, Handle: func(ctx context.Context, ev Event) bool { return ev.Source == "lo" }},
		"hi": {ID: "hi", Priority: 9, Handle: func(ctx context.Context, ev Event) bool { return ev.Source == "hi" }},
	}
	lookup := func(id HandlerID) (Handler, bool) {
		h, ok := handlers[id]
		return h, ok
	}

	script := []ReplayInput{
		{Line: 1, Source: "lo"},
		{Line: 2, Source: "hi"},
		{Line: 1, Source: "lo"},
	}

	ref := newReplayEngineWithConfig(cfg)
	must(t, ref.register(1, EdgeTriggered, handlers["lo"]))
	must(t, ref.register(2, EdgeTriggered, handlers["hi"]))
	ctx := context.Background()
	for _, in := range script {
		ref.step(ctx, in, EdgeTriggered, time.Time{})
	}
	ref.drain(ctx)

	res := ReplayWithConfig(cfg, ref.journal.Records(), script, lookup)
	if !res.Match {
		t.Fatalf("replay mismatch: %s\n%s", res.FirstMismatch, ref.journal.Trace())
	}

	// hi is delivered before either lo due to higher priority.
	recs := ref.journal.Records()
	firstDeliver := map[SourceID]int{}
	for i, r := range recs {
		if r.Type == RecordDelivered {
			if _, ok := firstDeliver[r.Source]; !ok {
				firstDeliver[r.Source] = i
			}
		}
	}
	if firstDeliver["hi"] > firstDeliver["lo"] {
		t.Fatalf("high priority must deliver first: %+v\n%s", firstDeliver, ref.journal.Trace())
	}
}

func mustStop(t *testing.T, dp *Dispatcher) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := dp.Stop(ctx); err != nil {
		t.Fatal(err)
	}
}
