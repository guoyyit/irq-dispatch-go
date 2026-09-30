package irq

import (
	"context"
	"fmt"
	"time"
)

// ReplayInput is one scripted external signal in a recorded input sequence.
type ReplayInput struct {
	Release bool
	Line    LineID
	Source  SourceID
	// At is the raise/release timestamp used for storm windows and timeouts.
	At time.Time
}

// ReplayResult compares a replayed trajectory against an original journal.
type ReplayResult struct {
	// Match reports whether every replay record equals the original.
	Match bool
	// OriginalCount and ReplayCount help diagnose divergent lengths.
	OriginalCount int
	ReplayCount   int
	// FirstMismatch is a human-readable description when Match is false.
	FirstMismatch string
}

// Replay drives a fresh dispatcher through inputs synchronously and returns
// whether its trajectory exactly matches original. Handler lookup is supplied
// by the caller so the same routine bodies participate; replay executes them
// inline (no goroutines), making the trajectory fully deterministic.
func Replay(original []Record, inputs []ReplayInput, handlerFunc func(HandlerID) (Handler, bool)) ReplayResult {
	return ReplayWithConfig(DefaultConfig(), original, inputs, handlerFunc)
}

// ReplayWithConfig is Replay with explicit storm/fault configuration, matching
// the configuration used for the original live run.
func ReplayWithConfig(cfg Config, original []Record, inputs []ReplayInput, handlerFunc func(HandlerID) (Handler, bool)) ReplayResult {
	res := ReplayResult{OriginalCount: len(original)}

	eng := newReplayEngineWithConfig(cfg)

	// Infer line trigger mode from records: level lines show release records.
	lineMode := map[LineID]TriggerMode{}
	for _, r := range original {
		if r.Type == RecordReleased {
			lineMode[r.Line] = LevelTriggered
		}
	}
	for _, r := range original {
		if _, ok := lineMode[r.Line]; !ok {
			lineMode[r.Line] = EdgeTriggered
		}
	}

	// Register every handler that participated, using its body from the caller
	// and its priority as observed in the original trajectory.
	registered := map[HandlerID]bool{}
	for _, r := range original {
		if r.Handler == "" || registered[r.Handler] {
			continue
		}
		h, ok := handlerFunc(r.Handler)
		if !ok {
			res.FirstMismatch = fmt.Sprintf("replay: no handler body for %q", r.Handler)
			return res
		}
		if r.Type == RecordClaimed {
			h.Priority = r.Priority
		}
		registered[r.Handler] = true
		if err := eng.register(r.Line, lineMode[r.Line], h); err != nil {
			res.FirstMismatch = err.Error()
			return res
		}
	}

	ctx := context.Background()
	var lastAt time.Time
	for _, in := range inputs {
		mode := lineMode[in.Line]
		if in.At.After(lastAt) {
			lastAt = in.At
		}
		eng.step(ctx, in, mode, in.At)
	}
	// Settle all storms at a point after every observation window elapsed.
	eng.settleStorms(ctx, lastAt.Add(cfg.Storm.Window+time.Millisecond))
	eng.drain(ctx)

	got := eng.journal.Records()
	res.ReplayCount = len(got)
	if len(got) != len(original) {
		res.FirstMismatch = fmt.Sprintf("length: original=%d replay=%d",
			len(original), len(got))
		res.Match = false
		return res
	}
	for i := range original {
		if !recordsEqual(original[i], got[i]) {
			res.FirstMismatch = fmt.Sprintf("record %d:\n  original=%s\n  replay  =%s",
				i+1, original[i], got[i])
			res.Match = false
			return res
		}
	}
	res.Match = true
	return res
}

// recordsEqual compares only behaviorally relevant fields (timing fields such
// as wall-clock deadlines are intentionally excluded).
func recordsEqual(a, b Record) bool {
	return a.Seq == b.Seq &&
		a.Type == b.Type &&
		a.Line == b.Line &&
		a.Source == b.Source &&
		a.Handler == b.Handler &&
		a.Priority == b.Priority &&
		a.Coalesced == b.Coalesced &&
		a.Fault == b.Fault &&
		a.Nested == b.Nested
}
