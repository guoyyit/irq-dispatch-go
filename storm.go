package irq

import "time"

// stormDecision tells the dispatcher how to treat one observed raise.
type stormDecision int

const (
	// stormPass delivers the raise as a normal interrupt event.
	stormPass stormDecision = iota
	// stormCoalesce merges the raise into the source's pending delivery.
	stormCoalesce
	// stormSuppress drops the raise due to a full backlog; the existence of a
	// pending interrupt is still retained, so final state is never lost.
	stormSuppress
)

// stormTracker detects per-source bursts and bounds backlog.
type stormTracker struct {
	policy  StormPolicy
	sources map[SourceID]*stormSource
}

// stormSource is the sliding-window burst state for one source.
type stormSource struct {
	// times are raise timestamps inside the current observation window.
	times []time.Time
	// active marks a storm currently in progress.
	active bool
	// backlog counts raises merged while the storm lasts (bounded).
	backlog int
	// quietAt is when the source is considered storm-free if no raise arrives.
	quietAt time.Time
}

// newStormTracker creates a tracker from policy.
func newStormTracker(p StormPolicy) *stormTracker {
	return &stormTracker{
		policy:  p,
		sources: make(map[SourceID]*stormSource),
	}
}

// observe records one raise from src at now and returns how to treat it.
func (t *stormTracker) observe(src SourceID, now time.Time) stormDecision {
	if t.policy.Threshold <= 0 || t.policy.Window <= 0 {
		return stormPass
	}
	s := t.sources[src]
	if s == nil {
		s = &stormSource{}
		t.sources[src] = s
	}

	cutoff := now.Add(-t.policy.Window)
	kept := s.times[:0]
	for _, ts := range s.times {
		if ts.After(cutoff) {
			kept = append(kept, ts)
		}
	}
	s.times = append(kept, now)

	if s.active {
		s.quietAt = now.Add(t.policy.Window)
		s.backlog++
		if t.policy.MaxBacklog > 0 && s.backlog > t.policy.MaxBacklog {
			s.backlog = t.policy.MaxBacklog
			return stormSuppress
		}
		return stormCoalesce
	}

	if len(s.times) > t.policy.Threshold {
		s.active = true
		s.quietAt = now.Add(t.policy.Window)
		s.backlog++
		return stormCoalesce
	}
	return stormPass
}

// settle ends storms whose observation window has elapsed. Sources that became
// quiet are returned so the dispatcher can flush their final merged state.
func (t *stormTracker) settle(now time.Time) []SourceID {
	var quiet []SourceID
	for src, s := range t.sources {
		if s.active && !now.Before(s.quietAt) {
			s.active = false
			s.times = s.times[:0]
			quiet = append(quiet, src)
		}
	}
	return quiet
}

// drainBacklog returns and resets the number of raises merged during a storm.
func (t *stormTracker) drainBacklog(src SourceID) int {
	s := t.sources[src]
	if s == nil {
		return 0
	}
	n := s.backlog
	s.backlog = 0
	return n
}

// isStorming reports whether src is currently in a storm.
func (t *stormTracker) isStorming(src SourceID) bool {
	s := t.sources[src]
	return s != nil && s.active
}

// backlog returns the current merged-but-not-delivered depth for src.
func (t *stormTracker) backlog(src SourceID) int {
	s := t.sources[src]
	if s == nil {
		return 0
	}
	return s.backlog
}

// totalBacklog sums every source's undelivered merged depth.
func (t *stormTracker) totalBacklog() int {
	n := 0
	for _, s := range t.sources {
		n += s.backlog
	}
	return n
}
