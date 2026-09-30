package irq

import (
	"context"
	"time"
)

// replayEngine is a synchronous, single-threaded re-implementation of the
// delivery state machine used only for deterministic replay. It mirrors the
// live scheduler's trigger, walk-order and masking rules but executes handlers
// inline, so there is no goroutine scheduling and no wall clock involved.
type replayEngine struct {
	lines    map[LineID]*lineState
	lineMode map[LineID]TriggerMode
	pending  []delivery
	journal  *Journal
	storm    *stormTracker
	cfg      Config
}

func newReplayEngine() *replayEngine {
	return &replayEngine{
		lines:    make(map[LineID]*lineState),
		lineMode: make(map[LineID]TriggerMode),
		journal:  newJournal(),
		storm:    newStormTracker(DefaultConfig().Storm),
	}
}

func newReplayEngineWithConfig(cfg Config) *replayEngine {
	e := newReplayEngine()
	e.cfg = cfg
	e.storm = newStormTracker(cfg.Storm)
	return e
}

func (e *replayEngine) register(line LineID, mode TriggerMode, h Handler) error {
	l := e.lines[line]
	if l == nil {
		l = newLineState(line, mode)
		e.lines[line] = l
		e.lineMode[line] = mode
	} else if l.mode != mode {
		return ErrLineConflict
	}
	return l.addHandler(h)
}

func (e *replayEngine) lineFor(line LineID, mode TriggerMode) *lineState {
	l := e.lines[line]
	if l == nil {
		l = newLineState(line, mode)
		e.lines[line] = l
		e.lineMode[line] = mode
	}
	return l
}

// step applies one input signal (raise/release) at time at.
func (e *replayEngine) step(ctx context.Context, in ReplayInput, mode TriggerMode, at time.Time) {
	l := e.lineFor(in.Line, mode)
	s := l.source(in.Source)

	if in.Release {
		if mode != LevelTriggered {
			return
		}
		s.asserted = false
		s.reported = false
		s.pending = false
		s.merged = 0
		e.journal.append(Record{Type: RecordReleased, Line: in.Line, Source: in.Source})
		return
	}

	if mode == LevelTriggered {
		if !s.asserted {
			s.asserted = true
			s.reported = false
			e.journal.append(Record{Type: RecordRaised, Line: in.Line, Source: in.Source})
			e.pending = append(e.pending, delivery{line: l, src: in.Source, prio: l.topPriority()})
		} else {
			s.merged++
			e.journal.append(Record{Type: RecordCoalesced, Line: in.Line, Source: in.Source, Coalesced: s.merged})
		}
		return
	}

	// Edge-triggered with the same storm rules as the live scheduler.
	decision := e.storm.observe(in.Source, at)
	if s.inflight {
		s.pending = true
		if decision == stormSuppress {
			e.journal.append(Record{Type: RecordSuppressed, Line: in.Line, Source: in.Source})
		} else {
			s.merged++
			e.journal.append(Record{Type: RecordCoalesced, Line: in.Line, Source: in.Source, Coalesced: s.merged})
		}
		return
	}
	if (decision == stormCoalesce || decision == stormSuppress) && e.storm.isStorming(in.Source) {
		if qi := e.queuedIndex(in.Line, in.Source); qi >= 0 {
			if decision == stormSuppress {
				e.journal.append(Record{Type: RecordSuppressed, Line: in.Line, Source: in.Source})
			} else {
				e.pending[qi].coalesced++
				e.journal.append(Record{Type: RecordCoalesced, Line: in.Line, Source: in.Source,
					Coalesced: e.pending[qi].coalesced})
			}
			return
		}
	}
	switch decision {
	case stormSuppress:
		s.pending = true
		e.journal.append(Record{Type: RecordSuppressed, Line: in.Line, Source: in.Source})
	case stormCoalesce:
		s.pending = true
		s.merged++
		e.journal.append(Record{Type: RecordCoalesced, Line: in.Line, Source: in.Source, Coalesced: s.merged})
	default:
		e.journal.append(Record{Type: RecordRaised, Line: in.Line, Source: in.Source})
		e.pending = append(e.pending, delivery{line: l, src: in.Source, prio: l.topPriority()})
	}
}

// drain walks all pending deliveries to completion, honoring priority nesting
// deterministically: higher priorities first, held equal/lower flushed after.
func (e *replayEngine) drain(ctx context.Context) {
	for len(e.pending) > 0 {
		idx := e.pick(0)
		if idx < 0 {
			// Only same/lower priority items remain; deliver them in FIFO.
			idx = 0
		}
		dlv := e.pending[idx]
		e.pending = append(e.pending[:idx], e.pending[idx+1:]...)
		e.deliver(ctx, dlv, 0)
	}
}

// settleStorms ends all quiet storms and enqueues the final aggregated event
// for each, mirroring the live scheduler's post-storm flush.
func (e *replayEngine) settleStorms(ctx context.Context, now time.Time) {
	for _, src := range e.storm.settle(now) {
		merged := e.storm.drainBacklog(src)
		if merged <= 0 {
			continue
		}
		for _, l := range e.lines {
			if s, ok := l.sources[src]; ok {
				if l.mode == EdgeTriggered && !s.inflight {
					e.pending = append(e.pending, delivery{
						line:      l,
						src:       src,
						prio:      l.topPriority(),
						coalesced: merged,
					})
				}
				break
			}
		}
	}
}

// pick returns the index of the highest-priority pending delivery strictly
// above mask, honoring FIFO among equal priorities.
func (e *replayEngine) pick(mask int) int {
	best := -1
	for i, dlv := range e.pending {
		if dlv.prio > mask && (best < 0 || dlv.prio > e.pending[best].prio) {
			best = i
		}
	}
	return best
}

// queuedIndex returns the pending-queue index for src, or -1.
func (e *replayEngine) queuedIndex(line LineID, src SourceID) int {
	for i := range e.pending {
		if e.pending[i].line.id == line && e.pending[i].src == src {
			return i
		}
	}
	return -1
}

// deliver walks one delivery's handler chain synchronously. Higher-priority
// interrupts that become pending mid-walk are recursively nested first.
func (e *replayEngine) deliver(ctx context.Context, dlv delivery, nested int) {
	s := dlv.line.source(dlv.src)
	if dlv.line.mode == EdgeTriggered {
		s.inflight = true
		s.pending = false
	} else {
		s.reported = true
	}

	e.journal.append(Record{
		Type:      RecordDelivered,
		Line:      dlv.line.id,
		Source:    dlv.src,
		Priority:  dlv.prio,
		Nested:    nested,
		Coalesced: dlv.coalesced,
	})

	for _, h := range dlv.line.handlers {
		if !h.enabled.Load() {
			continue
		}
		ev := Event{
			Line:      dlv.line.id,
			Source:    dlv.src,
			Mode:      dlv.line.mode,
			Coalesced: dlv.coalesced,
		}
		res := invokeSync(ctx, h.def, ev)

		// Nested high-priority work that the handler triggered is drained at
		// the current mask before observing this handler's result, matching
		// live preemption semantics for scripts that raise inside handlers.
		if hi := e.pick(dlv.prio); hi >= 0 {
			next := e.pending[hi]
			e.pending = append(e.pending[:hi], e.pending[hi+1:]...)
			e.deliver(ctx, next, nested+1)
		}

		switch {
		case res.Fault != FaultNone:
			h.noteFault(time.Time{}, 0, 0)
			e.journal.append(Record{
				Type:    RecordFault,
				Line:    dlv.line.id,
				Source:  dlv.src,
				Handler: h.def.ID,
				Fault:   res.Fault,
			})
			continue
		case res.Claimed:
			e.journal.append(Record{
				Type:     RecordClaimed,
				Line:     dlv.line.id,
				Source:   dlv.src,
				Handler:  h.def.ID,
				Priority: h.def.Priority,
			})
			e.finishEdge(dlv)
			return
		default:
			e.journal.append(Record{
				Type:    RecordRejected,
				Line:    dlv.line.id,
				Source:  dlv.src,
				Handler: h.def.ID,
			})
		}
	}

	e.journal.append(Record{
		Type:   RecordRejected,
		Line:   dlv.line.id,
		Source: dlv.src,
	})
	e.finishEdge(dlv)
}

func (e *replayEngine) finishEdge(dlv delivery) {
	s := dlv.line.source(dlv.src)
	if dlv.line.mode == EdgeTriggered {
		s.inflight = false
		if s.pending {
			s.pending = false
			merged := s.merged
			s.merged = 0
			e.pending = append(e.pending, delivery{
				line:      dlv.line,
				src:       dlv.src,
				prio:      dlv.line.topPriority(),
				coalesced: merged,
			})
		}
	}
}
