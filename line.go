package irq

import (
	"sync/atomic"
	"time"
)

// lineState holds per-line state: trigger mode, ordered handlers and the
// per-source pending bits of every device sharing the line.
type lineState struct {
	id       LineID
	mode     TriggerMode
	handlers []*handlerState
	sources  map[SourceID]*sourceState
}

// newLineState creates interrupt line state.
func newLineState(id LineID, mode TriggerMode) *lineState {
	return &lineState{
		id:      id,
		mode:    mode,
		sources: make(map[SourceID]*sourceState),
	}
}

// addHandler registers a handler, keeping the walk order sorted by descending
// priority (ties keep registration order). Duplicate IDs are rejected.
func (l *lineState) addHandler(h Handler) error {
	for _, existing := range l.handlers {
		if existing.def.ID == h.ID {
			return ErrDuplicateHandler
		}
	}
	hs := &handlerState{def: h}
	hs.enabled.Store(true)
	hs.disabledUntil.Store(time.Time{})
	pos := len(l.handlers)
	for i, existing := range l.handlers {
		if h.Priority > existing.def.Priority {
			pos = i
			break
		}
	}
	l.handlers = append(l.handlers, nil)
	copy(l.handlers[pos+1:], l.handlers[pos:])
	l.handlers[pos] = hs
	return nil
}

// source returns the state for src, creating it on first use.
func (l *lineState) source(src SourceID) *sourceState {
	s, ok := l.sources[src]
	if !ok {
		s = &sourceState{id: src}
		l.sources[src] = s
	}
	return s
}

// topPriority returns the priority of the highest-priority enabled handler,
// or the minimum int when no enabled handler exists.
func (l *lineState) topPriority() int {
	for _, h := range l.handlers {
		if h.enabled.Load() {
			return h.def.Priority
		}
	}
	return minInt
}

// enabledHandlers returns handlers eligible to be asked, in walk order.
func (l *lineState) enabledHandlers(now time.Time, recoverAfter time.Duration) []*handlerState {
	out := make([]*handlerState, 0, len(l.handlers))
	for _, h := range l.handlers {
		until, _ := h.disabledUntil.Load().(time.Time)
		if !h.enabled.Load() && !until.IsZero() && !now.Before(until) {
			h.enabled.Store(true)
			h.consec.Store(0)
			h.disabledUntil.Store(time.Time{})
		}
		if h.enabled.Load() {
			out = append(out, h)
		}
	}
	return out
}

const minInt = -int(^uint(0)>>1) - 1

// handlerState is a registered handler plus its fault-isolation status.
type handlerState struct {
	def Handler

	enabled       atomic.Bool
	consec        atomic.Int64
	disabledUntil atomic.Value
}

// noteFault records a fault and applies the auto-disable policy. It returns
// true if the fault caused the handler to be disabled.
func (h *handlerState) noteFault(now time.Time, disableAfter int, recoverAfter time.Duration) bool {
	c := h.consec.Add(1)
	if disableAfter > 0 && int(c) >= disableAfter {
		h.enabled.Store(false)
		if recoverAfter > 0 {
			h.disabledUntil.Store(now.Add(recoverAfter))
		} else {
			h.disabledUntil.Store(time.Time{})
		}
		return true
	}
	return false
}

// noteSuccess clears the consecutive-fault streak.
func (h *handlerState) noteSuccess() {
	h.consec.Store(0)
}

// recoverAt returns the configured auto-recovery deadline.
func (h *handlerState) recoverAt() time.Time {
	until, _ := h.disabledUntil.Load().(time.Time)
	return until
}

// sourceState tracks one device's pending interrupt state on its line.
type sourceState struct {
	id SourceID

	// Edge semantics:
	pending  bool // an edge arrived while busy/pending; existence must survive
	inflight bool // an edge delivery is currently walking handlers

	// Level semantics:
	asserted bool // request level currently held
	reported bool // the held level has already been reported

	// merged counts raises coalesced into the next delivery.
	merged int
}
