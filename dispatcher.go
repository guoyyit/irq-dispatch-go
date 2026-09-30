package irq

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"time"
)

// delivery is one source interrupt ready to walk the handlers of its line.
type delivery struct {
	line      *lineState
	src       SourceID
	prio      int
	coalesced int
	raisedAt  time.Time
}

// walk is a handler chain in flight for one source event.
type walk struct {
	d      delivery
	inv    *invocation
	cur    *handlerState
	nested int
}

// schedulerState is all mutable state owned by the single scheduler goroutine.
type schedulerState struct {
	storm      *stormTracker
	pending    []delivery
	active     []*walk
	maskPrio   int
	maskDepth  int
	timer      *time.Timer
	timerFired bool
	nextTimer  time.Time
}

// Common dispatcher errors.
var (
	ErrDispatcherStopped = errors.New("irq: dispatcher stopped")
	ErrAlreadyStarted    = errors.New("irq: dispatcher already started")
	ErrLineConflict      = errors.New("irq: line trigger mode conflict")
	ErrDuplicateHandler  = errors.New("irq: duplicate handler id")
)

// Dispatcher accepts raised interrupts and delivers them to handlers.
type Dispatcher struct {
	cfg     Config
	clock   Clock
	journal *Journal
	metrics *Metrics

	mu    sync.Mutex
	lines map[LineID]*lineState

	// inputs serializes external raises/releases into a total order.
	in chan inputMsg
	// wake is signaled after every post so the scheduler can wake without
	// consuming from in (preserving the ordered drain performed by pump).
	wake chan struct{}
	// inputs is the ordered external signal script used for deterministic replay.
	inputLog []ReplayInput

	startOnce  sync.Once
	stopOnce   sync.Once
	running    bool
	closed     bool
	wg         sync.WaitGroup
	rootCtx    context.Context
	rootCancel context.CancelFunc
}

// inputMsg is one externally-raised signal handed to the scheduler.
type inputMsg struct {
	release bool
	line    LineID
	src     SourceID
	at      time.Time
}

// New creates a dispatcher with the given configuration.
func New(cfg Config) *Dispatcher {
	return newDispatcher(cfg, wallClock{})
}

func newDispatcher(cfg Config, clock Clock) *Dispatcher {
	return &Dispatcher{
		cfg:     cfg,
		clock:   clock,
		journal: newJournal(),
		metrics: newMetrics(),
		lines:   make(map[LineID]*lineState),
		in:      make(chan inputMsg, 1024),
		wake:    make(chan struct{}, 1),
	}
}

// Register adds a handler to a line, creating the line on first use.
func (d *Dispatcher) Register(line LineID, mode TriggerMode, h Handler) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.running {
		// Registration after start is allowed only on lines that already
		// exist with the same mode, or brand new lines.
	}
	l := d.lines[line]
	if l == nil {
		l = newLineState(line, mode)
		d.lines[line] = l
	} else if l.mode != mode {
		return ErrLineConflict
	}
	return l.addHandler(h)
}

// Start runs the dispatch loop.
func (d *Dispatcher) Start() error {
	already := true
	d.startOnce.Do(func() {
		already = false
		d.mu.Lock()
		d.running = true
		d.closed = false
		d.rootCtx, d.rootCancel = context.WithCancel(context.Background())
		d.mu.Unlock()
		d.wg.Add(1)
		go d.scheduler(d.rootCtx)
	})
	if already {
		return ErrAlreadyStarted
	}
	return nil
}

// Stop halts the dispatch loop and drains pending work.
func (d *Dispatcher) Stop(ctx context.Context) error {
	d.stopOnce.Do(func() {
		d.mu.Lock()
		d.closed = true
		d.mu.Unlock()
		d.rootCancel()
	})
	done := make(chan struct{})
	go func() {
		d.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Raise posts an interrupt from src on the given line.
func (d *Dispatcher) Raise(line LineID, src SourceID) {
	d.post(inputMsg{line: line, src: src, at: d.clock.Now()})
}

// Release clears a level-triggered request from src.
func (d *Dispatcher) Release(line LineID, src SourceID) {
	d.post(inputMsg{release: true, line: line, src: src, at: d.clock.Now()})
}

func (d *Dispatcher) post(m inputMsg) {
	d.mu.Lock()
	closed := d.closed
	d.inputLog = append(d.inputLog, ReplayInput{
		Release: m.release,
		Line:    m.line,
		Source:  m.src,
		At:      m.at,
	})
	d.mu.Unlock()
	if closed {
		return
	}
	select {
	case d.in <- m:
	case <-d.rootCtx.Done():
		return
	}
	select {
	case d.wake <- struct{}{}:
	default:
	}
}

// Journal exposes the deterministic event log.
func (d *Dispatcher) Journal() *Journal { return d.journal }

// Metrics exposes runtime metrics.
func (d *Dispatcher) Metrics() *Metrics { return d.metrics }

// Inputs returns a copy of the ordered external raise/release script.
func (d *Dispatcher) Inputs() []ReplayInput {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]ReplayInput, len(d.inputLog))
	copy(out, d.inputLog)
	return out
}

// Replay re-runs the recorded input script through a synchronous engine and
// reports whether the resulting trajectory equals the live journal.
func (d *Dispatcher) Replay(handlerFunc func(HandlerID) (Handler, bool)) ReplayResult {
	return ReplayWithConfig(d.cfg, d.Journal().Records(), d.Inputs(), handlerFunc)
}

// HandlerStatus reports observable fault-isolation state for one handler.
type HandlerStatus struct {
	Line         LineID
	ID           HandlerID
	Enabled      bool
	ConsecFaults int
	RecoverAt    time.Time
}

// HandlerStatuses returns a snapshot of all registered handler states.
func (d *Dispatcher) HandlerStatuses() []HandlerStatus {
	d.mu.Lock()
	defer d.mu.Unlock()
	var out []HandlerStatus
	for _, l := range d.lines {
		for _, h := range l.handlers {
			out = append(out, HandlerStatus{
				Line:         l.id,
				ID:           h.def.ID,
				Enabled:      h.enabled.Load(),
				ConsecFaults: int(h.consec.Load()),
				RecoverAt:    h.recoverAt(),
			})
		}
	}
	return out
}

// scheduler is the single goroutine that owns all line/walk state. Serializing
// delivery decisions here is what makes the global dispatch trajectory a pure
// function of the ordered input stream (plus deterministic handler results).
func (d *Dispatcher) scheduler(ctx context.Context) {
	defer d.wg.Done()

	st := &schedulerState{
		storm: newStormTracker(d.cfg.Storm),
	}

	for {
		d.pump(ctx, st)

		d.metrics.setBacklog(len(st.pending) + st.storm.totalBacklog())
		now := d.clock.Now()

		// Deterministic completion intake: natural completions are drained
		// first, in walk-creation order, so a handler that finishes exactly at
		// its deadline is claimed normally rather than faulted.
		if d.drainCompletions(ctx, st) {
			continue
		}

		// Remaining in-flight invocations past their deadline are timed out.
		timedOutAny := false
		for _, w := range st.active {
			if w.inv != nil && !w.inv.timedOut && !w.inv.deadline.IsZero() &&
				!now.Before(w.inv.deadline) {
				w.inv.timedOut = true
				timedOutAny = true
			}
		}
		if timedOutAny {
			d.drainCompletions(ctx, st)
			continue
		}

		// Settle storms whose quiet window elapsed and flush final states.
		if settled := st.storm.settle(now); len(settled) > 0 {
			for _, src := range settled {
				d.flushAfterStorm(st, src)
			}
			continue
		}

		// Opportunistically evaluate handler auto-recovery windows even when
		// no walk is in flight, so recovered routines become observable.
		d.recoverHandlers(now)

		if ctx.Err() != nil {
			// Drain pending deliveries best-effort; leave once active handlers
			// have unwound after their context canceled.
			if len(st.active) == 0 {
				d.scheduleAll(ctx, st)
				if len(st.active) == 0 {
					return
				}
				continue
			}
		}

		if !d.wait(ctx, st) {
			continue
		}
	}
}

// drainCompletions consumes every currently-ready invocation result, in walk
// order. Timed-out invocations synthesize a fault without blocking.
func (d *Dispatcher) drainCompletions(ctx context.Context, st *schedulerState) bool {
	advanced := false
	for i := 0; i < len(st.active); i++ {
		w := st.active[i]
		if w.inv == nil {
			continue
		}
		if w.inv.timedOut {
			// Detach: any late natural result is dropped on the buffered chan.
			w.inv.cancel()
			late := w.inv
			go func() { <-late.done }()
			w.inv = nil
			d.finishInvocation(ctx, st, w, InvokeResult{Fault: FaultTimeout})
			advanced = true
			i--
			continue
		}
		select {
		case res := <-w.inv.done:
			w.inv = nil
			d.finishInvocation(ctx, st, w, res)
			advanced = true
			i--
		default:
		}
	}
	return advanced
}

// pump drains all currently-queued inputs and starts all eligible deliveries.
func (d *Dispatcher) pump(ctx context.Context, st *schedulerState) {
	for {
		select {
		case m := <-d.in:
			if m.release {
				d.handleRelease(st, m)
			} else {
				d.handleRaise(st, m)
			}
		default:
			d.scheduleAll(ctx, st)
			d.metrics.setBacklog(len(st.pending) + st.storm.totalBacklog())
			return
		}
	}
}

// handleRaise applies the trigger-semantics transition for one raise.
func (d *Dispatcher) handleRaise(st *schedulerState, m inputMsg) {
	d.mu.Lock()
	l := d.lines[m.line]
	d.mu.Unlock()
	if l == nil {
		l = newLineState(m.line, EdgeTriggered)
		d.mu.Lock()
		d.lines[m.line] = l
		d.mu.Unlock()
	}
	s := l.source(m.src)

	if l.mode == LevelTriggered {
		if !s.asserted {
			s.asserted = true
			s.reported = false
			d.journal.append(Record{Type: RecordRaised, Line: m.line, Source: m.src})
			d.enqueue(st, l, m.src, m.at)
			return
		}
		// Re-assertion while already held and pending/active: coalesce.
		s.merged++
		d.metrics.recordCoalesced(1)
		d.journal.append(Record{Type: RecordCoalesced, Line: m.line, Source: m.src, Coalesced: s.merged})
		return
	}

	// Edge-triggered.
	decision := st.storm.observe(m.src, m.at)
	if s.inflight {
		// Any pulse during delivery preserves existence for exactly one
		// follow-up; pending is cleared when that follow-up starts walking, so
		// pulses arriving later in time naturally form another follow-up.
		s.pending = true
		switch decision {
		case stormSuppress:
			d.metrics.recordSuppressed(1)
			d.journal.append(Record{Type: RecordSuppressed, Line: m.line, Source: m.src})
		default:
			s.merged++
			d.metrics.recordCoalesced(1)
			d.journal.append(Record{Type: RecordCoalesced, Line: m.line, Source: m.src, Coalesced: s.merged})
		}
		return
	}
	if (decision == stormCoalesce || decision == stormSuppress) &&
		st.storm.isStorming(m.src) {
		if qi := d.queuedIndex(st, m.line, m.src); qi >= 0 {
			if decision == stormSuppress {
				d.metrics.recordSuppressed(1)
				d.journal.append(Record{Type: RecordSuppressed, Line: m.line, Source: m.src})
			} else {
				st.pending[qi].coalesced++
				d.metrics.recordCoalesced(1)
				d.journal.append(Record{Type: RecordCoalesced, Line: m.line, Source: m.src,
					Coalesced: st.pending[qi].coalesced})
			}
			return
		}
	}
	switch decision {
	case stormSuppress:
		// Backlog cap reached: existence retained via pending/merged counters.
		s.pending = true
		d.metrics.recordSuppressed(1)
		d.journal.append(Record{Type: RecordSuppressed, Line: m.line, Source: m.src})
	case stormCoalesce:
		s.pending = true
		s.merged++
		d.metrics.recordCoalesced(1)
		d.journal.append(Record{Type: RecordCoalesced, Line: m.line, Source: m.src, Coalesced: s.merged})
	default:
		d.journal.append(Record{Type: RecordRaised, Line: m.line, Source: m.src})
		d.enqueue(st, l, m.src, m.at)
	}
}

// handleRelease clears a level request.
func (d *Dispatcher) handleRelease(st *schedulerState, m inputMsg) {
	d.mu.Lock()
	l := d.lines[m.line]
	d.mu.Unlock()
	if l == nil || l.mode != LevelTriggered {
		return
	}
	s := l.source(m.src)
	s.asserted = false
	s.reported = false
	s.pending = false
	s.merged = 0
	d.journal.append(Record{Type: RecordReleased, Line: m.line, Source: m.src})
}

// enqueue adds a source delivery to the ordered pending queue.
func (d *Dispatcher) enqueue(st *schedulerState, l *lineState, src SourceID, at time.Time) {
	st.pending = append(st.pending, delivery{
		line:     l,
		src:      src,
		prio:     l.topPriority(),
		raisedAt: at,
	})
}

// maskLevel returns the priority currently being serviced. Pending events at or
// below this level are held for later post-unmask delivery; strictly higher
// priorities preempt and enter a nested mask.
func maskLevel(active []*walk) int {
	level := minInt
	for _, w := range active {
		if w.d.prio > level {
			level = w.d.prio
		}
	}
	return level
}

// scheduleAll starts every pending delivery allowed by the current mask.
// Eligibility: strictly higher priority than the current mask, or no active
// handler at all. Held items keep their queue position (FIFO at equal prio).
func (d *Dispatcher) scheduleAll(ctx context.Context, st *schedulerState) {
	for {
		level := maskLevel(st.active)
		idx := -1
		for i, dlv := range st.pending {
			if len(st.active) == 0 || dlv.prio > level {
				// Same source may not have two overlapping walks.
				if d.isSourceActive(st, dlv.line.id, dlv.src) {
					continue
				}
				idx = i
				break
			}
		}
		if idx < 0 {
			return
		}
		dlv := st.pending[idx]
		st.pending = append(st.pending[:idx], st.pending[idx+1:]...)
		d.startWalk(ctx, st, dlv)
	}
}

func (d *Dispatcher) isSourceActive(st *schedulerState, line LineID, src SourceID) bool {
	for _, w := range st.active {
		if w.d.line.id == line && w.d.src == src {
			return true
		}
	}
	return false
}

// queuedIndex returns the pending-queue index for src, or -1.
func (d *Dispatcher) queuedIndex(st *schedulerState, line LineID, src SourceID) int {
	for i := range st.pending {
		if st.pending[i].line.id == line && st.pending[i].src == src {
			return i
		}
	}
	return -1
}

// startWalk begins asking handlers of dlv.line in priority/registration order.
func (d *Dispatcher) startWalk(ctx context.Context, st *schedulerState, dlv delivery) {
	now := d.clock.Now()
	s := dlv.line.source(dlv.src)
	if dlv.line.mode == EdgeTriggered {
		s.inflight = true
		s.pending = false
	} else {
		s.reported = true
	}

	nested := len(st.active)
	w := &walk{d: dlv, nested: nested}
	st.active = append(st.active, w)

	d.journal.append(Record{
		Type:      RecordDelivered,
		Line:      dlv.line.id,
		Source:    dlv.src,
		Priority:  dlv.prio,
		Nested:    nested,
		Coalesced: dlv.coalesced,
	})
	d.metrics.recordDelivered()
	d.metrics.RecordLatency(now.Sub(dlv.raisedAt))

	d.advanceWalk(ctx, st, w)
}

// advanceWalk asks the next enabled handler; if none remains the event is
// rejected by the whole line.
func (d *Dispatcher) advanceWalk(ctx context.Context, st *schedulerState, w *walk) {
	h := d.nextHandler(w, nil)
	d.invokeHandler(ctx, st, w, h)
}

// finishInvocation handles one handler result: claim moves on; decline asks the
// next handler; fault isolates the routine and continues the chain.
func (d *Dispatcher) finishInvocation(ctx context.Context, st *schedulerState, w *walk, res InvokeResult) {
	now := d.clock.Now()
	h := w.cur
	w.inv = nil
	w.cur = nil

	switch {
	case res.Fault == FaultPanic || res.Fault == FaultTimeout:
		d.metrics.recordFault()
		disabled := h.noteFault(now, d.cfg.Fault.DisableAfter, d.cfg.Fault.RecoverAfter)
		detail := "panic"
		if res.Fault == FaultTimeout {
			detail = "timeout"
		}
		if disabled {
			detail += ":disabled"
		}
		d.journal.append(Record{
			Type:    RecordFault,
			Line:    w.d.line.id,
			Source:  w.d.src,
			Handler: h.def.ID,
			Fault:   res.Fault,
			Detail:  detail,
		})
		// A faulty handler never blocks the chain: ask the next one.
		d.invokeHandler(ctx, st, w, d.nextHandler(w, h))
	case res.Claimed:
		h.noteSuccess()
		d.journal.append(Record{
			Type:     RecordClaimed,
			Line:     w.d.line.id,
			Source:   w.d.src,
			Handler:  h.def.ID,
			Priority: h.def.Priority,
		})
		d.completeWalk(st, w)
	default:
		d.journal.append(Record{
			Type:    RecordRejected,
			Line:    w.d.line.id,
			Source:  w.d.src,
			Handler: h.def.ID,
		})
		d.invokeHandler(ctx, st, w, d.nextHandler(w, h))
	}
}

// nextHandler returns the first enabled handler after prev (by identity), or
// nil when no further handler can be asked.
func (d *Dispatcher) nextHandler(w *walk, prev *handlerState) *handlerState {
	now := d.clock.Now()
	handlers := w.d.line.enabledHandlers(now, d.cfg.Fault.RecoverAfter)
	if prev == nil {
		if len(handlers) > 0 {
			return handlers[0]
		}
		return nil
	}
	past := false
	for _, cand := range handlers {
		if past {
			return cand
		}
		if cand == prev {
			past = true
		}
	}
	return nil
}

// invokeHandler asks h on behalf of w, or rejects the event if the chain ended.
func (d *Dispatcher) invokeHandler(ctx context.Context, st *schedulerState, w *walk, h *handlerState) {
	if h == nil {
		d.journal.append(Record{
			Type:   RecordRejected,
			Line:   w.d.line.id,
			Source: w.d.src,
		})
		d.completeWalk(st, w)
		return
	}
	ev := Event{
		Line:      w.d.line.id,
		Source:    w.d.src,
		Mode:      w.d.line.mode,
		Coalesced: w.d.coalesced,
	}
	w.cur = h
	w.inv = startInvocation(ctx, d.clock, h, w.d.line, w.d.src, ev, d.cfg.Fault.Timeout)
}

// completeWalk removes a finished walk and re-arms edge pending existence or
// level deassertion semantics.
func (d *Dispatcher) completeWalk(st *schedulerState, w *walk) {
	s := w.d.line.source(w.d.src)
	switch w.d.line.mode {
	case EdgeTriggered:
		s.inflight = false
		if s.pending {
			// Existence survived: deliver exactly one follow-up event carrying
			// the merged pulse count.
			s.pending = false
			merged := s.merged
			s.merged = 0
			idx := -1
			for i, dlv := range st.pending {
				if dlv.line.id == w.d.line.id && dlv.src == w.d.src {
					idx = i
					break
				}
			}
			dlv := delivery{
				line:      w.d.line,
				src:       w.d.src,
				prio:      w.d.line.topPriority(),
				coalesced: merged,
				raisedAt:  d.clock.Now(),
			}
			if idx >= 0 {
				st.pending[idx] = dlv
			} else {
				st.pending = append(st.pending, dlv)
			}
		}
	case LevelTriggered:
		if !s.asserted {
			s.reported = false
		}
		// If still asserted, it remains reported; a release cleared it above.
	}

	for i, active := range st.active {
		if active == w {
			st.active = append(st.active[:i], st.active[i+1:]...)
			break
		}
	}
}

// flushAfterStorm enqueues the final aggregated event when a storm quiets so
// the post-storm state is never lost.
func (d *Dispatcher) flushAfterStorm(st *schedulerState, src SourceID) {
	merged := st.storm.drainBacklog(src)
	if merged <= 0 {
		return
	}
	d.mu.Lock()
	var l *lineState
	for _, cand := range d.lines {
		if _, ok := cand.sources[src]; ok {
			l = cand
			break
		}
	}
	d.mu.Unlock()
	if l == nil {
		return
	}
	s := l.source(src)
	if l.mode == EdgeTriggered && !s.inflight {
		d.enqueue(st, l, src, d.clock.Now())
		st.pending[len(st.pending)-1].coalesced = merged
	}
}

// recoverHandlers re-enables handlers whose recovery window has elapsed.
func (d *Dispatcher) recoverHandlers(now time.Time) {
	if d.cfg.Fault.RecoverAfter <= 0 {
		return
	}
	d.mu.Lock()
	lines := make([]*lineState, 0, len(d.lines))
	for _, l := range d.lines {
		lines = append(lines, l)
	}
	d.mu.Unlock()
	for _, l := range lines {
		l.enabledHandlers(now, d.cfg.Fault.RecoverAfter)
	}
}

// wait blocks until external input or an in-flight invocation completes. It
// never selects among multiple invocation results directly; ready results are
// drained deterministically by the scheduler loop after wake-up.
func (d *Dispatcher) wait(ctx context.Context, st *schedulerState) bool {
	cases := []reflect.SelectCase{
		{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ctx.Done())},
		{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(d.wake)},
	}
	for _, w := range st.active {
		if w.inv != nil {
			cases = append(cases, reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(w.inv.notify),
			})
		}
	}

	// Also wake periodically so wall-clock deadlines and storm quiet windows
	// are observed even with no other events.
	interval := 5 * time.Millisecond
	if d.cfg.Storm.Window > 0 && d.cfg.Storm.Window < interval {
		interval = d.cfg.Storm.Window / 4
	}
	if interval <= 0 {
		interval = time.Millisecond
	}
	timer := time.NewTimer(interval)
	defer timer.Stop()
	cases = append(cases, reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(timer.C),
	})

	chosen, _, _ := reflect.Select(cases)
	return chosen != 0
}
