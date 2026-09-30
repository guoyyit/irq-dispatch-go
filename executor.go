package irq

import (
	"context"
	"time"
)

// FaultKind classifies an isolated handler failure.
type FaultKind int

const (
	FaultNone FaultKind = iota
	FaultPanic
	FaultTimeout
)

// InvokeResult is the outcome of one isolated handler invocation.
type InvokeResult struct {
	Claimed  bool
	Fault    FaultKind
	PanicVal any
}

// invocation is one in-flight, isolated handler execution. The dispatcher
// keeps running while waiting on Done, so higher-priority events can preempt.
type invocation struct {
	h        *handlerState
	line     *lineState
	src      SourceID
	ev       Event
	done     chan InvokeResult
	notify   chan struct{} // closed once when done is populated
	cancel   context.CancelFunc
	deadline time.Time
	timedOut bool
}

// startInvocation launches h in its own goroutine with panic recovery and a
// timeout context. Faults are delivered on Done and never escape the
// goroutine, so the dispatch loop and co-located handlers stay isolated.
func startInvocation(parent context.Context, clock Clock, h *handlerState,
	line *lineState, src SourceID, ev Event, defaultTimeout time.Duration,
) *invocation {
	timeout := h.def.Timeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}

	ctx, cancel := context.WithCancel(parent)
	inv := &invocation{
		h:      h,
		line:   line,
		src:    src,
		ev:     ev,
		done:   make(chan InvokeResult, 1),
		notify: make(chan struct{}),
		cancel: cancel,
	}
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, timeout)
		inv.cancel = cancel
		inv.deadline = clock.Now().Add(timeout)
	}

	go func() {
		defer func() {
			if r := recover(); r != nil {
				inv.done <- InvokeResult{Fault: FaultPanic, PanicVal: r}
				close(inv.notify)
			}
		}()
		claimed := h.def.Handle(ctx, ev)
		// If the context deadline fired, a return at that point is a timeout
		// fault regardless of the handler's claimed value.
		if ctx.Err() == context.DeadlineExceeded {
			inv.done <- InvokeResult{Fault: FaultTimeout}
		} else {
			inv.done <- InvokeResult{Claimed: claimed}
		}
		close(inv.notify)
	}()

	return inv
}

// invokeSync runs one handler inline, recovering panics. Used by deterministic
// replay, where no goroutine scheduling is allowed in the delivery path.
func invokeSync(ctx context.Context, h Handler, ev Event) (result InvokeResult) {
	defer func() {
		if r := recover(); r != nil {
			result = InvokeResult{Fault: FaultPanic, PanicVal: r}
		}
	}()
	return InvokeResult{Claimed: h.Handle(ctx, ev)}
}
