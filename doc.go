// Package irq implements an in-process virtual device interrupt subsystem.
//
// Multiple virtual devices share a finite set of interrupt lines and raise
// asynchronous interrupts that are reliably dispatched to handler routines,
// with faulty-routine isolation, interrupt-storm rate limiting, deterministic
// ordering and replay, and non-blocking runtime metrics.
//
// # Model
//
// A Device is attached to one Line and uses one of two trigger semantics:
//
//   - EdgeTriggered: each Raise is a pulse. Pulses that arrive before the
//     current response completes are coalesced into a single pending
//     interrupt; existence is never lost (a follow-up delivery always runs),
//     and Event.Coalesced reports how many pulses were folded together.
//   - LevelTriggered: Raise asserts a request level. While the level stays
//     asserted it is reported exactly once. Release deasserts it, clearing the
//     reported state so a later Raise is reported again.
//
// # Shared lines and handler walk
//
// Any number of handlers may register on one line, ordered by descending
// Priority (registration order breaks ties). On an interrupt each enabled
// handler is asked in turn. A handler returns false to decline; the walk then
// continues to the next handler and a decline never blocks later handlers.
// The first handler to return true claims the event and the walk stops. If
// every handler declines, the event is recorded as a line-level rejection.
//
// # Priority nesting and masking
//
// While a handler is executing it establishes a mask at the line's effective
// priority (the highest-priority enabled handler). A strictly higher-priority
// interrupt preempts and runs in a nested mask (Record.Nested increments).
// Equal- or lower-priority interrupts are held in an ordered pending queue and
// are delivered after the mask unwinds, FIFO at equal priority. Held
// interrupts are delivered exactly once each (no loss, no duplication).
//
// # Fault isolation
//
// Each handler runs in its own goroutine with panic recovery and a timeout
// context. A panic or timeout is isolated to that handler: the walk proceeds
// to the next handler and the dispatch loop is never blocked or crashed.
// Faults are observable via the journal and metrics. FaultPolicy controls
// auto-disable (DisableAfter consecutive faults) and auto-recovery
// (RecoverAfter). Handlers should observe ctx cancellation so a timed-out
// routine unwinds instead of leaking its goroutine.
//
// # Storms and rate limiting
//
// StormPolicy sets a per-source sliding Window and Threshold. More than
// Threshold raises within a window declare a storm: subsequent pulses are
// coalesced into one aggregated delivery and the per-source backlog is capped
// at MaxBacklog (excess pulses are suppressed but interrupt existence is
// retained). When the window passes with no new pulses the storm settles and a
// final aggregated event is flushed, so the post-storm state is never lost.
//
// # Determinism and replay
//
// All delivery decisions run on one scheduler goroutine fed by an ordered
// mailbox, and the complete trajectory is appended to a Journal. The recorded
// input script can be replayed through a synchronous engine (Replay /
// ReplayWithConfig); identical inputs reproduce an identical, goroutine-free
// trajectory. Time-based effects (storm windows, timeouts) require the same
// Config; for a trajectory that is a pure function of inputs, disable storm
// windows (Storm.Threshold/Window = 0) and use non-blocking handlers.
//
// # Metrics
//
// Metrics exposes delivered/coalesced/suppressed/fault counters, current
// backlog and a latency distribution (min/avg/p50/p90/p99/max). Writers use
// atomics and a fixed-size lock-free latency ring, so collection never blocks
// the dispatch path; Snapshot is safe to call concurrently.
//
// # Configuration
//
// See Config, FaultPolicy, StormPolicy and DefaultConfig for all options:
// handler timeout, consecutive-fault disable threshold, recovery duration,
// storm observation window, burst threshold and bounded backlog depth.
package irq
