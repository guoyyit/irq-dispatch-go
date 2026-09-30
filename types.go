package irq

import (
	"context"
	"time"
)

// TriggerMode selects edge- or level-triggered interrupt semantics.
type TriggerMode int

const (
	// EdgeTriggered coalesces repeated pulses into one pending interrupt.
	EdgeTriggered TriggerMode = iota
	// LevelTriggered reports once while the request level stays asserted.
	LevelTriggered
)

// String returns a human-readable trigger mode name.
func (m TriggerMode) String() string {
	switch m {
	case EdgeTriggered:
		return "edge"
	case LevelTriggered:
		return "level"
	default:
		return "unknown"
	}
}

// LineID identifies a shared interrupt line.
type LineID int

// SourceID identifies a virtual device raising interrupts.
type SourceID string

// HandlerID identifies a registered handler routine.
type HandlerID string

// Event is one interrupt occurrence offered to handlers.
type Event struct {
	// Seq is the global, monotonic sequence number of the event.
	Seq uint64
	// Line is the interrupt line the event was raised on.
	Line LineID
	// Source is the virtual device that raised the event.
	Source SourceID
	// Mode is the trigger semantics in effect for the line.
	Mode TriggerMode
	// Coalesced is how many raw raises were merged into this event.
	Coalesced int
}

// HandleFunc is a handler routine. It returns true if it claims the event.
type HandleFunc func(ctx context.Context, ev Event) bool

// Handler is a routine registered on one interrupt line.
type Handler struct {
	// ID uniquely identifies the handler.
	ID HandlerID
	// Priority orders handlers; higher runs first and may preempt lower.
	Priority int
	// Timeout bounds a single invocation; zero means no timeout.
	Timeout time.Duration
	// Handle is the routine body.
	Handle HandleFunc
}

// FaultPolicy configures faulty-handler isolation and recovery.
type FaultPolicy struct {
	// Timeout is the default per-handler invocation timeout.
	Timeout time.Duration
	// DisableAfter disables a handler after this many consecutive faults; 0 disables.
	DisableAfter int
	// RecoverAfter re-enables a disabled handler after this duration; 0 means never.
	RecoverAfter time.Duration
}

// StormPolicy configures burst detection and rate limiting per source.
type StormPolicy struct {
	// Window is the observation window for burst counting.
	Window time.Duration
	// Threshold is the max raises allowed within Window before a storm is declared.
	Threshold int
	// MaxBacklog bounds the pending depth for a storming source.
	MaxBacklog int
}

// Config is the full dispatcher configuration.
type Config struct {
	// Fault configures handler isolation and recovery.
	Fault FaultPolicy
	// Storm configures storm detection and rate limiting.
	Storm StormPolicy
}

// DefaultConfig returns sensible defaults: 100ms handler timeout, disable a handler after 3 consecutive faults, recover after 5s, and a storm threshold
// of 100 raises per 100ms window with a backlog cap of 1024.
func DefaultConfig() Config {
	return Config{
		Fault: FaultPolicy{
			Timeout:      2 * time.Second,
			DisableAfter: 3,
			RecoverAfter: 5 * time.Second,
		},
		Storm: StormPolicy{
			Window:     100 * time.Millisecond,
			Threshold:  100,
			MaxBacklog: 1024,
		},
	}
}
