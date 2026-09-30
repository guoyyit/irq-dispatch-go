package irq

import (
	"fmt"
	"strings"
	"sync"
)

// RecordType classifies a journal record.
type RecordType int

const (
	RecordRaised RecordType = iota
	RecordReleased
	RecordDelivered
	RecordClaimed
	RecordRejected
	RecordFault
	RecordCoalesced
	RecordSuppressed
)

// String returns a stable short name for the record type.
func (t RecordType) String() string {
	switch t {
	case RecordRaised:
		return "raised"
	case RecordReleased:
		return "released"
	case RecordDelivered:
		return "delivered"
	case RecordClaimed:
		return "claimed"
	case RecordRejected:
		return "rejected"
	case RecordFault:
		return "fault"
	case RecordCoalesced:
		return "coalesced"
	case RecordSuppressed:
		return "suppressed"
	default:
		return "unknown"
	}
}

// Record is one entry in the deterministic event log.
type Record struct {
	Seq       uint64
	Type      RecordType
	Line      LineID
	Source    SourceID
	Handler   HandlerID
	Priority  int
	Coalesced int
	Fault     FaultKind
	Nested    int
	Detail    string
}

// String renders the record as a single deterministic text line.
func (r Record) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%06d %-10s line=%d src=%s", r.Seq, r.Type, r.Line, r.Source)
	if r.Handler != "" {
		fmt.Fprintf(&b, " handler=%s", r.Handler)
	}
	if r.Priority != 0 {
		fmt.Fprintf(&b, " prio=%d", r.Priority)
	}
	if r.Nested > 0 {
		fmt.Fprintf(&b, " nested=%d", r.Nested)
	}
	if r.Coalesced > 0 {
		fmt.Fprintf(&b, " coalesced=%d", r.Coalesced)
	}
	if r.Fault != FaultNone {
		fmt.Fprintf(&b, " fault=%d", r.Fault)
	}
	if r.Detail != "" {
		fmt.Fprintf(&b, " %s", r.Detail)
	}
	return b.String()
}

// Journal records the full interrupt trajectory for replay.
//
// All dispatcher writes happen on the single scheduler goroutine, while
// Readers (metrics, tests) may call Records concurrently, hence the mutex.
type Journal struct {
	mu      sync.RWMutex
	records []Record
}

// newJournal creates an empty journal.
func newJournal() *Journal {
	return &Journal{}
}

// append adds one record. Returns the record for chaining.
func (j *Journal) append(r Record) Record {
	j.mu.Lock()
	r.Seq = uint64(len(j.records)) + 1
	j.records = append(j.records, r)
	j.mu.Unlock()
	return r
}

// Records returns a defensive copy of the full trajectory.
func (j *Journal) Records() []Record {
	j.mu.RLock()
	out := make([]Record, len(j.records))
	copy(out, j.records)
	j.mu.RUnlock()
	return out
}

// Trace renders all records, one per line, in delivery order.
func (j *Journal) Trace() string {
	recs := j.Records()
	var b strings.Builder
	for _, r := range recs {
		b.WriteString(r.String())
		b.WriteByte('\n')
	}
	return b.String()
}

// Fingerprint returns a compact deterministic signature of the trajectory
// (length-sensitive: identical sequences produce identical fingerprints).
func (j *Journal) Fingerprint() string {
	recs := j.Records()
	var h uint64 = 1469598103934665603
	write := func(data []byte) {
		for _, c := range data {
			h ^= uint64(c)
			h *= 1099511628211
		}
		h ^= 0xff
		h *= 1099511628211
	}
	for _, r := range recs {
		write([]byte(r.String()))
	}
	return fmt.Sprintf("%016x:%d", h, len(recs))
}
