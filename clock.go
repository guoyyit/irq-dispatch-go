package irq

import "time"

// Clock abstracts time so timeout and storm-window behavior is replayable.
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}

// wallClock uses the real process clock.
type wallClock struct{}

func (wallClock) Now() time.Time { return time.Now() }
func (wallClock) After(d time.Duration) <-chan time.Time {
	return time.After(d)
}

// scriptedClock returns scripted times and never fires timers on its own; it is
// used by replay to keep behavior deterministic.
type scriptedClock struct {
	now time.Time
}

func newScriptedClock(start time.Time) *scriptedClock {
	return &scriptedClock{now: start}
}

func (c *scriptedClock) Now() time.Time { return c.now }

// After returns a channel that only fires if the clock is advanced by Advance.
func (c *scriptedClock) After(d time.Duration) <-chan time.Time {
	return make(chan time.Time, 1)
}
