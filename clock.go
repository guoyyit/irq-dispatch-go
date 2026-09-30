package irq

import (
	"sync"
	"time"
)

// Clock 抽象时间来源，便于以逻辑时钟驱动确定性的分发与重放。
type Clock interface {
	Now() time.Time
}

// wallClock 使用系统墙上时间。
type wallClock struct{}

func (wallClock) Now() time.Time { return time.Now() }

// ManualClock 为可手动推进的单调逻辑时钟，刻度为纳秒。
type ManualClock struct {
	mu   sync.Mutex
	base time.Time
	tick time.Duration
}

// NewManualClock 创建从 start 开始的逻辑时钟；start 为零值时取 Unix 纪元。
func NewManualClock(start time.Time) *ManualClock {
	if start.IsZero() {
		start = time.Unix(0, 0)
	}
	return &ManualClock{base: start}
}

func (c *ManualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.base.Add(c.tick)
}

// Advance 将逻辑时钟向前推进 d。
func (c *ManualClock) Advance(d time.Duration) {
	if d <= 0 {
		return
	}
	c.mu.Lock()
	c.tick += d
	c.mu.Unlock()
}
