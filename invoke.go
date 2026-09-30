package irq

import (
	"context"
	"fmt"
	"runtime/debug"
	"time"
)

func (d *Dispatcher) pollHandlers(ctx context.Context, id uint64, notice pendingNotice) {
	d.done <- d.pollHandlersInline(ctx, id, notice)
}

func (d *Dispatcher) pollHandlersInline(ctx context.Context, id uint64, notice pendingNotice) invocationResult {
	d.mu.Lock()
	line := d.lines[notice.ref.line]
	handlerIDs := make([]SourceID, len(line.handlers))
	for i, candidate := range line.handlers {
		handlerIDs[i] = candidate.h.ID
	}
	d.mu.Unlock()

	for _, handlerID := range handlerIDs {
		d.mu.Lock()
		candidate := line.handlerByID(handlerID)
		if candidate == nil {
			d.mu.Unlock()
			continue
		}
		now := d.cfg.Clock.Now()
		available, recovered := candidate.fault.available(d.cfg.Fault, now)
		d.mu.Unlock()
		if !available {
			d.trace(TraceDeferred, notice.ref.line, notice.ref.source, candidate.h.ID, candidate.h.Priority, now, "disabled")
			continue
		}
		if recovered {
			d.trace(TraceRecovered, notice.ref.line, notice.ref.source, candidate.h.ID, notice.priority, now, "cooldown")
		}

		env := &Env{
			dispatcher: d,
			line:       notice.ref.line,
			source:     notice.ref.source,
			priority:   notice.priority,
			raisedAt:   notice.raisedAt,
		}

		d.trace(TraceAsk, notice.ref.line, notice.ref.source, candidate.h.ID, notice.priority, now, "")

		verdict, err, panicked, timedOut := d.invokeOnce(ctx, candidate, env)
		if err != nil || panicked || timedOut {
			reason := "error"
			if panicked {
				reason = "panic"
			}
			if timedOut {
				reason = "timeout"
			}
			d.reportFault(notice, candidate.h.ID, reason, err)
			// 故障例程视为不归属，继续询问同线后续例程，不波及其他例程。
			continue
		}

		if verdict == NotMine {
			d.metrics.recordRefuse()
			d.trace(TraceRefuse, notice.ref.line, notice.ref.source, candidate.h.ID, notice.priority, d.cfg.Clock.Now(), "")
			continue
		}

		return invocationResult{id: id, ref: notice.ref, handler: candidate.h.ID, verdict: Claimed}
	}

	// 全部例程拒绝：不存在归属者，按一次已处理完成结束，避免活锁。
	return invocationResult{id: id, ref: notice.ref, verdict: NotMine}
}

// invokeOnce 在独立 goroutine 中执行例程：
// panic 在本 goroutine 内 recover；超时后主循环不再等待该例程，
// 迟到结果将被丢弃，卡住的 goroutine 与主循环隔离。
func (d *Dispatcher) invokeOnce(ctx context.Context, candidate *registeredHandler, env *Env) (Verdict, error, bool, bool) {
	type outcome struct {
		verdict Verdict
		err     error
		panic   interface{}
	}
	resultCh := make(chan outcome, 1)

	go func() {
		out := outcome{}
		defer func() {
			if recovered := recover(); recovered != nil {
				out.panic = recovered
				debug.Stack()
			}
			resultCh <- out
		}()
		out.verdict, out.err = candidate.h.Fn(ctx, env)
	}()

	var timeoutCh <-chan time.Time
	if d.cfg.Fault.Timeout > 0 {
		timer := time.NewTimer(d.cfg.Fault.Timeout)
		defer timer.Stop()
		timeoutCh = timer.C
	}

	select {
	case out := <-resultCh:
		if out.panic != nil {
			return NotMine, fmt.Errorf("handler panic: %v", out.panic), true, false
		}
		return out.verdict, out.err, false, false
	case <-timeoutCh:
		return NotMine, fmt.Errorf("handler timeout after %s", d.cfg.Fault.Timeout), false, true
	case <-ctx.Done():
		return NotMine, ctx.Err(), false, false
	}
}

func (d *Dispatcher) reportFault(notice pendingNotice, handlerID SourceID, reason string, err error) {
	now := d.cfg.Clock.Now()
	d.metrics.recordFault()
	d.trace(TraceFault, notice.ref.line, notice.ref.source, handlerID, notice.priority, now, reason)

	d.mu.Lock()
	line := d.lines[notice.ref.line]
	record := FaultRecord{
		At:      now,
		Line:    notice.ref.line,
		Handler: handlerID,
		Reason:  reason,
	}
	if err != nil {
		record.Reason = reason + ": " + err.Error()
	}
	d.faults = append(d.faults, record)

	if candidate := line.handlerByID(handlerID); candidate != nil {
		if candidate.fault.recordFailure(d.cfg.Fault, now) {
			d.traceLocked(TraceDisabled, notice.ref.line, notice.ref.source, handlerID, notice.priority, now, "auto-disable")
		}
	}
	d.mu.Unlock()
}
