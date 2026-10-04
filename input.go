package irq

import (
	"context"
	"time"
)

func (d *Dispatcher) loop(ctx context.Context) {
	defer close(d.stopped)
	for {
		select {
		case <-ctx.Done():
			return
		case <-d.stopCh:
			return
		case event := <-d.inbox:
			if d.cfg.BarrierGated {
				d.consumeUntilBarrier(ctx, event)
			} else {
				d.handleInput(ctx, event)
			}
		case result := <-d.done:
			d.handleResult(ctx, result)
		case ref := <-d.release:
			d.handleRelease(ref)
		case <-d.kick:
		}
		d.schedule(ctx)
		d.updateQuiet()
	}
}

// consumeUntilBarrier 在屏障门控模式下从一个已取出的输入开始，连续接收
// 后续输入，直到遇到屏障；屏障点把此前整批统一派发一次并确认。
func (d *Dispatcher) consumeUntilBarrier(ctx context.Context, first InputEvent) {
	d.handleInput(ctx, first)
	for {
		select {
		case next := <-d.inbox:
			if next.Kind == OpBarrier {
				d.handleInput(ctx, next)
				d.updateQuiet()
				return
			}
			d.handleInput(ctx, next)
		default:
			// 屏障尚未入队（提交方仍在产生输入）：等待下一个输入或屏障，
			// 不能返回主循环，否则会提前派发不完整批次。
			select {
			case next := <-d.inbox:
				if next.Kind == OpBarrier {
					d.handleInput(ctx, next)
					d.updateQuiet()
					return
				}
				d.handleInput(ctx, next)
			case <-ctx.Done():
				d.schedule(ctx)
				d.updateQuiet()
				return
			case <-d.stopCh:
				d.schedule(ctx)
				d.updateQuiet()
				return
			}
		}
	}
}
func (d *Dispatcher) trace(kind TraceKind, line LineID, source SourceID, handler SourceID, priority Priority, at time.Time, detail string) {
	d.cfg.Journal.appendTrace(kind, line, source, handler, priority, at, detail)
}

func (d *Dispatcher) traceLocked(kind TraceKind, line LineID, source SourceID, handler SourceID, priority Priority, at time.Time, detail string) {
	d.cfg.Journal.appendTrace(kind, line, source, handler, priority, at, detail)
}

// handleInput 处理一个输入；返回 true 表示遇到屏障并已就地派发，
// 调用方不应继续排空屏障之后的输入。
func (d *Dispatcher) handleInput(ctx context.Context, event InputEvent) bool {
	if event.Kind == OpBarrier {
		d.trace(TraceInput, "", "", "", 0, event.At, "barrier")
		d.schedule(ctx)
		d.updateQuiet()
		select {
		case d.barrierAck <- event.Seq:
		default:
		}
		return true
	}

	d.mu.Lock()
	line, ok := d.lines[event.Line]
	d.mu.Unlock()
	if !ok {
		return false
	}

	d.trace(TraceInput, event.Line, event.Source, "", line.cfg.Priority, event.At, event.Kind.String())

	if event.Kind == OpDeassert {
		d.mu.Lock()
		line.deassert(event.Source)
		d.removePending(sourceRef{line: event.Line, source: event.Source})
		d.mu.Unlock()
		return false
	}

	d.metrics.recordRaised()
	ref := sourceRef{line: event.Line, source: event.Source}

	d.mu.Lock()
	state := d.stormForLocked(ref)
	decision := state.observe(event.At, d.hasPendingNoticeLocked(ref))
	fresh := line.raise(event.Source, event.Seq)
	d.mu.Unlock()

	switch decision {
	case StormMerge:
		d.metrics.recordStormMerge()
		d.trace(TraceStorm, event.Line, event.Source, "", line.cfg.Priority, event.At, "merge")
	case StormDelay:
		d.metrics.recordStormMerge()
		d.metrics.recordDeferred()
		d.trace(TraceStorm, event.Line, event.Source, "", line.cfg.Priority, event.At, "delay")
		d.armReleaseTimer(ref)
		return false
	}

	if fresh {
		d.enqueue(pendingNotice{
			ref:      ref,
			priority: line.cfg.Priority,
			raisedAt: event.At,
			seq:      event.Seq,
		})
	}
	return false
}

func (d *Dispatcher) handleRelease(ref sourceRef) {
	d.mu.Lock()
	state := d.storms[ref]
	line, ok := d.lines[ref.line]
	if state == nil || !ok {
		d.mu.Unlock()
		return
	}

	remaining := state.release()
	// 放行只作用于本来源，与同线其他来源的待处理状态无关：
	// 各来源的限速积压独立结算，先到者不顶替后到者的补投。
	switch {
	case line.undelivered(ref.source):
		// 触发状态仍未投递：补投一次。若通知已在队则本次放行
		// 合并进既有通知，不重复累积待处理条目。
		if !d.hasPendingNoticeLocked(ref) {
			d.nextNotice++
			d.pending = append(d.pending, pendingNotice{
				ref:      ref,
				priority: line.cfg.Priority,
				seq:      line.seqOf(ref.source),
				raisedAt: d.cfg.Clock.Now(),
			})
		}
	case line.inflight[ref.source]:
		// 例程正在投递：折算为一次滞留，例程返回后补投，最终请求不丢失。
		line.markRearmed(ref.source)
	default:
		// 已撤销或已投递：放行额度随积压结算，无需动作。
	}
	shouldArm := remaining > 0
	if !shouldArm {
		state.settled(d.cfg.Clock.Now())
	}
	d.refreshBacklogLocked()
	d.mu.Unlock()
	if shouldArm {
		d.armReleaseTimer(ref)
	}
	d.kickLoop()
}

func (d *Dispatcher) armReleaseTimer(ref sourceRef) {
	d.mu.Lock()
	state := d.storms[ref]
	armed := state != nil && !state.timerArmed
	if state != nil {
		state.timerArmed = true
	}
	delay := d.cfg.Storm.CoalesceDelay
	d.mu.Unlock()
	if !armed {
		return
	}
	go func() {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-timer.C:
			select {
			case d.release <- ref:
			case <-d.stopCh:
			}
		case <-d.stopCh:
		}
	}()
}

func (d *Dispatcher) handleResult(ctx context.Context, result invocationResult) {
	now := d.cfg.Clock.Now()

	d.mu.Lock()
	shouldKick := false
	line, ok := d.lines[result.ref.line]
	if !ok {
		delete(d.running, result.id)
		d.mu.Unlock()
		return
	}

	if invocation, exists := d.running[result.id]; exists {
		result.priority = invocation.priority
	}

	var invocationRaisedAt = now
	if invocation, exists := d.running[result.id]; exists {
		result.priority = invocation.priority
		invocationRaisedAt = invocation.raisedAt
		d.metrics.recordLatency(now.Sub(invocation.raisedAt))
	}

	rearm := line.markDelivered(result.ref.source)
	if result.verdict == Claimed {
		d.metrics.recordDelivered()
		d.metrics.recordClaim()
		d.traceLocked(TraceClaim, result.ref.line, result.ref.source, result.handler, result.priority, now, "")
		if candidate := line.handlerByID(result.handler); candidate != nil {
			candidate.fault.recordSuccess()
		}
	} else {
		d.traceLocked(TraceComplete, result.ref.line, result.ref.source, result.handler, result.priority, now, "no-owner")
	}

	delete(d.running, result.id)

	var rearmNotice *pendingNotice
	var rearmID uint64
	// 投递期间再次到达的边缘必须补投。保持 inflight，直接发起下一次在途投递，
	// 避免“重新入队 → goroutine 启动”窗口造成重复派发。
	if rearm {
		source := result.ref.source
		d.nextRun++
		rearmID = d.nextRun
		notice := pendingNotice{
			ref:      sourceRef{line: result.ref.line, source: source},
			priority: line.cfg.Priority,
			seq:      line.seqOf(source),
			raisedAt: invocationRaisedAt,
		}
		d.running[rearmID] = &activeInvocation{
			id:       rearmID,
			ref:      notice.ref,
			priority: notice.priority,
			raisedAt: notice.raisedAt,
		}
		d.traceLocked(TraceSchedule, notice.ref.line, notice.ref.source, "", notice.priority, now, "rearm")
		rearmNotice = &notice
		shouldKick = true
	}

	if state := d.storms[result.ref]; state != nil {
		state.settled(now)
	}

	d.refreshBacklogLocked()
	d.mu.Unlock()
	// 运行槽位释放可能使被屏蔽的待处理变为可调度。
	d.kickLoop()
	_ = shouldKick

	if rearmNotice != nil {
		if d.cfg.InlineDispatch {
			d.handleResult(ctx, d.pollHandlersInline(ctx, rearmID, *rearmNotice))
		} else {
			go d.pollHandlers(ctx, rearmID, *rearmNotice)
		}
	}
}
