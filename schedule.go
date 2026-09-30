package irq

import (
	"context"
	"sort"
)

func (d *Dispatcher) stormForLocked(ref sourceRef) *stormState {
	state, ok := d.storms[ref]
	if !ok {
		state = newStormState(d.cfg.Storm)
		d.storms[ref] = state
	}
	return state
}

// enqueue 加入一个待处理通知；相同“线+来源”的在途/待处理通知只保留一个，
// 序号记录最早到达，保证补投不重复。
func (d *Dispatcher) enqueue(notice pendingNotice) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, existing := range d.pending {
		if existing.ref == notice.ref {
			return
		}
	}
	if notice.seq == 0 {
		d.nextNotice++
		notice.seq = d.nextNotice
	}
	d.pending = append(d.pending, notice)
	d.refreshBacklogLocked()
	d.kickLoop()
}

func (d *Dispatcher) removePending(ref sourceRef) {
	kept := d.pending[:0]
	for _, notice := range d.pending {
		if notice.ref != ref {
			kept = append(kept, notice)
		}
	}
	d.pending = kept
	d.refreshBacklogLocked()
}

// sortPendingLocked 按“优先级降序、同优先级按到达序号升序”排序，
// 决定屏蔽退出后的补投顺序。
func (d *Dispatcher) sortPendingLocked() {
	sort.SliceStable(d.pending, func(i, j int) bool {
		if d.pending[i].priority != d.pending[j].priority {
			return d.pending[i].priority > d.pending[j].priority
		}
		return d.pending[i].seq < d.pending[j].seq
	})
}

func (d *Dispatcher) schedule(ctx context.Context) {
	for {
		d.mu.Lock()
		d.sortPendingLocked()

		if d.cfg.InlineDispatch && len(d.pending) > 0 {
			// 内联确定性模式：一次取走整批，按优先级/序号排序后顺序同步派发，
			// 派发过程不再重新排序或抢占，避免同批顺序漂移。
			batch := d.pending
			d.pending = nil
			invocations := make([]activeInvocation, 0, len(batch))
			notices := make([]pendingNotice, 0, len(batch))
			for _, notice := range batch {
				line := d.lines[notice.ref.line]
				if !line.beginDispatch(notice.ref.source) {
					d.pending = append(d.pending, notice)
					continue
				}
				d.nextRun++
				inv := activeInvocation{
					id:       d.nextRun,
					ref:      notice.ref,
					priority: notice.priority,
					raisedAt: notice.raisedAt,
				}
				d.running[inv.id] = &inv
				invocations = append(invocations, inv)
				notices = append(notices, notice)
				d.traceLocked(TraceSchedule, notice.ref.line, notice.ref.source, "", notice.priority, d.cfg.Clock.Now(), "")
			}
			d.refreshBacklogLocked()
			d.mu.Unlock()

			for i := range notices {
				result := d.pollHandlersInline(ctx, invocations[i].id, notices[i])
				d.handleResult(ctx, result)
			}
			continue
		}

		if len(d.pending) == 0 || len(d.running) >= d.cfg.MaxConcurrent {
			d.refreshBacklogLocked()
			d.mu.Unlock()
			return
		}

		slot := 0
		var deferredThisPass bool

		if !d.cfg.InlineDispatch {
			busyLines := make(map[LineID]bool)
			highestRunning := Priority(-1 << 31)
			for _, invocation := range d.running {
				busyLines[invocation.ref.line] = true
				if invocation.priority > highestRunning {
					highestRunning = invocation.priority
				}
			}

			slot = -1
			for index, notice := range d.pending {
				// 同线串行：同一线一次只投递一个来源。
				if busyLines[notice.ref.line] {
					deferredThisPass = true
					continue
				}
				// 全局嵌套屏蔽：只能被严格更高优先级的中断打断；
				// 同优先级或更低优先级在在途例程退出屏蔽后按序补投。
				if len(d.running) > 0 && notice.priority <= highestRunning {
					deferredThisPass = true
					continue
				}
				slot = index
				break
			}
		}

		if slot < 0 {
			if deferredThisPass && len(d.running) > 0 {
				d.metrics.recordDeferred()
			}
			d.refreshBacklogLocked()
			d.mu.Unlock()
			return
		}

		notice := d.pending[slot]
		d.pending = append(d.pending[:slot], d.pending[slot+1:]...)

		line := d.lines[notice.ref.line]
		if !line.beginDispatch(notice.ref.source) {
			// 状态在锁内已变化（如被撤销/已在途）：放回队首，本轮不派发。
			d.pending = append([]pendingNotice{notice}, d.pending...)
			d.refreshBacklogLocked()
			d.mu.Unlock()
			return
		}

		d.nextRun++
		invocation := &activeInvocation{
			id:       d.nextRun,
			ref:      notice.ref,
			priority: notice.priority,
			raisedAt: notice.raisedAt,
		}
		d.running[invocation.id] = invocation

		start := d.cfg.Clock.Now()
		d.traceLocked(TraceSchedule, notice.ref.line, notice.ref.source, "", notice.priority, start, "")
		d.refreshBacklogLocked()
		d.mu.Unlock()

		if d.cfg.InlineDispatch {
			result := d.pollHandlersInline(ctx, invocation.id, notice)
			d.handleResult(ctx, result)
		} else {
			go d.pollHandlers(ctx, invocation.id, notice)
		}
	}
}

func (d *Dispatcher) totalBacklogLocked() int {
	total := len(d.pending)
	for _, state := range d.storms {
		total += state.depth()
	}
	return total
}

func (d *Dispatcher) refreshBacklogLocked() {
	d.metrics.setBacklog(d.totalBacklogLocked())
}

func (d *Dispatcher) updateQuiet() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.loopGen++
	empty := d.hasNoWorkLocked()
	if empty {
		if d.quiet != nil {
			close(d.quiet)
			d.quiet = nil
		}
		return
	}
	// 一旦出现活动，立即（重新）建立一个未关闭的静默信号；
	// 这样等待方不可能收到活动开始之前残留的关闭信号。
	if d.quiet == nil {
		d.quiet = make(chan struct{})
	}
}

// hasNoWorkLocked 判定是否还有任何未完成工作：
// inbox 输入、待处理通知、在途例程以及风暴限速积压。
func (d *Dispatcher) hasNoWorkLocked() bool {
	if len(d.pending) != 0 || len(d.running) != 0 || len(d.inbox) != 0 {
		return false
	}
	for _, state := range d.storms {
		if state.depth() > 0 {
			return false
		}
	}
	return true
}
