package irq

// edgeState 是单个来源的边缘中断相位。
type edgeState int

const (
	edgeIdle    edgeState = iota // 无待处理
	edgePending                  // 有一次（可能合并了多次）待投递
	edgeActive                   // 正在投递，期间无新到达
	edgeRearmed                  // 正在投递，且期间又到达过一次
)

// irqLine 保存一条中断线的配置、注册例程与触发状态。
type irqLine struct {
	cfg      LineConfig
	handlers []*registeredHandler

	sourceOrder   []SourceID
	sourceIndex   map[SourceID]int
	edges         map[SourceID]edgeState
	inflight      map[SourceID]bool
	pendingSeq    map[SourceID]uint64
	levelAsserted map[SourceID]bool
	levelReported map[SourceID]bool
}

// registeredHandler 是线内注册例程的运行期表示。
type registeredHandler struct {
	h     Handler
	fault handlerFaultState
}

// newLine 依据配置创建中断线。
func newLine(cfg LineConfig) *irqLine {
	return &irqLine{
		cfg:           cfg,
		sourceIndex:   make(map[SourceID]int),
		edges:         make(map[SourceID]edgeState),
		inflight:      make(map[SourceID]bool),
		pendingSeq:    make(map[SourceID]uint64),
		levelAsserted: make(map[SourceID]bool),
		levelReported: make(map[SourceID]bool),
	}
}

// register 向线上追加一个处理例程。
func (l *irqLine) register(h Handler) {
	l.handlers = append(l.handlers, &registeredHandler{h: h})
}

func (l *irqLine) rememberSource(source SourceID) {
	if _, known := l.sourceIndex[source]; known {
		return
	}
	l.sourceIndex[source] = len(l.sourceOrder)
	l.sourceOrder = append(l.sourceOrder, source)
}

// raise 记录来源的一次中断请求，返回是否产生了新的待投递通知。
func (l *irqLine) raise(source SourceID, seq uint64) bool {
	l.rememberSource(source)
	if l.cfg.Mode == EdgeTriggered {
		if l.inflight[source] {
			// 已派发（无论例程是否已开始）：合并为一次滞留，不重复派发。
			l.edges[source] = edgeRearmed
			l.pendingSeq[source] = seq
			return false
		}
		switch l.edges[source] {
		case edgeIdle:
			l.edges[source] = edgePending
			l.pendingSeq[source] = seq
			return true
		case edgeActive:
			// 响应前再次出现：合并为一次滞留，不丢失存在性。
			l.edges[source] = edgeRearmed
			l.pendingSeq[source] = seq
			return false
		default:
			// edgePending / edgeRearmed：继续合并。
			return false
		}
	}
	wasAsserted := l.levelAsserted[source]
	l.levelAsserted[source] = true
	if !wasAsserted {
		l.pendingSeq[source] = seq
	}
	return !wasAsserted && !l.levelReported[source]
}

// seqOf 返回来源当前待投递脉冲对应的输入序号。
func (l *irqLine) seqOf(source SourceID) uint64 { return l.pendingSeq[source] }

// deassert 撤销来源的电平请求，并清除其触发状态。
func (l *irqLine) deassert(source SourceID) {
	l.rememberSource(source)
	l.levelAsserted[source] = false
	l.levelReported[source] = false
	l.edges[source] = edgeIdle
	delete(l.inflight, source)
}

// nextPending 返回最早进入待处理状态且尚未投递的来源。
func (l *irqLine) nextPending() (SourceID, bool) {
	for _, source := range l.sourceOrder {
		if l.cfg.Mode == EdgeTriggered && l.edges[source] == edgePending && !l.inflight[source] {
			return source, true
		}
		if l.cfg.Mode == LevelTriggered && l.levelAsserted[source] && !l.levelReported[source] {
			return source, true
		}
	}
	return "", false
}

// hasPending 报告线上是否仍有待投递来源。
func (l *irqLine) hasPending() bool {
	_, ok := l.nextPending()
	return ok
}

// beginDispatch 在调度临界区内原子地开始一次投递：
// 置位 inflight 并把 pending 切换为 active。返回 false 表示当前无可投递脉冲。
// 此后到达的边缘由 raise 记为 rearmed，从而既不重复派发也不丢失存在性。
func (l *irqLine) beginDispatch(source SourceID) bool {
	if l.cfg.Mode == EdgeTriggered {
		if l.inflight[source] || l.edges[source] != edgePending {
			return false
		}
		l.inflight[source] = true
		l.edges[source] = edgeActive
		return true
	}
	if l.cfg.Mode == LevelTriggered {
		if !l.levelAsserted[source] || l.levelReported[source] {
			return false
		}
		l.levelReported[source] = true
		return true
	}
	return false
}

// beginService 为执行侧保留的兼容入口；真正的状态转换已在 beginDispatch 完成。
func (l *irqLine) beginService(source SourceID) bool {
	if l.cfg.Mode == EdgeTriggered {
		if l.inflight[source] {
			return true
		}
		return false
	}
	return true
}

// markDelivered 结束一次投递，返回是否需要因投递期间的新边缘而补投。
func (l *irqLine) markDelivered(source SourceID) bool {
	if l.cfg.Mode == EdgeTriggered {
		if l.edges[source] == edgeRearmed {
			// 保持 inflight，直接把滞留脉冲作为下一次在途投递，
			// 关闭“清除在途 → 重新入队/调度”之间的重复派发窗口。
			l.edges[source] = edgeActive
			return true
		}
		delete(l.inflight, source)
		l.edges[source] = edgeIdle
		return false
	}
	return false
}

// backlog 返回线上尚未投递的来源数。
func (l *irqLine) backlog() int {
	count := 0
	for _, source := range l.sourceOrder {
		if l.cfg.Mode == EdgeTriggered && l.edges[source] == edgePending && !l.inflight[source] {
			count++
		}
		if l.cfg.Mode == LevelTriggered && l.levelAsserted[source] && !l.levelReported[source] {
			count++
		}
	}
	return count
}

func (l *irqLine) handlerByID(id SourceID) *registeredHandler {
	for _, handler := range l.handlers {
		if handler.h.ID == id {
			return handler
		}
	}
	return nil
}
