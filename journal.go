package irq

import (
	"context"
	"sync"
	"time"
)

// TraceKind 描述分发轨迹事件的种类。
type TraceKind int

const (
	TraceInput TraceKind = iota
	TraceSchedule
	TraceDispatchStart
	TraceAsk
	TraceClaim
	TraceRefuse
	TraceFault
	TraceDeferred
	TraceStorm
	TraceDisabled
	TraceRecovered
	TraceComplete
)

func (kind TraceKind) String() string {
	names := [...]string{
		"input", "schedule", "dispatch-start", "ask", "claim", "refuse",
		"fault", "deferred", "storm", "disabled", "recovered", "complete",
	}
	if int(kind) < 0 || int(kind) >= len(names) {
		return "unknown"
	}
	return names[kind]
}

// InputEvent 是进入系统的单个输入（可重放的最小单位）。
type InputEvent struct {
	Seq    uint64
	At     time.Time
	Kind   OpKind
	Line   LineID
	Source SourceID
}

// TraceEvent 是分发轨迹中的一条记录。
type TraceEvent struct {
	Seq      uint64
	At       time.Time
	Kind     TraceKind
	Line     LineID
	Source   SourceID
	Handler  SourceID
	Priority Priority
	Detail   string
}

// Journal 记录完整输入序列与分发轨迹，供重放比对。
type Journal struct {
	mu          sync.Mutex
	inputs      []InputEvent
	trace       []TraceEvent
	nextInput   uint64
	nextTrace   uint64
	recordInput bool
}

// NewJournal 创建空日志。
func NewJournal() *Journal {
	return &Journal{recordInput: true}
}

func newReplayJournal() *Journal {
	return &Journal{recordInput: false}
}

func (j *Journal) captureInput(kind OpKind, line LineID, source SourceID, at time.Time) InputEvent {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.nextInput++
	event := InputEvent{
		Seq:    j.nextInput,
		At:     at,
		Kind:   kind,
		Line:   line,
		Source: source,
	}
	if j.recordInput {
		j.inputs = append(j.inputs, event)
	}
	return event
}

func (j *Journal) appendTrace(kind TraceKind, line LineID, source SourceID, handler SourceID, priority Priority, at time.Time, detail string) TraceEvent {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.nextTrace++
	event := TraceEvent{
		Seq:      j.nextTrace,
		At:       at,
		Kind:     kind,
		Line:     line,
		Source:   source,
		Handler:  handler,
		Priority: priority,
		Detail:   detail,
	}
	j.trace = append(j.trace, event)
	return event
}

// Inputs 返回按序号排列的输入事件副本。
func (j *Journal) Inputs() []InputEvent {
	j.mu.Lock()
	defer j.mu.Unlock()
	out := make([]InputEvent, len(j.inputs))
	copy(out, j.inputs)
	return out
}

// Trace 返回按发生顺序排列的轨迹事件副本。
func (j *Journal) Trace() []TraceEvent {
	j.mu.Lock()
	defer j.mu.Unlock()
	out := make([]TraceEvent, len(j.trace))
	copy(out, j.trace)
	return out
}

// Reset 清空日志。
func (j *Journal) Reset() {
	j.mu.Lock()
	j.inputs = nil
	j.trace = nil
	j.nextInput = 0
	j.nextTrace = 0
	j.mu.Unlock()
}

// ReplayBuilder 以与首轮一致的配置、线与例程注册构造一个全新分发器。
// 传入的 journal 用于记录重放轨迹，构建者应把它放进 Config.Journal。
type ReplayBuilder func(journal *Journal) (*Dispatcher, error)

// Replay 依据记录的输入序列重放，并返回重放产生的分发轨迹。
// 调用方可使用 TracesEqual 与首轮 TracesEqual 比对。
func (j *Journal) Replay(builder ReplayBuilder) ([]TraceEvent, error) {
	inputs := j.Inputs()

	replayJournal := newReplayJournal()
	replay, err := builder(replayJournal)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	replay.Start(ctx)

	for _, input := range inputs {
		replay.feedInput(input)
	}

	if !replay.WaitQuiet(5 * time.Second) {
		replay.Stop()
		return nil, ErrReplayTimeout
	}
	replay.Stop()
	return replay.Journal().Trace(), nil
}

// TracesEqual 比较两条分发轨迹的结构是否完全一致；默认忽略绝对时间戳，
// 因为重放使用新的时间源，而投递顺序、来源、例程、优先级才是可复现语义。
func TracesEqual(a, b []TraceEvent) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		x, y := a[i], b[i]
		if x.Seq != y.Seq || x.Kind != y.Kind || x.Line != y.Line ||
			x.Source != y.Source || x.Handler != y.Handler ||
			x.Priority != y.Priority || x.Detail != y.Detail {
			return false
		}
	}
	return true
}

// DispatchDecision 是归一化后单个输入的分发结果，去除了执行时序交错。
type DispatchDecision struct {
	Line    LineID
	Source  SourceID
	Handler SourceID
	Claimed bool
}

// DispatchDecisions 从轨迹中提取按输入归属归一化的分发决策序列。
// 每个产生 Claim 的来源记录其认领例程；全部拒绝记录为空认领。
func DispatchDecisions(trace []TraceEvent) []DispatchDecision {
	var out []DispatchDecision
	for _, event := range trace {
		switch event.Kind {
		case TraceClaim:
			out = append(out, DispatchDecision{
				Line: event.Line, Source: event.Source, Handler: event.Handler, Claimed: true,
			})
		case TraceComplete:
			out = append(out, DispatchDecision{
				Line: event.Line, Source: event.Source, Handler: event.Handler, Claimed: false,
			})
		}
	}
	return out
}

// DecisionsEqual 比较两条归一化分发决策序列。
func DecisionsEqual(a, b []DispatchDecision) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
