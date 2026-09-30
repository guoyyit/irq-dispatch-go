package irq

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Config 汇总分发器全部可调配置项。
type Config struct {
	Clock         Clock
	Journal       *Journal
	Fault         FaultPolicy
	Storm         StormConfig
	MaxConcurrent int
	InboxSize     int
	// InlineDispatch 为 true 时分发在主循环 goroutine 内同步执行例程，
	// 消除并发执行引入的轨迹交错，供确定性测试与重放使用。
	InlineDispatch bool
	// BarrierGated 为 true 时普通输入只入待处理，直到 Barrier 点才统一派发，
	// 用于把“并发产生 → 屏障分批”的轨迹完全确定化。
	BarrierGated bool
}

// sourceRef 唯一标识“线 + 来源”。
type sourceRef struct {
	line   LineID
	source SourceID
}

// pendingNotice 是已识别、等待投递的中断通知。
type pendingNotice struct {
	ref      sourceRef
	priority Priority
	seq      uint64
	raisedAt time.Time
}

// activeInvocation 是一个正在执行的例程询问。
type activeInvocation struct {
	id       uint64
	ref      sourceRef
	handler  SourceID
	priority Priority
	raisedAt time.Time
}

// invocationResult 是例程执行结束后回传主循环的结果。
type invocationResult struct {
	id       uint64
	ref      sourceRef
	handler  SourceID
	verdict  Verdict
	err      error
	panicked bool
	priority Priority
}

// Dispatcher 是中断分发主循环的句柄。
type Dispatcher struct {
	mu      sync.Mutex
	cfg     Config
	lines   map[LineID]*irqLine
	storms  map[sourceRef]*stormState
	faults  []FaultRecord
	metrics *Metrics

	orderMu    sync.Mutex
	inbox      chan InputEvent
	done       chan invocationResult
	release    chan sourceRef
	kick       chan struct{}
	barrierAck chan uint64

	pending    []pendingNotice
	running    map[uint64]*activeInvocation
	quiet      chan struct{}
	stopCh     chan struct{}
	stopped    chan struct{}
	nextNotice uint64
	nextRun    uint64
	started    bool
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	loopGen    uint64
}

// New 创建分发器。
func New(cfg Config) *Dispatcher {
	if cfg.Clock == nil {
		cfg.Clock = wallClock{}
	}
	if cfg.Journal == nil {
		cfg.Journal = NewJournal()
	}
	if cfg.MaxConcurrent <= 0 {
		cfg.MaxConcurrent = 64
	}
	return &Dispatcher{
		cfg:        cfg,
		lines:      make(map[LineID]*irqLine),
		storms:     make(map[sourceRef]*stormState),
		metrics:    newMetrics(nil),
		inbox:      make(chan InputEvent, inboxSize(cfg.InboxSize)),
		done:       make(chan invocationResult),
		release:    make(chan sourceRef),
		kick:       make(chan struct{}, 1),
		barrierAck: make(chan uint64, 16),
		running:    make(map[uint64]*activeInvocation),
	}
}

// kickLoop 非阻塞地唤醒分发主循环，解决 select 窗口的错过唤醒。
func (d *Dispatcher) kickLoop() {
	select {
	case d.kick <- struct{}{}:
	default:
	}
}

func inboxSize(configured int) int {
	if configured > 0 {
		return configured
	}
	return 4096
}

// AddLine 注册一条中断线。
func (d *Dispatcher) AddLine(cfg LineConfig) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, exists := d.lines[cfg.ID]; exists {
		return
	}
	d.lines[cfg.ID] = newLine(cfg)
}

// Register 在中断线上注册处理例程。
func (d *Dispatcher) Register(line LineID, h Handler) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	target, ok := d.lines[line]
	if !ok {
		return fmt.Errorf("%w: %s", ErrLineNotFound, line)
	}
	if target.handlerByID(h.ID) != nil {
		return fmt.Errorf("%w: %s", ErrHandlerExists, h.ID)
	}
	target.register(h)
	return nil
}

// Raise 记录一次中断输入。
func (d *Dispatcher) Raise(line LineID, source SourceID) error {
	return d.submit(OpRaise, line, source)
}

// Deassert 撤销一次电平请求。
func (d *Dispatcher) Deassert(line LineID, source SourceID) error {
	return d.submit(OpDeassert, line, source)
}

// Barrier 在输入序列中记录一个确定性屏障：屏障之前已记录的中断
// 会在屏障点之前完成入队与派发，用于并发触发后的确定性重放。
func (d *Dispatcher) Barrier() error {
	event := d.cfg.Journal.captureInput(OpBarrier, "", "", d.cfg.Clock.Now())
	d.orderMu.Lock()
	d.inbox <- event
	d.orderMu.Unlock()
	select {
	case <-d.barrierAck:
		return nil
	case <-d.stopCh:
		return ErrNotRunning
	}
}

func (d *Dispatcher) submit(kind OpKind, line LineID, source SourceID) error {
	d.mu.Lock()
	if _, ok := d.lines[line]; !ok {
		d.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrLineNotFound, line)
	}
	d.mu.Unlock()

	event := d.cfg.Journal.captureInput(kind, line, source, d.cfg.Clock.Now())

	// 分配序号与入队在同一临界区内完成，保证并发提交按同一全序被主循环观察。
	d.orderMu.Lock()
	defer d.orderMu.Unlock()
	d.inbox <- event
	return nil
}

// feedInput 重放期间按记录输入投递，不再次写入输入日志。
func (d *Dispatcher) feedInput(event InputEvent) {
	d.inbox <- event
}

// Start 启动分发主循环。
func (d *Dispatcher) Start(ctx context.Context) {
	d.mu.Lock()
	if d.started {
		d.mu.Unlock()
		return
	}
	runCtx, cancel := context.WithCancel(ctx)
	d.cancel = cancel
	d.started = true
	d.stopCh = make(chan struct{})
	d.stopped = make(chan struct{})
	d.mu.Unlock()

	d.metrics.initStart(d.cfg.Clock.Now())
	go d.loop(runCtx)
}

// Stop 停止分发并等待在途例程结束。
func (d *Dispatcher) Stop() {
	d.mu.Lock()
	if !d.started {
		d.mu.Unlock()
		return
	}
	d.started = false
	cancel := d.cancel
	stopCh := d.stopCh
	stopped := d.stopped
	d.mu.Unlock()

	close(stopCh)
	if cancel != nil {
		cancel()
	}
	<-stopped
	d.wg.Wait()
}

// WaitQuiet 等待输入全部处理完成。
func (d *Dispatcher) WaitQuiet(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	sawEmpty := false
	for time.Now().Before(deadline) {
		d.mu.Lock()
		empty := d.hasNoWorkLocked()
		gen := d.loopGen
		d.mu.Unlock()
		// 两阶段确认：先观察到空；随后等待主循环至少再推进一代，
		// 再确认仍为空，从而覆盖“已派发但 goroutine 尚未登记 running”的窗口。
		if empty {
			if sawEmpty {
				// 给已派发的执行 goroutine 留出登记 running 的时间，消除启动竞态。
				time.Sleep(5 * time.Millisecond)
				d.mu.Lock()
				stillEmpty := d.hasNoWorkLocked()
				d.mu.Unlock()
				if stillEmpty {
					return true
				}
				sawEmpty = false
			} else {
				sawEmpty = true
			}
		} else {
			sawEmpty = false
		}
		time.Sleep(2 * time.Millisecond)
		_ = gen
	}
	return false
}

// Metrics 返回指标快照。
func (d *Dispatcher) Metrics() MetricsSnapshot {
	return d.metrics.snapshotAt(d.cfg.Clock.Now())
}

// Faults 返回可观测的故障记录。
func (d *Dispatcher) Faults() []FaultRecord {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]FaultRecord, len(d.faults))
	copy(out, d.faults)
	return out
}

// Journal 返回绑定的事件日志。
func (d *Dispatcher) Journal() *Journal { return d.cfg.Journal }

// DisableHandler 手动停用例程。
func (d *Dispatcher) DisableHandler(line LineID, h SourceID) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if target, ok := d.lines[line]; ok {
		if handler := target.handlerByID(h); handler != nil {
			handler.fault.disable(d.cfg.Clock.Now())
		}
	}
}

// EnableHandler 手动恢复例程。
func (d *Dispatcher) EnableHandler(line LineID, h SourceID) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if target, ok := d.lines[line]; ok {
		if handler := target.handlerByID(h); handler != nil {
			handler.fault.enable()
		}
	}
}
