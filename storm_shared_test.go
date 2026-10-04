package irq

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// TestSharedLineBlockedHandlerStormBacklogBounded 验证共享线与阻塞例程叠加限速时：
//   - 任意时刻积压深度不超过 MaxBacklog，放行不在待处理队列中重复累积；
//   - 存在未投递触发状态时 WaitQuiet 不报告静默；
//   - 例程返回后系统在有限时间内收敛，积压归零，来源恰好投递一次。
func TestSharedLineBlockedHandlerStormBacklogBounded(t *testing.T) {
	const maxBacklog = 3
	d := newTestDispatcher(t, Config{
		Storm: StormConfig{
			Window:        time.Second,
			Threshold:     3,
			Merge:         false,
			CoalesceDelay: 10 * time.Millisecond,
			MaxBacklog:    maxBacklog,
		},
	})
	d.AddLine(LineConfig{ID: "sh", Mode: EdgeTriggered, Priority: 1})

	enteredA := make(chan struct{})
	releaseA := make(chan struct{})
	var claimA, claimB atomic.Int64

	d.Register("sh", Handler{
		ID: "h",
		Fn: func(ctx context.Context, env *Env) (Verdict, error) {
			if env.Source() == "A" {
				claimA.Add(1)
				close(enteredA)
				<-releaseA
				return Claimed, nil
			}
			claimB.Add(1)
			return Claimed, nil
		},
	})

	devA := NewVirtualDevice(d, "A")
	devB := NewVirtualDevice(d, "B")

	// A 的例程进入并阻塞，占用共享线。
	if err := devA.Raise("sh"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-enteredA:
	case <-time.After(timeout2s()):
		t.Fatal("handler for A did not enter")
	}

	// B 在 A 阻塞期间进入限速风暴。
	for i := 0; i < 10; i++ {
		if err := devB.Raise("sh"); err != nil {
			t.Fatal(err)
		}
	}

	// 在限速放行期间持续采样：积压深度不得超过 MaxBacklog。
	maxSeen := 0
	samplingDeadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(samplingDeadline) {
		if backlog := d.Metrics().Backlog; backlog > maxSeen {
			maxSeen = backlog
		}
		time.Sleep(2 * time.Millisecond)
	}
	if maxSeen > maxBacklog {
		t.Fatalf("backlog exceeded MaxBacklog: saw %d, limit %d", maxSeen, maxBacklog)
	}

	// B 的触发状态尚未投递，系统绝不可报告静默。
	if d.WaitQuiet(300 * time.Millisecond) {
		t.Fatal("reported quiet while B undelivered behind blocked handler")
	}

	// 例程返回后系统必须在有限时间内收敛。
	close(releaseA)
	if !d.WaitQuiet(timeout2s()) {
		t.Fatal("did not converge after blocked handler returned")
	}

	// 每个来源恰好投递一次：不丢失、不重复。
	if got := claimA.Load(); got != 1 {
		t.Fatalf("claims for A = %d, want 1", got)
	}
	if got := claimB.Load(); got != 1 {
		t.Fatalf("claims for B = %d, want 1 (coalesced storm delivery)", got)
	}

	metrics := d.Metrics()
	if metrics.StormMerged == 0 {
		t.Fatal("expected storm activity to be recorded")
	}
	if metrics.Backlog != 0 {
		t.Fatalf("backlog after settle = %d, want 0", metrics.Backlog)
	}
}

// TestRateLimitedStormDeterministicReplay 验证：包含限速风暴的输入序列按事件日志
// 重放时在有限时间内结束、最终投递不丢失，且重放得到的投递决策与首轮完全一致。
func TestRateLimitedStormDeterministicReplay(t *testing.T) {
	storm := StormConfig{
		Window:        time.Second,
		Threshold:     3,
		Merge:         false,
		CoalesceDelay: 25 * time.Millisecond,
		MaxBacklog:    3,
	}
	build := func(journal *Journal) *Dispatcher {
		cfg := Config{
			Clock:          NewManualClock(time.Unix(0, 0)),
			Storm:          storm,
			InlineDispatch: true,
		}
		if journal != nil {
			cfg.Journal = journal
		}
		d := New(cfg)
		d.AddLine(LineConfig{ID: "L", Mode: EdgeTriggered, Priority: 1})
		d.Register("L", Handler{
			ID: "h",
			Fn: func(ctx context.Context, env *Env) (Verdict, error) {
				return Claimed, nil
			},
		})
		return d
	}

	first := build(nil)
	first.Start(context.Background())

	clock := first.cfg.Clock.(*ManualClock)
	dev := NewVirtualDevice(first, "noisy")
	// 10 次突发远超阈值：前 3 次放行，其余进入限速积压并按节奏补投。
	for i := 0; i < 10; i++ {
		clock.Advance(time.Millisecond)
		if err := dev.Raise("L"); err != nil {
			t.Fatal(err)
		}
	}
	if !first.WaitQuiet(timeout2s()) {
		t.Fatal("first run did not settle after rate-limited storm")
	}
	first.Stop()

	original := DispatchDecisions(first.Journal().Trace())
	// 3 次阈值内放行 + 3 次限速补投 = 6 次投递，最终状态不丢失。
	if len(original) != 6 {
		t.Fatalf("original decisions = %d, want 6", len(original))
	}
	for _, decision := range original {
		if !decision.Claimed {
			t.Fatalf("unexpected unclaimed decision: %+v", decision)
		}
	}

	replayed, err := first.Journal().Replay(func(journal *Journal) (*Dispatcher, error) {
		return build(journal), nil
	})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if replayDecisions := DispatchDecisions(replayed); !DecisionsEqual(original, replayDecisions) {
		t.Fatalf("replay decisions differ:\noriginal=%v\nreplayed =%v", original, replayDecisions)
	}
}

// TestStormFinalRequestAfterSettleDeliveredOnce 验证：限速风暴平息、观测窗口
// 过期之后，最终中断请求必须被投递恰好一次，且系统恢复静默、积压归零。
func TestStormFinalRequestAfterSettleDeliveredOnce(t *testing.T) {
	clock := NewManualClock(time.Unix(0, 0))
	d := newTestDispatcher(t, Config{
		Clock: clock,
		Storm: StormConfig{
			Window:        time.Second,
			Threshold:     3,
			Merge:         false,
			CoalesceDelay: 10 * time.Millisecond,
			MaxBacklog:    3,
		},
	})
	d.AddLine(LineConfig{ID: "L", Mode: EdgeTriggered, Priority: 1})

	var claims atomic.Int64
	d.Register("L", Handler{
		ID: "h",
		Fn: func(ctx context.Context, env *Env) (Verdict, error) {
			claims.Add(1)
			return Claimed, nil
		},
	})

	dev := NewVirtualDevice(d, "noisy")
	for i := 0; i < 10; i++ {
		clock.Advance(time.Millisecond)
		if err := dev.Raise("L"); err != nil {
			t.Fatal(err)
		}
	}
	if !d.WaitQuiet(timeout2s()) {
		t.Fatal("did not settle after rate-limited storm")
	}
	before := claims.Load()
	if before == 0 {
		t.Fatal("no delivery during storm")
	}

	// 观测窗口过期后，最终中断请求恰好投递一次。
	clock.Advance(2 * time.Second)
	if err := dev.Raise("L"); err != nil {
		t.Fatal(err)
	}
	if !d.WaitQuiet(timeout2s()) {
		t.Fatal("did not settle after final request")
	}
	if got := claims.Load() - before; got != 1 {
		t.Fatalf("final request delivered %d times, want exactly 1", got)
	}
	if backlog := d.Metrics().Backlog; backlog != 0 {
		t.Fatalf("backlog after settle = %d, want 0", backlog)
	}
}

// TestSharedLineMultiSourceRateLimitIndependent 验证同一条线的多个来源同时进入
// 限速时，各来源的补投相互独立：任一来源不得因其他来源先进入待处理而被跳过，
// 补投不重复，风暴结束后每个来源恰好投递一次。
func TestSharedLineMultiSourceRateLimitIndependent(t *testing.T) {
	d := newTestDispatcher(t, Config{
		Storm: StormConfig{
			Window:        time.Second,
			Threshold:     3,
			Merge:         false,
			CoalesceDelay: 10 * time.Millisecond,
			MaxBacklog:    3,
		},
	})
	d.AddLine(LineConfig{ID: "sh", Mode: EdgeTriggered, Priority: 1})

	enteredA := make(chan struct{})
	releaseA := make(chan struct{})
	var claimA, claimB, claimC atomic.Int64

	d.Register("sh", Handler{
		ID: "h",
		Fn: func(ctx context.Context, env *Env) (Verdict, error) {
			switch env.Source() {
			case "A":
				claimA.Add(1)
				close(enteredA)
				<-releaseA
			case "B":
				claimB.Add(1)
			default:
				claimC.Add(1)
			}
			return Claimed, nil
		},
	})

	devA := NewVirtualDevice(d, "A")
	devB := NewVirtualDevice(d, "B")
	devC := NewVirtualDevice(d, "C")

	// A 阻塞共享线，B 与 C 在此期间同时进入限速。
	if err := devA.Raise("sh"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-enteredA:
	case <-time.After(timeout2s()):
		t.Fatal("handler for A did not enter")
	}
	for i := 0; i < 8; i++ {
		if err := devB.Raise("sh"); err != nil {
			t.Fatal(err)
		}
		if err := devC.Raise("sh"); err != nil {
			t.Fatal(err)
		}
	}

	// 等待两个来源的限速积压在 A 阻塞期间全部放行完毕：
	// 放行结果合并进各自的待处理通知，积压收敛到每来源一条。
	drainDeadline := time.Now().Add(timeout2s())
	for {
		backlog := d.Metrics().Backlog
		if backlog <= 2 {
			break
		}
		if time.Now().After(drainDeadline) {
			t.Fatalf("storm backlog did not drain behind blocked handler, backlog = %d", backlog)
		}
		time.Sleep(2 * time.Millisecond)
	}

	// B 与 C 都尚未投递，系统不得报告静默。
	if d.WaitQuiet(300 * time.Millisecond) {
		t.Fatal("reported quiet while B and C undelivered behind blocked handler")
	}

	close(releaseA)
	if !d.WaitQuiet(timeout2s()) {
		t.Fatal("did not converge after blocked handler returned")
	}

	// 每个来源恰好投递一次：先进入待处理的来源不得挡住其他来源。
	if got := claimA.Load(); got != 1 {
		t.Fatalf("claims for A = %d, want 1", got)
	}
	if got := claimB.Load(); got != 1 {
		t.Fatalf("claims for B = %d, want 1", got)
	}
	if got := claimC.Load(); got != 1 {
		t.Fatalf("claims for C = %d, want 1", got)
	}
	if backlog := d.Metrics().Backlog; backlog != 0 {
		t.Fatalf("backlog after settle = %d, want 0", backlog)
	}
}

// TestSharedLineStormFinalRequestNotLost 验证：来源的限速积压在结算时，
// 不得因同线其他来源先进入待处理而被跳过；风暴结束后的最终中断请求必须
// 恰好投递一次，且在投递完成前系统不得报告静默。
func TestSharedLineStormFinalRequestNotLost(t *testing.T) {
	clock := NewManualClock(time.Unix(0, 0))
	d := newTestDispatcher(t, Config{
		Clock: clock,
		Storm: StormConfig{
			Window:        time.Second,
			Threshold:     3,
			Merge:         false,
			CoalesceDelay: 50 * time.Millisecond,
			MaxBacklog:    3,
		},
	})
	d.AddLine(LineConfig{ID: "sh", Mode: EdgeTriggered, Priority: 1})

	claims := make(chan SourceID, 16)
	enteredBlocker := make(chan struct{})
	releaseBlocker := make(chan struct{})

	d.Register("sh", Handler{
		ID: "h",
		Fn: func(ctx context.Context, env *Env) (Verdict, error) {
			if env.Source() == "Blocker" {
				close(enteredBlocker)
				<-releaseBlocker
				return Claimed, nil
			}
			claims <- env.Source()
			return Claimed, nil
		},
	})

	devY := NewVirtualDevice(d, "Y")
	devX := NewVirtualDevice(d, "X")
	devBlocker := NewVirtualDevice(d, "Blocker")

	expectClaim := func(want SourceID) {
		t.Helper()
		select {
		case got := <-claims:
			if got != want {
				t.Fatalf("claim = %s, want %s", got, want)
			}
		case <-time.After(timeout2s()):
			t.Fatalf("expected claim for %s", want)
		}
	}
	raise := func(dev *VirtualDevice) {
		t.Helper()
		clock.Advance(time.Millisecond)
		if err := dev.Raise("sh"); err != nil {
			t.Fatal(err)
		}
	}

	// Y 与 X 各自完成 3 次投递（未达风暴阈值），线状态回到空闲。
	for i := 0; i < 3; i++ {
		raise(devY)
		expectClaim("Y")
	}
	for i := 0; i < 3; i++ {
		raise(devX)
		expectClaim("X")
	}

	// X 继续突发进入限速：脉冲进入有界积压，等待放行补投。
	for i := 0; i < 6; i++ {
		raise(devX)
	}

	// Blocker 占用共享线；Y 在窗口过期后再次触发并进入待处理。
	raise(devBlocker)
	select {
	case <-enteredBlocker:
	case <-time.After(timeout2s()):
		t.Fatal("blocker handler did not enter")
	}
	clock.Advance(2 * time.Second)
	raise(devY)

	// X 的限速放行不得被先进入待处理的 Y 挡住；X、Y 均未投递完成前，
	// 系统不得报告静默。
	if d.WaitQuiet(300 * time.Millisecond) {
		t.Fatal("reported quiet while X/Y undelivered behind blocked handler")
	}

	close(releaseBlocker)
	if !d.WaitQuiet(timeout2s()) {
		t.Fatal("did not converge after blocked handler returned")
	}

	// 风暴结束后的最终中断请求恰好投递一次：X 与 Y 各补投一次。
	got := map[SourceID]int{}
	for i := 0; i < 2; i++ {
		select {
		case source := <-claims:
			got[source]++
		case <-time.After(timeout2s()):
			t.Fatal("missing final delivery after storm")
		}
	}
	if got["X"] != 1 || got["Y"] != 1 {
		t.Fatalf("final deliveries = %v, want one each for X and Y", got)
	}

	metrics := d.Metrics()
	if metrics.Claimed != 9 {
		t.Fatalf("claimed = %d, want 9 (3+3 early, 1 blocker, 1+1 final)", metrics.Claimed)
	}
	if metrics.Backlog != 0 {
		t.Fatalf("backlog after settle = %d, want 0", metrics.Backlog)
	}
}
