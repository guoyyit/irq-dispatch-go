package irq

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestSharedLineRateLimitBlockedHandlerBounded 覆盖“共享线 + 阻塞例程 + 限速”组合：
// 同线例程阻塞期间，另一来源进入限速后积压不得突破 MaxBacklog、不得误报静默；
// 例程返回后系统必须在有限时间内收敛，且积压归零。
func TestSharedLineRateLimitBlockedHandlerBounded(t *testing.T) {
	d := newTestDispatcher(t, Config{
		Storm: StormConfig{
			Window:        500 * time.Millisecond,
			Threshold:     2,
			Merge:         false,
			CoalesceDelay: 10 * time.Millisecond,
			MaxBacklog:    3,
		},
	})
	d.AddLine(LineConfig{ID: "sh", Mode: EdgeTriggered, Priority: 1})

	blockedEntered := make(chan struct{})
	unblock := make(chan struct{})
	var noisyClaims atomic.Int64

	d.Register("sh", Handler{
		ID: "h",
		Fn: func(ctx context.Context, env *Env) (Verdict, error) {
			if env.Source() == "blocked" {
				close(blockedEntered)
				<-unblock
				return Claimed, nil
			}
			noisyClaims.Add(1)
			return Claimed, nil
		},
	})

	// 阻塞来源占住共享线（同线串行，其他来源的通知只能排队）。
	if err := d.Raise("sh", "blocked"); err != nil {
		t.Fatal(err)
	}
	<-blockedEntered

	// 另一来源在观测窗口内突发，进入限速积压。
	noisy := NewVirtualDevice(d, "noisy")
	for i := 0; i < 12; i++ {
		if err := noisy.Raise("sh"); err != nil {
			t.Fatal(err)
		}
	}

	// 持续采样：积压深度任何时刻不得超过 MaxBacklog；
	// 阻塞未解除且存在未投递工作时，静默判定不得为真。
	maxDepth := 0
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		if depth := d.Metrics().Backlog; depth > maxDepth {
			maxDepth = depth
		}
		if d.WaitQuiet(5 * time.Millisecond) {
			t.Fatal("quiet reported while handler blocked with undelivered work")
		}
	}
	if maxDepth > 3 {
		t.Fatalf("backlog depth %d exceeds MaxBacklog 3", maxDepth)
	}
	if d.Metrics().StormMerged == 0 {
		t.Fatal("rate limiting never engaged for the noisy source")
	}

	// 例程返回后系统必须在有限时间内收敛。
	close(unblock)
	if !d.WaitQuiet(timeout2s()) {
		t.Fatal("did not converge after blocked handler returned")
	}
	if noisyClaims.Load() == 0 {
		t.Fatal("noisy source never delivered")
	}
	if got := d.Metrics().Backlog; got != 0 {
		t.Fatalf("backlog after settle = %d, want 0", got)
	}
}

// TestSharedLineMultiSourceRateLimitIndependent 覆盖同线多来源同时限速：
// 各来源的补投相互独立，先进入待处理的来源不得顶替其他来源的放行；
// 静默后任一来源的新请求仍必须恰好投递一次（无残留未投递状态）。
func TestSharedLineMultiSourceRateLimitIndependent(t *testing.T) {
	d := newTestDispatcher(t, Config{
		Storm: StormConfig{
			Window:        500 * time.Millisecond,
			Threshold:     2,
			Merge:         false,
			CoalesceDelay: 10 * time.Millisecond,
			MaxBacklog:    3,
		},
	})
	d.AddLine(LineConfig{ID: "sh", Mode: EdgeTriggered, Priority: 1})

	var mu sync.Mutex
	claims := map[SourceID]int{}
	d.Register("sh", Handler{
		ID: "h",
		Fn: func(ctx context.Context, env *Env) (Verdict, error) {
			mu.Lock()
			claims[env.Source()]++
			mu.Unlock()
			return Claimed, nil
		},
	})

	// 两个来源同时进入限速风暴。
	for i := 0; i < 10; i++ {
		if err := d.Raise("sh", "src-a"); err != nil {
			t.Fatal(err)
		}
		if err := d.Raise("sh", "src-b"); err != nil {
			t.Fatal(err)
		}
	}

	if !d.WaitQuiet(timeout2s()) {
		t.Fatal("did not settle after multi-source storm")
	}

	mu.Lock()
	a, b := claims["src-a"], claims["src-b"]
	mu.Unlock()
	if a == 0 || b == 0 {
		t.Fatalf("source delivery lost: src-a=%d src-b=%d", a, b)
	}
	if got := d.Metrics().Backlog; got != 0 {
		t.Fatalf("backlog after settle = %d, want 0", got)
	}

	// 静默后任一来源的新请求必须恰好投递一次。
	if err := d.Raise("sh", "src-b"); err != nil {
		t.Fatal(err)
	}
	if !d.WaitQuiet(timeout2s()) {
		t.Fatal("did not settle after post-storm raise")
	}
	mu.Lock()
	b2 := claims["src-b"]
	mu.Unlock()
	if b2 != b+1 {
		t.Fatalf("post-storm raise delivered %d times, want exactly 1", b2-b)
	}
}

// TestSharedLineRateLimitReleaseIsPerSource 确定性复现“同线多来源同时限速时，
// 先进入待处理状态的来源挡住其他来源补投”：阻塞占线期间，src-a 保持待处理边缘，
// src-b 的限速放行不得被 src-a 顶替；src-b 风暴期间的最终请求必须被投递。
func TestSharedLineRateLimitReleaseIsPerSource(t *testing.T) {
	d := newTestDispatcher(t, Config{
		Storm: StormConfig{
			Window:        500 * time.Millisecond,
			Threshold:     1,
			Merge:         false,
			CoalesceDelay: 10 * time.Millisecond,
			MaxBacklog:    3,
		},
	})
	d.AddLine(LineConfig{ID: "sh", Mode: EdgeTriggered, Priority: 1})

	blockEntered := make(chan struct{})
	unblock := make(chan struct{})
	var mu sync.Mutex
	claims := map[SourceID]int{}
	d.Register("sh", Handler{
		ID: "h",
		Fn: func(ctx context.Context, env *Env) (Verdict, error) {
			if env.Source() == "blocker" {
				close(blockEntered)
				<-unblock
				return Claimed, nil
			}
			mu.Lock()
			claims[env.Source()]++
			mu.Unlock()
			return Claimed, nil
		},
	})

	// 各来源先完成一次投递，确定性地建立来源注册顺序与空闲边缘。
	for _, source := range []SourceID{"src-a", "src-b"} {
		if err := d.Raise("sh", source); err != nil {
			t.Fatal(err)
		}
	}
	if !d.WaitQuiet(timeout2s()) {
		t.Fatal("warm-up deliveries did not settle")
	}

	// 阻塞来源占住共享线，此后所有通知只能排队。
	if err := d.Raise("sh", "blocker"); err != nil {
		t.Fatal(err)
	}
	<-blockEntered

	// src-a 进入限速：其边缘在阻塞期间持续保持待处理。
	if err := d.Raise("sh", "src-a"); err != nil {
		t.Fatal(err)
	}
	// src-b 进入限速：三次放行额度全部进入积压。
	for i := 0; i < 3; i++ {
		if err := d.Raise("sh", "src-b"); err != nil {
			t.Fatal(err)
		}
	}

	// 等待双方限速积压全部结算（放行定时器按 CoalesceDelay 逐个结算），
	// 确保解除阻塞发生在所有放行之后：此时 src-b 的补投必须已经入队。
	// 注意输入是异步处理的，必须先观察到积压累积，再等待其归零，
	// 否则等待会在积压尚未建立时提前退出。
	stormDepth := func(source SourceID) int {
		d.mu.Lock()
		defer d.mu.Unlock()
		if state, ok := d.storms[sourceRef{line: "sh", source: source}]; ok {
			return state.depth()
		}
		return 0
	}
	waitStorm := func(cond func() bool, what string) {
		deadline := time.Now().Add(timeout2s())
		for !cond() && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		if !cond() {
			t.Fatalf("storm backlog %s: src-a=%d src-b=%d",
				what, stormDepth("src-a"), stormDepth("src-b"))
		}
	}
	waitStorm(func() bool { return stormDepth("src-b") >= 1 }, "did not accumulate")
	waitStorm(func() bool {
		return stormDepth("src-a") == 0 && stormDepth("src-b") == 0
	}, "did not drain")

	// 解除阻塞，排队通知依次投递后系统必须收敛。
	close(unblock)
	if !d.WaitQuiet(timeout2s()) {
		t.Fatal("did not converge after blocked handler returned")
	}

	// src-b：预热 1 次 + 风暴合并补投 1 次，恰好 2 次；
	// 若其放行被 src-a 顶替，风暴期间的最终请求将被静默丢弃（只剩 1 次）。
	mu.Lock()
	b := claims["src-b"]
	a := claims["src-a"]
	mu.Unlock()
	if b != 2 {
		t.Fatalf("src-b claims = %d, want 2 (warm-up + coalesced storm delivery)", b)
	}
	if a != 2 {
		t.Fatalf("src-a claims = %d, want 2 (warm-up + rate-limited release)", a)
	}
	if got := d.Metrics().Backlog; got != 0 {
		t.Fatalf("backlog after settle = %d, want 0", got)
	}
}

// TestStormFinalRequestDeliveredExactlyOnce 覆盖风暴结束后的最终中断请求：
// 用逻辑时钟确定性地退出风暴窗口，最终请求必须恰好投递一次。
func TestStormFinalRequestDeliveredExactlyOnce(t *testing.T) {
	clock := NewManualClock(time.Unix(0, 0))
	d := newTestDispatcher(t, Config{
		Clock: clock,
		Storm: StormConfig{
			Window:        200 * time.Millisecond,
			Threshold:     2,
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
		if err := dev.Raise("L"); err != nil {
			t.Fatal(err)
		}
	}
	if !d.WaitQuiet(timeout2s()) {
		t.Fatal("did not settle after storm")
	}
	before := claims.Load()
	if before == 0 {
		t.Fatal("storm produced no delivery")
	}

	// 逻辑时钟越过观测窗口，风暴退出；最终中断请求必须恰好投递一次。
	clock.Advance(time.Second)
	if err := dev.Raise("L"); err != nil {
		t.Fatal(err)
	}
	if !d.WaitQuiet(timeout2s()) {
		t.Fatal("did not settle after final raise")
	}
	if got := claims.Load(); got != before+1 {
		t.Fatalf("final request delivered %d times, want exactly 1", got-before)
	}
	if got := d.Metrics().Backlog; got != 0 {
		t.Fatalf("backlog = %d, want 0", got)
	}
}

// TestRateLimitReleaseWhileHandlerInFlight 覆盖限速放行时例程正在投递的组合：
// 放行额度必须折算为滞留（rearmed），例程返回后补投恰好一次，系统有限时间收敛。
func TestRateLimitReleaseWhileHandlerInFlight(t *testing.T) {
	d := newTestDispatcher(t, Config{
		Storm: StormConfig{
			Window:        500 * time.Millisecond,
			Threshold:     2,
			Merge:         false,
			CoalesceDelay: 10 * time.Millisecond,
			MaxBacklog:    3,
		},
	})
	d.AddLine(LineConfig{ID: "L", Mode: EdgeTriggered, Priority: 1})

	entered := make(chan struct{})
	gate := make(chan struct{})
	var calls atomic.Int64
	d.Register("L", Handler{
		ID: "h",
		Fn: func(ctx context.Context, env *Env) (Verdict, error) {
			if calls.Add(1) == 1 {
				close(entered)
				<-gate
			}
			return Claimed, nil
		},
	})

	dev := NewVirtualDevice(d, "noisy")
	if err := dev.Raise("L"); err != nil {
		t.Fatal(err)
	}
	<-entered // 首次投递在途，例程阻塞。

	// 在途期间突发进入限速：积压累积后由放行定时器逐个结算，
	// 每次放行都命中“在途”分支而折算为滞留。
	for i := 0; i < 9; i++ {
		if err := dev.Raise("L"); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(timeout2s())
	for d.Metrics().StormMerged == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if d.Metrics().StormMerged == 0 {
		t.Fatal("rate limiting never engaged")
	}
	for depth := stormDepthOf(d, "L", "noisy"); depth != 0; depth = stormDepthOf(d, "L", "noisy") {
		if !time.Now().Before(deadline) {
			t.Fatalf("rate-limited backlog did not drain: %d", depth)
		}
		time.Sleep(time.Millisecond)
	}

	close(gate)
	if !d.WaitQuiet(timeout2s()) {
		t.Fatal("did not converge after in-flight handler returned")
	}
	// 首次投递 + 滞留补投，恰好 2 次；风暴期间的存在性不丢失也不重复。
	if got := calls.Load(); got != 2 {
		t.Fatalf("handler calls = %d, want 2 (initial + one rearmed)", got)
	}
	if got := d.Metrics().Backlog; got != 0 {
		t.Fatalf("backlog after settle = %d, want 0", got)
	}
}

// stormDepthOf 返回指定来源当前的限速积压深度（测试与实现同包，直接读取状态）。
func stormDepthOf(d *Dispatcher, line LineID, source SourceID) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	if state, ok := d.storms[sourceRef{line: line, source: source}]; ok {
		return state.depth()
	}
	return 0
}

// TestStalePendingNoticeDiscarded 覆盖防御路径：队列中出现过期通知
// （触发状态已不存在）时，调度器必须丢弃它并继续工作，不得卡住或死锁。
func TestStalePendingNoticeDiscarded(t *testing.T) {
	d := newTestDispatcher(t, Config{})
	d.AddLine(LineConfig{ID: "L", Mode: EdgeTriggered, Priority: 1})

	var claims atomic.Int64
	d.Register("L", Handler{
		ID: "h",
		Fn: func(ctx context.Context, env *Env) (Verdict, error) {
			claims.Add(1)
			return Claimed, nil
		},
	})

	// 注入一条无对应触发状态的过期通知（对应来源边缘处于 idle）。
	d.mu.Lock()
	d.pending = append(d.pending, pendingNotice{
		ref:      sourceRef{line: "L", source: "ghost"},
		priority: 1,
		seq:      1,
		raisedAt: d.cfg.Clock.Now(),
	})
	d.refreshBacklogLocked()
	d.mu.Unlock()
	d.kickLoop()

	// 过期通知被丢弃后，后续真实中断必须正常投递，系统必须收敛。
	if err := d.Raise("L", "real"); err != nil {
		t.Fatal(err)
	}
	if !d.WaitQuiet(timeout2s()) {
		t.Fatal("dispatcher stuck after stale notice")
	}
	if claims.Load() != 1 {
		t.Fatalf("claims = %d, want 1 (stale notice must not deliver)", claims.Load())
	}
	if got := d.Metrics().Backlog; got != 0 {
		t.Fatalf("backlog = %d, want 0", got)
	}
}

// TestRateLimitedStormDeterministicReplay 覆盖限速风暴下的确定性重放：
// 屏障门控 + 内联派发使限速决策只依赖记录的输入时间戳，
// 重放在有限时间内结束且投递决策与首轮完全一致。
func TestRateLimitedStormDeterministicReplay(t *testing.T) {
	storm := StormConfig{
		Window:        time.Second,
		Threshold:     2,
		Merge:         false,
		CoalesceDelay: 20 * time.Millisecond,
		MaxBacklog:    3,
	}
	build := func(journal ...*Journal) *Dispatcher {
		cfg := Config{InlineDispatch: true, BarrierGated: true, Storm: storm}
		if len(journal) > 0 {
			cfg.Journal = journal[0]
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

	first := build()
	first.Start(context.Background())
	dev := NewVirtualDevice(first, "noisy")
	for i := 0; i < 10; i++ {
		if err := dev.Raise("L"); err != nil {
			t.Fatal(err)
		}
	}
	if err := first.Barrier(); err != nil {
		t.Fatal(err)
	}
	if !first.WaitQuiet(5*time.Second) {
		first.Stop()
		t.Fatal("first run did not settle")
	}
	first.Stop()

	original := DispatchDecisions(first.Journal().Trace())
	if len(original) == 0 {
		t.Fatal("no dispatch decisions recorded")
	}
	for _, decision := range original {
		if !decision.Claimed {
			t.Fatalf("final request lost in first run: %+v", decision)
		}
	}

	replayed, err := first.Journal().Replay(func(journal *Journal) (*Dispatcher, error) {
		return build(journal), nil
	})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if !DecisionsEqual(original, DispatchDecisions(replayed)) {
		t.Fatalf("replay decisions differ:\noriginal=%v\nreplayed=%v",
			original, DispatchDecisions(replayed))
	}
}
