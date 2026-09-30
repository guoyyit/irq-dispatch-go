package irq

import (
	"context"
	"sync"
	"testing"
)

func TestEdgeCoalescingDoesNotLoseExistence(t *testing.T) {
	d := newTestDispatcher(t, Config{})
	d.AddLine(LineConfig{ID: "edge", Mode: EdgeTriggered, Priority: 1})

	var mu sync.Mutex
	var calls [][]SourceID
	entered := make(chan struct{}, 16)
	release := make(chan struct{})

	err := d.Register("edge", Handler{
		ID: "h",
		Fn: func(ctx context.Context, env *Env) (Verdict, error) {
			mu.Lock()
			calls = append(calls, []SourceID{env.Source()})
			mu.Unlock()
			entered <- struct{}{}
			<-release
			return Claimed, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	dev := NewVirtualDevice(d, "dev")
	dev.Raise("edge") // 第一次：进入处理
	<-entered

	// 处理尚未响应，连续多次边缘必须合并为一次后续待处理。
	dev.Raise("edge")
	dev.Raise("edge")
	dev.Raise("edge")
	// 屏障确保这 3 次输入都在例程响应前被分发器接收（异步输入需显式同步）。
	if err := d.Barrier(); err != nil {
		t.Fatal(err)
	}

	close(release)
	if !d.WaitQuiet(timeout2s()) {
		t.Fatal("did not settle")
	}

	if got := len(calls); got != 2 {
		t.Fatalf("handler calls = %d, want 2 (initial + one coalesced)", got)
	}

	snapshot := d.Metrics()
	if snapshot.Claimed != 2 {
		t.Fatalf("claimed = %d, want 2", snapshot.Claimed)
	}
}

func TestLevelReportedOnceAndClearsOnDeassert(t *testing.T) {
	d := newTestDispatcher(t, Config{})
	d.AddLine(LineConfig{ID: "level", Mode: LevelTriggered, Priority: 1})

	var mu sync.Mutex
	var calls int
	entered := make(chan struct{}, 16)
	release := make(chan struct{})

	err := d.Register("level", Handler{
		ID: "h",
		Fn: func(ctx context.Context, env *Env) (Verdict, error) {
			mu.Lock()
			calls++
			mu.Unlock()
			entered <- struct{}{}
			<-release
			return Claimed, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	dev := NewVirtualDevice(d, "dev")
	dev.Raise("level") // 置位电平
	<-entered

	// 电平保持期间的重复请求不得再次上报。
	dev.Raise("level")
	dev.Raise("level")
	close(release)
	if !d.WaitQuiet(timeout2s()) {
		t.Fatal("did not settle")
	}
	if calls != 1 {
		t.Fatalf("calls during held level = %d, want 1", calls)
	}

	// 撤销后可再次上报。
	dev.Deassert("level")
	second := make(chan struct{})
	d.DisableHandler("level", "h")
	d.EnableHandler("level", "h")
	_ = second

	dev.Raise("level")
	if !d.WaitQuiet(timeout2s()) {
		t.Fatal("did not settle after re-raise")
	}
	if calls != 2 {
		t.Fatalf("calls after re-assert = %d, want 2", calls)
	}
}
