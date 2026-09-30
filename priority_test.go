package irq

import (
	"context"
	"testing"
	"time"
)

// TestPriorityPreemptionAndDeferredDelivery 验证：
//   - 高优先级可在低优先级屏蔽窗口内被调度（抢占）；
//   - 同优先级/更低优先级在所有屏蔽退出后才开始询问例程（补投）；
//   - 补投不丢失、不重复。
func TestPriorityPreemptionAndDeferredDelivery(t *testing.T) {
	d := newTestDispatcher(t, Config{})
	d.AddLine(LineConfig{ID: "low", Mode: EdgeTriggered, Priority: 1})
	d.AddLine(LineConfig{ID: "mid", Mode: EdgeTriggered, Priority: 2})
	d.AddLine(LineConfig{ID: "high", Mode: EdgeTriggered, Priority: 3})

	lowEntered := make(chan struct{})
	releaseLow := make(chan struct{})
	highEntered := make(chan struct{})
	releaseHigh := make(chan struct{})
	midEntered := make(chan struct{}, 1)

	d.Register("low", Handler{
		ID: "low-h",
		Fn: func(ctx context.Context, env *Env) (Verdict, error) {
			lowEntered <- struct{}{}
			<-releaseLow
			return Claimed, nil
		},
	})
	d.Register("mid", Handler{
		ID: "mid-h",
		Fn: func(ctx context.Context, env *Env) (Verdict, error) {
			select {
			case midEntered <- struct{}{}:
			default:
			}
			return Claimed, nil
		},
	})
	d.Register("high", Handler{
		ID: "high-h",
		Fn: func(ctx context.Context, env *Env) (Verdict, error) {
			highEntered <- struct{}{}
			<-releaseHigh
			return Claimed, nil
		},
	})

	d.Raise("low", "dev-low")
	<-lowEntered

	// 先触发高优先级抢占并确认其进入屏蔽窗口；之后到达的同/低优先级必须等待。
	d.Raise("high", "dev-high")
	<-highEntered

	// 此刻 low(1) 与 high(3) 都在屏蔽窗口内；mid(2) 必须推迟。
	d.Raise("mid", "dev-mid")

	// low 与 high 都在屏蔽窗口内时，mid 的例程绝不能已被询问。
	select {
	case <-midEntered:
		t.Fatal("mid handler ran while a higher-priority window was still open")
	case <-time.After(30 * time.Millisecond):
	}

	close(releaseHigh)
	close(releaseLow)
	if !d.WaitQuiet(timeout2s()) {
		t.Fatal("did not settle")
	}

	// 屏蔽全部退出后 mid 必须补投且仅一次。
	select {
	case <-midEntered:
	case <-time.After(timeout2s()):
		t.Fatal("deferred mid was never delivered")
	}

	metrics := d.Metrics()
	if metrics.Claimed != 3 {
		t.Fatalf("claimed = %d, want 3 (no lost/duplicate)", metrics.Claimed)
	}
	if metrics.Deferred < 1 {
		t.Fatalf("deferred metric = %d, want >= 1", metrics.Deferred)
	}
}
