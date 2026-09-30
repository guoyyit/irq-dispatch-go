package irq

import (
	"context"
	"sync"
	"testing"
)

func buildReplaySystem(journal ...*Journal) *Dispatcher {
	cfg := Config{InlineDispatch: true, BarrierGated: true}
	if len(journal) > 0 {
		cfg.Journal = journal[0]
	}
	d := New(cfg)

	// 三条不同的线，优先级各不相同，全部使用边缘触发以保证每次输入独立投递。
	d.AddLine(LineConfig{ID: "lo", Mode: EdgeTriggered, Priority: 1})
	d.AddLine(LineConfig{ID: "md", Mode: EdgeTriggered, Priority: 2})
	d.AddLine(LineConfig{ID: "hi", Mode: EdgeTriggered, Priority: 3})

	claim := func(line LineID, id SourceID) {
		d.Register(line, Handler{
			ID: id,
			Fn: func(ctx context.Context, env *Env) (Verdict, error) {
				return Claimed, nil
			},
		})
	}
	claim("lo", "lo-h")
	claim("md", "md-h")
	claim("hi", "hi-h")
	return d
}

// TestConcurrentTriggerDeterministicReplay 验证：多路设备并发产生中断时，
// 输入侧分配唯一全序；按同一输入序列重放得到完全一致的归一化分发轨迹。
func TestConcurrentTriggerDeterministicReplay(t *testing.T) {
	first := buildReplaySystem()
	first.Start(context.Background())

	lines := []LineID{"lo", "md", "hi"}
	var wg sync.WaitGroup
	for round := 0; round < 6; round++ {
		for _, line := range lines {
			wg.Add(1)
			go func(line LineID) {
				defer wg.Done()
				if err := first.Raise(line, SourceID("dev-"+line)); err != nil {
					t.Error(err)
				}
			}(line)
		}
		wg.Wait()
		// 屏障划分批次：屏障之前的输入在该点完成接收与派发决策。
		if err := first.Barrier(); err != nil {
			t.Fatal(err)
		}
	}

	if !first.WaitQuiet(timeout2s()) {
		t.Fatal("first run did not settle")
	}
	first.Stop()

	originalTrace := first.Journal().Trace()
	if len(originalTrace) == 0 {
		t.Fatal("no trace recorded")
	}
	original := DispatchDecisions(originalTrace)
	if len(original) != 18 {
		t.Fatalf("original decisions = %d, want 18", len(original))
	}

	replayed, err := first.Journal().Replay(func(journal *Journal) (*Dispatcher, error) {
		return buildReplaySystem(journal), nil
	})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	replayDecisions := DispatchDecisions(replayed)

	if !DecisionsEqual(original, replayDecisions) {
		t.Fatalf("replay trace differs:\noriginal=%v\nreplayed =%v",
			original, replayDecisions)
	}

	// 每个输入都恰好被认领一次：不丢失、不重复。
	counts := map[LineID]int{}
	for _, decision := range replayDecisions {
		counts[decision.Line]++
		if !decision.Claimed {
			t.Fatalf("unexpected unclaimed decision: %+v", decision)
		}
	}
	for _, line := range lines {
		if counts[line] != 6 {
			t.Fatalf("line %s delivered %d times, want 6", line, counts[line])
		}
	}
}
