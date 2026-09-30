package irq

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestPriorityNesting verifies:
//   - a high-priority interrupt preempts an in-flight low-priority handler;
//   - an equal and a lower priority interrupt raised inside the mask are held;
//   - after the mask unwinds they are delivered in FIFO order, once each.
func TestPriorityNesting(t *testing.T) {
	dp := New(DefaultConfig())

	var mu sync.Mutex
	order := []string{}
	record := func(name string) {
		mu.Lock()
		order = append(order, name)
		mu.Unlock()
	}

	lowStarted := make(chan struct{})
	releaseLow := make(chan struct{})

	must(t, dp.Register(1, EdgeTriggered, Handler{
		ID:       "low",
		Priority: 1,
		Handle: func(ctx context.Context, ev Event) bool {
			record("low-start")
			close(lowStarted)
			select {
			case <-releaseLow:
				record("low-end")
			case <-ctx.Done():
			}
			return true
		},
	}))
	must(t, dp.Register(2, EdgeTriggered, Handler{
		ID:       "equal",
		Priority: 1,
		Handle: func(ctx context.Context, ev Event) bool {
			record("equal")
			return true
		},
	}))
	must(t, dp.Register(3, EdgeTriggered, Handler{
		ID:       "lower",
		Priority: 0,
		Handle: func(ctx context.Context, ev Event) bool {
			record("lower")
			return true
		},
	}))
	must(t, dp.Register(4, EdgeTriggered, Handler{
		ID:       "high",
		Priority: 9,
		Handle: func(ctx context.Context, ev Event) bool {
			record("high")
			return true
		},
	}))

	must(t, dp.Start())
	defer dp.Stop(context.Background())

	dp.Raise(1, "s-low")
	<-lowStarted

	// While the low mask is active: equal and lower are held, high preempts.
	dp.Raise(2, "s-equal")
	dp.Raise(3, "s-lower")
	time.Sleep(30 * time.Millisecond)
	dp.Raise(4, "s-high")

	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return contains(order, "high")
	}, "high preempts low")

	mu.Lock()
	before := fmt.Sprint(order)
	mu.Unlock()
	if containsAny(order, "equal", "lower") {
		t.Fatalf("equal/lower must be held during mask, got %s", before)
	}

	close(releaseLow)

	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return contains(order, "lower") && contains(order, "equal") && contains(order, "low-end")
	}, "held equal/lower delivered after unmask")

	mu.Lock()
	final := append([]string(nil), order...)
	mu.Unlock()

	// Every named event exactly once.
	for _, name := range []string{"low-start", "high", "equal", "lower", "low-end"} {
		if countOf(final, name) != 1 {
			t.Fatalf("%s delivered %d times in %v", name, countOf(final, name), final)
		}
	}
	// High nests before low completes; equal precedes lower (FIFO at hold).
	if indexOf(final, "high") > indexOf(final, "low-end") {
		t.Fatalf("high must nest before low ends: %v", final)
	}
	if indexOf(final, "equal") > indexOf(final, "lower") {
		t.Fatalf("held equal must be delivered before held lower: %v", final)
	}
}

func contains(list []string, v string) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}

func containsAny(list []string, vs ...string) bool {
	for _, v := range vs {
		if contains(list, v) {
			return true
		}
	}
	return false
}

func indexOf(list []string, v string) int {
	for i, x := range list {
		if x == v {
			return i
		}
	}
	return -1
}

func countOf(list []string, v string) int {
	n := 0
	for _, x := range list {
		if x == v {
			n++
		}
	}
	return n
}
