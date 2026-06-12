package worker

import (
	"context"
	"testing"

	"github.com/rstmyldrm7/go-notify/internal/domain"
	"github.com/rstmyldrm7/go-notify/internal/queue"
)

// jobWith builds a job carrying just enough to identify it in assertions.
func jobWith(id string, p domain.Priority) job {
	return job{priority: p, msg: queue.NotificationMessage{Recipient: id}}
}

// TestNextStrictPriority verifies the priority select always drains high before
// normal before low.
func TestNextStrictPriority(t *testing.T) {
	p := &Pool{
		highCh:   make(chan job, 4),
		normalCh: make(chan job, 4),
		lowCh:    make(chan job, 4),
	}

	// Interleave the loads so order can only come from the select, not insertion.
	p.lowCh <- jobWith("l1", domain.PriorityLow)
	p.normalCh <- jobWith("n1", domain.PriorityNormal)
	p.highCh <- jobWith("h1", domain.PriorityHigh)
	p.lowCh <- jobWith("l2", domain.PriorityLow)
	p.normalCh <- jobWith("n2", domain.PriorityNormal)
	p.highCh <- jobWith("h2", domain.PriorityHigh)

	want := []string{"h1", "h2", "n1", "n2", "l1", "l2"}
	for i, w := range want {
		j, ok := p.next(context.Background())
		if !ok {
			t.Fatalf("next returned !ok at step %d", i)
		}
		if got := j.msg.Recipient; got != w {
			t.Fatalf("step %d: got %q, want %q", i, got, w)
		}
	}
}

// TestNextUnblocksOnCancel ensures senders return cleanly on shutdown when all
// channels are empty.
func TestNextUnblocksOnCancel(t *testing.T) {
	p := &Pool{
		highCh:   make(chan job),
		normalCh: make(chan job),
		lowCh:    make(chan job),
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, ok := p.next(ctx); ok {
		t.Fatal("expected next to report shutdown (ok=false) on cancelled context")
	}
}
