package server

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Autumn-27/artex/agent"
	"github.com/Autumn-27/artex/db"
)

func TestPlannerResumeBusyUntilOldPlannerActuallyExits(t *testing.T) {
	engine := NewEngine(nil)
	opCtx, finishOperation, admitted := engine.beginTaskOperationContext(context.Background(), "42")
	if !admitted || opCtx == nil {
		t.Fatal("planner operation was not admitted")
	}
	exitPlanner, err := engine.beginPlannerRun("42")
	if err != nil {
		t.Fatal(err)
	}
	if !errors.Is(engine.plannerResumeError("42"), ErrPlannerStillExiting) {
		t.Fatalf("resume was not busy while old planner was live")
	}
	if got := engine.inflightCount("42"); got != 1 {
		t.Fatalf("operation reference=%d, want 1 before true exit", got)
	}

	exitPlanner()
	if err := engine.plannerResumeError("42"); err != nil {
		t.Fatalf("resume stayed busy after planner exit: %v", err)
	}
	if got := engine.inflightCount("42"); got != 1 {
		t.Fatalf("planner tracker released persistent operation reference early: %d", got)
	}
	finishOperation()
	if got := engine.inflightCount("42"); got != 0 {
		t.Fatalf("operation reference=%d after full cleanup, want 0", got)
	}
}

type joinedPlannerPending struct {
	done chan error
}

func (p *joinedPlannerPending) Error() string { return agent.ErrPlannerCleanup.Error() }
func (p *joinedPlannerPending) Unwrap() error { return agent.ErrPlannerCleanup }
func (p *joinedPlannerPending) WaitPlannerExit(ctx context.Context) error {
	select {
	case err := <-p.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestFinishPlannerRunWaitsEveryJoinedExit(t *testing.T) {
	for _, first := range []int{0, 1} {
		t.Run(map[int]string{0: "first_finishes_first", 1: "second_finishes_first"}[first], func(t *testing.T) {
			refs := atomic.Int32{}
			refs.Store(1)
			var exited atomic.Bool
			pending := []*joinedPlannerPending{{done: make(chan error, 1)}, {done: make(chan error, 1)}}
			finishPlannerRun("42", errors.Join(pending[0], pending[1]),
				func() { refs.Store(0) }, func() { exited.Store(true) })
			pending[first].done <- nil
			time.Sleep(30 * time.Millisecond)
			if refs.Load() != 1 || exited.Load() {
				t.Fatalf("released with another cleanup live: refs=%d exited=%v", refs.Load(), exited.Load())
			}
			pending[1-first].done <- nil
			awaitPlannerState(t, "joined planner exits did not release", func() bool { return refs.Load() == 0 && exited.Load() })
		})
	}
}

func TestFinishPlannerRunJoinedFailureRetainsProtection(t *testing.T) {
	refs := atomic.Int32{}
	refs.Store(1)
	var exited atomic.Bool
	pending := []*joinedPlannerPending{
		{done: make(chan error, 1)},
		{done: make(chan error, 1)},
	}
	finishPlannerRun("42", errors.Join(pending[0], pending[1]), func() { refs.Store(0) }, func() { exited.Store(true) })
	pending[0].done <- errors.Join(agent.ErrPlannerCleanup, errors.New("permanent cleanup failure"))
	time.Sleep(30 * time.Millisecond)
	if refs.Load() != 1 || exited.Load() {
		t.Fatalf("failure bypassed live sibling: refs=%d exited=%v", refs.Load(), exited.Load())
	}
	pending[1].done <- nil
	time.Sleep(50 * time.Millisecond)
	if refs.Load() != 1 || exited.Load() {
		t.Fatalf("permanent failure released protection: refs=%d exited=%v", refs.Load(), exited.Load())
	}
}

func TestPlannerPausedWakeDoesNotBlockResume(t *testing.T) {
	e := NewEngine(nil)
	e.Pause("42", agent.AbortPausedByUser)
	if exit, err := e.beginPlannerRun("42"); exit != nil || !errors.Is(err, db.ErrPlannerRunPaused) {
		t.Fatalf("paused wake acquired planner guard: exit=%v err=%v", exit != nil, err)
	}
	e.Resume(&Task{ID: "42", notify: make(chan struct{}, 1)})
	if e.IsPaused("42") {
		t.Fatal("empty paused wake blocked resume")
	}
}
