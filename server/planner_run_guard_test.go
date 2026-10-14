package server

import (
	"context"
	"errors"
	"testing"

	"github.com/Autumn-27/artex/agent"
)

func TestPlannerResumeBusyUntilOldPlannerActuallyExits(t *testing.T) {
	engine := NewEngine(nil)
	opCtx, finishOperation, admitted := engine.beginTaskOperationContext(context.Background(), "42")
	if !admitted || opCtx == nil {
		t.Fatal("planner operation was not admitted")
	}
	exitPlanner := engine.beginPlannerRun("42")
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

func TestPlannerPausedWakeDoesNotBlockResume(t *testing.T) {
	e := NewEngine(nil)
	e.Pause("42", agent.AbortPausedByUser)
	if e.beginPlannerRun("42") != nil {
		t.Fatal("paused wake acquired planner guard")
	}
	e.Resume(&Task{ID: "42", notify: make(chan struct{}, 1)})
	if e.IsPaused("42") {
		t.Fatal("empty paused wake blocked resume")
	}
}
