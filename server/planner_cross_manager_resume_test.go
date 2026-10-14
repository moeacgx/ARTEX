package server

import (
	"bytes"
	"context"
	"iter"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Autumn-27/artex/agent"
	"github.com/Autumn-27/norma/llm"
)

type countingPlannerProvider struct{ calls atomic.Int32 }

func (p *countingPlannerProvider) Stream(context.Context, llm.CompletionRequest) iter.Seq2[llm.StreamEvent, error] {
	return func(yield func(llm.StreamEvent, error) bool) {
		p.calls.Add(1)
		yield(llm.StreamEvent{Type: llm.SETextDelta, Text: "checked persisted graph"}, nil)
	}
}

func (p *countingPlannerProvider) Complete(ctx context.Context, req llm.CompletionRequest) (llm.Message, string, llm.Usage, error) {
	return accumulateStreamForTest(ctx, p.Stream, req)
}

func plannerControlHTTP(s *Server, taskID, action string) (int, string) {
	req := httptest.NewRequest(http.MethodPost, "/api/tasks/"+taskID+"/control", bytes.NewBufferString(`{"action":"`+action+`"}`))
	req.SetPathValue("id", taskID)
	rec := httptest.NewRecorder()
	s.control(rec, req)
	return rec.Code, rec.Body.String()
}

func TestPlannerPGCrossManagerResumeWaitsForTrueExit(t *testing.T) {
	mA, err := NewManager(t.TempDir(), "")
	if err != nil {
		t.Fatal(err)
	}
	defer mA.Close()
	defer restoreConcurrencySetting(t, mA)()
	if err = mA.SetConcurrency(false, defaultConcurrencyLimit); err != nil {
		t.Fatal(err)
	}
	taskA, err := mA.CreateTask("cross manager planner", "remain paused until true exit", nil, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer mA.DeleteTask(taskA.ID, DeleteTaskOptions{})
	if _, err = taskA.Store.AddGoal(map[string]any{"text": "cross-manager goal"}, "user"); err != nil {
		t.Fatal(err)
	}
	providerA := &delayedPlannerProvider{entered: make(chan struct{}), canceled: make(chan struct{}), release: make(chan struct{}), requests: make(chan llm.CompletionRequest, 1)}
	var releaseOnce sync.Once
	releaseA := func() { releaseOnce.Do(func() { close(providerA.release) }) }
	defer releaseA()
	sA := newAdmissionTestServer(mA, nil)
	plannerA := agent.NewPlanner(providerA, "synthetic", mA.dir, nil, 100000, 1)
	sA.engine.SetAuthoritativeAgentResolver(func(*Task) (*agent.Planner, *agent.Worker) { return plannerA, new(agent.Worker) })
	sA.engine.debounce = time.Millisecond
	sA.engine.started.Store(taskA.ID, true)
	ctxA, cancelA := context.WithCancel(context.Background())
	loopA := make(chan struct{})
	go func() { defer close(loopA); sA.engine.plannerLoop(ctxA, taskA) }()
	defer func() { cancelA(); releaseA(); <-loopA }()
	taskA.Notify()
	select {
	case <-providerA.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("manager A planner did not start")
	}
	if _, err = sA.applyTaskControl(taskA, "pause"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-providerA.canceled:
	case <-time.After(time.Second):
		t.Fatal("manager A planner was not canceled")
	}

	mB, err := NewManager(t.TempDir(), "")
	if err != nil {
		t.Fatal(err)
	}
	defer mB.Close()
	mB.LoadExisting()
	taskB, ok := mB.Task(taskA.ID)
	if !ok || !taskB.lifecycleSnapshot().Paused {
		t.Fatalf("manager B did not load paused task: ok=%v state=%+v", ok, taskB)
	}
	providerB := new(countingPlannerProvider)
	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelB()
	eB := NewEngine(mB)
	plannerB := agent.NewPlanner(providerB, "synthetic", mB.dir, nil, 100000, 1)
	eB.SetAuthoritativeAgentResolver(func(*Task) (*agent.Planner, *agent.Worker) { return plannerB, new(agent.Worker) })
	eB.Pause(taskB.ID, agent.AbortPausedByUser)
	sB := &Server{m: mB, engine: eB, ctx: ctxB, chatBusy: map[string]bool{}, chatCancel: map[string]context.CancelCauseFunc{}}

	code, body := plannerControlHTTP(sB, taskB.ID, "resume")
	if code != http.StatusConflict || !strings.Contains(body, ErrPlannerStillExiting.Error()) {
		t.Fatalf("manager B resume bypassed old planner: code=%d body=%s", code, body)
	}
	persisted, err := mB.pg.GetTask(mustPlannerID(t, taskB.ID))
	if err != nil || persisted == nil || !persisted.Paused {
		t.Fatalf("blocked resume changed DB paused state: task=%+v err=%v", persisted, err)
	}
	if providerB.calls.Load() != 0 {
		t.Fatalf("blocked resume called manager B model %d times", providerB.calls.Load())
	}

	releaseA()
	awaitPlannerState(t, "manager A planner did not truly exit", func() bool { return sA.engine.plannerResumeError(taskA.ID) == nil })
	deadline := time.Now().Add(5 * time.Second)
	for {
		code, body = plannerControlHTTP(sB, taskB.ID, "resume")
		if code == http.StatusOK {
			break
		}
		if code != http.StatusConflict || time.Now().After(deadline) {
			t.Fatalf("manager B resume after true exit: code=%d body=%s", code, body)
		}
		time.Sleep(20 * time.Millisecond)
	}
	persisted, err = mB.pg.GetTask(mustPlannerID(t, taskB.ID))
	if err != nil || persisted == nil || persisted.Paused {
		t.Fatalf("successful resume did not clear DB paused: task=%+v err=%v", persisted, err)
	}
	awaitPlannerState(t, "manager B planner did not call model after resume", func() bool { return providerB.calls.Load() > 0 })
}
