package server

import (
	"context"
	"errors"
	"iter"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Autumn-27/artex/agent"
	"github.com/Autumn-27/artex/db"
	"github.com/Autumn-27/norma/llm"
	"github.com/Autumn-27/norma/tool"
)

type delayedPlannerProvider struct {
	calls    atomic.Int32
	entered  chan struct{}
	canceled chan struct{}
	release  chan struct{}
	requests chan llm.CompletionRequest
}

func (p *delayedPlannerProvider) Stream(ctx context.Context, req llm.CompletionRequest) iter.Seq2[llm.StreamEvent, error] {
	return func(yield func(llm.StreamEvent, error) bool) {
		if p.calls.Add(1) == 1 {
			close(p.entered)
			<-ctx.Done()
			close(p.canceled)
			<-p.release
			yield(llm.StreamEvent{}, context.Cause(ctx))
			return
		}
		p.requests <- req
		yield(llm.StreamEvent{Type: llm.SETextDelta, Text: "重新读取当前图"}, nil)
	}
}

func (p *delayedPlannerProvider) Complete(ctx context.Context, req llm.CompletionRequest) (llm.Message, string, llm.Usage, error) {
	return accumulateStreamForTest(ctx, p.Stream, req)
}

func awaitPlannerState(t *testing.T, label string, check func() bool) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for !check() {
		select {
		case <-deadline.C:
			t.Fatal(label)
		case <-ticker.C:
		}
	}
}

func plannerPersistentCount(t *testing.T, m *Manager, id string) int {
	t.Helper()
	var count int
	if err := m.pg.QueryRow(`SELECT COALESCE(sum(active_count),0) FROM task_operation_activity WHERE task_id=$1`, id).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func TestPlannerPGLateExitBlocksResumeAndReplansPersistedGraph(t *testing.T) {
	m, err := NewManager(t.TempDir(), "")
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	defer restoreConcurrencySetting(t, m)()
	if err = m.SetConcurrency(false, defaultConcurrencyLimit); err != nil {
		t.Fatal(err)
	}
	task, err := m.CreateTask("planner timeout isolation", "initial goal", nil, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer m.DeleteTask(task.ID, DeleteTaskOptions{})
	if _, err = task.Store.AddGoal(map[string]any{"text": "初始持久目标"}, "user"); err != nil {
		t.Fatal(err)
	}
	p := &delayedPlannerProvider{entered: make(chan struct{}), canceled: make(chan struct{}), release: make(chan struct{}), requests: make(chan llm.CompletionRequest, 4)}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(p.release) }) }
	defer release()
	s := newAdmissionTestServer(m, nil)
	planner := agent.NewPlanner(p, "synthetic", m.dir, nil, 100000, 1)
	s.engine.SetAuthoritativeAgentResolver(func(*Task) (*agent.Planner, *agent.Worker) { return planner, new(agent.Worker) })
	s.engine.debounce = time.Millisecond
	s.engine.started.Store(task.ID, true)
	ctx, cancel := context.WithCancel(context.Background())
	loopExited := make(chan struct{})
	go func() { defer close(loopExited); s.engine.plannerLoop(ctx, task) }()
	defer func() {
		cancel()
		release()
		select {
		case <-loopExited:
		case <-time.After(5 * time.Second):
			t.Error("planner loop did not exit")
		}
	}()
	task.Notify()
	select {
	case <-p.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("planner did not start")
	}
	if _, err = s.applyTaskControl(task, "pause"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-p.canceled:
	case <-time.After(time.Second):
		t.Fatal("model was not canceled")
	}
	if _, err = s.applyTaskControl(task, "resume"); !errors.Is(err, ErrPlannerStillExiting) {
		t.Fatalf("resume=%v", err)
	}
	if count := plannerPersistentCount(t, m, task.ID); count != 1 {
		t.Fatalf("live planner count=%d", count)
	}
	persisted, err := m.pg.GetTask(mustPlannerID(t, task.ID))
	if err != nil || !persisted.Paused {
		t.Fatalf("busy resume changed paused state: task=%+v err=%v", persisted, err)
	}
	// 第二个连接池只验证持久占用屏障，不用本地 Planner 标志替代跨 Manager 计数。
	other, err := db.Open(os.Getenv("ARTEX_PG_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	var revision int64
	if err = m.pg.QueryRow(`SELECT proxy_revision FROM tasks WHERE id=$1`, task.ID).Scan(&revision); err != nil {
		t.Fatal(err)
	}
	if _, _, err = other.BeginTaskProxySwitch(mustPlannerID(t, task.ID), revision, db.TaskProxyModeDirect, nil); !errors.Is(err, db.ErrProxyInUse) {
		t.Fatalf("cross-manager switch=%v", err)
	}
	if _, err = task.Store.AddGoal(map[string]any{"text": "退出后必须读取的新持久目标"}, "user"); err != nil {
		t.Fatal(err)
	}
	release()
	awaitPlannerState(t, "planner still busy after actual exit", func() bool { return s.engine.plannerResumeError(task.ID) == nil })
	if count := plannerPersistentCount(t, m, task.ID); count != 0 {
		t.Fatalf("exited planner count=%d", count)
	}
	if _, err = s.applyTaskControl(task, "resume"); err != nil {
		t.Fatal(err)
	}
	select {
	case req := <-p.requests:
		var text strings.Builder
		for _, msg := range req.Messages {
			text.WriteString(msg.Text())
		}
		if !strings.Contains(text.String(), "退出后必须读取的新持久目标") {
			t.Fatal("resumed planner did not read persisted graph")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("resumed planner did not run")
	}
}

func mustPlannerID(t *testing.T, id string) int64 {
	t.Helper()
	n, err := parseTaskID(id)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestPlannerPGCleanupFailureRetainsPersistentReference(t *testing.T) {
	m, err := NewManager(t.TempDir(), "")
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	task, err := m.CreateTask("planner cleanup isolation", "goal", nil, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer m.DeleteTask(task.ID, DeleteTaskOptions{})
	e := NewEngine(m)
	ctx, finish, admitted := e.beginTaskOperationContext(context.Background(), task.ID)
	if !admitted {
		t.Fatal("operation rejected")
	}
	plannerExited := e.beginPlannerRun(task.ID)
	release := make(chan struct{})
	var cleanupOnce sync.Once
	closeCleanup := func() { cleanupOnce.Do(func() { close(release) }) }
	defer closeCleanup()
	previous := agent.ToolAugment
	defer func() { agent.ToolAugment = previous }()
	agent.ToolAugment = func(context.Context, string) ([]tool.CoreTool, agent.DeferredInfo, func()) {
		return nil, agent.DeferredInfo{}, func() { <-release }
	}
	planner := agent.NewPlanner(&scriptedLLMProvider{events: []llm.StreamEvent{{Type: llm.SETextDelta, Text: "done"}}}, "synthetic", m.dir, nil, 100000, 1)
	_, _, err = planner.Plan(ctx, mustPlannerID(t, task.ID), m.assets, task.Store, task.Goal, nil, nil)
	if !errors.Is(err, agent.ErrPlannerCleanup) {
		t.Fatalf("cleanup error lost: %v", err)
	}
	plannerErr := err
	finishPlannerRun(task.ID, plannerErr, finish, plannerExited)
	if plannerPersistentCount(t, m, task.ID) != 1 {
		t.Fatal("failed cleanup released persistent reference")
	}
	if e.beginPlannerRun(task.ID) != nil {
		t.Fatal("new planner bypassed retained barrier")
	}
	if _, err := sResumeForCleanupTest(e, task); !errors.Is(err, ErrPlannerStillExiting) {
		t.Fatalf("resume before cleanup exit=%v", err)
	}
	closeCleanup()
	awaitPlannerState(t, "late cleanup did not release persistent reference", func() bool { return plannerPersistentCount(t, m, task.ID) == 0 })
	if _, err := sResumeForCleanupTest(e, task); err != nil {
		t.Fatalf("resume after cleanup exit=%v", err)
	}
}

func sResumeForCleanupTest(e *Engine, task *Task) (bool, error) {
	if err := e.plannerResumeError(task.ID); err != nil {
		return false, err
	}
	return false, nil
}

func TestPlannerTimeoutRouterNeverReplays(t *testing.T) {
	for _, nonstream := range []bool{false, true} {
		t.Run(map[bool]string{false: "stream", true: "complete"}[nonstream], func(t *testing.T) {
			provider := &scriptedLLMProvider{err: errors.Join(agent.ErrLLMResponseBodyIdleTimeout, context.DeadlineExceeded)}
			hooks := taskLLMStreamHooks{current: func() (taskLLMSelection, error) {
				return taskLLMSelection{profileID: 1, provider: provider, retry: agent.RetryConfig{StreamAttempts: 3}}, nil
			}}
			var err error
			if nonstream {
				_, _, _, err = completeTaskLLM(context.Background(), "1", llm.CompletionRequest{}, hooks)
			} else {
				_, err = collectTaskLLMStream(streamTaskLLM(context.Background(), "1", llm.CompletionRequest{}, hooks))
			}
			if !errors.Is(err, context.DeadlineExceeded) || provider.calls != 1 {
				t.Fatalf("err=%v calls=%d", err, provider.calls)
			}
		})
	}
}
