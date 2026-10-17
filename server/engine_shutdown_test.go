package server

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Autumn-27/artex/agent"
)

type serverShutdownWaiter interface {
	Shutdown(context.Context) error
}

func shutdownForTest(t *testing.T, s *Server, ctx context.Context) error {
	t.Helper()
	waiter, ok := any(s).(serverShutdownWaiter)
	if !ok {
		t.Fatal("服务关停没有等待任务真实退出的入口")
	}
	return waiter.Shutdown(ctx)
}

func assertShutdownWaiting(t *testing.T, result <-chan error, stage string) {
	t.Helper()
	select {
	case err := <-result:
		t.Fatalf("%s 尚未退出，关停已返回: %v", stage, err)
	case <-time.After(40 * time.Millisecond):
	}
}

func TestServerShutdownWaitsForTaskRoutineTrueExit(t *testing.T) {
	e := NewEngine(nil)
	task := &Task{ID: "34", Status: "running"}
	s := &Server{engine: e, m: &Manager{tasks: map[string]*Task{task.ID: task}}}
	rt := e.registerTaskRoutines(context.Background(), task.ID, 1)
	cancelled, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	runTaskRoutine(rt, func(ctx context.Context) {
		<-ctx.Done()
		close(cancelled)
		<-release
	})
	result := make(chan error, 1)
	go func() { result <- shutdownForTest(t, s, t.Context()) }()
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("关停未取消任务循环")
	}
	assertShutdownWaiting(t, result, "任务循环")
	once.Do(func() { close(release) })
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("任务循环真实退出后关停仍未结束")
	}
	state := task.lifecycleSnapshot()
	if state.Status != "running" || state.Paused || state.Queued {
		t.Fatalf("关停改写了持久生命周期: %+v", state)
	}
}

type shutdownPlannerExit struct{ done <-chan struct{} }

func (shutdownPlannerExit) Error() string { return "等待测试规划清理退出" }
func (e shutdownPlannerExit) WaitPlannerExit(ctx context.Context) error {
	select {
	case <-e.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestServerShutdownWaitsForPendingPlannerCleanup(t *testing.T) {
	e := NewEngine(nil)
	s := &Server{engine: e}
	opCtx, finish, admitted := e.beginTaskOperationContext(e.execContextFor(context.Background(), "34"), "34")
	if !admitted {
		t.Fatal("测试操作未准入")
	}
	plannerExited, err := e.beginPlannerRun("34")
	if err != nil {
		t.Fatal(err)
	}
	cleanupExit := make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(cleanupExit) })
	finishPlannerRun("34", shutdownPlannerExit{done: cleanupExit}, finish, plannerExited)
	result := make(chan error, 1)
	go func() { result <- shutdownForTest(t, s, t.Context()) }()
	select {
	case <-opCtx.Done():
		if code, _, _, ok := agent.AbortReason(opCtx); !ok || code != "shutdown" {
			t.Fatalf("关停取消原因丢失: %q %v", code, ok)
		}
	case <-time.After(time.Second):
		t.Fatal("关停未取消规划操作")
	}
	assertShutdownWaiting(t, result, "Planner 异步清理")
	once.Do(func() { close(cleanupExit) })
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Planner 真实清理后关停仍未结束")
	}
	if e.inflightCount("34") != 0 || e.plannerResumeError("34") != nil {
		t.Fatal("关停成功时仍有 Planner 或操作占用")
	}
}

func TestServerShutdownTimeoutRetainsOperationAndRejectsNewAdmission(t *testing.T) {
	e := NewEngine(nil)
	s := &Server{engine: e}
	_, finish, admitted := e.beginTaskOperationContext(context.Background(), "34")
	if !admitted {
		t.Fatal("测试操作未准入")
	}
	defer finish()
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if err := shutdownForTest(t, s, ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("未退出操作应使关停明确超时: %v", err)
	}
	if e.inflightCount("34") != 1 {
		t.Fatal("关停超时提前释放了操作占用")
	}
	_, lateFinish, lateAdmitted := e.beginTaskOperationContext(context.Background(), "99")
	lateFinish()
	if lateAdmitted {
		t.Fatal("关停屏障允许新任务操作进入")
	}
	finish()
	if err := shutdownForTest(t, s, t.Context()); err != nil {
		t.Fatalf("重用关停等待未能观察到真实退出: %v", err)
	}
}

func TestServerShutdownWaitsForAsynchronousProxyRelease(t *testing.T) {
	e := NewEngine(nil)
	s := &Server{engine: e}
	barrier := e.proxyBarrier("34")
	barrier.mu.Lock()
	barrier.pending, barrier.releasing = true, true
	barrier.mu.Unlock()
	defer func() {
		barrier.mu.Lock()
		barrier.pending, barrier.releasing = false, false
		barrier.signal()
		barrier.mu.Unlock()
	}()
	result := make(chan error, 1)
	go func() { result <- shutdownForTest(t, s, t.Context()) }()
	assertShutdownWaiting(t, result, "异步出口回收")
	barrier.mu.Lock()
	barrier.pending, barrier.releasing = false, false
	barrier.signal()
	barrier.mu.Unlock()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("出口回收结束后关停仍未返回")
	}
}

func TestServerShutdownWaitsForProxyReleaseRegistration(t *testing.T) {
	e := NewEngine(nil)
	s := &Server{engine: e}
	e.proxyReleaseWG.Add(1)
	var registered sync.Once
	defer registered.Do(e.proxyReleaseWG.Done)
	result := make(chan error, 1)
	go func() { result <- shutdownForTest(t, s, t.Context()) }()
	assertShutdownWaiting(t, result, "退出回收请求登记")
	barrier := e.proxyBarrier("34")
	barrier.mu.Lock()
	barrier.pending = true
	barrier.mu.Unlock()
	defer func() {
		barrier.mu.Lock()
		barrier.pending = false
		barrier.mu.Unlock()
	}()
	registered.Do(e.proxyReleaseWG.Done)
	assertShutdownWaiting(t, result, "已登记的出口回收")
	barrier.mu.Lock()
	barrier.pending = false
	barrier.mu.Unlock()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("已登记的出口回收结束后关停仍未返回")
	}
}

func TestServerShutdownPreservesProxyReleaseFailure(t *testing.T) {
	e := NewEngine(nil)
	s := &Server{engine: e}
	failure := errors.New("远端出口没有确认释放")
	barrier := e.proxyBarrier("34")
	barrier.mu.Lock()
	barrier.releaseErr = failure
	barrier.mu.Unlock()
	if err := shutdownForTest(t, s, t.Context()); !errors.Is(err, failure) {
		t.Fatalf("关停把出口回收失败当成成功: %v", err)
	}
}

func TestServerShutdownWaitsForIndependentConversationAndTrigger(t *testing.T) {
	e := NewEngine(nil)
	s := &Server{engine: e, ctx: context.Background(), chatBusy: map[string]bool{"conv-12": true},
		chatCancel: map[string]context.CancelCauseFunc{}, triggerActive: map[string]int{"reporter": 1}}
	convCtx, _ := s.conversationRunContext(12, "conv-12")
	result := make(chan error, 1)
	go func() { result <- shutdownForTest(t, s, t.Context()) }()
	select {
	case <-convCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("独立会话未收到关停取消")
	}
	assertShutdownWaiting(t, result, "独立会话")
	s.chatMu.Lock()
	delete(s.chatBusy, "conv-12")
	delete(s.chatCancel, "conv-12")
	s.chatMu.Unlock()
	assertShutdownWaiting(t, result, "已准入的触发执行")
	s.queueMu.Lock()
	s.triggerActive["reporter"] = 0
	s.queueMu.Unlock()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("会话和触发执行均退出后关停仍未结束")
	}
}
