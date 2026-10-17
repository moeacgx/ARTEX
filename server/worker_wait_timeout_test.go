package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

// 真实任务循环回归：预算必须能结束正在等待的模型，并通过完成通知唤醒
// Planner；同一个槽位随后领取下一条意图，不能整条重放超时意图。
func TestWorkerPGHardTimeoutSettlesWakesPlannerAndContinues(t *testing.T) {
	t.Setenv("ARTEX_WORKER_SETTLEMENT_TIMEOUT", "80ms")
	s, task, first, second := workerWaitFixture(t)
	started := make(chan struct{})
	var firstCalls, modelActive atomic.Int32
	worker := agent.NewWorker(retestProvider{complete: func(ctx context.Context, _ llm.CompletionRequest) (llm.Message, string, llm.Usage, error) {
		modelActive.Add(1)
		defer modelActive.Add(-1)
		if agent.RunInfoFrom(ctx).IntentID == first {
			if firstCalls.Add(1) == 1 {
				close(started)
			}
			<-ctx.Done()
			return llm.Message{}, "", llm.Usage{}, context.Cause(ctx)
		}
		return followupText("第二条意图已实际执行")
	}}, "wait-fixture", s.m.dir, nil, 100000, 0)
	worker.SetNonStreaming(func() bool { return true })
	worker.SetRunTimeout(120 * time.Millisecond)
	planned := workerWaitPlanner(t, s, task, first, worker)
	s.engine.Run(t.Context(), task)
	workerWaitSignal(t, started, "第一条意图未实际调用模型")
	if s.engine.ActiveLLMCalls(task.ID) == 0 || s.engine.inflightCount(task.ID) == 0 {
		t.Fatal("模型等待期间没有持有真实运行引用")
	}
	workerWaitState(t, task, first, "exhausted")
	workerWaitState(t, task, second, "done")
	workerWaitSignal(t, planned, "超时意图结束后 NotifyDone 未唤醒真实 Planner 请求")
	if firstCalls.Load() != 1 {
		t.Fatalf("超时意图被整条重放，模型调用次数=%d", firstCalls.Load())
	}
	if modelActive.Load() != 0 {
		t.Fatal("模型返回后仍保留活动引用")
	}
	workerWaitTimeoutResult(t, s, first)
	workerWaitSameOwner(t, task, first, second)
	workerWaitShutdown(t, s, task)
}

// 取消信号不是退出证据：工具迟迟未返回时，旧意图和占用必须保留，
// 同一个 Worker 不得领取第二条；只有工具真实退出后才能收尾并继续。
func TestWorkerPGHardTimeoutWaitsForRealToolExit(t *testing.T) {
	t.Setenv("ARTEX_WORKER_SETTLEMENT_TIMEOUT", "80ms")
	s, task, first, second := workerWaitFixture(t)
	started, cancelled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	var exited, premature atomic.Bool
	probe := tool.Build(tool.Spec{Name: "DelayedExitProbe", Schema: map[string]any{"type": "object"}, Run: func(ctx context.Context, _ json.RawMessage, _ *tool.ToolContext) (tool.Result, error) {
		close(started)
		<-ctx.Done()
		close(cancelled)
		<-release
		exited.Store(true)
		return tool.Text("工具已真实退出"), nil
	}})
	var firstCalls atomic.Int32
	worker := agent.NewWorker(retestProvider{complete: func(ctx context.Context, _ llm.CompletionRequest) (llm.Message, string, llm.Usage, error) {
		if agent.RunInfoFrom(ctx).IntentID == first {
			if firstCalls.Add(1) == 1 {
				return followupTool("DelayedExitProbe", `{}`)
			}
			return followupText("已在预算结束后收尾")
		}
		if !exited.Load() {
			premature.Store(true)
		}
		return followupText("第二条意图已实际执行")
	}}, "wait-fixture", s.m.dir, nil, 100000, 0, probe)
	worker.SetNonStreaming(func() bool { return true })
	worker.SetRunTimeout(120 * time.Millisecond)
	planned := workerWaitPlanner(t, s, task, first, worker)
	s.engine.Run(t.Context(), task)
	workerWaitSignal(t, started, "工具未实际启动")
	workerWaitSignal(t, cancelled, "运行预算未取消工具")
	// 跨过探索预算和收尾硬限，仍不放行模拟工具的真实退出。
	time.Sleep(300 * time.Millisecond)
	for id, want := range map[int64]string{first: "running", second: "open"} {
		node, err := task.Store.GetNode(id)
		if err != nil || node == nil || node.State != want {
			t.Fatalf("工具未退出时意图 #%d 提前变更: node=%+v err=%v", id, node, err)
		}
	}
	if s.engine.ActiveLLMCalls(task.ID) == 0 || s.engine.inflightCount(task.ID) == 0 {
		t.Fatal("工具未真实退出就释放了模型或执行引用")
	}
	var refs int
	if err := s.m.pg.QueryRow(`SELECT active_count FROM task_operation_activity WHERE task_id=$1`, task.ID).Scan(&refs); err != nil || refs == 0 {
		t.Fatalf("工具未退出就释放持久操作引用: refs=%d err=%v", refs, err)
	}
	if err := s.m.ReleaseTaskProxyWhenIdle(t.Context(), task); !errors.Is(err, db.ErrProxyInUse) {
		t.Fatalf("工具未退出时出口回收没有受阻: %v", err)
	}
	var results int
	if err := s.m.pg.QueryRow(`SELECT count(*) FROM activity WHERE node_id=$1 AND kind='result'`, first).Scan(&results); err != nil || results != 0 {
		t.Fatalf("工具真实退出前提前报告完成: results=%d err=%v", results, err)
	}
	releaseOnce.Do(func() { close(release) })
	workerWaitState(t, task, first, "exhausted")
	workerWaitState(t, task, second, "done")
	workerWaitSignal(t, planned, "工具真实退出后的完成通知未实际唤醒 Planner")
	if premature.Load() {
		t.Fatal("工具真实退出前领取了第二条意图")
	}
	workerWaitTimeoutResult(t, s, first)
	workerWaitSameOwner(t, task, first, second)
	workerWaitShutdown(t, s, task)
}

func workerWaitFixture(t *testing.T) (*Server, *Task, int64, int64) {
	t.Helper()
	s := newTriggerRegressionServer(t)
	oldWorkers, existed, err := s.m.pg.GetSetting(settingWorkers)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if existed {
			err = s.m.pg.SetSetting(settingWorkers, oldWorkers)
		} else {
			_, err = s.m.pg.Exec(`DELETE FROM settings WHERE key=$1`, settingWorkers)
		}
		if err != nil {
			t.Errorf("恢复 Worker 槽位配置: %v", err)
		}
	})
	if err := s.m.SetWorkers(1); err != nil {
		t.Fatal(err)
	}
	proxy, err := s.m.pg.CreateProxy(&db.Proxy{Name: "Worker 超时隔离出口", Kind: db.ProxyKindHTTP, URL: "http://127.0.0.1:1", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := s.m.pg.Exec(`DELETE FROM outbound_proxies WHERE id=$1`, proxy.ID); err != nil {
			t.Errorf("删除隔离出口: %v", err)
		}
	})
	task, err := s.m.CreateTaskWithOptions("Worker 等待硬限回归", "验收所有真实执行结果", db.TaskCreateOptions{ProxyMode: db.TaskProxyModeFixed, ProxyID: &proxy.ID})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		workerWaitShutdown(t, s, task)
		if err := s.m.CloseProxyRuntime(); err != nil {
			t.Errorf("关闭测试出口运行时: %v", err)
		}
		if err := s.m.pg.DeleteTask(switchTaskID(t, task)); err != nil {
			t.Errorf("删除超时测试任务: %v", err)
		}
	})
	if _, err := task.Store.AddGoal(map[string]any{"text": "等待两条意图提供真实结论"}, "human"); err != nil {
		t.Fatal(err)
	}
	if err := s.m.SetTaskStatus(task.ID, "running"); err != nil {
		t.Fatal(err)
	}
	first, err := task.Store.AddIntent(map[string]any{"summary": "第一条持续等待"}, 2, nil, "planner")
	if err != nil {
		t.Fatal(err)
	}
	second, err := task.Store.AddIntent(map[string]any{"summary": "第二条可正常执行"}, 1, nil, "planner")
	if err != nil {
		t.Fatal(err)
	}
	s.engine.debounce = time.Millisecond
	return s, task, first, second
}

func workerWaitPlanner(t *testing.T, s *Server, task *Task, first int64, worker *agent.Worker) <-chan struct{} {
	t.Helper()
	planned := make(chan struct{})
	var once sync.Once
	planner := agent.NewPlanner(retestProvider{complete: func(_ context.Context, req llm.CompletionRequest) (llm.Message, string, llm.Usage, error) {
		// 心跳至少十分钟，测试没有额外 Notify。请求必须明确携带该意图的完成事件。
		for _, message := range req.Messages {
			if strings.Contains(message.Text(), fmt.Sprintf("意图 #%d（第一条持续等待）的 worker 结束", first)) {
				once.Do(func() { close(planned) })
			}
		}
		return followupText("已基于完成事件重新规划")
	}}, "wait-planner-fixture", s.m.dir, nil, 100000, 0)
	planner.SetNonStreaming(func() bool { return true })
	s.engine.SetAuthoritativeAgentResolver(func(*Task) (*agent.Planner, *agent.Worker) { return planner, worker })
	return planned
}

func workerWaitSignal(t *testing.T, signal <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(3 * time.Second):
		t.Fatal(message)
	}
}

func workerWaitState(t *testing.T, task *Task, id int64, want string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		node, err := task.Store.GetNode(id)
		if err != nil {
			t.Fatal(err)
		}
		if node != nil && node.State == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	node, _ := task.Store.GetNode(id)
	t.Fatalf("意图 #%d 未进入 %s: %+v", id, want, node)
}

func workerWaitTimeoutResult(t *testing.T, s *Server, id int64) {
	t.Helper()
	var count int
	var result string
	if err := s.m.pg.QueryRow(`SELECT count(*),COALESCE(string_agg(summary || ' ' || detail,' '),'') FROM activity WHERE node_id=$1 AND kind='result'`, id).Scan(&count, &result); err != nil {
		t.Fatal(err)
	}
	if count != 1 || (!strings.Contains(strings.ToLower(result), "timeout") && !strings.Contains(result, "超时")) {
		t.Fatalf("超时没有唯一明确终态结果: count=%d result=%q", count, result)
	}
}

func workerWaitSameOwner(t *testing.T, task *Task, first, second int64) {
	t.Helper()
	for _, id := range []int64{first, second} {
		node, err := task.Store.GetNode(id)
		if err != nil || node == nil || node.Owner != "work#1" {
			t.Fatalf("意图 #%d 未由同一槽位领取: node=%+v err=%v", id, node, err)
		}
	}
}

func workerWaitShutdown(t *testing.T, s *Server, task *Task) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	if err := s.Shutdown(ctx); err != nil {
		t.Errorf("等待 Worker 真实退出: %v", err)
		return
	}
	if s.engine.ActiveLLMCalls(task.ID) != 0 || s.engine.inflightCount(task.ID) != 0 {
		t.Error("正式关停后模型或执行引用未释放")
	}
	var refs int
	if err := s.m.pg.QueryRow(`SELECT COALESCE((SELECT active_count FROM task_operation_activity WHERE task_id=$1),0) + COALESCE((SELECT sum(active_count) FROM task_proxy_activity WHERE task_id=$1),0)`, task.ID).Scan(&refs); err != nil || refs != 0 {
		t.Errorf("正式关停后持久执行或出口引用未释放: refs=%d err=%v", refs, err)
	}
}
