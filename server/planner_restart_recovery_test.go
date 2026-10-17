package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Autumn-27/artex/agent"
	"github.com/Autumn-27/artex/db"
	"github.com/Autumn-27/norma/llm"
)

// 使用新的数据库连接与 Manager 走实际启动恢复路径，不复用旧 Engine 的内存标记。
func restartedPlannerServer(t *testing.T, previous *Server) *Server {
	t.Helper()
	dsn, _, err := db.DSN()
	if err != nil {
		t.Fatal(err)
	}
	pg, err := db.Open(dsn)
	if err != nil {
		t.Fatal(err)
	}
	m := &Manager{pg: pg, dir: previous.m.dir, assets: pg.Assets(), tasks: map[string]*Task{}}
	if err := m.InitProxyRuntime(t.Context()); err != nil {
		_ = pg.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	s := &Server{m: m, ctx: ctx, llmHealth: newLLMHealthRegistry(pg), taskAgents: map[string]*taskAgentBundle{}}
	s.engine = NewEngine(m)
	s.engine.debounce = time.Millisecond
	s.engine.taskReadiness = func(task *Task) bool { return s.taskRuntimeAvailable(task, "planner", "worker") }
	s.engine.resolveContext = func(ctx context.Context, task *Task) (*agent.Planner, *agent.Worker) {
		if !s.taskRuntimeAvailable(task, "planner", "worker") {
			return nil, nil
		}
		bundle, err := s.agentsForTask(task, ctx)
		if err != nil {
			return nil, nil
		}
		return bundle.pl, bundle.wk
	}
	t.Cleanup(func() {
		cancel()
		shutdownPlannerRestartServer(t, s)
		if err := m.Close(); err != nil {
			t.Errorf("关闭重启测试 Manager: %v", err)
		}
	})
	return s
}

func shutdownPlannerRestartServer(t *testing.T, s *Server) {
	t.Helper()
	// 测试上下文会先于 Cleanup 取消；独立超时仍须等待真实退出和异步出口回收。
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.Shutdown(ctx); err != nil {
		t.Errorf("关停重启测试服务: %v", err)
	}
}

func plannerRestartTask(t *testing.T, s *Server, opts db.TaskCreateOptions) *Task {
	t.Helper()
	task, err := s.m.CreateTaskWithOptions("重启自动规划回归", "核验保存的目标", opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		// 后创建的恢复实例已按 Cleanup 的逆序完整关停；创建者也须停止后台监听，
		// 最后才能删除任务及随后清理的模型、代理，禁止吞掉占用错误留下脏数据。
		shutdownPlannerRestartServer(t, s)
		if err := s.m.CloseProxyRuntime(); err != nil {
			t.Errorf("停止重启测试后台代理监听: %v", err)
		}
		id := mustPlannerID(t, task.ID)
		if err := s.m.pg.DeleteTask(id); err != nil {
			t.Errorf("删除重启测试任务 %s: %v", task.ID, err)
		}
	})
	if _, err := task.Store.AddGoal(map[string]any{"text": "重启后仍须验收的持久目标"}, "user"); err != nil {
		t.Fatal(err)
	}
	if err := s.m.SetTaskStatus(task.ID, "running"); err != nil {
		t.Fatal(err)
	}
	return task
}

func plannerRestartRequest(t *testing.T, requests <-chan llm.CompletionRequest) llm.CompletionRequest {
	t.Helper()
	select {
	case request := <-requests:
		return request
	case <-time.After(5 * time.Second):
		t.Fatal("没有新通知或手动继续时，恢复后的 Planner 未实际调用模型")
		return llm.CompletionRequest{}
	}
}

func plannerRestartAttemptFinished(s *Server, task *Task) bool {
	run, attempted := s.engine.plannerRuns.Load(task.ID)
	return attempted && run.(*atomic.Int64).Load() == 0 && len(task.notify) == 0
}

func restartCountingPlanner(t *testing.T, s *Server, requests chan<- llm.CompletionRequest) *agent.Planner {
	t.Helper()
	planner := agent.NewPlanner(retestProvider{complete: func(_ context.Context, req llm.CompletionRequest) (llm.Message, string, llm.Usage, error) {
		requests <- req
		return followupText("已读取恢复后的持久图")
	}}, "restart-fixture", s.m.dir, nil, 100000, 1)
	planner.SetNonStreaming(func() bool { return true })
	return planner
}

func TestPlannerPGRestartWakesWithActiveIntent(t *testing.T) {
	previous := newTriggerRegressionServer(t)
	task := plannerRestartTask(t, previous, db.TaskCreateOptions{ProxyMode: db.TaskProxyModeDirect})
	if _, err := task.Store.AddIntent(map[string]any{"summary": "重启时已有尚未结束的方向"}, 1, nil, "planner"); err != nil {
		t.Fatal(err)
	}
	s := restartedPlannerServer(t, previous)
	requests := make(chan llm.CompletionRequest, 4)
	planner := restartCountingPlanner(t, s, requests)
	worker := agent.NewWorker(retestProvider{complete: func(ctx context.Context, _ llm.CompletionRequest) (llm.Message, string, llm.Usage, error) {
		<-ctx.Done()
		return llm.Message{}, "", llm.Usage{}, context.Cause(ctx)
	}}, "restart-fixture", s.m.dir, nil, 100000, 1)
	worker.SetNonStreaming(func() bool { return true })
	s.engine.SetAuthoritativeAgentResolver(func(*Task) (*agent.Planner, *agent.Worker) { return planner, worker })
	s.restoreTaskRuntimes()
	req := plannerRestartRequest(t, requests)
	var text strings.Builder
	for _, msg := range req.Messages {
		text.WriteString(msg.Text())
	}
	if !strings.Contains(text.String(), "重启后仍须验收的持久目标") {
		t.Fatal("重启后的 Planner 没有读取持久目标")
	}
}

func TestPlannerPGUnavailableResolverRetriesWithoutNewNotification(t *testing.T) {
	previous := newTriggerRegressionServer(t)
	task := plannerRestartTask(t, previous, db.TaskCreateOptions{ProxyMode: db.TaskProxyModeDirect})
	s := restartedPlannerServer(t, previous)
	s.m.LoadExisting()
	restored, _ := s.m.Task(task.ID)
	restored.NotifyHint([]string{"暂不可用期间不可丢失的规划触发"})
	requests := make(chan llm.CompletionRequest, 4)
	planner := restartCountingPlanner(t, s, requests)
	var ready atomic.Bool
	attempted := make(chan struct{})
	var once sync.Once
	s.engine.SetAuthoritativeAgentResolver(func(*Task) (*agent.Planner, *agent.Worker) {
		once.Do(func() { close(attempted) })
		if !ready.Load() {
			return nil, nil
		}
		return planner, new(agent.Worker)
	})
	s.restoreTaskRuntimes()
	select {
	case <-attempted:
	case <-time.After(5 * time.Second):
		t.Fatal("恢复启动没有尝试解析任务模型")
	}
	// Worker 也使用同一解析器，须确认 Planner 自己已经结束这次不可用尝试。
	awaitPlannerState(t, "首次不可用规划尝试未结束", func() bool { return plannerRestartAttemptFinished(s, restored) })
	ready.Store(true)
	req := plannerRestartRequest(t, requests)
	var text strings.Builder
	for _, msg := range req.Messages {
		text.WriteString(msg.Text())
	}
	if !strings.Contains(text.String(), "暂不可用期间不可丢失的规划触发") {
		t.Fatal("暂不可用重试丢失了原规划触发")
	}
}

func TestPlannerPGProxyRecoversWithoutStateMutation(t *testing.T) {
	previous := newTriggerRegressionServer(t)
	var proxyRequests atomic.Int32
	requests := make(chan llm.CompletionRequest, 4)
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxyRequests.Add(1)
		var req llm.CompletionRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		requests <- req
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"choices":[{"message":{"role":"assistant","content":"代理恢复后已继续规划"},"finish_reason":"stop"}]}`)
	}))
	t.Cleanup(proxy.Close)
	// 模型地址故意不可直连；只有任务配置的本地代理能返回结果。
	profileID, _ := executionModels(t, previous, "http://planner-restart.invalid/v1")
	p, err := previous.m.pg.CreateProxy(&db.Proxy{Name: "重启恢复本地代理", Kind: db.ProxyKindHTTP, URL: proxy.URL, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = previous.m.pg.Exec(`DELETE FROM outbound_proxies WHERE id=$1`, p.ID) })
	task := plannerRestartTask(t, previous, db.TaskCreateOptions{ProxyMode: db.TaskProxyModeFixed, ProxyID: &p.ID, LLMMembers: []db.LLMModelSelection{{ProfileID: profileID}}})
	if _, err := previous.m.pg.Exec(`UPDATE outbound_proxies SET status='failed' WHERE id=$1`, p.ID); err != nil {
		t.Fatal(err)
	}
	s := restartedPlannerServer(t, previous)
	s.restoreTaskRuntimes()
	restored, ok := s.m.Task(task.ID)
	if !ok {
		t.Fatal("任务未从持久记录恢复")
	}
	// 等待首次唤醒的准入检查完成，不能靠恢复后才启动循环蒙混通过。
	awaitPlannerState(t, "启动规划未结束首轮准入检查", func() bool { return plannerRestartAttemptFinished(s, restored) })
	if proxyRequests.Load() != 0 {
		t.Fatal("不可用代理被绕过或仍被使用")
	}
	if _, err := previous.m.pg.Exec(`UPDATE outbound_proxies SET status='healthy' WHERE id=$1`, p.ID); err != nil {
		t.Fatal(err)
	}
	plannerRestartRequest(t, requests)
	if proxyRequests.Load() == 0 {
		t.Fatal("恢复后的模型调用未经过配置的代理")
	}
	persisted, err := s.m.pg.GetTask(mustPlannerID(t, task.ID))
	if err != nil || persisted == nil || persisted.Paused || persisted.Queued || persisted.Status != "running" || persisted.ProxyID == nil || *persisted.ProxyID != p.ID || persisted.ProxyRevision != 0 {
		t.Fatalf("恢复规划改变了任务控制状态或出口策略: task=%+v err=%v", persisted, err)
	}
}

func TestPlannerPGLeaseRecoveryWaitsForTrueExit(t *testing.T) {
	previous := newTriggerRegressionServer(t)
	task := plannerRestartTask(t, previous, db.TaskCreateOptions{ProxyMode: db.TaskProxyModeDirect})
	lease, err := previous.m.pg.AcquirePlannerRun(t.Context(), mustPlannerID(t, task.ID), false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lease.Close() })
	s := restartedPlannerServer(t, previous)
	requests := make(chan llm.CompletionRequest, 4)
	planner := restartCountingPlanner(t, s, requests)
	s.engine.SetAuthoritativeAgentResolver(func(*Task) (*agent.Planner, *agent.Worker) { return planner, new(agent.Worker) })
	s.restoreTaskRuntimes()
	select {
	case <-requests:
		t.Fatal("恢复规划越过仍存活的旧 Planner 会话锁")
	case <-time.After(2 * time.Second):
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	plannerRestartRequest(t, requests)
}

func TestPlannerPGRetryHonorsTaskControl(t *testing.T) {
	for _, state := range []string{"paused", "queued", "done", "goalless"} {
		t.Run(state, func(t *testing.T) {
			previous := newTriggerRegressionServer(t)
			task := plannerRestartTask(t, previous, db.TaskCreateOptions{ProxyMode: db.TaskProxyModeDirect})
			s := restartedPlannerServer(t, previous)
			requests := make(chan llm.CompletionRequest, 4)
			planner := restartCountingPlanner(t, s, requests)
			var ready atomic.Bool
			var attempts atomic.Int32
			s.engine.SetAuthoritativeAgentResolver(func(*Task) (*agent.Planner, *agent.Worker) {
				attempts.Add(1)
				if !ready.Load() {
					return nil, nil
				}
				return planner, new(agent.Worker)
			})
			s.restoreTaskRuntimes()
			restored, _ := s.m.Task(task.ID)
			awaitPlannerState(t, "未结束首次恢复尝试", func() bool { return attempts.Load() > 0 && plannerRestartAttemptFinished(s, restored) })
			switch state {
			case "paused":
				if err := s.m.SetTaskPaused(task.ID, true); err != nil {
					t.Fatal(err)
				}
				s.engine.Pause(task.ID, agent.AbortPausedByUser)
			case "queued":
				if err := s.m.EnqueueTask(task.ID, "resume"); err != nil {
					t.Fatal(err)
				}
			case "done":
				if err := s.m.SetTaskStatus(task.ID, "done"); err != nil {
					t.Fatal(err)
				}
			case "goalless":
				if _, err := s.m.pg.Exec(`UPDATE exploration_nodes SET state='met' WHERE exploration_id=$1 AND kind='goal'`, restored.ExpID); err != nil {
					t.Fatal(err)
				}
			}
			ready.Store(true)
			select {
			case <-requests:
				t.Fatalf("恢复重试越过 %s 状态", state)
			case <-time.After(2 * time.Second):
			}
		})
	}
}

func TestPlannerPGGracefulRestartAutomaticallyContinues(t *testing.T) {
	previous := newTriggerRegressionServer(t)
	task := plannerRestartTask(t, previous, db.TaskCreateOptions{ProxyMode: db.TaskProxyModeDirect})
	old := restartedPlannerServer(t, previous)
	provider := &delayedPlannerProvider{entered: make(chan struct{}), canceled: make(chan struct{}), release: make(chan struct{}), requests: make(chan llm.CompletionRequest, 4)}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(provider.release) }) }
	t.Cleanup(release)
	planner := agent.NewPlanner(provider, "restart-fixture", old.m.dir, nil, 100000, 1)
	old.engine.SetAuthoritativeAgentResolver(func(*Task) (*agent.Planner, *agent.Worker) { return planner, new(agent.Worker) })
	old.restoreTaskRuntimes()
	select {
	case <-provider.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("旧 Planner 未实际进入模型调用")
	}
	if plannerPersistentCount(t, old.m, task.ID) == 0 {
		t.Fatal("实际运行的旧 Planner 没有持久操作引用")
	}
	shutdownCtx, stop := context.WithTimeout(t.Context(), 8*time.Second)
	defer stop()
	stopped := make(chan error, 1)
	go func() { stopped <- old.Shutdown(shutdownCtx) }()
	select {
	case <-provider.canceled:
	case <-time.After(5 * time.Second):
		t.Fatal("关停没有取消实际模型调用")
	}
	select {
	case err := <-stopped:
		t.Fatalf("旧模型尚未真实退出，关停已提前返回: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if plannerPersistentCount(t, old.m, task.ID) == 0 {
		t.Fatal("旧模型未退出就提前清除了持久引用")
	}
	release()
	if err := <-stopped; err != nil {
		t.Fatal(err)
	}
	var operationCount, proxyCount, subjectCount int
	if err := old.m.pg.QueryRow(`SELECT
COALESCE((SELECT active_count FROM task_operation_activity WHERE task_id=$1),0),
COALESCE((SELECT active_count FROM task_proxy_activity WHERE task_id=$1),0),
(SELECT count(*) FROM proxy_subject_activity a JOIN proxy_subject_policies p ON p.id=a.policy_id WHERE p.task_id=$1)`, mustPlannerID(t, task.ID)).Scan(&operationCount, &proxyCount, &subjectCount); err != nil {
		t.Fatal(err)
	}
	if operationCount != 0 || proxyCount != 0 || subjectCount != 0 {
		t.Fatalf("真实关停后仍有本任务活动引用: operation=%d proxy=%d subject=%d", operationCount, proxyCount, subjectCount)
	}
	next := restartedPlannerServer(t, old)
	requests := make(chan llm.CompletionRequest, 4)
	nextPlanner := restartCountingPlanner(t, next, requests)
	next.engine.SetAuthoritativeAgentResolver(func(*Task) (*agent.Planner, *agent.Worker) { return nextPlanner, new(agent.Worker) })
	next.restoreTaskRuntimes()
	plannerRestartRequest(t, requests)
	persisted, err := next.m.pg.GetTask(mustPlannerID(t, task.ID))
	if err != nil || persisted == nil || persisted.Paused || persisted.Queued || persisted.Status != "running" {
		t.Fatalf("正常重启改变了任务控制状态: task=%+v err=%v", persisted, err)
	}
}
