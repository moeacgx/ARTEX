package server

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Autumn-27/artex/agent"
	"github.com/Autumn-27/artex/db"
	"github.com/Autumn-27/norma/llm"
)

func TestTaskFollowupMainPlannerWorkerLifecycle(t *testing.T) {
	for _, entry := range []string{"main_intent", "main_hint", "main_goal", "auto_hint", "auto_goal"} {
		t.Run(entry, func(t *testing.T) { testFollowupPlannerWorker(t, entry) })
	}
}

func testFollowupPlannerWorker(t *testing.T, entry string) {
	m, err := NewManager(t.TempDir(), "")
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if err = db.ConfigureProxySecrets(filepath.Join(m.dir, "proxy.key")); err != nil {
		t.Fatal(err)
	}
	if err = db.InitializeProxySecrets(); err != nil {
		t.Fatal(err)
	}
	if err = m.InitProxyRuntime(t.Context()); err != nil {
		t.Fatal(err)
	}
	defer m.CloseProxyRuntime()
	task, err := m.CreateTask("追加工作链路", "原目标", nil, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	s := &Server{m: m, ctx: ctx, engine: NewEngine(m)}
	s.engine.debounce = 10 * time.Millisecond
	defer func() { cancel(); s.engine.StopTask(task.ID); _, _ = m.DeleteTask(task.ID, DeleteTaskOptions{}) }()
	oldGoal, err := task.Store.AddNode(db.KindGoal, map[string]any{"text": "原目标"}, 0, "met", "human", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = m.SetTaskStatus(task.ID, "done"); err != nil {
		t.Fatal(err)
	}
	started, release := make(chan struct{}), make(chan struct{})
	var releaseOnce, startOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	var plannerCalls atomic.Int32
	var dispatched atomic.Bool
	var prematureExecution atomic.Bool
	planner := agent.NewPlanner(retestProvider{complete: func(_ context.Context, req llm.CompletionRequest) (llm.Message, string, llm.Usage, error) {
		plannerCalls.Add(1)
		row, readErr := m.pg.GetTask(mustTaskID(t, task.ID))
		if readErr != nil || row.Status != "running" || row.CompletedAt != nil {
			prematureExecution.Store(true)
		}
		goals, _ := task.Store.ListByKind(db.KindGoal, 100)
		facts, _ := task.Store.ListByKind(db.KindFact, 100)
		for _, g := range goals {
			if g.State == "open" {
				for _, fact := range facts {
					var payload map[string]any
					_ = json.Unmarshal(fact.Payload, &payload)
					if payload["summary"] == "追加结果" {
						return followupTool("prove_goal", fmt.Sprintf(`{"goal_id":%d,"evidence_id":%d,"reason":"追加结果已核验"}`, g.ID, fact.ID))
					}
				}
			}
		}
		if entry != "main_intent" && dispatched.CompareAndSwap(false, true) {
			return followupTool("add_intent", `{"summary":"核验追加本地结果"}`)
		}
		return followupText("等待追加 Worker 的实际结果")
	}}, "fixture", m.dir, nil, 10000, 3)
	planner.SetNonStreaming(func() bool { return true })
	var workerCalls atomic.Int32
	worker := agent.NewWorker(retestProvider{complete: func(c context.Context, req llm.CompletionRequest) (llm.Message, string, llm.Usage, error) {
		if workerCalls.Add(1) == 1 {
			startOnce.Do(func() { close(started) })
			select {
			case <-release:
			case <-c.Done():
				return llm.Message{}, "", llm.Usage{}, c.Err()
			}
			intents, _ := task.Store.ListByKind(db.KindIntent, 100)
			return followupTool("record_fact", fmt.Sprintf(`{"intent_id":%d,"summary":"追加结果","evidence":"本地合成核验完成"}`, intents[0].ID))
		}
		return followupText("追加工作已完成")
	}}, "fixture", m.dir, nil, 10000, 3)
	worker.SetNonStreaming(func() bool { return true })
	s.engine.SetAuthoritativeAgentResolver(func(*Task) (*agent.Planner, *agent.Worker) { return planner, worker })
	calls := 0
	main := agent.NewMainAgent(retestProvider{complete: func(context.Context, llm.CompletionRequest) (llm.Message, string, llm.Usage, error) {
		calls++
		if calls == 1 {
			name := map[string]string{"main_intent": "add_intent", "main_hint": "add_hint", "main_goal": "set_goals"}[entry]
			return followupTool(name, `{"summary":"核验追加本地结果","text":"核验追加本地结果"}`)
		}
		return followupText("已登记追加工作")
	}}, "fixture", m.dir, nil, 10000, 3)
	main.SetNonStreaming(func() bool { return true })
	tid, _ := strconv.ParseInt(task.ID, 10, 64)
	if entry == "auto_hint" || entry == "auto_goal" {
		name := map[string]string{"auto_hint": "add_task_hint", "auto_goal": "add_task_goal"}[entry]
		found := false
		for _, tool := range s.orchestrationTools() {
			if tool.Name() == name {
				found = true
				res, callErr := tool.Call(ctx, json.RawMessage(fmt.Sprintf(`{"task_id":%q,"text":"核验追加本地结果"}`, task.ID)), nil)
				if callErr != nil || res.IsError {
					t.Fatalf("Auto 追加失败: %v %s", callErr, res.Flatten())
				}
			}
		}
		if !found {
			t.Fatalf("缺少 Auto 工具 %s", name)
		}
	} else {
		_, err = main.Chat(agent.WithTaskAdmission(ctx, func() error { _, err := s.admitTask(task, "resume"); return err }), tid, 0, nil, task.Store, task.Goal, "请继续追加核验", nil, task.Notify, nil, task.NotifyGoal, task.NotifyHint)
	}
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(4 * time.Second):
		t.Fatal("没有新 Worker 执行追加工作")
	}
	deadline := time.Now().Add(3 * time.Second)
	for plannerCalls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if plannerCalls.Load() == 0 {
		t.Fatal("旧目标已达成后 Planner 未参与追加工作")
	}
	if prematureExecution.Load() {
		t.Fatal("Planner 执行前未取消完成状态或未清除完成时间")
	}
	if m.TaskStatus(task.ID) != "running" {
		t.Fatalf("追加工作期间状态 %s", m.TaskStatus(task.ID))
	}
	if open, _ := task.Store.HasOpenGoal(); !open {
		t.Fatal("追加工作没有独立未完成目标")
	}
	releaseOnce.Do(func() { close(release) })
	deadline = time.Now().Add(5 * time.Second)
	for m.TaskStatus(task.ID) != "done" && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if m.TaskStatus(task.ID) != "done" {
		t.Fatal("新目标验收后未完成")
	}
	old, _ := task.Store.GetNode(oldGoal)
	if old.State != "met" {
		t.Fatal("旧目标历史被改变")
	}
	intents, _ := task.Store.ListByKind(db.KindIntent, 100)
	if len(intents) != 1 || intents[0].State != "done" {
		t.Fatalf("追加 Worker 未正常完成: %+v", intents)
	}
}

func followupTool(name, input string) (llm.Message, string, llm.Usage, error) {
	return llm.Message{Role: llm.RoleAssistant, Content: []llm.ContentBlock{{Type: llm.BlockToolUse, ID: name, Name: name, Input: json.RawMessage(input)}}}, "tool_use", llm.Usage{}, nil
}
func followupText(text string) (llm.Message, string, llm.Usage, error) {
	return llm.Message{Role: llm.RoleAssistant, Content: []llm.ContentBlock{llm.TextBlock(text)}}, "end_turn", llm.Usage{}, nil
}
