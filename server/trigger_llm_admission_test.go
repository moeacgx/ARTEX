package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/Autumn-27/artex/agent"
	"github.com/Autumn-27/artex/db"
	actool "github.com/Autumn-27/norma/tool"
)

func newTriggerRegressionServer(t *testing.T) *Server {
	t.Helper()
	s := newModelExecutionServer(t)
	s.triggerQ = map[string][]triggeredRun{}
	s.triggerActive = map[string]int{}
	s.triggerCfg = map[string]triggerBehavior{}
	oldAugment, oldResolve, oldPrompt, oldBinding := agent.ToolAugment, agent.ToolResolve, agent.PromptOverride, agent.FindingTrafficBindingEnabled
	agent.ToolAugment, agent.ToolResolve, agent.PromptOverride, agent.FindingTrafficBindingEnabled = nil, nil, nil, nil
	t.Cleanup(func() {
		waitTriggerTestIdle(t, s)
		agent.ToolAugment, agent.ToolResolve, agent.PromptOverride, agent.FindingTrafficBindingEnabled = oldAugment, oldResolve, oldPrompt, oldBinding
	})
	return s
}

func TestTriggeredRunUsesAgentBoundProfileWithoutGlobalChatAgent(t *testing.T) {
	s := newTriggerRegressionServer(t)
	oldGlobalProxy, err := s.m.pg.GlobalProxyID()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.m.pg.SetGlobalProxyID(nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.m.pg.SetGlobalProxyID(oldGlobalProxy) })

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"choices":[{"message":{"role":"assistant","content":"agent-bound profile used"},"finish_reason":"stop"}]}`)
	}))
	defer upstream.Close()
	profileID, _ := executionModels(t, s, upstream.URL)
	key := "trigger_test_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	if _, err := s.m.pg.CreateAgent(key, "触发器测试", ""); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = s.m.pg.Exec(`DELETE FROM conversations WHERE agent_key=$1`, key)
		_ = s.m.pg.DeleteAgent(key)
	})
	if err := s.m.pg.SetAgentLLMProfile(key, &profileID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { waitTriggerTestIdle(t, s) })

	s.cfgMu.Lock()
	s.chatAgent = nil
	s.llmOn = false
	s.cfgMu.Unlock()
	s.StartTriggeredRun(key, "触发器测试", "请直接回复测试完成。", 0, false, "", "")

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		conversations, err := s.m.pg.ListConversations()
		if err != nil {
			t.Fatal(err)
		}
		for _, conversation := range conversations {
			if conversation.AgentKey != key {
				continue
			}
			activities, _, err := s.m.pg.ConvActivityList(conversation.ID, 0, 100)
			if err != nil {
				t.Fatal(err)
			}
			for _, activity := range activities {
				if activity.Kind == "result" && !activity.IsError && activity.Summary == "agent-bound profile used" {
					waitTriggerTestIdle(t, s)
					return
				}
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("triggered run did not use the agent-bound profile while the global ChatAgent was unavailable")
}

func waitTriggerTestIdle(t *testing.T, s *Server) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		s.queueMu.Lock()
		busy := false
		for _, active := range s.triggerActive {
			busy = busy || active > 0
		}
		s.queueMu.Unlock()
		if !busy {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Error("触发会话未在期限内退出")
}

func TestToolTriggerRetriesUnavailableAgentWithoutDuplicatingOtherTriggers(t *testing.T) {
	s := newTriggerRegressionServer(t)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"choices":[{"message":{"role":"assistant","content":"完成"},"finish_reason":"stop"}]}`)
	}))
	defer upstream.Close()
	keys := []string{}
	triggers := []*db.AgentTrigger{}
	var unavailableModel int64
	for i := 0; i < 2; i++ {
		profileID, models := executionModels(t, s, upstream.URL)
		key := "trigger_retry_" + strconv.FormatInt(time.Now().UnixNano(), 10)
		keys = append(keys, key)
		if _, err := s.m.pg.CreateAgent(key, "报告触发回归测试", ""); err != nil {
			t.Fatal(err)
		}
		if err := s.m.pg.SetAgentLLMProfile(key, &profileID); err != nil {
			t.Fatal(err)
		}
		toolName := "report_finding"
		if i == 1 {
			toolName = "REPORT_FINDING"
		}
		trigger, err := s.m.pg.CreateTrigger(&db.AgentTrigger{AgentKey: key, Enabled: true, OnToolCall: true, ToolNames: []string{toolName}})
		if err != nil {
			t.Fatal(err)
		}
		triggers = append(triggers, trigger)
		if i == 1 {
			unavailableModel = models.Models[0].ID
		}
		t.Cleanup(func() {
			_, _ = s.m.pg.Exec(`DELETE FROM conversations WHERE agent_key=$1`, key)
			_ = s.m.pg.DeleteTrigger(trigger.ID)
			_ = s.m.pg.DeleteAgent(key)
		})
	}
	t.Cleanup(func() { waitTriggerTestIdle(t, s) })
	task := executionTask(t, s, nil)
	var last int64
	if err := s.m.pg.QueryRow(`SELECT COALESCE(max(id),0) FROM activity`).Scan(&last); err != nil {
		t.Fatal(err)
	}
	if err := s.m.pg.SetSchedState(schedKeyLastToolCall, strconv.FormatInt(last, 10)); err != nil {
		t.Fatal(err)
	}
	id, err := task.Store.AppendActivity(db.Activity{Kind: "tool_result", Tool: "report_finding", Detail: "finding recorded"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.m.pg.Exec(`UPDATE llm_models SET enabled=false WHERE id=$1`, unavailableModel); err != nil {
		t.Fatal(err)
	}
	scheduler := newScheduler(s)
	scheduler.fireToolCalls(triggers)
	waitTriggerTestIdle(t, s)
	assertCount := func(key string, want int) {
		t.Helper()
		var count int
		if err := s.m.pg.QueryRow(`SELECT count(*) FROM conversations WHERE agent_key=$1`, key).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != want {
			t.Fatalf("agent %s: conversations=%d, want=%d", key, count, want)
		}
	}
	assertCount(keys[0], 1)
	assertCount(keys[1], 0)
	if scheduler.mustState(schedKeyLastToolCall) != strconv.FormatInt(id, 10) {
		t.Fatal("全局扫描位置未推进")
	}
	if _, err := s.m.pg.Exec(`UPDATE llm_models SET enabled=true WHERE id=$1`, unavailableModel); err != nil {
		t.Fatal(err)
	}
	// 重建调度器，验证未准入事件的位置从数据库恢复。
	scheduler = newScheduler(s)
	scheduler.fireToolCalls(triggers)
	waitTriggerTestIdle(t, s)
	assertCount(keys[0], 1)
	assertCount(keys[1], 1)
	if s.chatAgentRef() != nil {
		t.Fatal("测试意外初始化了全局 ChatAgent")
	}
	triggers[1].Enabled = false
	if err := s.m.pg.UpdateTrigger(triggers[1]); err != nil {
		t.Fatal(err)
	}
	if _, err := task.Store.AppendActivity(db.Activity{Kind: "tool_result", Tool: "report_finding", Detail: "disabled event"}); err != nil {
		t.Fatal(err)
	}
	scheduler.fireToolCalls(triggers[:1])
	waitTriggerTestIdle(t, s)
	triggers[1].Enabled = true
	if err := s.m.pg.UpdateTrigger(triggers[1]); err != nil {
		t.Fatal(err)
	}
	scheduler.fireToolCalls(triggers)
	waitTriggerTestIdle(t, s)
	assertCount(keys[0], 2)
	assertCount(keys[1], 1)
}

func TestReporterSavesReportAfterStartupProxyRecovers(t *testing.T) {
	s := newTriggerRegressionServer(t)
	const reportText = "## 详细报告\n\n已读取保存的测试证据，没有重新探测目标。"
	var nodeID int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Messages []struct {
				Role string `json:"role"`
			} `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		for _, m := range req.Messages {
			if m.Role == "tool" {
				fmt.Fprint(w, `{"choices":[{"message":{"role":"assistant","content":"报告已保存"},"finish_reason":"stop"}]}`)
				return
			}
		}
		args, _ := json.Marshal(map[string]any{"finding_id": nodeID, "report": reportText, "evidence_version": 0})
		_ = json.NewEncoder(w).Encode(map[string]any{"choices": []any{map[string]any{"message": map[string]any{"role": "assistant", "tool_calls": []any{map[string]any{"id": "save-report", "type": "function", "function": map[string]any{"name": "update_finding_report", "arguments": string(args)}}}}, "finish_reason": "tool_calls"}}})
	}))
	defer upstream.Close()
	profileID, _ := executionModels(t, s, upstream.URL)
	oldActive, err := s.m.pg.ActiveProfile()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if oldActive != nil {
			_ = s.m.pg.SetActiveProfile(oldActive.ID)
		} else {
			_, _ = s.m.pg.Exec(`UPDATE llm_profiles SET is_default=false WHERE id=$1`, profileID)
		}
	})
	if err := s.m.pg.SetActiveProfile(profileID); err != nil {
		t.Fatal(err)
	}
	key := "reporter_test_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	if _, err := s.m.pg.CreateAgent(key, "报告写入测试", ""); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = s.m.pg.Exec(`DELETE FROM conversations WHERE agent_key=$1`, key)
		_ = s.m.pg.DeleteAgent(key)
	})
	task := executionTask(t, s, nil)
	tid, _ := strconv.ParseInt(task.ID, 10, 64)
	finding, err := task.Store.RecordFinding(t.Context(), db.RecordFindingInput{TaskID: tid, Name: "本地报告回归", Summary: "测试摘要", Evidence: "保存的测试证据", Severity: "low"})
	if err != nil {
		t.Fatal(err)
	}
	nodeID = finding.NodeID
	t.Cleanup(func() { _, _ = s.m.pg.DeleteFinding(finding.FindingID) })
	agent.ToolAugment = func(context.Context, string) ([]actool.CoreTool, agent.DeferredInfo, func()) {
		return []actool.CoreTool{s.toolUpdateFindingReport()}, agent.DeferredInfo{}, func() {}
	}
	proxy, err := s.m.pg.CreateProxy(&db.Proxy{Name: "启动未就绪测试", Kind: db.ProxyKindWarp, URL: "socks5://127.0.0.1:19119", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	proxyID := proxy.ID
	// 保留上次 healthy 的数据库状态，但新运行时尚未恢复本地 WARP 进程。
	if _, err := s.m.pg.Exec(`UPDATE outbound_proxies SET status='healthy' WHERE id=$1`, proxyID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = s.m.pg.Exec(`DELETE FROM outbound_proxies WHERE id=$1`, proxyID) })
	oldProxy, err := s.m.pg.GlobalProxyID()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.m.pg.Exec(`INSERT INTO settings(key,value) VALUES('global_proxy_id',$1) ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value`, strconv.FormatInt(proxyID, 10)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.m.pg.SetGlobalProxyID(oldProxy) })
	cfg, ok := s.loadLLMConfig()
	if !ok {
		t.Fatal("测试默认模型无效")
	}
	if err := s.applyLLM(cfg); !errors.Is(err, db.ErrProxyUnavailable) || s.chatAgentRef() != nil {
		t.Fatalf("启动时代理故障未保留真实原因：%v", err)
	}
	if s.StartTriggeredRun(key, "报告测试", "根据保存证据写报告", tid, false, "", "") {
		t.Fatal("未就绪出口被绕过")
	}
	if err := s.m.pg.SetGlobalProxyID(nil); err != nil {
		t.Fatal(err)
	}
	if !s.StartTriggeredRun(key, "报告测试", "根据保存证据写报告", tid, false, "", "") {
		t.Fatal("出口恢复后仍被启动失败的全局实例阻拦")
	}
	waitTriggerTestIdle(t, s)
	got, err := s.m.pg.GetFinding(finding.FindingID)
	if err != nil || got == nil || got.Report != reportText || got.ReportEvidenceVersion != 0 {
		t.Fatalf("报告未正确写回：%+v, %v", got, err)
	}
	if s.chatAgentRef() != nil {
		t.Fatal("测试意外重建了启动期全局实例")
	}
}
