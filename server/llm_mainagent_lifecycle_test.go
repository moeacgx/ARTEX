package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Autumn-27/artex/agent"
	"github.com/Autumn-27/artex/db"
	actool "github.com/Autumn-27/norma/tool"
)

func TestLLMTerminalMainAgentReacquiresLeaseAndDrainsRealTool(t *testing.T) {
	s := newModelExecutionServer(t)
	started, unblock := make(chan struct{}), make(chan struct{})
	var startOnce, unblockOnce sync.Once
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "chat/completions") {
			var req struct {
				Model    string `json:"model"`
				Messages []struct {
					Role string `json:"role"`
				} `json:"messages"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			if req.Model != "real-a" {
				t.Errorf("MainAgent发送了错误模型: %s", req.Model)
			}
			w.Header().Set("Content-Type", "application/json")
			used := false
			for _, m := range req.Messages {
				if m.Role == "tool" {
					used = true
				}
			}
			if !used {
				fmt.Fprint(w, `{"choices":[{"message":{"role":"assistant","tool_calls":[{"id":"call_http","type":"function","function":{"name":"test_proxies","arguments":"{}"}}]},"finish_reason":"tool_calls"}]}`)
			} else {
				fmt.Fprint(w, `{"choices":[{"message":{"role":"assistant","content":"finished"},"finish_reason":"stop"}]}`)
			}
			return
		}
		startOnce.Do(func() { close(started) })
		<-unblock
		fmt.Fprint(w, "真实任务工具完成")
	}))
	defer upstream.Close()
	t.Cleanup(func() { unblockOnce.Do(func() { close(unblock) }) })
	admin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/healthz":
			w.WriteHeader(200)
		case r.URL.Path == "/api/v1/system/info":
			fmt.Fprint(w, `{"auth_version":"V1"}`)
		case r.URL.Path == "/api/v1/platforms":
			fmt.Fprint(w, `{"items":[{"id":"p","name":"Default"}]}`)
		case r.Method == "DELETE":
			w.WriteHeader(204)
		default:
			w.WriteHeader(404)
		}
	}))
	defer admin.Close()
	u, _ := url.Parse(upstream.URL)
	port, _ := strconv.Atoi(u.Port())
	p, err := s.m.pg.CreateProxy(&db.Proxy{Name: "MainAgent生命周期", Kind: db.ProxyKindResin, Protocol: "http", Host: u.Hostname(), Port: port, Password: "synthetic", ResinAdminURL: admin.URL, ResinAdminToken: "synthetic", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = s.m.pg.Exec(`DELETE FROM outbound_proxies WHERE id=$1`, p.ID) })
	id, _ := executionModels(t, s, "http://model.invalid/v1")
	task, err := s.m.CreateTaskWithOptions("MainAgent生命周期", "", db.TaskCreateOptions{LLMMembers: []db.LLMModelSelection{{ProfileID: id}}, ProxyMode: db.TaskProxyModeFixed, ProxyID: &p.ID})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { n, _ := parseTaskID(task.ID); _ = s.m.pg.DeleteTask(n) })
	ctx, finish, ok := s.engine.beginTaskOperationContext(t.Context(), task.ID)
	if !ok {
		t.Fatal("未准入")
	}
	if _, _, err := s.m.TaskToolProxy(ctx, task); err != nil {
		finish()
		t.Fatal(err)
	}
	finish()
	if err := s.m.SetTaskStatus(task.ID, "done"); err != nil {
		t.Fatal(err)
	}
	waitModelTaskProxyIdle(t, s, task.ID)
	old := agent.ToolAugment
	oldResolve := agent.ToolResolve
	// 本用例显式授权合成代理管理工具；不继承其他 Server 用例安装的全局数据库绑定。
	agent.ToolResolve = nil
	agent.ToolAugment = func(context.Context, string) ([]actool.CoreTool, agent.DeferredInfo, func()) {
		return []actool.CoreTool{roTool("test_proxies", "测试代理中心管理工具的正式准入HTTP调用", objSchema(map[string]any{}), func(ctx context.Context, _ json.RawMessage) (actool.Result, error) {
			if !s.m.TaskProxyOperationAdmitted(ctx, task.ID) {
				return actool.Errorf("缺少正式准入凭证"), nil
			}
			return s.runHTTPTool(ctx, json.RawMessage(`{"url":"http://target.invalid/blocked"}`), nil, nil)
		})}, agent.DeferredInfo{}, func() {}
	}
	t.Cleanup(func() { agent.ToolAugment = old; agent.ToolResolve = oldResolve })
	api := modelHTTPClient(t, s)
	api("POST", "/api/chat?task="+task.ID, `{"message":"调用HTTP工具"}`, 202)
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		unblockOnce.Do(func() { close(unblock) })
		t.Fatal("终态MainAgent未启动真实工具")
	}
	api("POST", fmt.Sprintf("/api/proxies/%d/resin/release", p.ID), fmt.Sprintf(`{"task_id":%q}`, task.ID), 409)
	api("DELETE", fmt.Sprintf("/api/proxies/%d", p.ID), "", 409)
	if s.engine.inflightCount(task.ID) < 1 {
		t.Fatal("主Agent工具期间无操作屏障")
	}
	unblockOnce.Do(func() { close(unblock) })
	deadline := time.Now().Add(5 * time.Second)
	for {
		s.chatMu.Lock()
		busy := s.chatBusy[task.ID]
		s.chatMu.Unlock()
		if !busy {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("主Agent工具轮次未退出")
		}
		time.Sleep(10 * time.Millisecond)
	}
	waitModelTaskProxyIdle(t, s, task.ID)
	if s.engine.inflightCount(task.ID) != 0 {
		t.Fatal("主Agent结束后占用未解除")
	}
}
