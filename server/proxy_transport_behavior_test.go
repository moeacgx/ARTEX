package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Autumn-27/artex/db"
	"github.com/Autumn-27/artex/sidequestion"
	"github.com/Autumn-27/norma/llm"
)

type syntheticMCPRequest struct {
	ID     int    `json:"id"`
	Method string `json:"method"`
}

func syntheticMCPResult(method, label string) any {
	switch method {
	case "initialize":
		return map[string]any{"protocolVersion": "2025-06-18", "capabilities": map[string]any{}, "serverInfo": map[string]any{"name": "synthetic", "version": "1"}}
	case "tools/list":
		return map[string]any{"tools": []any{map[string]any{"name": "probe", "inputSchema": map[string]any{"type": "object"}}}}
	default:
		return map[string]any{"content": []any{map[string]any{"type": "text", "text": label}}}
	}
}

func syntheticMCPProxy(t *testing.T, label string, deleteStarted chan struct{}, deleteExit <-chan struct{}) *httptest.Server {
	t.Helper()
	messages := make(chan []byte, 16)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/probe" {
			fmt.Fprint(w, label)
			return
		}
		if r.Method == http.MethodGet && r.URL.Path == "/sse" {
			w.Header().Set("Content-Type", "text/event-stream")
			fmt.Fprint(w, "event: endpoint\ndata: http://synthetic.invalid/message\n\n")
			w.(http.Flusher).Flush()
			for {
				select {
				case data := <-messages:
					fmt.Fprintf(w, "event: message\ndata: %s\n\n", data)
					w.(http.Flusher).Flush()
				case <-r.Context().Done():
					return
				}
			}
		}
		if r.Method == http.MethodDelete {
			if deleteStarted != nil {
				close(deleteStarted)
				<-deleteExit
			}
			w.WriteHeader(204)
			return
		}
		var req syntheticMCPRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad rpc", 400)
			return
		}
		if req.ID == 0 {
			w.WriteHeader(202)
			return
		}
		data, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": syntheticMCPResult(req.Method, label)})
		if r.URL.Path == "/message" {
			messages <- data
			w.WriteHeader(202)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Mcp-Session-Id", "synthetic-session")
		w.Write(data)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// 独立测试进程通过实际 HTTP 代理取值，验证 stdio 环境绑定并等待真实进程退出。
func init() {
	if os.Getenv("ARTEX_SYNTHETIC_MCP_HELPER") == "1" {
		runProxyMCPStdioHelper()
	}
}

func runProxyMCPStdioHelper() {
	decoder, encoder := json.NewDecoder(os.Stdin), json.NewEncoder(os.Stdout)
	for {
		var req syntheticMCPRequest
		if decoder.Decode(&req) != nil {
			os.Exit(0)
		}
		if req.ID == 0 {
			continue
		}
		label := "stdio"
		if req.Method == "tools/call" {
			proxy, err := url.Parse(os.Getenv("HTTP_PROXY"))
			if err != nil || proxy.Host == "" || os.Getenv("NO_PROXY") != "" {
				os.Exit(3)
			}
			client := &http.Client{Transport: &http.Transport{Proxy: http.ProxyURL(proxy)}, Timeout: 3 * time.Second}
			response, err := client.Get("http://synthetic.invalid/probe")
			if err != nil {
				os.Exit(4)
			}
			data, _ := io.ReadAll(response.Body)
			response.Body.Close()
			client.CloseIdleConnections()
			label = string(data)
		}
		encoder.Encode(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": syntheticMCPResult(req.Method, label)})
	}
}

func TestProxyPGGroupMCPAllTransportsAndCleanupDrain(t *testing.T) {
	s, _, task, g, a, _ := groupFixture(t)
	s.m.closeProxyFailover()
	s.m.pg.SetMaxOpenConns(1)
	started, exit := make(chan struct{}), make(chan struct{})
	var unblock sync.Once
	proxy := syntheticMCPProxy(t, "MCP_A", started, exit)
	t.Cleanup(func() { unblock.Do(func() { close(exit) }) })
	a.URL = proxy.URL
	if _, err := s.m.pg.UpdateProxy(a); err != nil {
		t.Fatal(err)
	}
	policy := saveGroupPolicy(t, s, task, g, true, 3)
	ctx, cancel := context.WithCancel(t.Context())
	ctx, finish, ok := s.engine.beginTaskOperationContext(ctx, task.ID)
	if !ok {
		t.Fatal("正式准入失败")
	}
	defer cancel()
	defer finish()
	var httpClient mcpClient
	for _, transport := range []string{"http", "sse", "stdio"} {
		config := &db.MCPServer{Name: "synthetic", Transport: transport, URL: "http://synthetic.invalid/rpc"}
		if transport == "sse" {
			config.URL = "http://synthetic.invalid/sse"
		}
		if transport == "stdio" {
			config.Command, _ = os.Executable()
			config.Args = json.RawMessage(`["-test.run=^$"]`)
			config.Env = json.RawMessage(`{"ARTEX_SYNTHETIC_MCP_HELPER":"1"}`)
		}
		client, err := connectMCP(ctx, config)
		if err != nil {
			t.Fatalf("%s握手: %v", transport, err)
		}
		tools, err := client.Tools(ctx)
		if err != nil || len(tools) != 1 {
			t.Fatalf("%s发现: %v", transport, err)
		}
		result, err := tools[0].Call(ctx, []byte(`{}`), nil)
		if err != nil || result.IsError || !strings.Contains(result.Flatten(), "MCP_A") {
			t.Fatalf("%s出口: %s %v", transport, result.Flatten(), err)
		}
		if transport == "http" {
			httpClient = client
		} else if err = client.Close(); err != nil {
			t.Fatal(err)
		}
	}
	newGroupIntent(t, s, policy)
	closed := make(chan error, 1)
	go func() { closed <- httpClient.Close() }()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("MCP清理未进入")
	}
	cancel()
	if err := s.m.applyProxyFailover(t.Context(), policy.Subject); !errors.Is(err, db.ErrProxyInUse) {
		t.Fatalf("cancel提前释放清理占用: %v", err)
	}
	request, err := s.m.pg.LatestProxySwitchRequest(policy.Subject)
	if err != nil || request.State != "draining" {
		t.Fatalf("draining未持久: %+v %v", request, err)
	}
	unblock.Do(func() { close(exit) })
	if err = <-closed; err != nil {
		t.Fatal(err)
	}
	finish()
	applyGroupEventually(t, s.m, policy.Subject)
}

func TestProxyPGGroupConversationsModelAndSideEgress(t *testing.T) {
	s, _, _, _, a, b := groupFixture(t)
	s.m.closeProxyFailover()
	s.m.pg.SetMaxOpenConns(1)
	if err := s.m.pg.EnsureLLMRecordsTable(); err != nil {
		t.Fatal(err)
	}
	if err := s.m.pg.EnsureLLMUsageTable(); err != nil {
		t.Fatal(err)
	}
	s.m.llmRecOn = true
	for i, p := range []*db.Proxy{a, b} {
		label := []string{"CONV_A", "CONV_B"}[i]
		proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"choices":[{"message":{"role":"assistant","content":%q},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":2}}`, label)
		}))
		t.Cleanup(proxy.Close)
		p.URL = proxy.URL
		if _, err := s.m.pg.UpdateProxy(p); err != nil {
			t.Fatal(err)
		}
	}
	profileID, _ := executionModels(t, s, "http://synthetic.invalid")
	cfg, _, ok := s.loadSelectionConfig(db.LLMModelSelection{ProfileID: profileID})
	if !ok {
		t.Fatal("合成模型配置无效")
	}
	var subjects []db.ProxySubject
	for _, p := range []*db.Proxy{a, b} {
		g, err := s.m.pg.CreateProxyGroup("对话专属合成组", true)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = s.m.pg.SetProxyGroupMembers(g.ID, g.Revision, []int64{p.ID}); err != nil {
			t.Fatal(err)
		}
		c, err := s.m.pg.CreateConversation("auto", "合成对话", &profileID)
		if err != nil {
			t.Fatal(err)
		}
		subject := db.ProxySubject{Kind: "conversation", ID: c.ID}
		subjects = append(subjects, subject)
		t.Cleanup(func() {
			_ = s.m.pg.DeleteConversation(c.ID)
			_, _ = s.m.pg.Exec(`DELETE FROM proxy_groups WHERE id=$1`, g.ID)
		})
		groups := []int64{g.ID}
		if _, err = s.m.setProxyPolicy(t.Context(), subject, 0, db.ProxyPolicyPatch{Mode: db.TaskProxyModeGroup, AllowedGroupIDs: &groups}); err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	for i, subject := range subjects {
		wg.Go(func() {
			ctx, _, finish, err := s.beginConversationProxy(t.Context(), subject.ID)
			if err != nil {
				t.Error(err)
				return
			}
			defer finish()
			provider := conversationPolicyProvider{s: s, subject: subject, agentKey: "auto"}
			message, _, _, err := provider.Complete(ctx, llm.CompletionRequest{Messages: []llm.Message{llm.UserText("合成对话")}})
			if err != nil || len(message.Content) == 0 || message.Content[0].Text != []string{"CONV_A", "CONV_B"}[i] {
				t.Errorf("双对话模型串线: %+v %v", message, err)
			}
		})
	}
	wg.Wait()
	for i, subject := range subjects {
		ctx, provider, finish, err := s.admitSideProxy(t.Context(), sidequestion.Parent{ConversationID: subject.ID}, sideModel(cfg, profileID, "synthetic"))
		if err != nil {
			t.Fatal(err)
		}
		message, _, _, err := provider.Complete(ctx, llm.CompletionRequest{Messages: []llm.Message{llm.UserText("合成旁路")}})
		finish()
		if err != nil || len(message.Content) == 0 || message.Content[0].Text != []string{"CONV_A", "CONV_B"}[i] {
			t.Fatalf("旁路模型绕过组: %+v %v", message, err)
		}
	}
	var records int
	if err := s.m.pg.QueryRow(`SELECT count(*) FROM llm_records WHERE profile_name=(SELECT name FROM llm_profiles WHERE id=$1) AND status='ok'`, profileID).Scan(&records); err != nil || records != 4 {
		t.Fatalf("对话录制: %d %v", records, err)
	}
}
