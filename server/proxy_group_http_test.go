package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Autumn-27/artex/db"
	"github.com/Autumn-27/artex/llmrec"
	"github.com/Autumn-27/artex/traffic"
	"github.com/Autumn-27/norma/llm"
	actool "github.com/Autumn-27/norma/tool"
)

func TestProxyPGGroupActualHTTPModelToolsRecordingAcrossManagers(t *testing.T) {
	s, _, task, g, a, b := groupFixture(t)
	s.m.closeProxyFailover()
	s.m.pg.SetMaxOpenConns(1)
	if e := s.m.pg.EnsureLLMRecordsTable(); e != nil {
		t.Fatal(e)
	}
	if e := s.m.pg.EnsureLLMUsageTable(); e != nil {
		t.Fatal(e)
	}
	s.m.llmRecOn = true
	var counts [4]atomic.Int32
	respond := func(label string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if strings.HasSuffix(r.URL.Path, "/chat/completions") {
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprintf(w, `{"choices":[{"message":{"role":"assistant","content":%q},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":2}}`, label)
			} else {
				fmt.Fprint(w, label)
			}
		}
	}
	servers := make([]*httptest.Server, 4)
	for i, label := range []string{"A", "B", "direct", "outside"} {
		var connections sync.Map
		servers[i] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if i < 2 && r.Header.Get("Proxy-Authorization") != "Basic cHJveHktdXNlcjpzeW50aGV0aWMtc2VjcmV0" {
				t.Error("组出口认证丢失")
			}
			if r.Method == http.MethodConnect {
				conn, rw, e := w.(http.Hijacker).Hijack()
				if e != nil {
					t.Error(e)
					return
				}
				connections.Store(conn, true)
				defer func() { connections.Delete(conn); conn.Close() }()
				fmt.Fprint(rw, "HTTP/1.1 200 Connection Established\r\n\r\n")
				if e = rw.Flush(); e != nil {
					return
				}
				for {
					request, e := http.ReadRequest(rw.Reader)
					if e != nil {
						return
					}
					counts[i].Add(1)
					record := httptest.NewRecorder()
					respond(label)(record, request)
					request.Body.Close()
					response := record.Result()
					response.ContentLength = int64(record.Body.Len())
					e = response.Write(rw)
					response.Body.Close()
					if e != nil {
						return
					}
					if e = rw.Flush(); e != nil {
						return
					}
				}
			}
			counts[i].Add(1)
			respond(label)(w, r)
		}))
		t.Cleanup(servers[i].Close)
		t.Cleanup(func() {
			connections.Range(func(k, v any) bool { _ = k.(interface{ Close() error }).Close(); return true })
		})
	}
	for i, p := range []*db.Proxy{a, b} {
		p.URL = strings.Replace(servers[i].URL, "http://", "http://proxy-user:synthetic-secret@", 1)
		if _, e := s.m.pg.UpdateProxy(p); e != nil {
			t.Fatal(e)
		}
	}
	outside := createRuntimeProxy(t, s, servers[3].URL)
	id, _ := executionModels(t, s, servers[2].URL)
	profile, e := s.m.pg.ProfileByID(id)
	if e != nil {
		t.Fatal(e)
	}
	profile.ProxyMode = "fixed"
	profile.ProxyID = &outside.ID
	if _, e = s.m.pg.SaveProfile(profile); e != nil {
		t.Fatal(e)
	}
	p := saveGroupPolicy(t, s, task, g, true, 3)
	router, e := traffic.Open(t.TempDir(), "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	s.m.traffic = router
	s.m.trafficOn = true
	t.Cleanup(func() { s.m.CloseProxyRuntime(); router.Close() })
	secondPG, e := db.Open(proxyTestDSN(t))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { secondPG.Close() })
	secondPG.SetMaxOpenConns(1)
	secondTask := &Task{ID: task.ID, Status: "done"}
	secondTask.setProxyState(task.proxyState())
	second := &Manager{pg: secondPG, dir: s.m.dir, tasks: map[string]*Task{task.ID: secondTask}, llmRecOn: true, traffic: router, trafficOn: true}
	if e = second.InitProxyRuntime(t.Context()); e != nil {
		t.Fatal(e)
	}
	second.closeProxyFailover()
	t.Cleanup(func() { second.CloseProxyRuntime() })
	secondServer := &Server{m: second, engine: NewEngine(second)}
	selection := db.LLMModelSelection{ProfileID: id}
	var oldProvider llm.Provider
	run := func(server *Server, current *Task, want string) {
		t.Helper()
		ctx, finish, ok := server.engine.beginTaskOperationContext(t.Context(), task.ID)
		if !ok {
			t.Fatal("真实操作准入失败")
		}
		defer finish()
		ctx = llmrec.WithTaskID(ctx, task.ID)
		prov, _, ok := server.providerForTaskSelection(selection, current, ctx)
		if !ok {
			t.Fatal("模型解析失败")
		}
		message, _, _, e := prov.Complete(ctx, llm.CompletionRequest{Messages: []llm.Message{{Role: llm.RoleUser, Content: []llm.ContentBlock{{Type: llm.BlockText, Text: "合成请求"}}}}})
		if e != nil {
			t.Fatal(e)
		}
		if len(message.Content) == 0 || message.Content[0].Text != want {
			t.Fatalf("LLM实际出口不匹配: %+v", message)
		}
		addr, ca, e := server.m.TaskToolProxy(ctx, current)
		if e != nil {
			t.Fatal(e)
		}
		tool := actool.NewWebFetch(actool.WebFetchConfig{Proxy: addr, CACert: ca, StrictProxy: true})
		input, _ := json.Marshal(map[string]any{"url": servers[2].URL + "/probe"})
		result, e := tool.Call(ctx, input, nil)
		if e != nil || result.IsError || !strings.Contains(result.Flatten(), want) {
			t.Fatalf("工具出口: %s %v", result.Flatten(), e)
		}
		if want == "A" {
			oldProvider = prov
		}
	}
	run(s, task, "A")
	newGroupIntent(t, s, p)
	applyGroupEventually(t, s.m, p.Subject)
	if _, _, _, e = oldProvider.Complete(t.Context(), llm.CompletionRequest{}); !errors.Is(e, db.ErrProxyRevisionConflict) {
		t.Fatal("旧provider跨越新revision", e)
	}
	run(secondServer, secondTask, "B")
	after, e := s.m.pg.GetProxyPolicy(p.Subject)
	if e != nil {
		t.Fatal(e)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		_, e = s.m.setProxyPolicy(t.Context(), p.Subject, after.State.Revision, db.ProxyPolicyPatch{Mode: db.TaskProxyModeDirect})
		if !errors.Is(e, db.ErrProxyInUse) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal(e)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if e != nil {
		t.Fatal(e)
	}
	run(secondServer, secondTask, "direct")
	if counts[0].Load() != 2 || counts[1].Load() != 2 || counts[2].Load() != 2 || counts[3].Load() != 0 {
		t.Fatalf("实际出口A/B/direct/outside=%d/%d/%d/%d", counts[0].Load(), counts[1].Load(), counts[2].Load(), counts[3].Load())
	}
	var records int
	if e = s.m.pg.QueryRow(`SELECT count(*) FROM llm_records WHERE task_id=$1 AND status='ok'`, task.ID).Scan(&records); e != nil || records != 3 {
		t.Fatalf("录制丢失: records=%d err=%v", records, e)
	}
}

func TestProxyPGGroupHTTPFailuresDoNotRotateTargetStatuses(t *testing.T) {
	for _, status := range []int{403, 429} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			s, _, task, g, a, _ := groupFixture(t)
			s.m.closeProxyFailover()
			proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Retry-After", "1")
				w.WriteHeader(status)
				fmt.Fprint(w, "目标拒绝或限流")
			}))
			defer proxy.Close()
			a.URL = proxy.URL
			if _, e := s.m.pg.UpdateProxy(a); e != nil {
				t.Fatal(e)
			}
			p := saveGroupPolicy(t, s, task, g, true, 3)
			ctx, finish, ok := s.engine.beginTaskOperationContext(t.Context(), task.ID)
			if !ok {
				t.Fatal("准入失败")
			}
			defer finish()
			result, e := actool.NewWebFetch(actool.WebFetchConfig{Proxy: a.URL, StrictProxy: true}).Call(ctx, []byte(`{"url":"http://synthetic.invalid/page"}`), nil)
			if e != nil || !strings.Contains(result.Flatten(), fmt.Sprint(status)) {
				t.Fatalf("目标状态未返回错误: %v %s", e, result.Flatten())
			}
			proof, e := s.m.pg.ProxyFailureEvidence(p.Subject)
			if e != nil || len(proof) != 0 {
				t.Fatalf("目标状态伪造全局故障: %+v %v", proof, e)
			}
			r, e := s.m.pg.LatestProxySwitchRequest(p.Subject)
			if e != nil || r != nil {
				t.Fatalf("目标状态触发轮换: %+v %v", r, e)
			}
		})
	}
}
