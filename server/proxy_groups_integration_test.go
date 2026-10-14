package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Autumn-27/artex/agent"
	"github.com/Autumn-27/artex/db"
)

func groupFixture(t *testing.T) (*Server, *http.ServeMux, *Task, *db.ProxyGroup, *db.Proxy, *db.Proxy) {
	t.Helper()
	s, mux, task := switchFixture(t)
	s.registerProxyGroupRoutes(mux)
	a := createRuntimeProxy(t, s, "http://127.0.0.1:19111")
	b := createRuntimeProxy(t, s, "http://127.0.0.1:19112")
	g, e := s.m.pg.CreateProxyGroup("合成测试组", true)
	if e != nil {
		t.Fatal(e)
	}
	g, e = s.m.pg.SetProxyGroupMembers(g.ID, g.Revision, []int64{a.ID, b.ID})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		_, _ = s.m.pg.Exec(`DELETE FROM proxy_policy_groups WHERE policy_id IN (SELECT id FROM proxy_subject_policies WHERE task_id=$1)`, switchTaskID(t, task))
		_, _ = s.m.pg.Exec(`DELETE FROM proxy_group_members WHERE group_id=$1`, g.ID)
		_, _ = s.m.pg.Exec(`DELETE FROM proxy_groups WHERE id=$1`, g.ID)
	})
	return s, mux, task, g, a, b
}
func saveGroupPolicy(t *testing.T, s *Server, task *Task, g *db.ProxyGroup, enabled bool, budget int) db.ProxyPolicy {
	t.Helper()
	groups := []int64{g.ID}
	cooldown := 0
	p, e := s.m.setProxyPolicy(t.Context(), db.ProxySubject{Kind: "task", ID: switchTaskID(t, task)}, task.proxyState().Revision, db.ProxyPolicyPatch{Mode: db.TaskProxyModeGroup, AllowedGroupIDs: &groups, AgentFailoverEnabled: &enabled, FailoverCooldownSeconds: &cooldown, FailoverMaxSwitches: &budget})
	if e != nil {
		t.Fatal(e)
	}
	return p
}

func TestProxyPGGroupsHTTPAndAuthorizationSemantics(t *testing.T) {
	s, mux, task, g, a, _ := groupFixture(t)
	path := "/api/tasks/" + task.ID + "/proxy"
	body := fmt.Sprintf(`{"expected_revision":0,"proxy_mode":"group","allowed_group_ids":["%d"],"agent_failover_enabled":true,"failover_max_switches":2}`, g.ID)
	code, data, out := proxyAPI(t, mux, "PUT", path, body)
	if code != 200 || data["proxy_mode"] != "group" || data["proxy_id"] != float64(a.ID) || strings.Contains(out, "v1:") {
		t.Fatalf("保存组策略: %d %s", code, out)
	}
	code, _, out = proxyAPI(t, mux, "PUT", path, `{"expected_revision":0,"proxy_mode":"group"}`)
	if code != 409 || !strings.Contains(out, "proxy_revision_conflict") {
		t.Fatalf("缺少CAS: %d %s", code, out)
	}
	subject := db.ProxySubject{Kind: "task", ID: switchTaskID(t, task)}
	p, e := s.m.pg.GetProxyPolicy(subject)
	if e != nil {
		t.Fatal(e)
	}
	usedAuth := p.AuthorizationRevision
	_, e = s.m.pg.Exec(`UPDATE proxy_subject_policies SET switches_used=1 WHERE id=$1`, p.ID)
	if e != nil {
		t.Fatal(e)
	}
	p, e = s.m.setProxyPolicy(t.Context(), subject, p.State.Revision, db.ProxyPolicyPatch{Mode: db.TaskProxyModeGroup})
	if e != nil || p.AuthorizationRevision != usedAuth || p.SwitchesUsed != 1 || len(p.AllowedGroupIDs) != 1 {
		t.Fatalf("省略权限重置预算: %+v %v", p, e)
	}
	clear := []int64{}
	p, e = s.m.setProxyPolicy(t.Context(), subject, p.State.Revision, db.ProxyPolicyPatch{Mode: db.TaskProxyModeDirect, AllowedGroupIDs: &clear})
	if e != nil || len(p.AllowedGroupIDs) != 0 || p.AuthorizationRevision != usedAuth+1 || p.SwitchesUsed != 0 {
		t.Fatalf("明确清除授权: %+v %v", p, e)
	}
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest("DELETE", fmt.Sprintf("/api/proxy-groups/%d?expected_revision=%d", g.ID, g.Revision), nil))
	if w.Code != 204 {
		t.Fatalf("清除后删除: %d %s", w.Code, w.Body.String())
	}
}

func TestProxyPGGroupDirectActivityAcrossManagersAndRevocation(t *testing.T) {
	s, _, task, g, a, _ := groupFixture(t)
	id := switchTaskID(t, task)
	state, e := s.m.pg.AcquireTaskOperation(id)
	if e != nil {
		t.Fatal(e)
	}
	if state.Mode != db.TaskProxyModeDirect {
		t.Fatal(state.Mode)
	}
	second := &Manager{pg: s.m.pg, dir: s.m.dir}
	if e = second.InitProxyRuntime(t.Context()); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { second.CloseProxyRuntime() })
	groups := []int64{g.ID}
	_, e = second.setProxyPolicy(t.Context(), db.ProxySubject{Kind: "task", ID: id}, 0, db.ProxyPolicyPatch{Mode: db.TaskProxyModeGroup, AllowedGroupIDs: &groups})
	if !errors.Is(e, db.ErrProxyInUse) {
		t.Fatalf("直连真实活动未保护: %v", e)
	}
	if e = s.m.pg.ReleaseTaskOperation(id); e != nil {
		t.Fatal(e)
	}
	p := saveGroupPolicy(t, s, task, g, true, 3)
	if _, e = s.m.pg.AcquireTaskOperation(id); e != nil {
		t.Fatal(e)
	}
	_, e = s.m.pg.SetProxyGroupMembers(g.ID, g.Revision, []int64{})
	if !errors.Is(e, db.ErrProxyInUse) {
		t.Fatalf("活动成员移除未拦截: %v", e)
	}
	g, e = s.m.pg.UpdateProxyGroup(g.ID, g.Revision, g.Name, false)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.m.pg.ValidateProxyPolicy(p.Subject); !errors.Is(e, db.ErrProxyAuthorization) {
		t.Fatalf("撤权后准入未关闭: %v", e)
	}
	if e = s.m.pg.ReleaseTaskOperation(id); e != nil {
		t.Fatal(e)
	}
	if _, e = s.m.pg.AcquireTaskOperation(id); e == nil {
		t.Fatal("禁组后仍可执行")
	}
	_ = a
}

func TestProxyPGGroupToolIntentDrainsAndBudgetPersists(t *testing.T) {
	s, _, task, g, a, b := groupFixture(t)
	p := saveGroupPolicy(t, s, task, g, true, 1)
	s.m.closeProxyFailover()
	ctx, finish, ok := s.engine.beginTaskOperationContext(t.Context(), task.ID)
	if !ok {
		t.Fatal("正式准入失败")
	}
	defer finish()
	proof, e := s.m.pg.RecordProxyFailureEvidence(p.Subject, p.State.Revision, a.ID, "connection_failure")
	if e != nil {
		t.Fatal(e)
	}
	tools := s.proxyFailoverTools()
	var requestID string
	input, _ := json.Marshal(map[string]any{"expected_revision": p.State.Revision, "reason": "合成代理连接失败", "evidence": map[string]any{"kind": "connection_failure", "reference": proof}})
	result, e := tools[1].Call(ctx, input, nil)
	if e != nil || result.IsError || !agent.ProxyRunStopRequested(ctx) {
		t.Fatalf("工具没有即返/退出标记: %v %s", e, result.Flatten())
	}
	var data struct {
		Request struct {
			ID string `json:"id"`
		} `json:"request"`
	}
	_ = json.Unmarshal([]byte(result.Flatten()), &data)
	requestID = data.Request.ID
	duplicate, e := s.m.pg.RequestProxyFailover(p.Subject, db.ProxyFailoverInput{ExpectedRevision: p.State.Revision, Reason: "重试", Evidence: db.ProxyFailureEvidence{Kind: "connection_failure", Reference: proof}})
	if e != nil || duplicate.ID != requestID {
		t.Fatal("单有效意图未复用", e)
	}
	if e = s.m.applyProxyFailover(t.Context(), p.Subject); !errors.Is(e, db.ErrProxyInUse) {
		t.Fatalf("仍活动却切换: %v", e)
	}
	r, e := s.m.pg.GetProxySwitchRequest(p.Subject, requestID)
	if e != nil || r.State != "draining" {
		t.Fatalf("退出阶段未持久化: %+v %v", r, e)
	}
	finish()
	deadline := time.Now().Add(2 * time.Second)
	for {
		e = s.m.applyProxyFailover(t.Context(), p.Subject)
		if !errors.Is(e, db.ErrProxyInUse) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("真实退出后切换仍被阻塞", e)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if e != nil {
		t.Fatal(e)
	}
	after, e := s.m.pg.GetProxyPolicy(p.Subject)
	if e != nil || after.State.ID == nil || *after.State.ID != b.ID || after.SwitchesUsed != 1 || after.AuthorizationRevision != p.AuthorizationRevision {
		t.Fatalf("切换预算: %+v %v", after, e)
	}
	r, e = s.m.pg.GetProxySwitchRequest(p.Subject, requestID)
	if e != nil || r.State != "succeeded" {
		t.Fatalf("状态: %+v %v", r, e)
	}
	proof, e = s.m.pg.RecordProxyFailureEvidence(p.Subject, after.State.Revision, b.ID, "connection_failure")
	if e != nil {
		t.Fatal(e)
	}
	_, e = s.m.pg.RequestProxyFailover(p.Subject, db.ProxyFailoverInput{ExpectedRevision: after.State.Revision, Reason: "再次失败", Evidence: db.ProxyFailureEvidence{Kind: "connection_failure", Reference: proof}})
	if !errors.Is(e, db.ErrProxyBudget) {
		t.Fatalf("预算未持久限制: %v", e)
	}
	if result, e = tools[1].Call(context.Background(), input, nil); e != nil || !result.IsError {
		t.Fatal("非正式context可冒充主体")
	}
}

func TestProxyPGConversationsIndependentEgressAndSingleConnection(t *testing.T) {
	s, _ := proxyIntegration(t)
	s.m.pg.SetMaxOpenConns(1)
	newProxy := func(label string) *db.Proxy {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, label) }))
		t.Cleanup(srv.Close)
		return createRuntimeProxy(t, s, srv.URL)
	}
	a, b := newProxy("出口A"), newProxy("出口B")
	var subjects []db.ProxySubject
	for _, proxy := range []*db.Proxy{a, b} {
		c, e := s.m.pg.CreateConversation("auto", "隔离对话", nil)
		if e != nil {
			t.Fatal(e)
		}
		subject := db.ProxySubject{Kind: "conversation", ID: c.ID}
		subjects = append(subjects, subject)
		t.Cleanup(func() { _ = s.m.pg.DeleteConversation(c.ID) })
		if _, e = s.m.setProxyPolicy(t.Context(), subject, 0, db.ProxyPolicyPatch{Mode: db.TaskProxyModeFixed, ProxyID: &proxy.ID}); e != nil {
			t.Fatal(e)
		}
	}
	var finishes []func()
	for i, subject := range subjects {
		ctx, p, finish, e := s.beginConversationProxy(t.Context(), subject.ID)
		if e != nil {
			t.Fatal(e)
		}
		finishes = append(finishes, finish)
		addr, _, e := s.m.TaskToolProxy(ctx, proxySubjectRuntimeTask(p))
		if e != nil {
			t.Fatal(e)
		}
		transport, e := s.taskHTTPTransport(ctx, httpExec{}, nil)
		if e != nil {
			t.Fatal(e)
		}
		client := &http.Client{Transport: transport, Timeout: time.Second}
		resp, e := client.Get("http://synthetic.invalid/resource")
		if e != nil {
			t.Fatal(e)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if string(body) != []string{"出口A", "出口B"}[i] || addr == "" {
			t.Fatalf("对话出口串线: %s", body)
		}
		transport.CloseIdleConnections()
		if _, e = s.m.setProxyPolicy(t.Context(), subject, p.State.Revision, db.ProxyPolicyPatch{Mode: db.TaskProxyModeDirect}); !errors.Is(e, db.ErrProxyInUse) {
			t.Fatalf("活跃对话可换出口: %v", e)
		}
	}
	for _, finish := range finishes {
		finish()
	}
	for _, subject := range subjects {
		p, e := s.m.pg.GetProxyPolicy(subject)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = s.m.setProxyPolicy(t.Context(), subject, p.State.Revision, db.ProxyPolicyPatch{Mode: db.TaskProxyModeDirect}); e != nil {
			t.Fatal(e)
		}
	}
}
