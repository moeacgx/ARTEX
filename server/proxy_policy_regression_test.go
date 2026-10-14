package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Autumn-27/artex/db"
)

func newGroupIntent(t *testing.T, s *Server, p db.ProxyPolicy) db.ProxySwitchRequest {
	t.Helper()
	proof, e := s.m.pg.RecordProxyFailureEvidence(p.Subject, p.State.Revision, *p.State.ID, "connection_failure")
	if e != nil {
		t.Fatal(e)
	}
	r, e := s.m.pg.RequestProxyFailover(p.Subject, db.ProxyFailoverInput{ExpectedRevision: p.State.Revision, Reason: "合成基础设施故障", Evidence: db.ProxyFailureEvidence{Kind: "connection_failure", Reference: proof}})
	if e != nil {
		t.Fatal(e)
	}
	return r
}

func TestProxyPGGroupCommitRevocationAndManualRecovery(t *testing.T) {
	s, mux, task, g, _, _ := groupFixture(t)
	s.m.closeProxyFailover()
	p := saveGroupPolicy(t, s, task, g, true, 2)
	r := newGroupIntent(t, s, p)
	x, e := s.m.pg.AcquireProxyPolicyExecutor(t.Context(), p.Subject)
	if e != nil {
		t.Fatal(e)
	}
	old, claimed, e := x.ClaimFailover()
	if e != nil {
		x.Close()
		t.Fatal(e)
	}
	if e = s.m.ReleaseTaskProxy(t.Context(), proxySubjectRuntimeTask(old)); e != nil {
		x.Close()
		t.Fatal(e)
	}
	g, e = s.m.pg.UpdateProxyGroup(g.ID, g.Revision, g.Name, false)
	if e != nil {
		x.Close()
		t.Fatal(e)
	}
	if _, e = x.CompleteFailover(claimed); e == nil {
		x.Close()
		t.Fatal("提交未复核组撤权")
	}
	if e = x.RequireFailoverRecovery(claimed, "proxy_commit_failed"); e != nil {
		x.Close()
		t.Fatal(e)
	}
	x.Close()
	if _, e = s.m.pg.AcquireTaskOperation(p.Subject.ID); e == nil {
		t.Fatal("不确定提交恢复了旧运行")
	}
	request, e := s.m.pg.GetProxySwitchRequest(p.Subject, r.ID)
	if e != nil || request.State != "recovery_required" {
		t.Fatalf("结果未保留屏障: %+v %v", request, e)
	}
	path := "/api/tasks/" + task.ID + "/proxy/policy-recovery"
	code, _, body := proxyAPI(t, mux, "POST", path, fmt.Sprintf(`{"expected_revision":%d,"action":"retry"}`, p.State.Revision))
	if code == 200 {
		t.Fatal("撤权后重试仍提交", body)
	}
	code, _, body = proxyAPI(t, mux, "POST", path, fmt.Sprintf(`{"expected_revision":%d,"action":"abort_direct"}`, p.State.Revision))
	if code != 400 {
		t.Fatalf("group逃逸: %d %s", code, body)
	}
	code, _, body = proxyAPI(t, mux, "POST", path, fmt.Sprintf(`{"expected_revision":%d,"action":"replace_policy","proxy_mode":"direct","allowed_group_ids":[]}`, p.State.Revision))
	if code != 200 {
		t.Fatalf("人工新策略未解封: %d %s", code, body)
	}
	after, e := s.m.pg.GetProxyPolicy(p.Subject)
	if e != nil || after.State.Mode != db.TaskProxyModeDirect || after.State.Revision != p.State.Revision+1 {
		t.Fatalf("人工恢复状态: %+v %v", after, e)
	}
}

func TestProxyPGGroupRecoveryRetryKeepsBudgetAndSnapshot(t *testing.T) {
	s, _, task, g, _, b := groupFixture(t)
	s.m.closeProxyFailover()
	p := saveGroupPolicy(t, s, task, g, true, 2)
	r := newGroupIntent(t, s, p)
	x, e := s.m.pg.AcquireProxyPolicyExecutor(t.Context(), p.Subject)
	if e != nil {
		t.Fatal(e)
	}
	_, claimed, e := x.ClaimFailover()
	if e != nil {
		x.Close()
		t.Fatal(e)
	}
	if e = x.RequireFailoverRecovery(claimed, "proxy_commit_failed"); e != nil {
		x.Close()
		t.Fatal(e)
	}
	x.Close()
	after, e := s.m.recoverSubjectProxyPolicy(t.Context(), p.Subject, p.State.Revision, "retry", nil)
	if e != nil || after.SwitchesUsed != 1 || after.AuthorizationRevision != p.AuthorizationRevision || after.State.ID == nil || *after.State.ID != b.ID {
		t.Fatalf("重试重置预算或目标: %+v %v", after, e)
	}
	request, e := s.m.pg.GetProxySwitchRequest(p.Subject, r.ID)
	if e != nil || request.State != "succeeded" {
		t.Fatalf("重试状态: %+v %v", request, e)
	}
}

func TestProxyPGLateActivityTokensAndConversationDelete(t *testing.T) {
	s, mux, task := switchFixture(t)
	s.registerProxyGroupRoutes(mux)
	s.m.closeProxyFailover()
	first, e := s.m.pg.AcquireTaskOperation(switchTaskID(t, task))
	if e != nil {
		t.Fatal(e)
	}
	if e = s.m.pg.ReleaseTaskOperationToken(switchTaskID(t, task), first.OperationToken); e != nil {
		t.Fatal(e)
	}
	second, e := s.m.pg.AcquireTaskOperation(switchTaskID(t, task))
	if e != nil {
		t.Fatal(e)
	}
	if e = s.m.pg.ReleaseTaskOperationToken(switchTaskID(t, task), first.OperationToken); !errors.Is(e, db.ErrProxyActivityToken) {
		t.Fatal("迟到根token未拒绝", e)
	}
	if x, e := s.m.pg.AcquireProxyPolicyExecutor(t.Context(), db.ProxySubject{Kind: "task", ID: switchTaskID(t, task)}); e == nil {
		x.Close()
		t.Fatal("迟到释放解除了新操作锁")
	}
	if e = s.m.pg.ReleaseTaskOperationToken(switchTaskID(t, task), second.OperationToken); e != nil {
		t.Fatal(e)
	}
	c, e := s.m.pg.CreateConversation("auto", "删除真实活动", nil)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = s.m.pg.DeleteConversation(c.ID) })
	mux.HandleFunc("DELETE /api/conversations/{id}", s.pgDeleteConversation)
	p, token, e := s.m.pg.AcquireConversationOperation(c.ID)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.m.pg.ReleaseConversationOperation(c.ID, token); e != nil {
		t.Fatal(e)
	}
	_, next, e := s.m.pg.AcquireConversationOperation(c.ID)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.m.pg.ReleaseConversationOperation(c.ID, token); e == nil {
		t.Fatal("迟到对话token未拒绝")
	}
	ioToken, e := s.m.pg.AcquireProxyIOOperation(p.Subject, p.State.Revision, nil)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.m.pg.ReleaseProxyIOOperation(p.Subject, ioToken); e != nil {
		t.Fatal(e)
	}
	ioNext, e := s.m.pg.AcquireProxyIOOperation(p.Subject, p.State.Revision, nil)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.m.pg.ReleaseProxyIOOperation(p.Subject, ioToken); e == nil {
		t.Fatal("迟到IO token未拒绝")
	}
	code, _, body := proxyAPI(t, mux, "DELETE", fmt.Sprintf("/api/conversations/%d", c.ID), "")
	if code != 409 || !strings.Contains(body, "proxy_busy") {
		t.Fatalf("cancel等同退出: %d %s", code, body)
	}
	if e = s.m.pg.ReleaseProxyIOOperation(p.Subject, ioNext); e != nil {
		t.Fatal(e)
	}
	if e = s.m.pg.ReleaseConversationOperation(c.ID, next); e != nil {
		t.Fatal(e)
	}
	code, _, body = proxyAPI(t, mux, "DELETE", fmt.Sprintf("/api/conversations/%d", c.ID), "")
	if code != 200 {
		t.Fatalf("空闲删除: %d %s", code, body)
	}
}

func TestProxyPGGroupCandidatesExhaustedAndRestartBudget(t *testing.T) {
	s, _, task, g, _, b := groupFixture(t)
	s.m.closeProxyFailover()
	p := saveGroupPolicy(t, s, task, g, true, 2)
	b.Enabled = false
	if _, e := s.m.pg.UpdateProxy(b); e != nil {
		t.Fatal(e)
	}
	proof, e := s.m.pg.RecordProxyFailureEvidence(p.Subject, p.State.Revision, *p.State.ID, "connection_failure")
	if e != nil {
		t.Fatal(e)
	}
	_, e = s.m.pg.RequestProxyFailover(p.Subject, db.ProxyFailoverInput{ExpectedRevision: p.State.Revision, Reason: "无候选", Evidence: db.ProxyFailureEvidence{Kind: "connection_failure", Reference: proof}})
	if !errors.Is(e, db.ErrProxyCandidatesExhausted) {
		t.Fatal("耗尽未failclosed", e)
	}
	if _, e = s.m.pg.Exec(`UPDATE proxy_subject_policies SET switches_used=2,cooldown_until=now()+interval '60 seconds' WHERE id=$1`, p.ID); e != nil {
		t.Fatal(e)
	}
	second, e := db.Open(proxyTestDSN(t))
	if e != nil {
		t.Fatal(e)
	}
	defer second.Close()
	loaded, e := second.GetProxyPolicy(p.Subject)
	if e != nil || loaded.SwitchesUsed != 2 || loaded.CooldownUntil == nil || !loaded.CooldownUntil.After(time.Now()) {
		t.Fatalf("重开丢失预算: %+v %v", loaded, e)
	}
	if _, e = s.m.runtimeSnapshot(&Task{ID: task.ID, ProxyMode: db.TaskProxyModeGroup}); !errors.Is(e, db.ErrProxyUnavailable) {
		t.Fatal("空group落入直连", e)
	}
}

func proxyTestDSN(t *testing.T) string {
	t.Helper()
	dsn, _, e := db.DSN()
	if e != nil || !proxyIntegrationDSNAllowed(dsn) {
		t.Fatal("需要本owner独立PG")
	}
	return dsn
}

func applyGroupEventually(t *testing.T, m *Manager, subject db.ProxySubject) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	for {
		e := m.applyProxyFailover(ctx, subject)
		if e == nil {
			return
		}
		if !errors.Is(e, db.ErrProxyInUse) {
			t.Fatal(e)
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
}
