package server

import (
	"context"
	"net/http"

	"github.com/Autumn-27/artex/db"
)

func (s *Server) recoverSubjectProxyPolicy(w http.ResponseWriter, r *http.Request) {
	var req struct {
		proxyPolicyHTTPInput
		Action string `json:"action"`
	}
	if !decodeProxyHTTP(w, r, &req) {
		return
	}
	subject, e := proxySubjectRequest(r)
	if e != nil {
		s.writeProxyPolicyError(w, e)
		return
	}
	expected, e := parseProxyRevision(req.Revision)
	if e != nil {
		s.writeProxyPolicyError(w, e)
		return
	}
	var patch *db.ProxyPolicyPatch
	switch req.Action {
	case "replace_policy", "confirm_released_replace_policy":
		_, v, err := req.proxyPolicyHTTPInput.patch()
		if err != nil {
			s.writeProxyPolicyError(w, err)
			return
		}
		patch = &v
	case "retry", "confirm_released":
		if req.Mode != "" || len(req.ID)+len(req.Groups)+len(req.Enabled)+len(req.Cooldown)+len(req.Budget) > 0 {
			s.writeProxyPolicyError(w, db.ErrProxyInvalidMode)
			return
		}
	default:
		s.writeProxyPolicyError(w, db.ErrProxyInvalidMode)
		return
	}
	p, e := s.m.recoverSubjectProxyPolicy(r.Context(), subject, expected, req.Action, patch)
	if e != nil {
		s.writeProxyPolicyError(w, e)
		return
	}
	out, e := s.proxyPolicyDTO(p)
	if e != nil {
		s.writeProxyPolicyError(w, e)
		return
	}
	writeJSON(w, 200, out)
}

func (m *Manager) recoverSubjectProxyPolicy(ctx context.Context, subject db.ProxySubject, expected int64, action string, patch *db.ProxyPolicyPatch) (db.ProxyPolicy, error) {
	x, e := m.pg.AcquireProxyPolicyExecutor(ctx, subject)
	if e != nil {
		return db.ProxyPolicy{}, e
	}
	defer x.Close()
	recovery, e := x.PendingRecovery(expected)
	if e != nil {
		return recovery.Policy, e
	}
	task := proxySubjectRuntimeTask(recovery.Policy)
	if action == "confirm_released" || action == "confirm_released_replace_policy" {
		// 与既有人工 Resin 核对入口一致；不重新发送不确定的 DELETE。
		if e = m.confirmTaskLeaseReleased(task); e != nil {
			return recovery.Policy, db.ErrProxySwitchRecovery
		}
	}
	if e = m.ReleaseTaskProxy(ctx, task); e != nil {
		return recovery.Policy, db.ErrProxySwitchRecovery
	}
	var p db.ProxyPolicy
	if patch != nil {
		p, e = x.ReplaceRecovery(recovery, *patch)
	} else if recovery.Request != nil {
		p, e = x.CompleteFailover(*recovery.Request)
	} else {
		p, e = x.Complete(expected, recovery.Token)
	}
	if e != nil {
		return recovery.Policy, e
	}
	if subject.Kind == "task" {
		if current, ok := m.Task(i64s(subject.ID)); ok {
			e = m.refreshTaskProxyState(current, p.State)
		}
	}
	return p, e
}
