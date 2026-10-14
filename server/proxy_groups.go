package server

import (
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Autumn-27/artex/db"
)

type ProxyGroupMemberDTO struct {
	ProxyID  string    `json:"proxy_id"`
	Position int       `json:"position"`
	Proxy    *ProxyDTO `json:"proxy"`
}
type ProxyGroupDTO struct {
	ID       string                `json:"id"`
	Name     string                `json:"name"`
	Enabled  bool                  `json:"enabled"`
	Revision int64                 `json:"revision"`
	Members  []ProxyGroupMemberDTO `json:"members"`
}
type ProxySwitchRequestDTO struct {
	ID                    string  `json:"id"`
	SubjectKind           string  `json:"subject_kind"`
	SubjectID             string  `json:"subject_id"`
	BaseRevision          int64   `json:"base_revision"`
	AuthorizationRevision int64   `json:"authorization_revision"`
	State                 string  `json:"state"`
	FromProxyID           *string `json:"from_proxy_id"`
	ToProxyID             *string `json:"to_proxy_id"`
	Reason                string  `json:"reason"`
	ErrorCode             string  `json:"error_code"`
	CreatedAt             string  `json:"created_at"`
	UpdatedAt             string  `json:"updated_at"`
	CompletedAt           *string `json:"completed_at"`
	ResultRevision        *int64  `json:"result_revision"`
}
type ProxyPolicyDTO struct {
	SubjectKind             string                 `json:"subject_kind"`
	SubjectID               string                 `json:"subject_id"`
	ProxyMode               db.TaskProxyMode       `json:"proxy_mode"`
	ProxyID                 *string                `json:"proxy_id"`
	ProxyRevision           int64                  `json:"proxy_revision"`
	AllowedGroupIDs         []string               `json:"allowed_group_ids"`
	AgentFailoverEnabled    bool                   `json:"agent_failover_enabled"`
	FailoverCooldownSeconds int                    `json:"failover_cooldown_seconds"`
	FailoverMaxSwitches     int                    `json:"failover_max_switches"`
	AuthorizationRevision   int64                  `json:"authorization_revision"`
	Proxy                   *ProxyDTO              `json:"proxy"`
	ExitVerifiedAt          *string                `json:"exit_verified_at"`
	LatestSwitch            *ProxySwitchRequestDTO `json:"latest_switch"`
	CooldownUntil           *string                `json:"cooldown_until"`
	RemainingSwitches       int                    `json:"remaining_switches"`
	RecoveryRequired        bool                   `json:"recovery_required"`
}

func proxyIDString(id *int64) *string {
	if id == nil {
		return nil
	}
	v := i64s(*id)
	return &v
}
func proxyTimeString(t *time.Time) *string {
	if t == nil {
		return nil
	}
	v := rfc3339(*t)
	return &v
}
func proxySwitchDTO(r db.ProxySwitchRequest) *ProxySwitchRequestDTO {
	return &ProxySwitchRequestDTO{ID: r.ID, SubjectKind: r.Subject.Kind, SubjectID: i64s(r.Subject.ID), BaseRevision: r.BaseRevision, AuthorizationRevision: r.AuthorizationRevision, State: r.State, FromProxyID: proxyIDString(r.FromProxyID), ToProxyID: proxyIDString(r.ToProxyID), Reason: r.Reason, ErrorCode: r.ErrorCode, CreatedAt: rfc3339(r.CreatedAt), UpdatedAt: rfc3339(r.UpdatedAt), CompletedAt: proxyTimeString(r.CompletedAt), ResultRevision: r.ResultRevision}
}
func (s *Server) proxyGroupDTO(g *db.ProxyGroup) *ProxyGroupDTO {
	out := &ProxyGroupDTO{ID: i64s(g.ID), Name: g.Name, Enabled: g.Enabled, Revision: g.Revision, Members: []ProxyGroupMemberDTO{}}
	for _, m := range g.Members {
		out.Members = append(out.Members, ProxyGroupMemberDTO{ProxyID: i64s(m.ProxyID), Position: m.Position, Proxy: s.runtimeProxyDTO(m.Proxy)})
	}
	return out
}
func (s *Server) proxyPolicyDTO(p db.ProxyPolicy) (*ProxyPolicyDTO, error) {
	ids := []string{}
	for _, id := range p.AllowedGroupIDs {
		ids = append(ids, i64s(id))
	}
	out := &ProxyPolicyDTO{SubjectKind: p.Subject.Kind, SubjectID: i64s(p.Subject.ID), ProxyMode: p.State.Mode, ProxyID: proxyIDString(p.State.ID), ProxyRevision: p.State.Revision, AllowedGroupIDs: ids, AgentFailoverEnabled: p.AgentFailoverEnabled, FailoverCooldownSeconds: p.FailoverCooldownSeconds, FailoverMaxSwitches: p.FailoverMaxSwitches, AuthorizationRevision: p.AuthorizationRevision, ExitVerifiedAt: proxyTimeString(p.ExitVerifiedAt), CooldownUntil: proxyTimeString(p.CooldownUntil), RemainingSwitches: max(0, p.FailoverMaxSwitches-p.SwitchesUsed)}
	if p.State.ID != nil {
		proxy, e := s.m.pg.GetProxy(*p.State.ID)
		if e != nil {
			return nil, e
		}
		out.Proxy = s.runtimeProxyDTO(proxy)
	}
	r, e := s.m.pg.LatestProxySwitchRequest(p.Subject)
	if e != nil {
		return nil, e
	}
	if r != nil {
		out.LatestSwitch = proxySwitchDTO(*r)
	}
	out.RecoveryRequired = p.SwitchToken != "" || (r != nil && r.State == "recovery_required")
	return out, nil
}

func decodeProxyHTTP(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if e := d.Decode(v); e != nil {
		writeErr(w, 400, "代理请求格式无效")
		return false
	}
	if d.Decode(new(any)) != io.EOF {
		writeErr(w, 400, "代理请求必须只有一个 JSON 对象")
		return false
	}
	return true
}
func parseProxyIDString(v string) (int64, error) {
	id, e := strconv.ParseInt(v, 10, 64)
	if e != nil || id <= 0 || i64s(id) != v {
		return 0, db.ErrProxyInvalidMode
	}
	return id, nil
}
func parseProxyIDList(raw json.RawMessage) ([]int64, error) {
	if string(raw) == "null" {
		return nil, db.ErrProxyInvalidMode
	}
	var ids []string
	if len(raw) == 0 || json.Unmarshal(raw, &ids) != nil || ids == nil {
		return nil, db.ErrProxyInvalidMode
	}
	out := []int64{}
	seen := map[int64]bool{}
	for _, v := range ids {
		id, e := parseProxyIDString(v)
		if e != nil || seen[id] {
			return nil, db.ErrProxyInvalidMode
		}
		seen[id] = true
		out = append(out, id)
	}
	return out, nil
}
func proxySubjectRequest(r *http.Request) (db.ProxySubject, error) {
	kind := r.PathValue("subject")
	if kind == "tasks" {
		kind = "task"
	} else if kind == "conversations" {
		kind = "conversation"
	} else if strings.HasPrefix(r.URL.Path, "/api/tasks/") {
		kind = "task"
	} else if strings.HasPrefix(r.URL.Path, "/api/conversations/") {
		kind = "conversation"
	}
	id, e := parseProxyIDString(r.PathValue("id"))
	return db.ProxySubject{Kind: kind, ID: id}, e
}

func (s *Server) registerProxyGroupRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/proxy-groups", s.listProxyGroups)
	mux.HandleFunc("POST /api/proxy-groups", s.createProxyGroup)
	mux.HandleFunc("GET /api/proxy-groups/{id}", s.getProxyGroup)
	mux.HandleFunc("PUT /api/proxy-groups/{id}", s.updateProxyGroup)
	mux.HandleFunc("DELETE /api/proxy-groups/{id}", s.deleteProxyGroup)
	mux.HandleFunc("PUT /api/proxy-groups/{id}/members", s.setProxyGroupMembers)
	for _, subject := range []string{"tasks", "conversations"} {
		mux.HandleFunc("GET /api/"+subject+"/{id}/proxy", s.getSubjectProxy)
		mux.HandleFunc("POST /api/"+subject+"/{id}/proxy/switch-requests", s.requestSubjectFailover)
		mux.HandleFunc("GET /api/"+subject+"/{id}/proxy/switch-requests/{request_id}", s.getSubjectSwitchRequest)
		mux.HandleFunc("POST /api/"+subject+"/{id}/proxy/policy-recovery", s.recoverSubjectProxyPolicy)
	}
	mux.HandleFunc("PUT /api/conversations/{id}/proxy", s.setSubjectProxy)
}
func (s *Server) writeProxyPolicyError(w http.ResponseWriter, err error) {
	status, code, message := 500, "proxy_operation_failed", "代理操作失败"
	switch {
	case errors.Is(err, sql.ErrNoRows):
		status, code, message = 404, "proxy_not_found", "代理主体或分组不存在"
	case errors.Is(err, db.ErrProxyGroupRevisionConflict):
		status, code, message = 409, "proxy_group_revision_conflict", err.Error()
	case errors.Is(err, db.ErrProxyRevisionConflict):
		status, code, message = 409, "proxy_revision_conflict", err.Error()
	case errors.Is(err, db.ErrProxyInUse), errors.Is(err, db.ErrProxyGroupReferenced):
		status, code, message = 409, "proxy_busy", err.Error()
	case errors.Is(err, db.ErrProxySwitchRecovery):
		status, code, message = 409, "proxy_recovery_required", err.Error()
	case errors.Is(err, db.ErrProxyCandidatesExhausted):
		status, code, message = 409, "proxy_candidates_exhausted", err.Error()
	case errors.Is(err, db.ErrProxyAuthorization):
		status, code, message = 403, "proxy_not_authorized", err.Error()
	case errors.Is(err, db.ErrProxyBudget):
		status, code, message = 409, "proxy_budget_exhausted", err.Error()
	case errors.Is(err, db.ErrProxyCooldown):
		status, code, message = 409, "proxy_cooldown", err.Error()
	case errors.Is(err, db.ErrProxyEvidence):
		status, code, message = 400, "proxy_evidence_invalid", err.Error()
	case errors.Is(err, db.ErrProxyResinScope):
		status, code, message = 400, "proxy_resin_scope_unsupported", err.Error()
	case errors.Is(err, db.ErrProxyInvalidMode), errors.Is(err, db.ErrProxyIDRequired):
		status, code, message = 400, "proxy_policy_invalid", "代理策略参数无效"
	case errors.Is(err, db.ErrProxyUnavailable):
		status, code, message = 409, "proxy_unavailable", err.Error()
	}
	writeJSON(w, status, map[string]any{"error": message, "code": code})
}
func (s *Server) listProxyGroups(w http.ResponseWriter, r *http.Request) {
	groups, e := s.m.pg.ListProxyGroups()
	if e != nil {
		s.writeProxyPolicyError(w, e)
		return
	}
	out := []*ProxyGroupDTO{}
	for _, g := range groups {
		out = append(out, s.proxyGroupDTO(g))
	}
	writeJSON(w, 200, map[string]any{"groups": out})
}
func (s *Server) createProxyGroup(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name    string `json:"name"`
		Enabled *bool  `json:"enabled"`
	}
	if !decodeProxyHTTP(w, r, &req) {
		return
	}
	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}
	g, e := s.m.pg.CreateProxyGroup(req.Name, enabled)
	if e != nil {
		s.writeProxyPolicyError(w, e)
		return
	}
	writeJSON(w, 201, s.proxyGroupDTO(g))
}
func (s *Server) getProxyGroup(w http.ResponseWriter, r *http.Request) {
	id, e := parseProxyIDString(r.PathValue("id"))
	if e != nil {
		s.writeProxyPolicyError(w, e)
		return
	}
	g, e := s.m.pg.GetProxyGroup(id)
	if e != nil {
		s.writeProxyPolicyError(w, e)
		return
	}
	writeJSON(w, 200, s.proxyGroupDTO(g))
}
func (s *Server) updateProxyGroup(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name     string          `json:"name"`
		Enabled  *bool           `json:"enabled"`
		Revision json.RawMessage `json:"expected_revision"`
	}
	if !decodeProxyHTTP(w, r, &req) {
		return
	}
	id, e := parseProxyIDString(r.PathValue("id"))
	if e != nil {
		s.writeProxyPolicyError(w, e)
		return
	}
	revision, e := parseProxyRevision(req.Revision)
	if e != nil || req.Enabled == nil {
		s.writeProxyPolicyError(w, db.ErrProxyInvalidMode)
		return
	}
	g, e := s.m.pg.UpdateProxyGroup(id, revision, req.Name, *req.Enabled)
	if e != nil {
		s.writeProxyPolicyError(w, e)
		return
	}
	writeJSON(w, 200, s.proxyGroupDTO(g))
}
func (s *Server) deleteProxyGroup(w http.ResponseWriter, r *http.Request) {
	id, e := parseProxyIDString(r.PathValue("id"))
	if e != nil {
		s.writeProxyPolicyError(w, e)
		return
	}
	revision, e := parseProxyRevision(json.RawMessage(r.URL.Query().Get("expected_revision")))
	if e != nil {
		s.writeProxyPolicyError(w, e)
		return
	}
	if e = s.m.pg.DeleteProxyGroup(id, revision); e != nil {
		s.writeProxyPolicyError(w, e)
		return
	}
	w.WriteHeader(204)
}
func (s *Server) setProxyGroupMembers(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Revision json.RawMessage `json:"expected_revision"`
		IDs      json.RawMessage `json:"proxy_ids"`
	}
	if !decodeProxyHTTP(w, r, &req) {
		return
	}
	id, e := parseProxyIDString(r.PathValue("id"))
	if e != nil {
		s.writeProxyPolicyError(w, e)
		return
	}
	revision, e := parseProxyRevision(req.Revision)
	if e != nil {
		s.writeProxyPolicyError(w, e)
		return
	}
	ids, e := parseProxyIDList(req.IDs)
	if e != nil {
		s.writeProxyPolicyError(w, e)
		return
	}
	g, e := s.m.pg.SetProxyGroupMembers(id, revision, ids)
	if e != nil {
		s.writeProxyPolicyError(w, e)
		return
	}
	writeJSON(w, 200, s.proxyGroupDTO(g))
}
func (s *Server) getSubjectProxy(w http.ResponseWriter, r *http.Request) {
	subject, e := proxySubjectRequest(r)
	if e != nil {
		s.writeProxyPolicyError(w, e)
		return
	}
	p, e := s.m.pg.GetProxyPolicy(subject)
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

type proxyPolicyHTTPInput struct {
	Mode     db.TaskProxyMode `json:"proxy_mode"`
	ID       json.RawMessage  `json:"proxy_id"`
	Revision json.RawMessage  `json:"expected_revision"`
	Groups   json.RawMessage  `json:"allowed_group_ids"`
	Enabled  json.RawMessage  `json:"agent_failover_enabled"`
	Cooldown json.RawMessage  `json:"failover_cooldown_seconds"`
	Budget   json.RawMessage  `json:"failover_max_switches"`
}

func (req proxyPolicyHTTPInput) patch() (int64, db.ProxyPolicyPatch, error) {
	p := db.ProxyPolicyPatch{Mode: req.Mode}
	revision, e := parseProxyRevision(req.Revision)
	if e != nil {
		return 0, p, e
	}
	if len(req.ID) > 0 && string(req.ID) != "null" {
		var text string
		if json.Unmarshal(req.ID, &text) != nil {
			var id int64
			if json.Unmarshal(req.ID, &id) != nil || id <= 0 {
				return 0, p, db.ErrProxyInvalidMode
			}
			text = i64s(id)
		}
		id, e := parseProxyIDString(text)
		if e != nil {
			return 0, p, e
		}
		p.ProxyID = &id
	}
	if len(req.Groups) > 0 {
		ids, e := parseProxyIDList(req.Groups)
		if e != nil {
			return 0, p, e
		}
		p.AllowedGroupIDs = &ids
	}
	if len(req.Enabled) > 0 {
		var v bool
		if string(req.Enabled) == "null" || json.Unmarshal(req.Enabled, &v) != nil {
			return 0, p, db.ErrProxyInvalidMode
		}
		p.AgentFailoverEnabled = &v
	}
	for _, pair := range []struct {
		raw json.RawMessage
		dst **int
	}{{req.Cooldown, &p.FailoverCooldownSeconds}, {req.Budget, &p.FailoverMaxSwitches}} {
		if len(pair.raw) > 0 {
			var v int
			if string(pair.raw) == "null" || json.Unmarshal(pair.raw, &v) != nil {
				return 0, p, db.ErrProxyInvalidMode
			}
			*pair.dst = &v
		}
	}
	return revision, p, nil
}
func (s *Server) setSubjectProxy(w http.ResponseWriter, r *http.Request) {
	var req proxyPolicyHTTPInput
	if !decodeProxyHTTP(w, r, &req) {
		return
	}
	subject, e := proxySubjectRequest(r)
	if e != nil {
		s.writeProxyPolicyError(w, e)
		return
	}
	revision, patch, e := req.patch()
	if e != nil {
		s.writeProxyPolicyError(w, e)
		return
	}
	p, e := s.m.setProxyPolicy(r.Context(), subject, revision, patch)
	if e != nil {
		if subject.Kind == "task" && (errors.Is(e, db.ErrProxyRevisionConflict) || errors.Is(e, db.ErrProxyInUse) || errors.Is(e, db.ErrProxySwitchRecovery)) {
			s.writeProxySwitchError(w, i64s(subject.ID), e)
			return
		}
		if errors.Is(e, db.ErrProxyRevisionConflict) {
			current, readErr := s.m.pg.GetProxyPolicy(subject)
			if readErr == nil {
				writeJSON(w, 409, map[string]any{"error": e.Error(), "code": "proxy_revision_conflict", "current_revision": current.State.Revision})
				return
			}
		}
		s.writeProxyPolicyError(w, e)
		return
	}
	out, e := s.proxyPolicyDTO(p)
	if e != nil {
		s.writeProxyPolicyError(w, e)
		return
	}
	if subject.Kind == "task" {
		t, ok := s.m.Task(i64s(subject.ID))
		if !ok {
			row, e := s.m.pg.GetTask(subject.ID)
			if e != nil {
				s.writeProxyPolicyError(w, e)
				return
			}
			t = &Task{ID: i64s(subject.ID), Description: row.Description}
			t.setProxyState(p.State)
		}
		dto := taskDTO(t, t.lifecycleSnapshot().Status, p.State)
		raw, _ := json.Marshal(dto)
		var data map[string]any
		_ = json.Unmarshal(raw, &data)
		policyRaw, _ := json.Marshal(out)
		var fields map[string]any
		_ = json.Unmarshal(policyRaw, &fields)
		for key, value := range fields {
			if key == "proxy_id" || key == "proxy_mode" || key == "proxy_revision" {
				continue
			}
			data[key] = value
		}
		writeJSON(w, 200, data)
		return
	}
	writeJSON(w, 200, out)
}

type proxyFailoverHTTPInput struct {
	Revision json.RawMessage         `json:"expected_revision"`
	Reason   string                  `json:"reason"`
	Evidence db.ProxyFailureEvidence `json:"evidence"`
	Target   *string                 `json:"target_proxy_id"`
}

func (req proxyFailoverHTTPInput) input() (db.ProxyFailoverInput, error) {
	out := db.ProxyFailoverInput{Reason: req.Reason, Evidence: req.Evidence}
	var e error
	out.ExpectedRevision, e = parseProxyRevision(req.Revision)
	if e != nil {
		return out, e
	}
	if req.Target != nil {
		id, e := parseProxyIDString(*req.Target)
		if e != nil {
			return out, e
		}
		out.TargetProxyID = &id
	}
	return out, nil
}
func (s *Server) requestSubjectFailover(w http.ResponseWriter, r *http.Request) {
	var req proxyFailoverHTTPInput
	if !decodeProxyHTTP(w, r, &req) {
		return
	}
	subject, e := proxySubjectRequest(r)
	if e != nil {
		s.writeProxyPolicyError(w, e)
		return
	}
	input, e := req.input()
	if e != nil {
		s.writeProxyPolicyError(w, e)
		return
	}
	request, e := s.m.pg.RequestProxyFailover(subject, input)
	if e != nil {
		s.writeProxyPolicyError(w, e)
		return
	}
	s.m.wakeProxyFailover()
	writeJSON(w, 202, proxySwitchDTO(request))
}
func (s *Server) getSubjectSwitchRequest(w http.ResponseWriter, r *http.Request) {
	subject, e := proxySubjectRequest(r)
	if e != nil {
		s.writeProxyPolicyError(w, e)
		return
	}
	request, e := s.m.pg.GetProxySwitchRequest(subject, r.PathValue("request_id"))
	if e != nil {
		s.writeProxyPolicyError(w, e)
		return
	}
	writeJSON(w, 200, proxySwitchDTO(request))
}
