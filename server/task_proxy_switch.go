package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"reflect"
	"strconv"

	"github.com/Autumn-27/artex/db"
)

const maxProxyRevision int64 = 9007199254740991

// 新准入已持久占用后再刷新；关闭本 Manager 的旧路由，不重复回收其他 Manager 已处理的租约。
func (m *Manager) refreshTaskProxyState(t *Task, state db.TaskProxyState) error {
	if t.proxyState().Revision == state.Revision {
		return nil
	}
	rt, err := m.proxyRuntime()
	if err != nil {
		old := t.proxyState()
		if old.Snapshot.ID == 0 && state.Snapshot.ID == 0 {
			t.setProxyState(state)
			return nil
		}
		return err
	}
	lock := rt.taskLock(t.ID)
	lock.Lock()
	defer lock.Unlock()
	if t.proxyState().Revision == state.Revision {
		return nil
	}
	rt.mu.Lock()
	route, bridge := rt.routes[t.ID], rt.bridges[t.ID]
	delete(rt.routes, t.ID)
	delete(rt.bridges, t.ID)
	rt.mu.Unlock()
	if route != nil {
		_ = route.Close()
	}
	if bridge != nil {
		_ = bridge.Close()
	}
	t.setProxyState(state)
	return nil
}

func (m *Manager) confirmTaskLeaseReleased(t *Task) error {
	rt, err := m.proxyRuntime()
	if err != nil {
		return err
	}
	lock := rt.taskLock(t.ID)
	lock.Lock()
	defer lock.Unlock()
	path, err := leasePath(rt, t.ID)
	if err != nil {
		return err
	}
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var record resinLeaseRecord
	if json.Unmarshal(b, &record) != nil || !reflect.DeepEqual(record.Snapshot, t.proxyState().Snapshot) {
		return db.ErrProxySwitchRecovery
	}
	if record.State != "uncertain" && record.State != "releasing" && record.State != "released" {
		return db.ErrProxySwitchRecovery
	}
	record.State = "released"
	return writeLease(path, record)
}

func (t *Task) proxyState() db.TaskProxyState {
	if t == nil {
		return db.TaskProxyState{}
	}
	t.proxyMu.RLock()
	defer t.proxyMu.RUnlock()
	return db.TaskProxyState{Mode: t.ProxyMode, ID: cloneInt64Ptr(t.ProxyID), Snapshot: t.ProxySnapshot, Revision: t.ProxyRevision}
}

func (t *Task) setProxyState(s db.TaskProxyState) {
	t.proxyMu.Lock()
	defer t.proxyMu.Unlock()
	t.ProxyMode, t.ProxyID, t.ProxySnapshot, t.ProxyRevision = s.Mode, cloneInt64Ptr(s.ID), s.Snapshot, s.Revision
}

func (s *Server) registerTaskProxyRoutes(mux *http.ServeMux) {
	mux.HandleFunc("PUT /api/tasks/{id}/proxy", s.setSubjectProxy)
	mux.HandleFunc("POST /api/tasks/{id}/proxy/recovery", s.recoverTaskProxy)
}

type taskProxySwitchRequest struct {
	Mode     db.TaskProxyMode `json:"proxy_mode"`
	ID       *proxyIDInput    `json:"proxy_id"`
	Revision json.RawMessage  `json:"expected_revision"`
	Action   string           `json:"action"`
}

func parseProxyRevision(raw json.RawMessage) (int64, error) {
	var revision int64
	if len(raw) == 0 || string(raw) == "null" {
		return 0, db.ErrProxyInvalidMode
	}
	if err := json.Unmarshal(raw, &revision); err != nil || revision < 0 || revision > maxProxyRevision {
		return 0, db.ErrProxyInvalidMode
	}
	return revision, nil
}

func (s *Server) switchTaskProxy(w http.ResponseWriter, r *http.Request) {
	var req taskProxySwitchRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&req); err != nil {
		writeJSON(w, 400, map[string]any{"error": "任务代理参数无效"})
		return
	}
	revision, err := parseProxyRevision(req.Revision)
	if err != nil || (req.Mode != "inherit" && req.Mode != "direct" && req.Mode != "fixed") || (req.Mode == "fixed" && req.ID == nil) || (req.Mode != "fixed" && req.ID != nil) {
		writeJSON(w, 400, map[string]any{"error": "需要有效的代理模式和非负安全整数 expected_revision"})
		return
	}
	row, err := s.m.switchTaskProxy(r.Context(), r.PathValue("id"), revision, req.Mode, req.ID.Int64())
	if err != nil {
		s.writeProxySwitchError(w, r.PathValue("id"), err)
		return
	}
	s.writeSwitchedTask(w, row)
}

func (s *Server) writeSwitchedTask(w http.ResponseWriter, row *db.Task) {
	t, ok := s.m.Task(strconv.FormatInt(row.ID, 10))
	if !ok {
		t = taskFromPG(row, s.m.pg.Exploration(row.ExplorationID), s.m.interceptor)
	}
	writeJSON(w, 200, taskDTO(t, row.Status, db.TaskProxyState{Mode: row.ProxyMode, ID: row.ProxyID, Snapshot: row.ProxySnapshot, Revision: row.ProxyRevision}))
}

func (s *Server) writeProxySwitchError(w http.ResponseWriter, id string, err error) {
	if errors.Is(err, sql.ErrNoRows) {
		writeJSON(w, 404, map[string]any{"error": "任务不存在"})
		return
	}
	var revision int64
	if n, e := strconv.ParseInt(id, 10, 64); e == nil {
		if row, e := s.m.pg.GetTask(n); e == nil && row != nil {
			revision = row.ProxyRevision
		}
	}
	if errors.Is(err, db.ErrProxyRevisionConflict) {
		writeJSON(w, 409, map[string]any{"error": "任务代理版本已变化，请刷新任务", "code": "proxy_revision_conflict", "current_revision": revision})
		return
	}
	if errors.Is(err, db.ErrProxyInUse) || errors.Is(err, db.ErrProxySwitchRecovery) {
		writeJSON(w, 409, map[string]any{"error": "任务出口正忙或切换需要人工恢复", "code": "proxy_busy", "current_revision": revision})
		return
	}
	if errors.Is(err, db.ErrProxyUnavailable) || errors.Is(err, db.ErrProxyInvalidMode) || errors.Is(err, db.ErrProxyIDRequired) {
		writeJSON(w, 400, map[string]any{"error": "目标代理不可用或参数无效"})
		return
	}
	writeJSON(w, 503, map[string]any{"error": "代理切换未完成，请刷新并核对恢复状态", "code": "proxy_busy", "current_revision": revision})
}

func (m *Manager) switchTaskProxy(ctx context.Context, id string, expected int64, mode db.TaskProxyMode, selected *int64) (*db.Task, error) {
	n, err := strconv.ParseInt(id, 10, 64)
	if err != nil || n <= 0 {
		return nil, sql.ErrNoRows
	}
	b := m.taskProxyBarrier(id)
	b.mu.Lock()
	if b.releasing || (m.proxyEngine != nil && m.proxyEngine.inflightCount(id) > 0) {
		b.mu.Unlock()
		return nil, db.ErrProxyInUse
	}
	b.releasing = true
	b.mu.Unlock()
	defer func() { b.mu.Lock(); b.releasing = false; b.signal(); b.mu.Unlock() }()
	executor, err := m.pg.AcquireTaskProxySwitchExecutor(ctx, n)
	if err != nil {
		return nil, err
	}
	old, token, err := executor.Begin(expected, mode, selected)
	if err != nil {
		executor.Close()
		return nil, err
	}
	t := &Task{ID: id}
	t.setProxyState(old)
	if err = m.ReleaseTaskProxy(ctx, t); err != nil {
		executor.Close()
		return nil, db.ErrProxySwitchRecovery
	}
	state, err := executor.Complete(expected, token)
	executor.Close()
	if err != nil {
		return nil, db.ErrProxySwitchRecovery
	}
	if current, ok := m.Task(id); ok {
		current.setProxyState(state)
	}
	return m.pg.GetTask(n)
}

// 不重采样 inherit，不重复不明确的远端 DELETE。人工确认仅在已核对远端后使用。
func (s *Server) recoverTaskProxy(w http.ResponseWriter, r *http.Request) {
	var req taskProxySwitchRequest
	if json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&req) != nil {
		writeJSON(w, 400, map[string]any{"error": "恢复参数无效"})
		return
	}
	revision, err := parseProxyRevision(req.Revision)
	if err != nil || (req.Action != "retry" && req.Action != "confirm_released" && req.Action != "clear_stale_activity" && req.Action != "abort_direct" && req.Action != "confirm_released_direct") {
		writeJSON(w, 400, map[string]any{"error": "恢复需要 expected_revision 和有效恢复操作"})
		return
	}
	row, err := s.m.recoverTaskProxy(r.Context(), r.PathValue("id"), revision, req.Action)
	if err != nil {
		s.writeProxySwitchError(w, r.PathValue("id"), err)
		return
	}
	s.writeSwitchedTask(w, row)
}

func (m *Manager) recoverTaskProxy(ctx context.Context, id string, expected int64, action string) (*db.Task, error) {
	n, err := strconv.ParseInt(id, 10, 64)
	if err != nil || n <= 0 {
		return nil, sql.ErrNoRows
	}
	policy, err := m.pg.GetProxyPolicy(db.ProxySubject{Kind: "task", ID: n})
	if err != nil {
		return nil, err
	}
	if policy.State.Mode == db.TaskProxyModeGroup || policy.SwitchToken != "" {
		return nil, db.ErrProxySwitchRecovery
	}
	b := m.taskProxyBarrier(id)
	b.mu.Lock()
	if b.releasing || (m.proxyEngine != nil && m.proxyEngine.inflightCount(id) > 0) {
		b.mu.Unlock()
		return nil, db.ErrProxyInUse
	}
	b.releasing = true
	b.mu.Unlock()
	defer func() { b.mu.Lock(); b.releasing = false; b.signal(); b.mu.Unlock() }()
	executor, err := m.pg.AcquireTaskProxySwitchExecutor(ctx, n)
	if err != nil {
		return nil, err
	}
	old, token, err := executor.Pending()
	if errors.Is(err, sql.ErrNoRows) && (action == "clear_stale_activity" || action == "confirm_released") {
		row, getErr := m.pg.GetTask(n)
		if getErr != nil || row == nil {
			executor.Close()
			if getErr == nil {
				getErr = sql.ErrNoRows
			}
			return nil, getErr
		}
		old = db.TaskProxyState{Mode: row.ProxyMode, ID: row.ProxyID, Snapshot: row.ProxySnapshot, Revision: row.ProxyRevision}
		if old.Revision != expected {
			executor.Close()
			return nil, db.ErrProxyRevisionConflict
		}
		t := &Task{ID: id}
		t.setProxyState(old)
		if action == "confirm_released" {
			if err = m.confirmTaskLeaseReleased(t); err != nil {
				executor.Close()
				return nil, db.ErrProxySwitchRecovery
			}
		}
		if err = m.ReleaseTaskProxy(ctx, t); err == nil {
			err = executor.ClearStaleTaskOperations()
		}
		executor.Close()
		if err != nil {
			return nil, db.ErrProxySwitchRecovery
		}
		return m.pg.GetTask(n)
	}
	if err != nil {
		executor.Close()
		return nil, err
	}
	if old.Revision != expected {
		executor.Close()
		return nil, db.ErrProxyRevisionConflict
	}
	t := &Task{ID: id}
	t.setProxyState(old)
	if action == "confirm_released" || action == "confirm_released_direct" {
		if err = m.confirmTaskLeaseReleased(t); err != nil {
			executor.Close()
			return nil, db.ErrProxySwitchRecovery
		}
	}
	if err = m.ReleaseTaskProxy(ctx, t); err != nil {
		executor.Close()
		return nil, db.ErrProxySwitchRecovery
	}
	if action == "abort_direct" || action == "confirm_released_direct" {
		state, err := executor.AbortToDirect(expected, token)
		executor.Close()
		if err != nil {
			return nil, db.ErrProxySwitchRecovery
		}
		if current, ok := m.Task(id); ok {
			current.setProxyState(state)
		}
		return m.pg.GetTask(n)
	}
	state, err := executor.Complete(expected, token)
	executor.Close()
	if err != nil {
		return nil, db.ErrProxySwitchRecovery
	}
	if current, ok := m.Task(id); ok {
		current.setProxyState(state)
	}
	return m.pg.GetTask(n)
}
