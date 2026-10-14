package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Autumn-27/artex/db"
	"github.com/Autumn-27/artex/egress"
	"github.com/Autumn-27/artex/traffic"
)

// ResolveTaskProxy applies the task selection contract before any network client
// is created. A fixed or inherited unhealthy proxy fails closed; direct is the
// only explicit way to bypass the proxy center.
func ResolveTaskProxy(mode db.TaskProxyMode, fixed, global *db.Proxy) (*db.Proxy, error) {
	normalized, err := db.NormalizeTaskProxyMode(mode)
	if err != nil {
		return nil, err
	}
	switch normalized {
	case db.TaskProxyModeDirect:
		return nil, nil
	case db.TaskProxyModeFixed, db.TaskProxyModeGroup:
		return usableTaskProxy(fixed)
	case db.TaskProxyModeInherit:
		if global == nil {
			return nil, nil
		}
		return usableTaskProxy(global)
	default:
		return nil, fmt.Errorf("unsupported proxy mode %q", normalized)
	}
}

func usableTaskProxy(p *db.Proxy) (*db.Proxy, error) {
	if p == nil || !p.Enabled || p.Status == db.ProxyStatusFailed || p.Status == db.ProxyStatusStopped {
		return nil, db.ErrProxyUnavailable
	}
	if p.URL == "" && (p.Kind != db.ProxyKindResin || p.Host == "" || p.Port == 0) {
		return nil, db.ErrProxyUnavailable
	}
	return p, nil
}

type proxyRuntimeState struct {
	mu      sync.Mutex
	tasks   sync.Map // taskID -> *sync.Mutex；仅同一任务的租约操作串行。
	closed  bool
	warp    *egress.WarpManager
	dir     string
	routes  map[string]*traffic.TaskRoute
	bridges map[string]*egress.HTTPBridge
	health  *WarpHealthController
}

func (rt *proxyRuntimeState) taskLock(id string) *sync.Mutex {
	v, _ := rt.tasks.LoadOrStore(id, &sync.Mutex{})
	return v.(*sync.Mutex)
}

var proxyRuntimes sync.Map // *Manager -> *proxyRuntimeState

func (m *Manager) proxyRuntime() (*proxyRuntimeState, error) {
	if v, ok := proxyRuntimes.Load(m); ok {
		return v.(*proxyRuntimeState), nil
	}
	return nil, errors.New("代理运行时未初始化")
}

// InitProxyRuntime 必须在服务接受任务前完成；迁移、恢复失败均阻止启动。
func (m *Manager) InitProxyRuntime(ctx context.Context) error {
	if _, ok := proxyRuntimes.Load(m); ok {
		return nil
	}
	if err := db.InitializeProxySecrets(); err != nil {
		return err
	}
	if err := m.pg.EnsureProxyActivitySchema(); err != nil {
		return err
	}
	if err := m.pg.MigrateTaskProxySwitch(); err != nil {
		return err
	}
	if err := m.pg.MigrateWarpHealth(ctx); err != nil {
		return err
	}
	if err := m.pg.MigrateLegacyProxy(); err != nil {
		return err
	}
	if _, err := m.pg.ListProxies(); err != nil {
		return errors.New("代理凭据无法解密，拒绝启动")
	}
	dir := os.Getenv("ARTEX_WARP_DIR")
	if dir == "" {
		dir = filepath.Join(m.dir, "egress", "warp")
	}
	warp, err := egress.NewWarpManager(dir)
	if err != nil {
		return err
	}
	controller, err := NewWarpHealthController(m.pg, warp, db.DefaultWarpHealthPolicy())
	if err != nil {
		warp.Close()
		return err
	}
	ids, err := m.pg.WarpHealthProxyIDs(ctx)
	if err != nil {
		warp.Close()
		return err
	}
	for _, id := range ids {
		if err = controller.ImportProfile(ctx, id); err != nil {
			warp.Close()
			return err
		}
	}
	controller.SetRepairGuard(m.warpHealthRepairGuard)
	if err = controller.Start(ctx); err != nil {
		warp.Close()
		return err
	}
	proxyRuntimes.Store(m, &proxyRuntimeState{warp: warp, health: controller, dir: filepath.Join(m.dir, "egress", "leases"), routes: map[string]*traffic.TaskRoute{}, bridges: map[string]*egress.HTTPBridge{}})
	m.startProxyFailover(ctx)
	return nil
}

func (m *Manager) CloseProxyRuntime() error {
	m.closeProxyFailover()
	v, ok := proxyRuntimes.Load(m)
	if !ok {
		return nil
	}
	rt := v.(*proxyRuntimeState)
	var healthErr error
	if rt.health != nil {
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		healthErr = rt.health.Close(closeCtx)
		cancel()
	}
	if healthErr != nil {
		return healthErr
	}
	ids, err := m.pg.WarpHealthProxyIDs(context.Background())
	if err != nil {
		return err
	}
	for _, id := range ids {
		if err = m.pg.WithWarpHealthLifecycle(context.Background(), func(tx *sql.Tx) error {
			return db.WarpHealthIdleTx(context.Background(), tx, id)
		}); err != nil {
			return err
		}
	}
	proxyRuntimes.Delete(m)
	rt.mu.Lock()
	rt.closed = true
	routes, bridges := rt.routes, rt.bridges
	rt.routes = map[string]*traffic.TaskRoute{}
	rt.bridges = map[string]*egress.HTTPBridge{}
	rt.mu.Unlock()
	for _, route := range routes {
		_ = route.Close()
	}
	for _, bridge := range bridges {
		_ = bridge.Close()
	}
	return errors.Join(healthErr, rt.warp.Close())
}

func (m *Manager) warpHealthRepairGuard(ctx context.Context, tx *sql.Tx, id int64) error {
	if err := db.WarpHealthIdleTx(ctx, tx, id); err != nil {
		return err
	}
	var busy bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM proxy_subject_activity WHERE proxy_id=$1)
OR EXISTS(SELECT 1 FROM proxy_subject_policies WHERE reserved_proxy_id=$1 AND switch_token<>'')
OR EXISTS(SELECT 1 FROM proxy_switch_requests WHERE state IN ('pending','draining','applying','recovery_required') AND (from_proxy_id=$1 OR to_proxy_id=$1))`, id).Scan(&busy); err != nil {
		return err
	}
	if busy {
		return db.ErrProxyInUse
	}
	return nil
}

func snapshotProxyURL(s db.ProxySnapshot, taskID string) string {
	if s.Kind == db.ProxyKindResin {
		return resinTaskProxyURL(&db.Proxy{Kind: s.Kind, Host: s.Host, Port: s.Port, Protocol: s.Protocol, Password: s.Password, ResinPlatform: s.Platform}, taskID)
	}
	return s.URL
}

func (m *Manager) runtimeSnapshot(t *Task) (db.ProxySnapshot, error) {
	state := t.proxyState()
	if t != nil {
		if _, err := db.NormalizeTaskProxyMode(state.Mode); err != nil {
			return db.ProxySnapshot{}, err
		}
	}
	if t == nil || state.Mode == db.TaskProxyModeDirect {
		return db.ProxySnapshot{}, nil
	}
	if state.Snapshot.ID == 0 {
		if state.Mode == db.TaskProxyModeFixed || state.Mode == db.TaskProxyModeGroup || state.ID != nil {
			return db.ProxySnapshot{}, db.ErrProxyUnavailable
		}
		return db.ProxySnapshot{}, nil
	}
	p, err := m.pg.GetProxy(state.Snapshot.ID)
	if err != nil {
		return db.ProxySnapshot{}, db.ErrProxyUnavailable
	}
	if _, err = usableTaskProxy(p); err != nil {
		return db.ProxySnapshot{}, err
	}
	s, err := m.pg.ResolveProxySnapshot(state.Snapshot)
	if err != nil {
		return db.ProxySnapshot{}, errors.New("任务代理快照解密失败")
	}
	if _, err := egress.ProxyURL(snapshotProxyURL(s, t.ID)); err != nil {
		return db.ProxySnapshot{}, err
	}
	if s.Kind == db.ProxyKindWarp {
		rt, err := m.proxyRuntime()
		if err != nil {
			return db.ProxySnapshot{}, err
		}
		if _, err = rt.warp.Connection(warpProfileID(s.ID)); err != nil {
			return db.ProxySnapshot{}, db.ErrProxyUnavailable
		}
	}
	return s, nil
}

// TaskToolProxy 返回工具专属出口，捕获开启时使用不可变上游的任务 MITM。
func (m *Manager) TaskToolProxy(ctx context.Context, t *Task) (addr, ca string, err error) {
	rt, err := m.proxyRuntime()
	if err != nil {
		return "", "", err
	}
	if t != nil {
		lock := rt.taskLock(t.ID)
		lock.Lock()
		defer lock.Unlock()
	}
	s, err := m.runtimeSnapshot(t)
	if err != nil {
		return "", "", err
	}
	if s.ID != 0 {
		addr = snapshotProxyURL(s, t.ID)
	}
	rt.mu.Lock()
	closed := rt.closed
	rt.mu.Unlock()
	if closed {
		return "", "", errors.New("代理运行时已关闭")
	}
	if s.Kind == db.ProxyKindResin {
		if err = m.prepareResinLease(ctx, rt, t, s); err != nil {
			return "", "", err
		}
	}
	if m.traffic != nil && m.TrafficEnabled() && t != nil {
		rt.mu.Lock()
		route := rt.routes[t.ID]
		rt.mu.Unlock()
		if route == nil {
			route, err = m.traffic.NewTaskRoute(ctx, addr)
			if err != nil {
				return "", "", err
			}
			rt.mu.Lock()
			closed = rt.closed
			if !closed {
				rt.routes[t.ID] = route
			}
			rt.mu.Unlock()
			if closed {
				_ = route.Close()
				return "", "", errors.New("代理运行时已关闭")
			}
		}
		return route.ProxyAddr(), m.traffic.CACertPath(), nil
	}
	// Chromium 不支持 SOCKS 账号认证；工具统一使用任务独立认证桥，仍固定原出口。
	if t != nil && addr != "" {
		u, parseErr := egress.ProxyURL(addr)
		if parseErr != nil {
			return "", "", parseErr
		}
		if u.Scheme == "socks5" || u.Scheme == "https" {
			rt.mu.Lock()
			bridge := rt.bridges[t.ID]
			if bridge == nil {
				bridge, err = egress.NewHTTPBridge(addr)
				if err == nil {
					rt.bridges[t.ID] = bridge
				}
			}
			rt.mu.Unlock()
			if err != nil {
				return "", "", err
			}
			addr = bridge.ProxyAddr()
		}
	}
	return addr, "", nil
}

// ResolveLLMProxySelection 只在可信运行边界解析渠道中心引用，不把凭据写回渠道。
func (m *Manager) ResolveLLMProxySelection(ctx context.Context, t *Task, legacyURL, mode string, proxyID *int64) (string, error) {
	if t != nil {
		taskMode, err := db.NormalizeTaskProxyMode(t.proxyState().Mode)
		if err != nil {
			return "", err
		}
		if taskMode == db.TaskProxyModeDirect || taskMode == db.TaskProxyModeFixed || taskMode == db.TaskProxyModeGroup {
			return m.ResolveLLMProxy(ctx, t, "")
		}
	}
	if mode != "fixed" && proxyID != nil {
		return "", db.ErrProxyInvalidMode
	}
	switch mode {
	case "":
		return m.ResolveLLMProxy(ctx, t, legacyURL)
	case "inherit":
		return m.ResolveLLMProxy(ctx, t, "")
	case "direct":
		return "", nil
	case "fixed":
		if proxyID == nil || *proxyID <= 0 {
			return "", db.ErrProxyIDRequired
		}
		p, err := m.pg.GetProxy(*proxyID)
		if err != nil {
			return "", db.ErrProxyUnavailable
		}
		if _, err := usableTaskProxy(p); err != nil {
			return "", err
		}
		if p.Kind == db.ProxyKindResin {
			// 渠道出口不能复用工具的单槽租约；独立回收合同完成前明确拒绝。
			return "", errors.New("Resin 渠道引用需要独立租约生命周期，当前不支持")
		}
		snapshot, err := p.SnapshotEncrypted()
		if err != nil {
			return "", err
		}
		id := "llm"
		if t != nil {
			id = t.ID
		}
		selected := &Task{ID: id, ProxyMode: db.TaskProxyModeFixed, ProxyID: proxyID, ProxySnapshot: snapshot}
		return m.ResolveLLMProxy(ctx, selected, "")
	default:
		return "", db.ErrProxyInvalidMode
	}
}

// ResolveLLMProxy 固定/直连覆盖渠道；继承使用创建快照，非任务继承当前全局。
func (m *Manager) ResolveLLMProxy(ctx context.Context, t *Task, channelProxy string) (string, error) {
	if t != nil {
		if _, err := db.NormalizeTaskProxyMode(t.proxyState().Mode); err != nil {
			return "", err
		}
	}
	if t != nil && t.proxyState().Mode == db.TaskProxyModeDirect {
		return "", nil
	}
	if (t == nil || (t.proxyState().Mode != db.TaskProxyModeFixed && t.proxyState().Mode != db.TaskProxyModeGroup)) && strings.TrimSpace(channelProxy) != "" {
		if _, err := egress.ProxyURL(channelProxy); err != nil {
			return "", err
		}
		return channelProxy, nil
	}
	if t == nil {
		id, err := m.pg.GlobalProxyID()
		if err != nil {
			return "", err
		}
		if id == nil {
			return "", nil
		}
		p, err := m.pg.GetProxy(*id)
		if err != nil {
			return "", db.ErrProxyUnavailable
		}
		if _, err = usableTaskProxy(p); err != nil {
			return "", err
		}
		s, err := p.SnapshotEncrypted()
		if err != nil {
			return "", err
		}
		t = &Task{ID: "llm", ProxyMode: db.TaskProxyModeInherit, ProxySnapshot: s}
	}
	s, err := m.runtimeSnapshot(t)
	if err != nil {
		return "", err
	}
	if s.ID == 0 {
		return "", nil
	}
	if s.Kind == db.ProxyKindResin {
		rt, err := m.proxyRuntime()
		if err != nil {
			return "", err
		}
		lock := rt.taskLock(t.ID)
		lock.Lock()
		defer lock.Unlock()
		if err = m.prepareResinLease(ctx, rt, t, s); err != nil {
			return "", err
		}
	}
	return snapshotProxyURL(s, t.ID), nil
}

type resinLeaseRecord struct {
	State    string           `json:"state"`
	Snapshot db.ProxySnapshot `json:"snapshot"`
	AdminURL string           `json:"admin_url"`
}

func leasePath(rt *proxyRuntimeState, id string) (string, error) {
	if id != "llm" {
		n, err := strconv.ParseInt(id, 10, 64)
		if err != nil || n <= 0 {
			return "", errors.New("任务编号无效")
		}
	}
	return filepath.Join(rt.dir, "task_"+id+".json"), nil
}
func writeLease(path string, record resinLeaseRecord) error {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	b, err := json.Marshal(record)
	if err != nil {
		return err
	}
	if err = os.WriteFile(path+".tmp", b, 0600); err != nil {
		return err
	}
	return os.Rename(path+".tmp", path)
}
func (m *Manager) prepareResinLease(ctx context.Context, rt *proxyRuntimeState, t *Task, s db.ProxySnapshot) error {
	path, err := leasePath(rt, t.ID)
	if err != nil {
		return err
	}
	b, err := os.ReadFile(path)
	if err == nil {
		var record resinLeaseRecord
		if json.Unmarshal(b, &record) != nil {
			return errors.New("Resin 租约状态不明确，拒绝自动重新使用")
		}
		if record.State == "active" {
			if record.Snapshot.ID != s.ID || !reflect.DeepEqual(record.Snapshot, t.proxyState().Snapshot) {
				return errors.New("旧 Resin 租约尚未回收，拒绝替换身份")
			}
			return nil
		}
		if record.State != "released" {
			return errors.New("Resin 租约状态不明确，拒绝自动重新使用")
		}
		// 终态 MainAgent 也可以正式准入；只认仍存活的操作 context 证明。
		if !m.TaskProxyOperationAdmitted(ctx, t.ID) {
			return errors.New("Resin 租约重获需要有效的任务操作准入")
		}
		err = os.ErrNotExist
	}
	if !os.IsNotExist(err) {
		return err
	}
	p, err := m.pg.GetProxy(s.ID)
	if err != nil {
		return db.ErrProxyUnavailable
	}
	if err = resinConfig(p).Probe(ctx); err != nil {
		return err
	}
	return writeLease(path, resinLeaseRecord{State: "active", Snapshot: t.proxyState().Snapshot, AdminURL: p.ResinAdminURL})
}

// ReleaseTaskProxy 持久化回收意图；不明确结果保留材料，禁止自动重复回收。
func (m *Manager) ReleaseTaskProxy(ctx context.Context, t *Task) error {
	if t == nil {
		return nil
	}
	rt, err := m.proxyRuntime()
	if err != nil {
		if t.proxyState().Snapshot.Kind != db.ProxyKindResin {
			// 运行时已整体关闭时，普通出口没有外部租约需要再次回收。
			return nil
		}
		return err
	}
	lock := rt.taskLock(t.ID)
	lock.Lock()
	defer lock.Unlock()
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
	state := t.proxyState()
	if state.Snapshot.Kind != db.ProxyKindResin {
		return nil
	}
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
	if json.Unmarshal(b, &record) != nil {
		return errors.New("Resin 租约记录无效")
	}
	if record.Snapshot.ID != state.Snapshot.ID || !reflect.DeepEqual(record.Snapshot, state.Snapshot) {
		return errors.New("Resin 租约身份与旧出口不一致")
	}
	if record.State == "released" {
		return nil
	}
	if record.State != "active" {
		return errors.New("Resin 回收结果不明确，需人工核对")
	}
	s, err := m.pg.ResolveProxySnapshot(record.Snapshot)
	if err != nil {
		return errors.New("Resin 租约凭据解密失败")
	}
	record.State = "releasing"
	if err = writeLease(path, record); err != nil {
		return err
	}
	if ctx.Err() != nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
	}
	err = (egress.ResinConfig{AdminURL: record.AdminURL, AdminToken: s.AdminToken, Platform: s.Platform}).Release(ctx, "artex_task_"+t.ID)
	if err != nil {
		record.State = "uncertain"
		_ = writeLease(path, record)
		return err
	}
	record.State = "released"
	return writeLease(path, record)
}
