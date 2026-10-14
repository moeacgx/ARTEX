package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Autumn-27/artex/db"
	"github.com/Autumn-27/artex/egress"
	"github.com/google/uuid"
)

// 仅替代外部进程/公网边界；claim、SQL、调度、准入及 DTO 均执行真实实现。
type healthRuntimeFixture struct {
	mu                       sync.Mutex
	p                        egress.WarpProfile
	probeErr                 error
	probes, repairs          int
	entered, release         chan struct{}
	stopEntered, stopRelease chan struct{}
	stopErr, materialErr     error
	stops                    int
	recoverAtProbe           int
	resultIP                 string
	getEntered, getRelease   chan struct{}
}

func (f *healthRuntimeFixture) HealthMaterial(string) (egress.WarpProfile, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.p, "material", f.materialErr
}
func (f *healthRuntimeFixture) Get(string) (egress.WarpProfile, error) {
	f.mu.Lock()
	p, entered, release := f.p, f.getEntered, f.getRelease
	f.mu.Unlock()
	if entered != nil {
		entered <- struct{}{}
	}
	if release != nil {
		<-release
	}
	return p, nil
}
func (f *healthRuntimeFixture) Probe(ctx context.Context, _ string) (egress.WarpProfile, error) {
	f.mu.Lock()
	f.probes++
	entered, release := f.entered, f.release
	f.mu.Unlock()
	if entered != nil {
		select {
		case entered <- struct{}{}:
		default:
		}
	}
	if release != nil {
		<-release
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.probeErr != nil && (f.recoverAtProbe == 0 || f.probes < f.recoverAtProbe) {
		f.p.Phase, f.p.EgressIP = "failed", ""
	} else {
		f.p.Phase, f.p.EgressIP = "ready", "8.8.8.8"
		if f.resultIP != "" {
			f.p.EgressIP = f.resultIP
		}
		return f.p, nil
	}
	return f.p, f.probeErr
}
func (f *healthRuntimeFixture) Reconnect(context.Context, string, string) (egress.WarpProfile, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.repairs++
	f.p.Phase, f.p.EgressIP = "ready", "8.8.8.8"
	return f.p, nil
}
func (f *healthRuntimeFixture) ConfirmStopped(context.Context, string) error {
	f.mu.Lock()
	f.stops++
	entered, release, err := f.stopEntered, f.stopRelease, f.stopErr
	f.mu.Unlock()
	if entered != nil {
		entered <- struct{}{}
	}
	if release != nil {
		<-release
	}
	return err
}

func healthControllerFixture(t *testing.T) (*WarpHealthController, *db.DB, *healthRuntimeFixture, int64) {
	t.Helper()
	dsn := os.Getenv("ARTEX_WARP_HEALTH_TEST_DSN")
	if dsn == "" {
		t.Skip("需要专用隔离数据库")
	}
	u, parseErr := url.Parse(dsn)
	if parseErr != nil || u.Scheme != "postgres" || u.Hostname() != "127.0.0.1" || u.Port() == "" || u.RawQuery != "sslmode=disable" || u.Fragment != "" ||
		!(strings.HasPrefix(u.Path, "/artex_warp_") && strings.HasSuffix(u.Path, "_test") || u.Host == "127.0.0.1:25432" && u.Path == "/artex_proxy_verify_test") {
		t.Fatal("拒绝非本地专用健康测试数据库")
	}
	store, err := db.Open(dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	if err = store.EnsureProxyActivitySchema(); err != nil {
		t.Fatal(err)
	}
	if err = store.MigrateWarpHealth(t.Context()); err != nil {
		t.Fatal(err)
	}
	var id int64
	if err = store.QueryRow(`INSERT INTO outbound_proxies(name,kind,enabled,status) VALUES('控制器专用','warp',true,'healthy') RETURNING id`).Scan(&id); err != nil {
		t.Fatal(err)
	}
	runtime := &healthRuntimeFixture{p: egress.WarpProfile{Desired: true, Registered: true, RegistrationAttempted: true, Generated: true, Phase: "ready", EgressIP: "8.8.8.8"}}
	c, err := NewWarpHealthController(store, runtime, db.DefaultWarpHealthPolicy())
	if err != nil {
		t.Fatal(err)
	}
	if err = c.ImportProfile(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	return c, store, runtime, id
}
func runHealthFailures(t *testing.T, c *WarpHealthController, d *db.DB, id int64, count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		if _, err := d.Exec(`UPDATE warp_proxy_health SET next_probe_at=NULL WHERE proxy_id=$1`, id); err != nil {
			t.Fatal(err)
		}
		if err := c.RunOnce(t.Context(), id); err != nil {
			t.Fatal(err)
		}
	}
}
func TestWarpHealthControllerThresholdSameIPAndPolicy(t *testing.T) {
	c, d, f, id := healthControllerFixture(t)
	f.probeErr = errors.New("offline")
	runHealthFailures(t, c, d, id, 2)
	if f.repairs != 0 {
		t.Fatal("未达阈值重连")
	}
	c.SetAutoRecoveryAllowed(func(context.Context, int64) (bool, error) { return false, nil })
	runHealthFailures(t, c, d, id, 1)
	if f.repairs != 0 {
		t.Fatal("关闭维护仍重连")
	}
	c.SetAutoRecoveryAllowed(func(context.Context, int64) (bool, error) { return true, nil })
	runHealthFailures(t, c, d, id, 1)
	if f.repairs != 1 {
		t.Fatalf("未重连: %d", f.repairs)
	}
	dto, err := c.DTO(t.Context(), id)
	if err != nil || dto.EgressIP != "8.8.8.8" || dto.State != "ready" {
		t.Fatalf("相同IP未恢复 %+v %v", dto, err)
	}
	f.p.Phase, f.p.EgressIP = "failed", ""
	dto, _ = c.DTO(t.Context(), id)
	if dto.EgressIP != "" || dto.State == "ready" {
		t.Fatalf("DTO使用旧DB状态 %+v", dto)
	}
}
func TestWarpHealthControllerActiveDoesNotReconnect(t *testing.T) {
	c, d, f, id := healthControllerFixture(t)
	token := uuid.NewString()
	if err := d.AcquireWarpHealthActivity(t.Context(), id, token); err != nil {
		t.Fatal(err)
	}
	f.probeErr = errors.New("offline")
	runHealthFailures(t, c, d, id, 4)
	if f.repairs != 0 || f.probes != 4 {
		t.Fatalf("活动时行为 probe=%d repair=%d", f.probes, f.repairs)
	}
	if err := d.ReleaseWarpHealthActivity(t.Context(), token); err != nil {
		t.Fatal(err)
	}
	runHealthFailures(t, c, d, id, 1)
	if f.repairs != 1 {
		t.Fatal("空闲后没有恢复")
	}
}
func TestWarpHealthControllerCancelLateResultUnknown(t *testing.T) {
	c, d, f, id := healthControllerFixture(t)
	f.entered = make(chan struct{}, 1)
	f.release = make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.RunOnce(ctx, id) }()
	<-f.entered
	cancel()
	deadline := time.Now().Add(time.Second)
	for {
		s, _ := d.WarpHealth(t.Context(), id)
		if s.ClaimState == "unknown" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("取消未建立unknown屏障")
		}
		time.Sleep(time.Millisecond)
	}
	if err := c.ResolveUnknown(t.Context(), id); !errors.Is(err, db.ErrWarpHealthBlocked) {
		t.Fatalf("真实执行未退出却解封: %v", err)
	}
	close(f.release)
	<-done
	s, _ := d.WarpHealth(t.Context(), id)
	if s.ClaimState != "unknown" || s.EgressIP != "" {
		t.Fatalf("迟到成功清屏障: %+v", s)
	}
	if err := c.ResolveUnknown(t.Context(), id); err != nil {
		t.Fatal(err)
	}
}

func TestWarpHealthControllerCloseWaitsDirectRunOnce(t *testing.T) {
	c, d, f, id := healthControllerFixture(t)
	f.entered = make(chan struct{}, 1)
	f.release = make(chan struct{})
	done := make(chan error, 1)
	go func() { done <- c.RunOnce(t.Context(), id) }()
	select {
	case <-f.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("执行没有启动")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	err := c.Close(ctx)
	close(f.release)
	<-done
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close 将真实执行尚未退出误报完成: %v", err)
	}
	s, _ := d.WarpHealth(t.Context(), id)
	if s.ClaimState != "unknown" || s.EgressIP != "" {
		t.Fatalf("关闭取消未保留 unknown: %+v", s)
	}
}

func TestWarpHealthControllerSingleConnectionTwoManagersActualExit(t *testing.T) {
	c, d, f, id := healthControllerFixture(t)
	d.SetMaxOpenConns(1)
	other, err := db.Open(os.Getenv("ARTEX_WARP_HEALTH_TEST_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	other.SetMaxOpenConns(1)
	c2, err := NewWarpHealthController(other, f, db.DefaultWarpHealthPolicy())
	if err != nil {
		t.Fatal(err)
	}
	f.entered = make(chan struct{}, 1)
	f.release = make(chan struct{})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.RunOnce(ctx, id) }()
	select {
	case <-f.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("单连接控制器卡死")
	}
	if err = c2.RunOnce(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	if err = c2.ResolveUnknown(t.Context(), id); !errors.Is(err, db.ErrWarpHealthBlocked) {
		t.Fatalf("另一 Manager 抢占未退出执行: %v", err)
	}
	cancel()
	deadline := time.Now().Add(2 * time.Second)
	for {
		s, e := d.WarpHealth(t.Context(), id)
		if e != nil {
			t.Fatal(e)
		}
		if s.ClaimState == "unknown" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("未建立 unknown")
		}
		time.Sleep(time.Millisecond)
	}
	close(f.release)
	<-done
	f.stopEntered = make(chan struct{}, 1)
	f.stopRelease = make(chan struct{})
	resolved := make(chan error, 1)
	go func() { resolved <- c2.ResolveUnknown(t.Context(), id) }()
	select {
	case <-f.stopEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("没有实际停止确认")
	}
	s, _ := d.WarpHealth(t.Context(), id)
	if s.ClaimState != "unknown" || s.Claim.Token == "" {
		t.Fatal("底层退出未完成已经解封")
	}
	if err = c.ResolveUnknown(t.Context(), id); !errors.Is(err, db.ErrWarpHealthBlocked) {
		t.Fatalf("恢复中另一 Manager 获准: %v", err)
	}
	close(f.stopRelease)
	if err = <-resolved; err != nil {
		t.Fatal(err)
	}
	got, _ := d.WarpHealth(t.Context(), id)
	if got.Claim.Token != "" || got.Revision != s.Revision+1 || got.EgressIP != "" {
		t.Fatalf("原 token/revision 未原子解封: %+v", got)
	}
}

func TestWarpHealthControllerResolveChecksActivityBeforeStop(t *testing.T) {
	c, d, f, id := healthControllerFixture(t)
	token := uuid.NewString()
	if err := d.AcquireWarpHealthActivity(t.Context(), id, token); err != nil {
		t.Fatal(err)
	}
	claim, err := d.BeginWarpHealthProbe(t.Context(), id, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err = d.MarkWarpHealthUnknown(t.Context(), claim); err != nil {
		t.Fatal(err)
	}
	err = c.ResolveUnknown(t.Context(), id)
	if !errors.Is(err, db.ErrWarpHealthBlocked) || f.stops != 0 {
		t.Fatalf("活动中人工恢复先停止: stop=%d err=%v", f.stops, err)
	}
	if err = d.ReleaseWarpHealthActivity(t.Context(), token); err != nil {
		t.Fatal(err)
	}
}

func TestWarpHealthControllerPolicyOffStillProbesAndReprobeFirst(t *testing.T) {
	c, d, f, id := healthControllerFixture(t)
	f.probeErr = errors.New("secret-password-in-runtime-error")
	s, _ := d.WarpHealth(t.Context(), id)
	if err := d.WithWarpHealthLifecycle(t.Context(), func(tx *sql.Tx) error { return db.SetWarpAutoRecoveryTx(t.Context(), tx, id, s.Revision, false) }); err != nil {
		t.Fatal(err)
	}
	runHealthFailures(t, c, d, id, 4)
	if f.probes != 4 || f.repairs != 0 {
		t.Fatalf("关维护影响探测: probe=%d repair=%d", f.probes, f.repairs)
	}
	s, _ = d.WarpHealth(t.Context(), id)
	if err := d.WithWarpHealthLifecycle(t.Context(), func(tx *sql.Tx) error { return db.SetWarpAutoRecoveryTx(t.Context(), tx, id, s.Revision, true) }); err != nil {
		t.Fatal(err)
	}
	f.recoverAtProbe = 6
	runHealthFailures(t, c, d, id, 1)
	if f.repairs != 0 {
		t.Fatal("原地复探已恢复仍重连")
	}
	dto, err := c.DTO(t.Context(), id)
	if err != nil || dto.State != "ready" || dto.EgressIP != "8.8.8.8" {
		t.Fatalf("同IP原地恢复失败: %+v %v", dto, err)
	}
	raw, _ := json.Marshal(dto)
	for _, secret := range []string{"material", "claim_token", "owner_pid", "secret-password"} {
		if strings.Contains(string(raw), secret) {
			t.Fatalf("DTO 泄露 %s", secret)
		}
	}
}

func TestWarpHealthControllerUnknownMaterialAndImportOnce(t *testing.T) {
	c, d, f, id := healthControllerFixture(t)
	f.materialErr = errors.New("unknown registration")
	if err := c.SyncProfile(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	if err := c.RunOnce(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	s, _ := d.WarpHealth(t.Context(), id)
	if s.Desired || s.Material != "" || f.probes != 0 || f.repairs != 0 {
		t.Fatalf("未知材料自动执行: %+v", s)
	}
	f.materialErr = nil
	if err := c.ImportProfile(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	s, _ = d.WarpHealth(t.Context(), id)
	if s.Desired {
		t.Fatal("重复导入覆盖新 DB 意图")
	}
}

func TestWarpHealthControllerRejectsNonPublicRuntimeIP(t *testing.T) {
	c, d, f, id := healthControllerFixture(t)
	f.resultIP = "10.0.0.1"
	if err := c.RunOnce(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	s, _ := d.WarpHealth(t.Context(), id)
	dto, err := c.DTO(t.Context(), id)
	if s.State == "ready" || s.EgressIP != "" || err != nil || dto.EgressIP != "" {
		t.Fatalf("控制器接受内网出口: %+v %+v %v", s, dto, err)
	}
}

func TestWarpHealthControllerReadErrorDefersRepairAndKeepsProbing(t *testing.T) {
	c, d, f, id := healthControllerFixture(t)
	f.probeErr = errors.New("offline")
	c.SetAutoRecoveryAllowed(func(context.Context, int64) (bool, error) { return false, errors.New("策略读取失败") })
	runHealthFailures(t, c, d, id, 4)
	s, _ := d.WarpHealth(t.Context(), id)
	if f.repairs != 0 || f.probes != 4 || s.State != "deferred" {
		t.Fatalf("读取失败仍重连或停止检测: %+v", s)
	}
	c.SetAutoRecoveryAllowed(nil)
	var guardCalls int
	c.SetRepairGuard(func(ctx context.Context, tx *sql.Tx, id int64) error {
		guardCalls++
		var revision int64
		if err := tx.QueryRowContext(ctx, `SELECT revision FROM warp_proxy_health WHERE proxy_id=$1`, id).Scan(&revision); err != nil {
			return err
		}
		return db.ErrWarpHealthBlocked
	})
	runHealthFailures(t, c, d, id, 1)
	if guardCalls != 1 || f.repairs != 0 || f.probes != 5 {
		t.Fatal("repair 未在同 Tx 权威复核")
	}
}

func TestWarpHealthControllerUnknownExitFailureAndForeignOwnerStayBlocked(t *testing.T) {
	c, d, f, id := healthControllerFixture(t)
	claim, err := d.BeginWarpHealthProbe(t.Context(), id, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err = d.MarkWarpHealthUnknown(t.Context(), claim); err != nil {
		t.Fatal(err)
	}
	f.stopErr = egress.ErrWarpExitUnknown
	if err = c.ResolveUnknown(t.Context(), id); !errors.Is(err, egress.ErrWarpExitUnknown) {
		t.Fatalf("没有真实退出也解封: %v", err)
	}
	s, _ := d.WarpHealth(t.Context(), id)
	if s.Claim.Token != claim.Token || s.ClaimState != "unknown" || s.Revision != claim.Revision {
		t.Fatalf("退出失败清屏障: %+v", s)
	}
	f.stopErr = nil
	f.stops = 0
	if _, err = d.Exec(`UPDATE warp_proxy_health SET owner_host='unverified-other-host' WHERE proxy_id=$1`, id); err != nil {
		t.Fatal(err)
	}
	if err = c.ResolveUnknown(t.Context(), id); !errors.Is(err, egress.ErrWarpExitUnknown) || f.stops != 0 {
		t.Fatalf("同 PID 其他宿主绕过真实证明: %v stop=%d", err, f.stops)
	}
}

func TestWarpHealthDTOReadsHealthBarrierWithRuntimeAtomically(t *testing.T) {
	c, d, f, id := healthControllerFixture(t)
	d.SetMaxOpenConns(1)
	if err := c.RunOnce(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	f.getEntered = make(chan struct{}, 1)
	f.getRelease = make(chan struct{})
	dtoDone := make(chan error, 1)
	go func() { _, err := c.DTO(t.Context(), id); dtoDone <- err }()
	select {
	case <-f.getEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("DTO 没有读取 runtime")
	}
	mutationDone := make(chan error, 1)
	go func() {
		mutationDone <- d.WithWarpHealthLifecycle(t.Context(), func(tx *sql.Tx) error {
			_, err := tx.Exec(`UPDATE warp_proxy_health SET state='repairing',egress_ip='' WHERE proxy_id=$1`, id)
			return err
		})
	}()
	select {
	case err := <-mutationDone:
		close(f.getRelease)
		<-dtoDone
		t.Fatalf("DTO 的 DB/runtime 快照可被 repair 插入: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(f.getRelease)
	if err := <-dtoDone; err != nil {
		t.Fatal(err)
	}
	if err := <-mutationDone; err != nil {
		t.Fatal(err)
	}
	dto, err := c.DTO(t.Context(), id)
	if err != nil || dto.EgressIP != "" {
		t.Fatalf("repairing 暴露有效IP: %+v %v", dto, err)
	}
}
