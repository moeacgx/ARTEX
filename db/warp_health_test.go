package db

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func warpHealthDB(t *testing.T) *DB {
	t.Helper()
	dsn := os.Getenv("ARTEX_WARP_HEALTH_TEST_DSN")
	if dsn == "" {
		t.Skip("仅允许专用健康测试数据库")
	}
	u, parseErr := url.Parse(dsn)
	if parseErr != nil || u.Scheme != "postgres" || u.Hostname() != "127.0.0.1" || u.Port() == "" || u.RawQuery != "sslmode=disable" || u.Fragment != "" ||
		!(strings.HasPrefix(u.Path, "/artex_warp_") && strings.HasSuffix(u.Path, "_test") || u.Host == "127.0.0.1:25432" && u.Path == "/artex_proxy_verify_test") {
		t.Fatal("拒绝非本地专用健康测试数据库")
	}
	d, err := Open(dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close() })
	if err = d.EnsureProxyActivitySchema(); err != nil {
		t.Fatal(err)
	}
	if err = d.MigrateWarpHealth(t.Context()); err != nil {
		t.Fatal(err)
	}
	return d
}
func healthProxy(t *testing.T, d *DB) int64 {
	t.Helper()
	var id int64
	err := d.QueryRow(`INSERT INTO outbound_proxies(name,kind,enabled,status) VALUES('健康专用测试','warp',true,'healthy') RETURNING id`).Scan(&id)
	if err != nil {
		t.Fatal(err)
	}
	return id
}
func healthIntent(t *testing.T, d *DB, id int64) {
	t.Helper()
	if err := d.SyncWarpHealthIntent(t.Context(), id, WarpHealthIntent{Desired: true, Material: "complete-material", ProfileRevision: 7}); err != nil {
		t.Fatal(err)
	}
}

func TestWarpHealthMigrationDoesNotInferDesiredFromHealthy(t *testing.T) {
	d := warpHealthDB(t)
	id := healthProxy(t, d)
	if err := d.MigrateWarpHealth(t.Context()); err != nil {
		t.Fatal(err)
	}
	s, err := d.WarpHealth(t.Context(), id)
	if err != nil || s.Desired || s.Material != "" {
		t.Fatalf("迁移复活档案: %+v %v", s, err)
	}
	if _, err = d.BeginWarpHealthProbe(t.Context(), id, time.Now()); !errors.Is(err, ErrWarpHealthBlocked) {
		t.Fatalf("未知材料获得探测: %v", err)
	}
}

func TestWarpHealthClaimAdmissionAndRevision(t *testing.T) {
	d := warpHealthDB(t)
	d.SetMaxOpenConns(1)
	id := healthProxy(t, d)
	healthIntent(t, d, id)
	now := time.Now()
	claim, err := d.BeginWarpHealthProbe(t.Context(), id, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = d.BeginWarpHealthProbe(t.Context(), id, now); !errors.Is(err, ErrWarpHealthBlocked) {
		t.Fatalf("第二执行者: %v", err)
	}
	// 同一事务连接的 helper 不得二次借连接。
	err = d.withWarpHealth(t.Context(), func(tx *sql.Tx) error { return CheckWarpHealthAdmissionTx(t.Context(), tx, id) })
	if err != nil {
		t.Fatal(err)
	}
	if err = d.SyncWarpHealthIntent(t.Context(), id, WarpHealthIntent{}); err != nil {
		t.Fatal(err)
	}
	if _, err = d.CompleteWarpHealthProbe(t.Context(), claim, "8.8.8.8", true, now, DefaultWarpHealthPolicy()); !errors.Is(err, ErrWarpHealthStale) {
		t.Fatalf("迟到回写成功: %v", err)
	}
	s, _ := d.WarpHealth(t.Context(), id)
	if s.Desired || s.EgressIP != "" {
		t.Fatalf("停止被覆盖: %+v", s)
	}
}

func TestWarpHealthThresholdCooldownActiveAndUnknown(t *testing.T) {
	d := warpHealthDB(t)
	id := healthProxy(t, d)
	healthIntent(t, d, id)
	policy := DefaultWarpHealthPolicy()
	now := time.Now()
	activityToken := uuid.NewString()
	var claim WarpHealthClaim
	for i := 0; i < 3; i++ {
		var err error
		claim, err = d.BeginWarpHealthProbe(t.Context(), id, now)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = d.CompleteWarpHealthProbe(t.Context(), claim, "", false, now, policy); err != nil {
			t.Fatal(err)
		}
		claim, err = d.BeginWarpHealthRepair(t.Context(), claim, now, policy)
		if i < 2 {
			if !errors.Is(err, ErrWarpHealthBlocked) {
				t.Fatalf("未达到阈值却重连: %v", err)
			}
		} else if err != nil {
			t.Fatal(err)
		}
		now = now.Add(time.Minute)
	}
	if err := d.AcquireWarpHealthActivity(t.Context(), id, activityToken); !errors.Is(err, ErrWarpHealthBlocked) {
		t.Fatalf("修复期间新活动获准: %v", err)
	}
	if err := d.MarkWarpHealthUnknown(t.Context(), claim); err != nil {
		t.Fatal(err)
	}
	if _, err := d.CompleteWarpHealthRepair(t.Context(), claim, "8.8.8.8", true, now, policy); !errors.Is(err, ErrWarpHealthStale) {
		t.Fatalf("取消后的迟到成功清除屏障: %v", err)
	}
	if _, err := d.BeginWarpHealthProbe(t.Context(), id, now.Add(24*time.Hour)); !errors.Is(err, ErrWarpHealthBlocked) {
		t.Fatalf("未知claim被超时夺权: %v", err)
	}
	if err := d.ResolveWarpHealthUnknown(t.Context(), claim, nil); !errors.Is(err, ErrWarpHealthBlocked) {
		t.Fatalf("未经退出确认就解封: %v", err)
	}
	execution, err := d.TryWarpHealthExecution(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	if err = d.ResolveWarpHealthUnknown(t.Context(), claim, execution); err != nil {
		t.Fatal(err)
	}
	execution.Close()
	if _, err = d.Exec(`UPDATE outbound_proxies SET status='healthy' WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
	if err := d.AcquireWarpHealthActivity(t.Context(), id, activityToken); err != nil {
		t.Fatal(err)
	}
	// 活动期间探测允许，破坏性重连延期。
	claim, err = d.BeginWarpHealthProbe(t.Context(), id, now.Add(24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = d.CompleteWarpHealthProbe(t.Context(), claim, "", false, now.Add(24*time.Hour), policy); err != nil {
		t.Fatal(err)
	}
	if _, err = d.BeginWarpHealthRepair(t.Context(), claim, now.Add(24*time.Hour), policy); !errors.Is(err, ErrWarpHealthBlocked) {
		t.Fatalf("活动引用未保护: %v", err)
	}
	if err = d.ReleaseWarpHealthActivity(t.Context(), activityToken); err != nil {
		t.Fatal(err)
	}
}

func TestWarpHealthTwoManagersOnlyOneExecutor(t *testing.T) {
	d := warpHealthDB(t)
	other := warpHealthDB(t)
	id := healthProxy(t, d)
	healthIntent(t, d, id)
	var wg sync.WaitGroup
	wins := make(chan WarpHealthClaim, 2)
	for _, store := range []*DB{d, other} {
		wg.Go(func() {
			c, e := store.BeginWarpHealthProbe(context.Background(), id, time.Now())
			if e == nil {
				wins <- c
			} else if !errors.Is(e, ErrWarpHealthBlocked) {
				t.Error(e)
			}
		})
	}
	wg.Wait()
	close(wins)
	if len(wins) != 1 {
		t.Fatalf("执行者数量=%d", len(wins))
	}
}

func TestWarpHealthExecutionDoesNotHoldBusinessConnection(t *testing.T) {
	d := warpHealthDB(t)
	d.SetMaxOpenConns(1)
	id := healthProxy(t, d)
	healthIntent(t, d, id)
	execution, err := d.TryWarpHealthExecution(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	defer execution.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()
	s, err := d.WarpHealth(ctx, id)
	if err != nil || !s.Desired {
		t.Fatalf("独立执行 session 阻塞单连接业务池: %+v %v", s, err)
	}
	claim, err := execution.BeginWarpHealthProbe(t.Context(), id, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	owner, _ := os.Hostname()
	s, err = execution.WarpHealth(t.Context(), id)
	if err != nil || s.OwnerHost != owner || s.OwnerPID != os.Getpid() {
		t.Fatalf("claim 未记录真实 owner: %+v %v", s, err)
	}
	if _, err = execution.CompleteWarpHealthProbe(t.Context(), claim, "8.8.8.8", true, time.Now(), DefaultWarpHealthPolicy()); err != nil {
		t.Fatal(err)
	}
}

func TestWarpHealthPolicyCASMissingStaleAndDesired(t *testing.T) {
	d := warpHealthDB(t)
	id := healthProxy(t, d)
	healthIntent(t, d, id)
	s, err := d.WarpHealth(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	set := func(id, revision int64, enabled bool) error {
		return d.WithWarpHealthLifecycle(t.Context(), func(tx *sql.Tx) error { return SetWarpAutoRecoveryTx(t.Context(), tx, id, revision, enabled) })
	}
	if err = set(id+1000000, 1, false); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("不存在的策略应返回 sql.ErrNoRows: %v", err)
	}
	if err = set(id, s.Revision+1, false); !errors.Is(err, ErrWarpHealthStale) {
		t.Fatalf("旧 revision 未拒绝: %v", err)
	}
	if err = set(id, s.Revision, false); err != nil {
		t.Fatal(err)
	}
	got, _ := d.WarpHealth(t.Context(), id)
	if !got.Desired || got.AutoRecoverEnabled || got.Revision != s.Revision+1 {
		t.Fatalf("开关误改 Desired: %+v", got)
	}
	if err = set(id, got.Revision, true); err != nil {
		t.Fatal(err)
	}
}

func TestWarpHealthPolicyAndRepairSerialize(t *testing.T) {
	for _, repairFirst := range []bool{false, true} {
		t.Run(fmt.Sprint(repairFirst), func(t *testing.T) {
			d := warpHealthDB(t)
			id := healthProxy(t, d)
			healthIntent(t, d, id)
			now := time.Now()
			var claim WarpHealthClaim
			for i := 0; i < 3; i++ {
				var err error
				claim, err = d.BeginWarpHealthProbe(t.Context(), id, now)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = d.CompleteWarpHealthProbe(t.Context(), claim, "", false, now, DefaultWarpHealthPolicy()); err != nil {
					t.Fatal(err)
				}
				now = now.Add(time.Minute)
			}
			if repairFirst {
				repair, err := d.BeginWarpHealthRepair(t.Context(), claim, now, DefaultWarpHealthPolicy())
				if err != nil {
					t.Fatal(err)
				}
				err = d.WithWarpHealthLifecycle(t.Context(), func(tx *sql.Tx) error { return SetWarpAutoRecoveryTx(t.Context(), tx, id, claim.Revision, false) })
				if !errors.Is(err, ErrWarpHealthBlocked) {
					t.Fatalf("repair 被策略抢占: %v", err)
				}
				if err = d.MarkWarpHealthUnknown(t.Context(), repair); err != nil {
					t.Fatal(err)
				}
				err = d.WithWarpHealthLifecycle(t.Context(), func(tx *sql.Tx) error { return SetWarpAutoRecoveryTx(t.Context(), tx, id, claim.Revision, true) })
				if !errors.Is(err, ErrWarpHealthBlocked) {
					t.Fatalf("unknown 被策略解封: %v", err)
				}
			} else {
				if err := d.WithWarpHealthLifecycle(t.Context(), func(tx *sql.Tx) error { return SetWarpAutoRecoveryTx(t.Context(), tx, id, claim.Revision, false) }); err != nil {
					t.Fatal(err)
				}
				if _, err := d.BeginWarpHealthRepair(t.Context(), claim, now, DefaultWarpHealthPolicy()); !errors.Is(err, ErrWarpHealthBlocked) {
					t.Fatalf("策略先关闭仍获 repair: %v", err)
				}
			}
		})
	}
}

func TestWarpHealthActivityLateReleaseOnlyOriginalToken(t *testing.T) {
	d := warpHealthDB(t)
	id := healthProxy(t, d)
	healthIntent(t, d, id)
	one, two := uuid.NewString(), uuid.NewString()
	if err := d.AcquireWarpHealthActivity(t.Context(), id, one); err != nil {
		t.Fatal(err)
	}
	if err := d.AcquireWarpHealthActivity(t.Context(), id, one); err == nil {
		t.Fatal("重复活动 token 未拒绝")
	}
	if err := d.ReleaseWarpHealthActivity(t.Context(), one); err != nil {
		t.Fatal(err)
	}
	if err := d.AcquireWarpHealthActivity(t.Context(), id, two); err != nil {
		t.Fatal(err)
	}
	if err := d.ReleaseWarpHealthActivity(t.Context(), one); err != nil {
		t.Fatal(err)
	}
	if err := d.WithWarpHealthLifecycle(t.Context(), func(tx *sql.Tx) error { return WarpHealthIdleTx(t.Context(), tx, id) }); !errors.Is(err, ErrWarpHealthBlocked) {
		t.Fatalf("迟到回调释放新活动: %v", err)
	}
	if err := d.ReleaseWarpHealthActivity(t.Context(), two); err != nil {
		t.Fatal(err)
	}
}

func TestWarpHealthRepairBackoffAndStopped(t *testing.T) {
	d := warpHealthDB(t)
	id := healthProxy(t, d)
	healthIntent(t, d, id)
	now := time.Now()
	policy := DefaultWarpHealthPolicy()
	for _, delay := range []time.Duration{300 * time.Second, 600 * time.Second, 1200 * time.Second, 2400 * time.Second, 3600 * time.Second, 3600 * time.Second} {
		if _, err := d.Exec(`UPDATE warp_proxy_health SET failures=3,next_probe_at=NULL WHERE proxy_id=$1`, id); err != nil {
			t.Fatal(err)
		}
		probe, err := d.BeginWarpHealthProbe(t.Context(), id, now)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = d.CompleteWarpHealthProbe(t.Context(), probe, "", false, now, policy); err != nil {
			t.Fatal(err)
		}
		repair, err := d.BeginWarpHealthRepair(t.Context(), probe, now, policy)
		if err != nil {
			t.Fatal(err)
		}
		s, err := d.CompleteWarpHealthRepair(t.Context(), repair, "", false, now, policy)
		if err != nil {
			t.Fatal(err)
		}
		if s.NextRecoveryAt == nil || s.NextRecoveryAt.Sub(now).Round(time.Millisecond) != delay {
			t.Fatalf("退避错误: %+v want=%v", s, delay)
		}
		if _, err = d.BeginWarpHealthRepair(t.Context(), probe, now.Add(time.Second), policy); !errors.Is(err, ErrWarpHealthBlocked) {
			t.Fatalf("冷却被绕过: %v", err)
		}
		now = now.Add(delay)
	}
	if _, err := d.Exec(`UPDATE outbound_proxies SET status='stopped' WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
	if err := d.SyncWarpHealthIntent(t.Context(), id, WarpHealthIntent{Desired: true, Material: "complete-material"}); err != nil {
		t.Fatal(err)
	}
	s, _ := d.WarpHealth(t.Context(), id)
	if s.Desired {
		t.Fatal("磁盘 Desired 覆盖明确 DB stopped")
	}
	if _, err := d.BeginWarpHealthProbe(t.Context(), id, now.Add(time.Hour)); !errors.Is(err, ErrWarpHealthBlocked) {
		t.Fatalf("手动 stop 自动启动: %v", err)
	}
}

func TestWarpHealthOriginalUUIDReplayPendingAndDeleteTombstone(t *testing.T) {
	d := warpHealthDB(t)
	proxySecretsMu.Lock()
	previous := proxySecretsPath
	proxySecretsMu.Unlock()
	t.Cleanup(func() { proxySecretsMu.Lock(); proxySecretsPath = previous; proxySecretsMu.Unlock() })
	if err := ConfigureProxySecrets(filepath.Join(t.TempDir(), "key")); err != nil {
		t.Fatal(err)
	}
	key := uuid.NewString()
	payload := sha256.Sum256([]byte("health-uuid-compatibility"))
	proxy := &Proxy{Name: "健康兼容回归", Kind: ProxyKindWarp, Enabled: true}
	first, intent, err := d.CreateWarpProxyOnce(key, payload, proxy)
	if err != nil {
		t.Fatal(err)
	}
	if !intent.Created || intent.State != "pending" || intent.ExecutionToken == "" {
		t.Fatalf("创建意图异常: %+v", intent)
	}
	if err = d.MigrateWarpHealth(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err = d.SyncWarpHealthIntent(t.Context(), first.ID, WarpHealthIntent{Desired: true, Material: "complete-material"}); err != nil {
		t.Fatal(err)
	}
	if _, err = d.BeginWarpHealthProbe(t.Context(), first.ID, time.Now()); !errors.Is(err, ErrWarpHealthBlocked) {
		t.Fatalf("pending 创建被健康调度启动: %v", err)
	}
	replay, pending, err := d.CreateWarpProxyOnce(key, payload, proxy)
	if err != nil || replay.ID != first.ID || pending.Created || pending.State != "pending" {
		t.Fatalf("pending 同 UUID 未复用: %+v %v", pending, err)
	}
	connection := &Proxy{URL: "socks5://user:pass@127.0.0.1:12345", Host: "127.0.0.1", Port: 12345, Username: "user", Password: "pass"}
	if _, err = d.CompleteWarpProxyCreate(key, first.ID, uuid.NewString(), connection, ""); !errors.Is(err, ErrWarpCreateConflict) {
		t.Fatalf("错误执行令牌完成创建: %v", err)
	}
	if _, err = d.CompleteWarpProxyCreate(key, first.ID, intent.ExecutionToken, connection, ""); err != nil {
		t.Fatal(err)
	}
	if err = d.SyncWarpHealthIntent(t.Context(), first.ID, WarpHealthIntent{Desired: true, Material: "complete-material"}); err != nil {
		t.Fatal(err)
	}
	replay, complete, err := d.CreateWarpProxyOnce(key, payload, proxy)
	if err != nil || replay.ID != first.ID || complete.Created || complete.State != "succeeded" {
		t.Fatalf("完成同 UUID 未复用: %+v %v", complete, err)
	}
	changed := sha256.Sum256([]byte("changed"))
	if _, _, err = d.CreateWarpProxyOnce(key, changed, proxy); !errors.Is(err, ErrWarpCreateConflict) {
		t.Fatalf("异载荷未拒绝: %v", err)
	}
	if err = d.DeleteProxy(first.ID); err != nil {
		t.Fatal(err)
	}
	if _, _, err = d.CreateWarpProxyOnce(key, payload, proxy); !errors.Is(err, ErrWarpCreateGone) {
		t.Fatalf("旧 UUID 删除墓碑被健康迁移复活: %v", err)
	}
	if _, err = d.WarpHealth(t.Context(), first.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("删除遗留健康行: %v", err)
	}
}

func TestWarpHealthManualClaimStartsStoppedAndCommitsAtomically(t *testing.T) {
	d := warpHealthDB(t)
	d.SetMaxOpenConns(1)
	id := healthProxy(t, d)
	if _, err := d.Exec(`UPDATE outbound_proxies SET status='stopped' WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
	if err := d.MigrateWarpHealth(t.Context()); err != nil {
		t.Fatal(err)
	}
	s, _ := d.WarpHealth(t.Context(), id)
	if err := d.WithWarpHealthLifecycle(t.Context(), func(tx *sql.Tx) error { return SetWarpAutoRecoveryTx(t.Context(), tx, id, s.Revision, false) }); err != nil {
		t.Fatal(err)
	}
	execution, err := d.TryWarpHealthExecution(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	defer execution.Close()
	claim, err := execution.BeginWarpHealthManual(t.Context(), id, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = d.WithWarpHealthLifecycle(t.Context(), func(tx *sql.Tx) error { return CheckWarpHealthMutationTx(t.Context(), tx, id) }); !errors.Is(err, ErrWarpHealthBlocked) {
		t.Fatalf("manual claim 未建立互斥: %v", err)
	}
	intent := WarpHealthIntent{Desired: true, Material: "same-material", ProfileRevision: 12}
	err = execution.CompleteWarpHealthManual(t.Context(), claim, intent, func(tx *sql.Tx) error {
		_, e := tx.ExecContext(t.Context(), `UPDATE outbound_proxies SET status='healthy' WHERE id=$1`, id)
		return e
	})
	if err != nil {
		t.Fatal(err)
	}
	s, _ = d.WarpHealth(t.Context(), id)
	if !s.Desired || s.AutoRecoverEnabled || s.Material != "same-material" || s.Claim.Token != "" || s.Revision != claim.Revision+1 {
		t.Fatalf("manual 完成不原子/误改策略: %+v", s)
	}
	if err = execution.CompleteWarpHealthManual(t.Context(), claim, intent, nil); !errors.Is(err, ErrWarpHealthStale) {
		t.Fatalf("迟到手动完成覆盖新意图: %v", err)
	}
}

func TestWarpHealthManualClaimGuardAndUnknown(t *testing.T) {
	d := warpHealthDB(t)
	id := healthProxy(t, d)
	healthIntent(t, d, id)
	execution, err := d.TryWarpHealthExecution(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	defer execution.Close()
	if _, err = execution.BeginWarpHealthManual(t.Context(), id, func(context.Context, *sql.Tx, int64) error { return ErrWarpHealthBlocked }); !errors.Is(err, ErrWarpHealthBlocked) {
		t.Fatalf("manual 忽略权威 guard: %v", err)
	}
	claim, err := execution.BeginWarpHealthManual(t.Context(), id, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = execution.MarkWarpHealthUnknown(t.Context(), claim); err != nil {
		t.Fatal(err)
	}
	called := false
	err = execution.CompleteWarpHealthManual(t.Context(), claim, WarpHealthIntent{Desired: true, Material: "same-material"}, func(*sql.Tx) error { called = true; return nil })
	if !errors.Is(err, ErrWarpHealthStale) || called {
		t.Fatalf("unknown 接受迟到完成/执行持久回调: %v", err)
	}
}

func TestWarpHealthManualCompletionRollbackKeepsClaim(t *testing.T) {
	d := warpHealthDB(t)
	id := healthProxy(t, d)
	healthIntent(t, d, id)
	execution, err := d.TryWarpHealthExecution(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	defer execution.Close()
	claim, err := execution.BeginWarpHealthManual(t.Context(), id, nil)
	if err != nil {
		t.Fatal(err)
	}
	boom := errors.New("持久化失败")
	err = execution.CompleteWarpHealthManual(t.Context(), claim, WarpHealthIntent{}, func(tx *sql.Tx) error {
		if _, e := tx.Exec(`UPDATE outbound_proxies SET status='stopped' WHERE id=$1`, id); e != nil {
			return e
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("未回滚失败回调: %v", err)
	}
	s, _ := d.WarpHealth(t.Context(), id)
	proxy, _ := d.GetProxy(id)
	if s.Claim.Token != claim.Token || s.Revision != claim.Revision || proxy.Status != ProxyStatusHealthy {
		t.Fatalf("manual 部分提交: %+v %+v", s, proxy)
	}
	if err = execution.MarkWarpHealthUnknown(t.Context(), claim); err != nil {
		t.Fatal(err)
	}
}

func TestWarpHealthPolicyCASWaitsForConcurrentRepairGuard(t *testing.T) {
	d := warpHealthDB(t)
	other := warpHealthDB(t)
	id := healthProxy(t, d)
	healthIntent(t, d, id)
	now := time.Now()
	var previous WarpHealthClaim
	for i := 0; i < 3; i++ {
		var err error
		previous, err = d.BeginWarpHealthProbe(t.Context(), id, now)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = d.CompleteWarpHealthProbe(t.Context(), previous, "", false, now, DefaultWarpHealthPolicy()); err != nil {
			t.Fatal(err)
		}
		now = now.Add(time.Minute)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	repairDone := make(chan error, 1)
	go func() {
		_, err := d.BeginWarpHealthRepairWithGuard(t.Context(), previous, now, DefaultWarpHealthPolicy(), func(context.Context, *sql.Tx, int64) error { close(entered); <-release; return nil })
		repairDone <- err
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("guard 未进入")
	}
	casDone := make(chan error, 1)
	started := make(chan struct{})
	go func() {
		close(started)
		casDone <- other.WithWarpHealthLifecycle(t.Context(), func(tx *sql.Tx) error { return SetWarpAutoRecoveryTx(t.Context(), tx, id, previous.Revision, false) })
	}()
	<-started
	select {
	case err := <-casDone:
		close(release)
		<-repairDone
		t.Fatalf("CAS 未等待同生命周期锁: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	if err := <-repairDone; err != nil {
		t.Fatal(err)
	}
	if err := <-casDone; !errors.Is(err, ErrWarpHealthBlocked) {
		t.Fatalf("并发 CAS 覆盖 repair: %v", err)
	}
}
