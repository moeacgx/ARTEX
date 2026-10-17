package server

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Autumn-27/artex/db"
	"github.com/Autumn-27/artex/egress"
	"github.com/google/uuid"
)

func containerExitFixture(t *testing.T) (*db.DB, *egress.WarpManager, WarpContainerExitRequest, string) {
	t.Helper()
	c, store, _, id := healthControllerFixture(t)
	t.Cleanup(func() {
		if err := c.Close(context.Background()); err != nil {
			t.Error(err)
		}
		if _, err := store.Exec(`DELETE FROM outbound_proxies WHERE id=$1`, id); err != nil {
			t.Error(err)
		}
	})
	execution, err := store.TryWarpHealthExecution(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := execution.BeginWarpHealthProbe(t.Context(), id, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := execution.MarkWarpHealthUnknown(t.Context(), claim); err != nil {
		t.Fatal(err)
	}
	if err := execution.Close(); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	runtime, err := egress.NewWarpManager(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Close() })
	profile := filepath.Join(root, warpProfileID(id))
	if err := os.Mkdir(profile, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(profile, "process.json")
	data := []byte(`{"host":"previous-container","pid":321,"exited":false}`)
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(data)
	return store, runtime, WarpContainerExitRequest{ProxyID: id, ExpectedRevision: claim.Revision, ExpectedClaim: claim.Token, ExpectedProcessSHA256: hex.EncodeToString(digest[:])}, path
}

func assertContainerExitReceipt(t *testing.T, path string, exited bool) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var record struct {
		Host   string `json:"host"`
		PID    int    `json:"pid"`
		Exited bool   `json:"exited"`
	}
	if err := json.Unmarshal(data, &record); err != nil {
		t.Fatal(err)
	}
	if record.Host != "previous-container" || record.PID != 321 || record.Exited != exited {
		t.Fatalf("退出记录改变了身份或错误解除屏障: %+v", record)
	}
}

func TestWarpContainerExitConfirmedKeepsHealthClaim(t *testing.T) {
	store, runtime, req, path := containerExitFixture(t)
	// 宿主命令只连接已有 schema，不能通过 db.Open 迁移或重写默认配置。
	conn, err := sql.Open("pgx", os.Getenv("ARTEX_WARP_HEALTH_TEST_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	maintenanceStore := &db.DB{DB: conn}
	before, err := store.WarpHealth(t.Context(), req.ProxyID)
	if err != nil {
		t.Fatal(err)
	}
	called := false
	err = ConfirmWarpContainerProcessExit(t.Context(), maintenanceStore, runtime, req, func(ctx context.Context, host string, pid int) error {
		called = true
		if host != "previous-container" || pid != 321 {
			t.Fatalf("验证了错误进程: host=%q pid=%d", host, pid)
		}
		other, err := store.TryWarpHealthExecution(ctx, req.ProxyID)
		if other != nil {
			_ = other.Close()
		}
		if !errors.Is(err, db.ErrWarpHealthBlocked) {
			t.Fatalf("写退出收据期间未保持健康执行独占锁: %v", err)
		}
		return nil
	})
	if err != nil || !called {
		t.Fatalf("确认失败: called=%v err=%v", called, err)
	}
	assertContainerExitReceipt(t, path, true)
	after, err := store.WarpHealth(t.Context(), req.ProxyID)
	if err != nil || after.Claim != before.Claim || after.ClaimState != before.ClaimState || after.Revision != before.Revision || after.OwnerHost != before.OwnerHost || after.OwnerPID != before.OwnerPID {
		t.Fatalf("退出收据意外改变了数据库健康屏障: before=%+v after=%+v err=%v", before, after, err)
	}
}

func TestWarpContainerExitRejectsChangedOrBusyState(t *testing.T) {
	for _, condition := range []string{"revision", "claim", "active", "execution", "local_execution", "warp_activity", "task_activity", "reserved_policy"} {
		t.Run(condition, func(t *testing.T) {
			store, runtime, req, path := containerExitFixture(t)
			switch condition {
			case "revision":
				req.ExpectedRevision++
			case "claim":
				req.ExpectedClaim = uuid.NewString()
			case "active":
				if _, err := store.Exec(`UPDATE warp_proxy_health SET claim_state='active' WHERE proxy_id=$1`, req.ProxyID); err != nil {
					t.Fatal(err)
				}
			case "execution":
				lock, err := store.TryWarpHealthExecution(t.Context(), req.ProxyID)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = lock.Close() })
			case "local_execution":
				warpHealthExecutions.Store(req.ExpectedClaim, true)
				t.Cleanup(func() { warpHealthExecutions.Delete(req.ExpectedClaim) })
			case "warp_activity":
				token := uuid.NewString()
				if _, err := store.Exec(`INSERT INTO warp_proxy_health_activity(activity_token,proxy_id) VALUES($1,$2)`, token, req.ProxyID); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _, _ = store.Exec(`DELETE FROM warp_proxy_health_activity WHERE activity_token=$1`, token) })
			case "task_activity", "reserved_policy":
				task, err := store.CreateTaskWithOptions("容器退出占用回归", "", db.TaskCreateOptions{ProxyMode: db.TaskProxyModeDirect})
				if err != nil {
					t.Fatal(err)
				}
				if condition == "task_activity" {
					_, err = store.Exec(`INSERT INTO task_proxy_activity(task_id,proxy_id,active_count) VALUES($1,$2,1)`, task.ID, req.ProxyID)
				} else {
					_, err = store.Exec(`INSERT INTO proxy_subject_policies(task_id,reserved_proxy_id,switch_token) VALUES($1,$2,$3) ON CONFLICT(task_id) DO UPDATE SET reserved_proxy_id=EXCLUDED.reserved_proxy_id,switch_token=EXCLUDED.switch_token`, task.ID, req.ProxyID, uuid.NewString())
				}
				t.Cleanup(func() {
					_, _ = store.Exec(`DELETE FROM task_proxy_activity WHERE task_id=$1`, task.ID)
					_, _ = store.Exec(`DELETE FROM proxy_subject_policies WHERE task_id=$1`, task.ID)
					if err := store.DeleteTask(task.ID); err != nil {
						t.Error(err)
					}
				})
				if err != nil {
					t.Fatal(err)
				}
			}
			called := false
			err := ConfirmWarpContainerProcessExit(t.Context(), store, runtime, req, func(context.Context, string, int) error { called = true; return nil })
			if err == nil || called {
				t.Fatalf("不满足维护条件仍进入宿主确认: called=%v err=%v", called, err)
			}
			assertContainerExitReceipt(t, path, false)
		})
	}
}

func TestWarpContainerExitRechecksAfterEvidence(t *testing.T) {
	for _, change := range []string{"evidence_failure", "revision_changed"} {
		t.Run(change, func(t *testing.T) {
			store, runtime, req, path := containerExitFixture(t)
			err := ConfirmWarpContainerProcessExit(t.Context(), store, runtime, req, func(context.Context, string, int) error {
				if change == "evidence_failure" {
					return errors.New("旧容器仍在运行")
				}
				_, err := store.Exec(`UPDATE warp_proxy_health SET revision=revision+1 WHERE proxy_id=$1`, req.ProxyID)
				return err
			})
			if err == nil {
				t.Fatal("证据失败或健康版本改变仍写入退出记录")
			}
			assertContainerExitReceipt(t, path, false)
		})
	}
}
