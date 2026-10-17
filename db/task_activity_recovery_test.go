package db

import (
	"database/sql"
	"errors"
	"testing"

	"github.com/google/uuid"
)

func TestTaskActivityRecoveryReleasesOnlyOwnedWarpTokens(t *testing.T) {
	first, _, task := plannerRunTestDBs(t)
	if err := first.EnsureProxyActivitySchema(); err != nil {
		t.Fatal(err)
	}
	if err := first.MigrateWarpHealth(t.Context()); err != nil {
		t.Fatal(err)
	}
	proxyID := healthProxy(t, first)
	t.Cleanup(func() { _, _ = first.Exec(`DELETE FROM outbound_proxies WHERE id=$1`, proxyID) })
	other, err := first.CreateTask("其他任务引用", "保留其他任务占用", nil, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = first.DeleteTaskCascade(other.ID, false, false) })
	seed := func(id int64, kinds ...string) {
		t.Helper()
		policy, err := first.GetProxyPolicy(ProxySubject{Kind: "task", ID: id})
		if err != nil {
			t.Fatal(err)
		}
		for _, kind := range kinds {
			token := uuid.NewString()
			if _, err := first.Exec(`INSERT INTO proxy_subject_activity(token,policy_id,revision,proxy_id,backend_pid,kind) VALUES($1,$2,0,$3,2147483647,$4)`, token, policy.ID, proxyID, kind); err != nil {
				t.Fatal(err)
			}
			if _, err := first.Exec(`INSERT INTO warp_proxy_health_activity(activity_token,proxy_id) VALUES($1,$2)`, token, proxyID); err != nil {
				t.Fatal(err)
			}
		}
	}
	seed(task.ID, "task", "io")
	seed(other.ID, "io")
	if _, err := first.Exec(`INSERT INTO task_operation_activity(task_id,active_count,operation_owners) VALUES($1,1,'{"2147483647":1}')`, task.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := first.Exec(`INSERT INTO task_proxy_activity(task_id,proxy_id,active_count) VALUES($1,$2,1)`, task.ID, proxyID); err != nil {
		t.Fatal(err)
	}
	clear := func(id int64) {
		t.Helper()
		executor, err := first.AcquireTaskProxySwitchExecutor(t.Context(), id)
		if err != nil {
			t.Fatal(err)
		}
		defer executor.Close()
		if err := executor.ClearStaleTaskOperations(); err != nil {
			t.Fatal(err)
		}
	}
	clear(task.ID)
	var tokens, taskRefs, proxyRefs int
	if err := first.QueryRow(`SELECT (SELECT count(*) FROM warp_proxy_health_activity WHERE proxy_id=$1),
		(SELECT active_count FROM task_operation_activity WHERE task_id=$2),
		(SELECT count(*) FROM task_proxy_activity WHERE task_id=$2)`, proxyID, task.ID).Scan(&tokens, &taskRefs, &proxyRefs); err != nil {
		t.Fatal(err)
	}
	if tokens != 1 || taskRefs != 0 || proxyRefs != 0 {
		t.Fatalf("恢复未精确释放本任务引用：WARP=%d，操作=%d，代理=%d", tokens, taskRefs, proxyRefs)
	}
	checkIdle := func() error {
		return first.WithWarpHealthLifecycle(t.Context(), func(tx *sql.Tx) error {
			return WarpHealthIdleTx(t.Context(), tx, proxyID)
		})
	}
	if err := checkIdle(); !errors.Is(err, ErrWarpHealthBlocked) {
		t.Fatalf("其他任务的活动未继续保护 WARP：%v", err)
	}
	clear(other.ID)
	if err := checkIdle(); err != nil {
		t.Fatalf("正式恢复后 WARP 仍被幽灵引用阻塞：%v", err)
	}
	if row, err := first.GetTask(task.ID); err != nil || row == nil || row.Status != task.Status || row.Paused != task.Paused || row.ProxyRevision != task.ProxyRevision {
		t.Fatalf("引用恢复改变了任务控制状态或出口版本：%+v，%v", row, err)
	}
}
