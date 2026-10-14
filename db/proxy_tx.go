package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// 所有代理生命周期变更和任务出口快照都获取同一个事务锁，避免先查后写竞态。
const proxyLifecycleLock int64 = 0x4152544558505258

func lockProxyLifecycle(tx *sql.Tx) error {
	_, err := tx.Exec(`SELECT pg_advisory_xact_lock($1)`, proxyLifecycleLock)
	return err
}

// EnsureProxyActivitySchema 在代理运行时启动时调用；异常退出遗留的占用不自动清零。
func (d *DB) EnsureProxyActivitySchema() error {
	if err := d.withProxyLifecycle(func(tx *sql.Tx) error {
		_, err := tx.Exec(`CREATE TABLE IF NOT EXISTS task_proxy_activity (
task_id bigint PRIMARY KEY REFERENCES tasks(id) ON DELETE CASCADE,
proxy_id bigint NOT NULL REFERENCES outbound_proxies(id) ON DELETE RESTRICT,
active_count bigint NOT NULL CHECK (active_count > 0),
exclusive_release boolean NOT NULL DEFAULT false
);
ALTER TABLE task_proxy_activity ADD COLUMN IF NOT EXISTS exclusive_release boolean NOT NULL DEFAULT false;
CREATE INDEX IF NOT EXISTS task_proxy_activity_proxy_idx ON task_proxy_activity(proxy_id);`)
		return err
	}); err != nil {
		return err
	}
	return d.MigrateTaskProxySwitch()
}

// AcquireTaskProxyActivity 与停删改共锁，从已提交的绑定和快照取得真实出口。
// 是否允许终态/暂停任务执行由上层正式操作准入决定，不能用任务状态代替活动引用。
func (d *DB) AcquireTaskProxyActivity(taskID int64) error {
	return d.acquireTaskProxyActivity(taskID, false)
}

// AcquireIdleTaskProxyActivity 为回收登记独占保护，跨 Manager 或遗留占用均拒绝。
func (d *DB) AcquireIdleTaskProxyActivity(taskID int64) error {
	return d.acquireTaskProxyActivity(taskID, true)
}

func (d *DB) acquireTaskProxyActivity(taskID int64, requireIdle bool) error {
	if taskID <= 0 {
		return ErrProxyUnavailable
	}
	return d.withProxyLifecycle(func(tx *sql.Tx) error {
		return acquireTaskProxyActivityTx(tx, taskID, requireIdle)
	})
}

func acquireTaskProxyActivityTx(tx *sql.Tx, taskID int64, requireIdle bool) error {
	policy, err := proxyPolicyTx(tx, ProxySubject{Kind: "task", ID: taskID})
	if err != nil {
		return err
	}
	if err = proxyPolicyBarrierTx(tx, policy); err != nil {
		return err
	}
	if err = validateProxyPolicyTx(tx, policy); err != nil {
		return err
	}
	var blocked bool
	if err := tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM task_operation_activity WHERE task_id=$1 AND (switch_token<>'' OR ($2 AND active_count>0)))`, taskID, requireIdle).Scan(&blocked); err != nil {
		return err
	}
	if blocked {
		return ErrProxyInUse
	}
	var mode TaskProxyMode
	var bound sql.NullInt64
	var raw []byte
	var present bool
	if err := tx.QueryRow(`SELECT COALESCE(proxy_mode,'inherit'),proxy_id,COALESCE(proxy_snapshot,'{}'::jsonb),deleted_at IS NULL FROM tasks WHERE id=$1`, taskID).Scan(&mode, &bound, &raw, &present); err != nil {
		return err
	}
	if !present {
		return ErrProxyUnavailable
	}
	normalized, err := NormalizeTaskProxyMode(mode)
	if err != nil {
		return err
	}
	if normalized == TaskProxyModeDirect {
		return nil
	}
	var snapshot ProxySnapshot
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		return ErrProxyUnavailable
	}
	id := snapshot.ID
	if bound.Valid {
		if id != 0 && id != bound.Int64 {
			return ErrProxyUnavailable
		}
		id = bound.Int64
	}
	if id == 0 && normalized == TaskProxyModeInherit {
		return nil
	}
	// 回收独占标记也约束其他 Manager；普通引用仍可合法并发累加。
	var busy bool
	if err := tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM task_proxy_activity WHERE task_id=$1 AND (exclusive_release OR ($2 AND active_count>0)))`, taskID, requireIdle).Scan(&busy); err != nil {
		return err
	}
	if busy {
		return ErrProxyInUse
	}
	p, err := getProxyTx(tx, id)
	if err != nil {
		return err
	}
	if err := usableProxy(p); err != nil {
		return err
	}
	if !requireIdle {
		if err := CheckWarpHealthAdmissionTx(context.Background(), tx, id); err != nil {
			return err
		}
	}
	result, err := tx.Exec(`INSERT INTO task_proxy_activity(task_id,proxy_id,active_count,exclusive_release) VALUES($1,$2,1,$3)
ON CONFLICT(task_id) DO UPDATE SET active_count=task_proxy_activity.active_count+1
WHERE task_proxy_activity.proxy_id=EXCLUDED.proxy_id
AND NOT task_proxy_activity.exclusive_release AND NOT EXCLUDED.exclusive_release`, taskID, id, requireIdle)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err == nil && n != 1 {
		return ErrProxyUnavailable
	}
	return err
}

// ReleaseTaskProxyActivity 仅在真实操作退出后调用；context 取消不能提前解除占用。
func (d *DB) ReleaseTaskProxyActivity(taskID int64) error {
	return d.withProxyLifecycle(func(tx *sql.Tx) error {
		return releaseTaskProxyActivityTx(tx, taskID)
	})
}

func releaseTaskProxyActivityTx(tx *sql.Tx, taskID int64) error {
	result, err := tx.Exec(`DELETE FROM task_proxy_activity WHERE task_id=$1 AND active_count=1`, taskID)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil || n != 0 {
		return err
	}
	_, err = tx.Exec(`UPDATE task_proxy_activity SET active_count=active_count-1 WHERE task_id=$1 AND active_count>1`, taskID)
	return err
}

// WithIdleProxy 在任务快照的同一事务锁内执行运行器生命周期操作。
func (d *DB) WithIdleProxy(id int64, fn func(*Proxy) error) (*Proxy, error) {
	var out *Proxy
	err := d.withProxyLifecycle(func(tx *sql.Tx) error {
		p, err := getProxyTx(tx, id)
		if err != nil {
			return err
		}
		// 创建意图与生命周期准入共用当前事务，不在回调中二次借连接。
		pending, err := warpCreatePendingTx(tx, id)
		if err != nil {
			return err
		}
		if pending {
			return ErrProxyInUse
		}
		used, err := proxyInUseTx(tx, id)
		if err != nil {
			return err
		}
		if used {
			return ErrProxyInUse
		}
		global, err := globalProxyIDTx(tx)
		if err != nil {
			return err
		}
		if global != nil && *global == id {
			return ErrProxyInUse
		}
		if err := fn(p); err != nil {
			return err
		}
		u, err := encryptProxySecret(p.URL)
		if err != nil {
			return err
		}
		pw, err := encryptProxySecret(p.Password)
		if err != nil {
			return err
		}
		_, err = tx.Exec(`UPDATE outbound_proxies SET url=$2,host=$3,port=$4,username=$5,password=$6,protocol=$7,status=$8,last_error=$9,last_checked_at=now() WHERE id=$1`, id, u, p.Host, p.Port, p.Username, pw, p.Protocol, p.Status, p.LastError)
		out = p
		return err
	})
	return out, err
}

// ProxyTerminalTasks 仅返回允许人工回收租约的结束任务；不隐式回收运行任务。
func (d *DB) ProxyTerminalTasks(id int64, taskID string) ([]*Task, error) {
	rows, err := d.Query(`SELECT id,proxy_mode,proxy_snapshot,proxy_revision FROM tasks WHERE proxy_id=$1 AND status IN ('done','failed','timeout') AND ($2='' OR id::text=$2)`, id, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Task
	for rows.Next() {
		var t Task
		var raw []byte
		if err := rows.Scan(&t.ID, &t.ProxyMode, &raw, &t.ProxyRevision); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(raw, &t.ProxySnapshot); err != nil {
			return nil, err
		}
		out = append(out, &t)
	}
	if taskID != "" && len(out) == 0 {
		return nil, ErrProxyInUse
	}
	return out, rows.Err()
}

// MigrateLegacyProxy 原子导入旧配置并删除明文，重复启动不再导入。
func (d *DB) MigrateLegacyProxy() error {
	return d.withProxyLifecycle(func(tx *sql.Tx) error {
		var raw string
		err := tx.QueryRow(`SELECT value FROM settings WHERE key='global_proxy'`).Scan(&raw)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		var present bool
		if err = tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM settings WHERE key='global_proxy_id')`).Scan(&present); err != nil {
			return err
		}
		if !present && strings.TrimSpace(raw) != "" {
			kind := ProxyKindHTTP
			if strings.HasPrefix(raw, "https://") {
				kind = ProxyKindHTTPS
			}
			if strings.HasPrefix(raw, "socks5://") {
				kind = ProxyKindSOCKS5
			}
			p := &Proxy{Name: "旧全局代理", Kind: kind, URL: strings.TrimSpace(raw), Enabled: true}
			if err := ValidateProxy(p); err != nil {
				return errors.New("旧 global_proxy 迁移失败：代理配置无效")
			}
			u, err := encryptProxySecret(p.URL)
			if err != nil {
				return err
			}
			pw, err := encryptProxySecret(p.Password)
			if err != nil {
				return err
			}
			var id int64
			if err = tx.QueryRow(`INSERT INTO outbound_proxies(name,kind,protocol,url,host,port,username,password,enabled,status) VALUES($1,$2,$3,$4,$5,$6,$7,$8,true,'unknown') RETURNING id`, p.Name, p.Kind, p.Protocol, u, p.Host, p.Port, p.Username, pw).Scan(&id); err != nil {
				return err
			}
			if _, err = tx.Exec(`INSERT INTO settings(key,value) VALUES('global_proxy_id',$1)`, strconv.FormatInt(id, 10)); err != nil {
				return err
			}
		}
		_, err = tx.Exec(`DELETE FROM settings WHERE key='global_proxy'`)
		return err
	})
}

func (d *DB) withProxyLifecycle(fn func(*sql.Tx) error) error {
	tx, err := d.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := lockProxyLifecycle(tx); err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func globalProxyIDTx(tx *sql.Tx) (*int64, error) {
	var value string
	err := tx.QueryRow(`SELECT value FROM settings WHERE key='global_proxy_id'`).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(value) == "" {
		return nil, nil
	}
	id, err := strconv.ParseInt(value, 10, 64)
	if err != nil || id <= 0 {
		return nil, fmt.Errorf("invalid global proxy id %q", value)
	}
	return &id, nil
}

func getProxyTx(tx *sql.Tx, id int64) (*Proxy, error) {
	if id <= 0 {
		return nil, ErrProxyUnavailable
	}
	p, err := scanProxy(tx.QueryRow(`SELECT id,name,kind,url,host,port,username,password,resin_platform,resin_admin_url,resin_admin_token,enabled,status,last_error,last_checked_at,created_at,updated_at,protocol FROM outbound_proxies WHERE id=$1`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrProxyUnavailable
	}
	return p, err
}

func proxyInUseTx(tx *sql.Tx, id int64) (bool, error) {
	var used bool
	err := tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM tasks WHERE proxy_id=$1 AND deleted_at IS NULL AND status NOT IN ('done','failed','timeout'))
OR EXISTS(SELECT 1 FROM task_proxy_activity WHERE proxy_id=$1 AND active_count>0)
OR EXISTS(SELECT 1 FROM task_operation_activity a JOIN tasks t ON t.id=a.task_id WHERE a.switch_token<>'' AND (a.target_id=$1 OR t.proxy_id=$1))
OR EXISTS(SELECT 1 FROM proxy_subject_activity WHERE proxy_id=$1)
OR EXISTS(SELECT 1 FROM conversations WHERE proxy_id=$1)
OR EXISTS(SELECT 1 FROM proxy_subject_policies p LEFT JOIN tasks t ON t.id=p.task_id LEFT JOIN conversations c ON c.id=p.conversation_id WHERE p.switch_token<>'' AND (p.reserved_proxy_id=$1 OR t.proxy_id=$1 OR c.proxy_id=$1))
OR EXISTS(SELECT 1 FROM proxy_switch_requests WHERE state IN ('pending','draining','applying','recovery_required') AND (from_proxy_id=$1 OR to_proxy_id=$1))`, id).Scan(&used)
	return used, err
}

func usableProxy(p *Proxy) error {
	if p == nil || !p.Enabled || p.Status == ProxyStatusFailed || p.Status == ProxyStatusStopped {
		return ErrProxyUnavailable
	}
	if p.Kind == ProxyKindResin {
		if p.Host == "" || p.Port < 1 || p.Password == "" {
			return ErrProxyUnavailable
		}
		return nil
	}
	if p.Kind == ProxyKindWarp {
		if p.Status != ProxyStatusHealthy {
			return ErrProxyUnavailable
		}
		return nil
	}
	if p.URL == "" {
		return ErrProxyUnavailable
	}
	return nil
}
