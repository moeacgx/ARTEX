package db

import (
	"context"
	"crypto/rand"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
)

type taskOperationLeaseKey struct {
	database *sql.DB
	taskID   int64
}
type taskOperationLease struct {
	conn *sql.Conn
	refs int
	txMu *sync.Mutex
}

type taskOperationSession struct {
	conn  *sql.Conn
	txMu  *sync.Mutex
	tasks int
}

var taskOperationLeases = struct {
	sync.Mutex
	leases   map[taskOperationLeaseKey]*taskOperationLease
	sessions map[*sql.DB]*taskOperationSession
}{leases: map[taskOperationLeaseKey]*taskOperationLease{}, sessions: map[*sql.DB]*taskOperationSession{}}

// 会话屏障使用独立小池，不能占住工具、模型和恢复查询需要的业务连接。
// 连接串只由 Open 的实际参数登记，禁止从后来变化的环境变量推断目标库。
var taskOperationLockPools sync.Map // *sql.DB -> *sql.DB

func (d *DB) initTaskOperationLockPool(dsn string) error {
	pool, err := sql.Open("pgx", dsn)
	if err != nil {
		return err
	}
	pool.SetMaxOpenConns(8)
	pool.SetMaxIdleConns(1)
	pool.SetConnMaxIdleTime(time.Minute)
	taskOperationLockPools.Store(d.DB, pool)
	return nil
}

func (d *DB) taskOperationLockConn(ctx context.Context) (*sql.Conn, error) {
	pool, ok := taskOperationLockPools.Load(d.DB)
	if !ok {
		return nil, ErrProxySwitchRecovery
	}
	bounded, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return pool.(*sql.DB).Conn(bounded)
}

func (d *DB) Close() error {
	var lockErr error
	if pool, ok := taskOperationLockPools.LoadAndDelete(d.DB); ok {
		lockErr = pool.(*sql.DB).Close()
	}
	return errors.Join(lockErr, d.DB.Close())
}

func taskProxySessionKey(id int64) int64 { return int64(0x5357500000000000) ^ id }

func (d *DB) acquireTaskOperationSession(id int64) (*taskOperationLease, bool, error) {
	k := taskOperationLeaseKey{database: d.DB, taskID: id}
	taskOperationLeases.Lock()
	defer taskOperationLeases.Unlock()
	if lease := taskOperationLeases.leases[k]; lease != nil {
		lease.refs++
		return lease, false, nil
	}
	// 每实例仅保留一个共享锁会话；业务池即使只有一条连接也能正常出站。
	session := taskOperationLeases.sessions[d.DB]
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if session == nil {
		conn, err := d.taskOperationLockConn(ctx)
		if err != nil {
			return nil, false, err
		}
		session = &taskOperationSession{conn: conn, txMu: &sync.Mutex{}}
	}
	session.txMu.Lock()
	defer session.txMu.Unlock()
	var locked bool
	if err := session.conn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock_shared($1)`, taskProxySessionKey(id)).Scan(&locked); err != nil || !locked {
		if session.tasks == 0 {
			_ = session.conn.Close()
		}
		if err != nil {
			return nil, false, err
		}
		return nil, false, ErrProxyInUse
	}
	lease := &taskOperationLease{conn: session.conn, refs: 1, txMu: session.txMu}
	session.tasks++
	taskOperationLeases.sessions[d.DB] = session
	taskOperationLeases.leases[k] = lease
	return lease, true, nil
}

func (d *DB) releaseTaskOperationSession(id int64) error {
	k := taskOperationLeaseKey{database: d.DB, taskID: id}
	taskOperationLeases.Lock()
	defer taskOperationLeases.Unlock()
	lease := taskOperationLeases.leases[k]
	if lease == nil {
		return nil
	}
	lease.refs--
	if lease.refs > 0 {
		return nil
	}
	lease.txMu.Lock()
	defer lease.txMu.Unlock()
	var unlocked bool
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := lease.conn.QueryRowContext(ctx, `SELECT pg_advisory_unlock_shared($1)`, taskProxySessionKey(id)).Scan(&unlocked); err != nil || !unlocked {
		_ = lease.conn.Raw(func(any) error { return driver.ErrBadConn })
		delete(taskOperationLeases.leases, k)
		delete(taskOperationLeases.sessions, d.DB)
		_ = lease.conn.Close()
		if err != nil {
			return err
		}
		return ErrProxySwitchRecovery
	}
	delete(taskOperationLeases.leases, k)
	session := taskOperationLeases.sessions[d.DB]
	if session != nil {
		session.tasks--
		if session.tasks > 0 {
			return nil
		}
	}
	delete(taskOperationLeases.sessions, d.DB)
	return lease.conn.Close()
}

// 每任务会话锁只排斥同任务的切换执行者；网络期间没有生命周期事务锁。
type TaskProxySwitchExecutor struct {
	d    *DB
	conn *sql.Conn
	id   int64
	key  int64
}

func (d *DB) AcquireTaskProxySwitchExecutor(ctx context.Context, id int64) (*TaskProxySwitchExecutor, error) {
	conn, err := d.taskOperationLockConn(ctx)
	if err != nil {
		return nil, err
	}
	key := taskProxySessionKey(id)
	var locked bool
	if err = conn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock($1)`, key).Scan(&locked); err != nil || !locked {
		conn.Close()
		if err != nil {
			return nil, err
		}
		return nil, ErrProxyInUse
	}
	return &TaskProxySwitchExecutor{d: d, conn: conn, id: id, key: key}, nil
}

func (x *TaskProxySwitchExecutor) Close() {
	// 解除失败时丢弃连接，不能把仍持会话锁的连接还回池。
	var unlocked bool
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := x.conn.QueryRowContext(ctx, `SELECT pg_advisory_unlock($1)`, x.key).Scan(&unlocked); err != nil || !unlocked {
		_ = x.conn.Raw(func(any) error { return driver.ErrBadConn })
	}
	_ = x.conn.Close()
}

func (d *DB) withProxySwitchLifecycle(conn *sql.Conn, fn func(*sql.Tx) error) error {
	if conn == nil {
		return d.withProxyLifecycle(fn)
	}
	tx, err := conn.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = lockProxyLifecycle(tx); err != nil {
		return err
	}
	if err = fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func (x *TaskProxySwitchExecutor) Begin(expected int64, mode TaskProxyMode, id *int64) (TaskProxyState, string, error) {
	return x.d.beginTaskProxySwitch(x.conn, x.id, expected, mode, id)
}
func (x *TaskProxySwitchExecutor) Pending() (TaskProxyState, string, error) {
	return x.d.pendingTaskProxySwitch(x.conn, x.id)
}
func (x *TaskProxySwitchExecutor) Complete(expected int64, token string) (TaskProxyState, error) {
	return x.d.completeTaskProxySwitch(x.conn, x.id, expected, token)
}
func (x *TaskProxySwitchExecutor) AbortToDirect(expected int64, token string) (TaskProxyState, error) {
	return x.d.abortTaskProxySwitchToDirect(x.conn, x.id, expected, token)
}

func (x *TaskProxySwitchExecutor) AcquireIdleTaskProxyActivity() error {
	return x.d.withProxySwitchLifecycle(x.conn, func(tx *sql.Tx) error { return acquireTaskProxyActivityTx(tx, x.id, true) })
}
func (x *TaskProxySwitchExecutor) AcquireIdleTaskProxyActivityRevision(expected int64) error {
	return x.d.withProxySwitchLifecycle(x.conn, func(tx *sql.Tx) error {
		state, e := taskProxyStateTx(tx, x.id)
		if e != nil {
			return e
		}
		if state.Revision != expected {
			return ErrProxyRevisionConflict
		}
		if state.Mode == TaskProxyModeGroup {
			// 清理只关闭旧本地资源，撤权不能阻止清理；仍持独占会话锁与持久占用。
			policy, err := proxyPolicyTx(tx, ProxySubject{Kind: "task", ID: x.id})
			if err != nil {
				return err
			}
			if err = proxyPolicyBarrierTx(tx, policy); err != nil {
				return err
			}
			if err = policyActivitiesIdleTx(tx, policy); err != nil {
				return err
			}
			if state.ID == nil || *state.ID <= 0 {
				return ErrProxyUnavailable
			}
			_, err = tx.Exec(`INSERT INTO task_proxy_activity(task_id,proxy_id,active_count,exclusive_release) VALUES($1,$2,1,true)`, x.id, *state.ID)
			return err
		}
		return acquireTaskProxyActivityTx(tx, x.id, true)
	})
}
func (x *TaskProxySwitchExecutor) ReleaseTaskProxyActivity() error {
	return x.d.withProxySwitchLifecycle(x.conn, func(tx *sql.Tx) error { return releaseTaskProxyActivityTx(tx, x.id) })
}

var ErrProxyRevisionConflict = errors.New("任务代理版本已变化")
var ErrProxySwitchRecovery = errors.New("任务代理切换需人工恢复")
var ErrProxyActivityToken = errors.New("原操作活动 token 不存在")

// 持久屏障独立于代理引用，直连任务也必须保护真实在途操作。
func (d *DB) MigrateTaskProxySwitch() error {
	return d.withProxyLifecycle(func(tx *sql.Tx) error {
		_, err := tx.Exec(`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS proxy_revision bigint NOT NULL DEFAULT 0 CHECK(proxy_revision>=0);
CREATE TABLE IF NOT EXISTS task_operation_activity (
 task_id bigint PRIMARY KEY REFERENCES tasks(id) ON DELETE CASCADE,
 active_count bigint NOT NULL DEFAULT 0 CHECK(active_count>=0),
 operation_owners jsonb NOT NULL DEFAULT '{}'::jsonb,
 switch_token text NOT NULL DEFAULT '',
 target_mode text NOT NULL DEFAULT '', target_id bigint REFERENCES outbound_proxies(id) ON DELETE RESTRICT,
 target_snapshot jsonb NOT NULL DEFAULT '{}'::jsonb
);
ALTER TABLE task_operation_activity ADD COLUMN IF NOT EXISTS operation_owners jsonb NOT NULL DEFAULT '{}'::jsonb;`)
		return err
	})
}

type TaskProxyState struct {
	Mode           TaskProxyMode
	ID             *int64
	Snapshot       ProxySnapshot
	Revision       int64
	OperationToken string `json:"-"`
}

func taskProxyStateTx(tx *sql.Tx, id int64) (TaskProxyState, error) {
	var s TaskProxyState
	var raw []byte
	err := tx.QueryRow(`SELECT proxy_mode,proxy_id,proxy_snapshot,proxy_revision FROM tasks WHERE id=$1 AND deleted_at IS NULL`, id).Scan(&s.Mode, &s.ID, &raw, &s.Revision)
	if err == nil {
		err = json.Unmarshal(raw, &s.Snapshot)
	}
	return s, err
}

// 普通准入与切换共锁，返回准入时的权威快照，保证另一 Manager 的缓存更新。
func (d *DB) AcquireTaskOperation(id int64) (TaskProxyState, error) {
	var s TaskProxyState
	lease, _, err := d.acquireTaskOperationSession(id)
	if err != nil {
		return s, err
	}
	lease.txMu.Lock()
	err = d.withProxySwitchLifecycle(lease.conn, func(tx *sql.Tx) error {
		var err error
		policy, err := proxyPolicyTx(tx, ProxySubject{Kind: "task", ID: id})
		if err != nil {
			return err
		}
		if err = proxyPolicyBarrierTx(tx, policy); err != nil {
			return err
		}
		if err = validateProxyPolicyTx(tx, policy); err != nil {
			return err
		}
		if err = checkProxyActivityOwnersTx(tx, policy); err != nil {
			return err
		}
		s, err = taskProxyStateTx(tx, id)
		if err != nil {
			return err
		}
		var switching bool
		if err = tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM task_operation_activity WHERE task_id=$1 AND switch_token<>'')`, id).Scan(&switching); err != nil {
			return err
		}
		if switching {
			return ErrProxySwitchRecovery
		}
		// 每份持久引用须对应仍持共享锁的会话，禁止两个新 Manager 洗掉残留。
		var stale bool
		err = tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM task_operation_activity a WHERE task_id=$1 AND (
a.active_count<>COALESCE((SELECT sum(value::bigint) FROM jsonb_each_text(a.operation_owners)),0)
OR EXISTS(SELECT 1 FROM jsonb_each_text(a.operation_owners) o WHERE NOT EXISTS(
SELECT 1 FROM pg_locks l WHERE l.locktype='advisory' AND l.database=(SELECT oid FROM pg_database WHERE datname=current_database())
AND l.classid=(($2::bigint >> 32) & 4294967295)::oid AND l.objid=($2::bigint & 4294967295)::oid AND l.objsubid=1 AND l.granted AND l.mode='ShareLock' AND l.pid=o.key::int))))`, id, taskProxySessionKey(id)).Scan(&stale)
		if err != nil {
			return err
		}
		if stale {
			return ErrProxySwitchRecovery
		}
		if err = acquireTaskProxyActivityTx(tx, id, false); err != nil {
			return err
		}
		_, err = tx.Exec(`INSERT INTO task_operation_activity(task_id,active_count,operation_owners) VALUES($1,1,jsonb_build_object(pg_backend_pid()::text,1)) ON CONFLICT(task_id) DO UPDATE SET active_count=task_operation_activity.active_count+1,
operation_owners=jsonb_set(task_operation_activity.operation_owners,ARRAY[pg_backend_pid()::text],to_jsonb(COALESCE((task_operation_activity.operation_owners->>pg_backend_pid()::text)::bigint,0)+1))`, id)
		if err != nil {
			return err
		}
		s.OperationToken = uuid.NewString()
		if s.ID != nil {
			if err = AcquireWarpHealthActivityTx(context.Background(), tx, *s.ID, s.OperationToken); err != nil {
				return err
			}
		}
		_, err = tx.Exec(`INSERT INTO proxy_subject_activity(token,policy_id,revision,proxy_id,backend_pid,kind) VALUES($1,$2,$3,$4,pg_backend_pid(),'task')`, s.OperationToken, policy.ID, s.Revision, s.ID)
		return err
	})
	lease.txMu.Unlock()
	if err != nil {
		_ = d.releaseTaskOperationSession(id)
	}
	return s, err
}

func (d *DB) ReleaseTaskOperation(id int64) error {
	return d.ReleaseTaskOperationToken(id, "")
}

func (d *DB) ReleaseTaskOperationToken(id int64, token string) error {
	k := taskOperationLeaseKey{database: d.DB, taskID: id}
	taskOperationLeases.Lock()
	lease := taskOperationLeases.leases[k]
	taskOperationLeases.Unlock()
	if lease == nil {
		return ErrProxySwitchRecovery
	}
	lease.txMu.Lock()
	missingToken := false
	err := d.withProxySwitchLifecycle(lease.conn, func(tx *sql.Tx) error {
		if token == "" {
			if err := tx.QueryRow(`SELECT a.token FROM proxy_subject_activity a JOIN proxy_subject_policies p ON p.id=a.policy_id WHERE p.task_id=$1 AND a.backend_pid=pg_backend_pid() AND a.kind='task' ORDER BY a.created_at,a.token LIMIT 1`, id).Scan(&token); err != nil {
				missingToken = errors.Is(err, sql.ErrNoRows)
				return ErrProxySwitchRecovery
			}
		}
		var matches bool
		if err := tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM proxy_subject_activity a JOIN proxy_subject_policies p ON p.id=a.policy_id WHERE a.token=$1 AND p.task_id=$2 AND a.backend_pid=pg_backend_pid() AND a.kind='task')`, token, id).Scan(&matches); err != nil {
			return err
		}
		if !matches {
			missingToken = true
			return ErrProxySwitchRecovery
		}
		var owned int64
		if err := tx.QueryRow(`SELECT COALESCE((operation_owners->>pg_backend_pid()::text)::bigint,0) FROM task_operation_activity WHERE task_id=$1`, id).Scan(&owned); err != nil {
			return err
		}
		if owned <= 0 {
			return ErrProxySwitchRecovery
		}
		if err := releaseTaskProxyActivityTx(tx, id); err != nil {
			return err
		}
		_, err := tx.Exec(`UPDATE task_operation_activity SET active_count=active_count-1,
operation_owners=CASE WHEN $2=1 THEN operation_owners-pg_backend_pid()::text ELSE jsonb_set(operation_owners,ARRAY[pg_backend_pid()::text],to_jsonb($2::bigint-1)) END WHERE task_id=$1 AND active_count>0`, id, owned)
		if err != nil {
			return err
		}
		_, err = tx.Exec(`DELETE FROM proxy_subject_activity WHERE token=$1`, token)
		if err != nil {
			return err
		}
		err = ReleaseWarpHealthActivityTx(context.Background(), tx, token)
		return err
	})
	lease.txMu.Unlock()
	if missingToken {
		return errors.Join(err, ErrProxyActivityToken)
	}
	if err != nil {
		// 数据计数失败时保留持久占用，但释放会话锁；这样人工独占恢复可
		// 在确认没有真实操作后清理残留，而不是被已结束的操作永久锁住。
		return errors.Join(err, d.releaseTaskOperationSession(id))
	}
	return d.releaseTaskOperationSession(id)
}

func (x *TaskProxySwitchExecutor) ClearStaleTaskOperations() error {
	return x.d.withProxySwitchLifecycle(x.conn, func(tx *sql.Tx) error {
		if _, err := tx.Exec(`DELETE FROM task_proxy_activity WHERE task_id=$1`, x.id); err != nil {
			return err
		}
		if _, err := tx.Exec(`DELETE FROM proxy_subject_activity WHERE policy_id=(SELECT id FROM proxy_subject_policies WHERE task_id=$1)`, x.id); err != nil {
			return err
		}
		_, err := tx.Exec(`UPDATE task_operation_activity SET active_count=0,operation_owners='{}'::jsonb WHERE task_id=$1 AND switch_token=''`, x.id)
		return err
	})
}

// BeginTaskProxySwitch 先检查 CAS，再采样默认，目标和独占意图同事务持久化。
func (d *DB) BeginTaskProxySwitch(id, expected int64, mode TaskProxyMode, selected *int64) (TaskProxyState, string, error) {
	return d.beginTaskProxySwitch(nil, id, expected, mode, selected)
}

func (d *DB) beginTaskProxySwitch(conn *sql.Conn, id, expected int64, mode TaskProxyMode, selected *int64) (TaskProxyState, string, error) {
	var old TaskProxyState
	var token string
	err := d.withProxySwitchLifecycle(conn, func(tx *sql.Tx) error {
		var err error
		old, err = taskProxyStateTx(tx, id)
		if err != nil {
			return err
		}
		if old.Revision != expected {
			return ErrProxyRevisionConflict
		}
		if old.Revision >= 9007199254740991 {
			return ErrProxyInvalidMode
		}
		var status string
		var queued, paused bool
		if err = tx.QueryRow(`SELECT status,queued,paused FROM tasks WHERE id=$1`, id).Scan(&status, &queued, &paused); err != nil {
			return err
		}
		if queued || (status == "running" && !paused) || (status != "paused" && !paused && status != "done" && status != "failed" && status != "timeout") {
			return ErrProxyInUse
		}
		var busy, switching, staleOwners bool
		if err = tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM task_operation_activity WHERE task_id=$1 AND active_count>0) OR EXISTS(SELECT 1 FROM task_proxy_activity WHERE task_id=$1),
EXISTS(SELECT 1 FROM task_operation_activity WHERE task_id=$1 AND switch_token<>''),
EXISTS(SELECT 1 FROM task_operation_activity WHERE task_id=$1 AND active_count=0 AND operation_owners<>'{}'::jsonb)`, id).Scan(&busy, &switching, &staleOwners); err != nil {
			return err
		}
		if switching {
			return ErrProxySwitchRecovery
		}
		if staleOwners {
			return ErrProxySwitchRecovery
		}
		if busy {
			return ErrProxyInUse
		}
		normalized, err := NormalizeTaskProxyMode(mode)
		if err != nil {
			return err
		}
		if normalized == TaskProxyModeGroup || old.Mode == TaskProxyModeGroup {
			return ErrProxyAuthorization
		}
		if normalized != TaskProxyModeFixed && selected != nil {
			return ErrProxyInvalidMode
		}
		if normalized == TaskProxyModeFixed && (selected == nil || *selected <= 0) {
			return ErrProxyIDRequired
		}
		targetID := selected
		if normalized == TaskProxyModeInherit {
			targetID, err = globalProxyIDTx(tx)
			if err != nil {
				return err
			}
		}
		var snap ProxySnapshot
		if targetID != nil {
			p, err := getProxyTx(tx, *targetID)
			if err != nil {
				return err
			}
			if err = usableProxy(p); err != nil {
				return err
			}
			snap, err = p.SnapshotEncrypted()
			if err != nil {
				return err
			}
		}
		raw, err := json.Marshal(snap)
		if err != nil {
			return err
		}
		var key [16]byte
		if _, err = rand.Read(key[:]); err != nil {
			return err
		}
		token = hex.EncodeToString(key[:])
		_, err = tx.Exec(`INSERT INTO task_operation_activity(task_id,switch_token,target_mode,target_id,target_snapshot) VALUES($1,$2,$3,$4,$5) ON CONFLICT(task_id) DO UPDATE SET switch_token=EXCLUDED.switch_token,target_mode=EXCLUDED.target_mode,target_id=EXCLUDED.target_id,target_snapshot=EXCLUDED.target_snapshot`, id, token, normalized, targetID, raw)
		return err
	})
	return old, token, err
}

// 生命周期准入不能在锁外把已封锁任务重新置为 running/queued。
func (d *DB) WithTaskProxyMutation(id int64, fn func(*sql.Tx) error) error {
	return d.withProxyLifecycle(func(tx *sql.Tx) error {
		policy, err := proxyPolicyTx(tx, ProxySubject{Kind: "task", ID: id})
		if err != nil {
			return err
		}
		if err = proxyPolicyBarrierTx(tx, policy); err != nil {
			return err
		}
		var switching bool
		if err := tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM task_operation_activity WHERE task_id=$1 AND switch_token<>'')`, id).Scan(&switching); err != nil {
			return err
		}
		if switching {
			return ErrProxyInUse
		}
		return fn(tx)
	})
}

func (d *DB) ListTaskProxyStates() (map[int64]TaskProxyState, error) {
	rows, err := d.Query(`SELECT id,proxy_mode,proxy_id,proxy_snapshot,proxy_revision FROM tasks WHERE deleted_at IS NULL`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int64]TaskProxyState{}
	for rows.Next() {
		var id int64
		var state TaskProxyState
		var raw []byte
		if err = rows.Scan(&id, &state.Mode, &state.ID, &raw, &state.Revision); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(raw, &state.Snapshot); err != nil {
			return nil, err
		}
		out[id] = state
	}
	return out, rows.Err()
}

func (d *DB) PendingTaskProxySwitch(id int64) (TaskProxyState, string, error) {
	return d.pendingTaskProxySwitch(nil, id)
}

func (d *DB) pendingTaskProxySwitch(conn *sql.Conn, id int64) (TaskProxyState, string, error) {
	var s TaskProxyState
	var token string
	err := d.withProxySwitchLifecycle(conn, func(tx *sql.Tx) error {
		var err error
		s, err = taskProxyStateTx(tx, id)
		if err != nil {
			return err
		}
		var owners []byte
		err = tx.QueryRow(`SELECT switch_token,operation_owners FROM task_operation_activity WHERE task_id=$1 AND switch_token<>'' AND active_count=0`, id).Scan(&token, &owners)
		if err == nil && string(owners) != "{}" {
			return ErrProxySwitchRecovery
		}
		return err
	})
	return s, token, err
}

// 完成时重验目标，失败保持独占屏障，不能跑回已经释放的旧出口。
func (d *DB) CompleteTaskProxySwitch(id, expected int64, token string) (TaskProxyState, error) {
	return d.completeTaskProxySwitch(nil, id, expected, token)
}

func (d *DB) abortTaskProxySwitchToDirect(conn *sql.Conn, id, expected int64, token string) (TaskProxyState, error) {
	var out TaskProxyState
	err := d.withProxySwitchLifecycle(conn, func(tx *sql.Tx) error {
		old, err := taskProxyStateTx(tx, id)
		if err != nil {
			return err
		}
		if old.Revision != expected {
			return ErrProxyRevisionConflict
		}
		if old.Mode == TaskProxyModeGroup {
			return ErrProxyAuthorization
		}
		var pending string
		var owners []byte
		if err = tx.QueryRow(`SELECT switch_token,operation_owners FROM task_operation_activity WHERE task_id=$1 AND switch_token<>'' AND active_count=0`, id).Scan(&pending, &owners); err != nil || pending != token || string(owners) != "{}" {
			return ErrProxySwitchRecovery
		}
		if _, err = tx.Exec(`UPDATE tasks SET proxy_mode='direct',proxy_id=NULL,proxy_snapshot='{}'::jsonb,proxy_revision=proxy_revision+1 WHERE id=$1`, id); err != nil {
			return err
		}
		if _, err = tx.Exec(`UPDATE task_operation_activity SET switch_token='',target_mode='',target_id=NULL,target_snapshot='{}'::jsonb WHERE task_id=$1`, id); err != nil {
			return err
		}
		out, err = taskProxyStateTx(tx, id)
		return err
	})
	return out, err
}

func (d *DB) completeTaskProxySwitch(conn *sql.Conn, id, expected int64, token string) (TaskProxyState, error) {
	var out TaskProxyState
	err := d.withProxySwitchLifecycle(conn, func(tx *sql.Tx) error {
		old, err := taskProxyStateTx(tx, id)
		if err != nil {
			return err
		}
		if old.Revision != expected {
			return ErrProxyRevisionConflict
		}
		var mode TaskProxyMode
		var selected *int64
		var raw []byte
		var owners []byte
		if err = tx.QueryRow(`SELECT target_mode,target_id,target_snapshot,operation_owners FROM task_operation_activity WHERE task_id=$1 AND switch_token=$2 AND switch_token<>'' AND active_count=0`, id, token).Scan(&mode, &selected, &raw, &owners); err != nil || string(owners) != "{}" {
			return ErrProxySwitchRecovery
		}
		if selected != nil {
			p, err := getProxyTx(tx, *selected)
			if err != nil {
				return err
			}
			if err = usableProxy(p); err != nil {
				return err
			}
		}
		if _, err = tx.Exec(`UPDATE tasks SET proxy_mode=$2,proxy_id=$3,proxy_snapshot=$4,proxy_revision=proxy_revision+1 WHERE id=$1`, id, mode, selected, raw); err != nil {
			return err
		}
		if _, err = tx.Exec(`UPDATE task_operation_activity SET switch_token='',target_mode='',target_id=NULL,target_snapshot='{}'::jsonb WHERE task_id=$1`, id); err != nil {
			return err
		}
		out, err = taskProxyStateTx(tx, id)
		return err
	})
	return out, err
}
