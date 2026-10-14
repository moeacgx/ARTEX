package db

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

var (
	ErrWarpHealthBlocked = errors.New("WARP 健康生命周期暂不可操作")
	ErrWarpHealthStale   = errors.New("WARP 健康版本或执行令牌已变化")
)

type WarpHealthPolicy struct {
	ProbeInterval            time.Duration
	FailureThreshold         int
	RecoveryMin, RecoveryMax time.Duration
}

func DefaultWarpHealthPolicy() WarpHealthPolicy {
	return WarpHealthPolicy{time.Minute, 3, 5 * time.Minute, time.Hour}
}
func (p WarpHealthPolicy) Normalized() WarpHealthPolicy {
	// 产品只开放自动维护开关，周期、阈值及退避保持固定。
	return DefaultWarpHealthPolicy()
}
func (p WarpHealthPolicy) RecoveryDelay(attempts int) time.Duration {
	p = p.Normalized()
	d := p.RecoveryMin
	for i := 1; i < attempts && d < p.RecoveryMax; i++ {
		d *= 2
		if d > p.RecoveryMax {
			d = p.RecoveryMax
		}
	}
	return d
}

type WarpHealthIntent struct {
	Desired         bool
	Material        string
	ProfileRevision uint64
}
type WarpHealthClaim struct {
	ProxyID  int64
	Token    string
	Revision int64
	Kind     string
}
type WarpHealthState struct {
	ProxyID                                    int64
	Desired, Imported, AutoRecoverEnabled      bool
	Material                                   string
	ProfileRevision                            uint64
	Revision                                   int64
	State, EgressIP, LastError                 string
	Failures, RecoveryAttempts                 int
	LastCheckedAt, NextProbeAt, NextRecoveryAt *time.Time
	Claim                                      WarpHealthClaim
	ClaimState                                 string
	OwnerHost                                  string
	OwnerPID                                   int
}
type WarpHealthRepairGuard func(context.Context, *sql.Tx, int64) error

// MigrateWarpHealth 不从 healthy 推断意图，旧明确停止档案保持停止。
func (d *DB) MigrateWarpHealth(ctx context.Context) error {
	return d.withWarpHealth(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS warp_proxy_health (
proxy_id bigint PRIMARY KEY REFERENCES outbound_proxies(id) ON DELETE CASCADE,
desired boolean NOT NULL DEFAULT false,
imported boolean NOT NULL DEFAULT false,
auto_recover_enabled boolean NOT NULL DEFAULT true,
material text NOT NULL DEFAULT '',
profile_revision bigint NOT NULL DEFAULT 0,
revision bigint NOT NULL DEFAULT 1 CHECK(revision>0),
state text NOT NULL DEFAULT 'unknown',
egress_ip text NOT NULL DEFAULT '',last_error text NOT NULL DEFAULT '',
failures integer NOT NULL DEFAULT 0 CHECK(failures>=0),
recovery_attempts integer NOT NULL DEFAULT 0 CHECK(recovery_attempts>=0),
last_checked_at timestamptz,next_probe_at timestamptz,next_recovery_at timestamptz,
claim_token uuid,claim_kind text NOT NULL DEFAULT '',claim_state text NOT NULL DEFAULT '',
last_probe_token uuid,
owner_host text NOT NULL DEFAULT '',owner_pid bigint NOT NULL DEFAULT 0,
CHECK ((claim_token IS NULL AND claim_kind='' AND claim_state='') OR (claim_token IS NOT NULL AND claim_kind IN ('probe','repair') AND claim_state IN ('active','unknown')))
);
ALTER TABLE warp_proxy_health ADD COLUMN IF NOT EXISTS owner_host text NOT NULL DEFAULT '';
ALTER TABLE warp_proxy_health ADD COLUMN IF NOT EXISTS owner_pid bigint NOT NULL DEFAULT 0;
CREATE TABLE IF NOT EXISTS warp_proxy_health_activity (
activity_token uuid PRIMARY KEY,
proxy_id bigint NOT NULL REFERENCES outbound_proxies(id) ON DELETE RESTRICT
);
CREATE INDEX IF NOT EXISTS warp_health_activity_proxy_idx ON warp_proxy_health_activity(proxy_id);
INSERT INTO warp_proxy_health(proxy_id,state) SELECT id,CASE WHEN status='stopped' THEN 'stopped' ELSE 'unknown' END FROM outbound_proxies WHERE kind='warp' ON CONFLICT DO NOTHING;`)
		return err
	})
}

func (d *DB) WithWarpHealthLifecycle(ctx context.Context, fn func(*sql.Tx) error) error {
	return d.withWarpHealth(ctx, fn)
}
func (d *DB) withWarpHealth(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := d.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = warpHealthLock(ctx, tx); err != nil {
		return err
	}
	if err = fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}
func warpHealthLock(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, proxyLifecycleLock)
	return err
}

// withWarpHealthConn 在已经持有独立执行 session 的连接上运行短事务。
// 这样执行锁与生命周期锁可以同时存在，而不会在 MaxOpenConns=1 时等待自己。
func withWarpHealthConn(ctx context.Context, conn *sql.Conn, fn func(*sql.Tx) error) error {
	if conn == nil {
		return ErrWarpHealthBlocked
	}
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = warpHealthLock(ctx, tx); err != nil {
		return err
	}
	if err = fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func warpHealthOwner() (string, int) {
	host, err := os.Hostname()
	if err != nil {
		host = ""
	}
	return host, os.Getpid()
}

const healthColumns = `proxy_id,desired,imported,auto_recover_enabled,material,profile_revision,revision,state,egress_ip,last_error,failures,recovery_attempts,last_checked_at,next_probe_at,next_recovery_at,COALESCE(claim_token::text,''),claim_kind,claim_state,owner_host,owner_pid`

func scanWarpHealth(row interface{ Scan(...any) error }) (WarpHealthState, error) {
	var s WarpHealthState
	err := row.Scan(&s.ProxyID, &s.Desired, &s.Imported, &s.AutoRecoverEnabled, &s.Material, &s.ProfileRevision, &s.Revision, &s.State, &s.EgressIP, &s.LastError, &s.Failures, &s.RecoveryAttempts, &s.LastCheckedAt, &s.NextProbeAt, &s.NextRecoveryAt, &s.Claim.Token, &s.Claim.Kind, &s.ClaimState, &s.OwnerHost, &s.OwnerPID)
	s.Claim.ProxyID, s.Claim.Revision = s.ProxyID, s.Revision
	return s, err
}
func healthTx(ctx context.Context, tx *sql.Tx, id int64) (WarpHealthState, error) {
	return scanWarpHealth(tx.QueryRowContext(ctx, `SELECT `+healthColumns+` FROM warp_proxy_health WHERE proxy_id=$1`, id))
}
func (d *DB) WarpHealth(ctx context.Context, id int64) (WarpHealthState, error) {
	return scanWarpHealth(d.QueryRowContext(ctx, `SELECT `+healthColumns+` FROM warp_proxy_health WHERE proxy_id=$1`, id))
}

// DTO 在同一短事务内读取健康屏障、代理状态与 pending，再读取无网络的 runtime 快照。
func WarpHealthViewTx(ctx context.Context, tx *sql.Tx, id int64) (WarpHealthState, *Proxy, bool, error) {
	if err := warpHealthLock(ctx, tx); err != nil {
		return WarpHealthState{}, nil, false, err
	}
	s, err := healthTx(ctx, tx, id)
	if err != nil {
		return s, nil, false, err
	}
	proxy, err := getProxyTx(tx, id)
	if err != nil {
		return s, nil, false, err
	}
	pending, err := warpCreatePendingTx(tx, id)
	return s, proxy, pending, err
}
func (d *DB) WarpHealthProxyIDs(ctx context.Context) ([]int64, error) {
	rows, err := d.QueryContext(ctx, `SELECT id FROM outbound_proxies WHERE kind='warp' ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err = rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func CheckWarpHealthAdmissionTx(ctx context.Context, tx *sql.Tx, id int64) error {
	if id == 0 {
		return nil
	}
	if err := warpHealthLock(ctx, tx); err != nil {
		return err
	}
	p, err := getProxyTx(tx, id)
	if err != nil {
		return err
	}
	if p.Kind != ProxyKindWarp {
		return usableProxy(p)
	}
	s, err := healthTx(ctx, tx, id)
	if err != nil {
		return err
	}
	pending, err := warpCreatePendingTx(tx, id)
	if err != nil {
		return err
	}
	if pending || !s.Desired || s.Material == "" || s.ClaimState == "unknown" || s.Claim.Kind == "repair" {
		return ErrWarpHealthBlocked
	}
	return usableProxy(p)
}
func AcquireWarpHealthActivityTx(ctx context.Context, tx *sql.Tx, id int64, token string) error {
	if id == 0 {
		return nil
	}
	if _, err := uuid.Parse(token); err != nil {
		return ErrWarpHealthBlocked
	}
	if err := CheckWarpHealthAdmissionTx(ctx, tx, id); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO warp_proxy_health_activity(activity_token,proxy_id) VALUES($1,$2)`, token, id)
	return err
}
func ReleaseWarpHealthActivityTx(ctx context.Context, tx *sql.Tx, token string) error {
	if token == "" {
		return nil
	}
	if _, err := uuid.Parse(token); err != nil {
		return ErrWarpHealthBlocked
	}
	if err := warpHealthLock(ctx, tx); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `DELETE FROM warp_proxy_health_activity WHERE activity_token=$1`, token)
	return err
}
func (d *DB) AcquireWarpHealthActivity(ctx context.Context, id int64, token string) error {
	return d.withWarpHealth(ctx, func(tx *sql.Tx) error { return AcquireWarpHealthActivityTx(ctx, tx, id, token) })
}
func (d *DB) ReleaseWarpHealthActivity(ctx context.Context, token string) error {
	return d.withWarpHealth(ctx, func(tx *sql.Tx) error { return ReleaseWarpHealthActivityTx(ctx, tx, token) })
}

func CheckWarpHealthMutationTx(ctx context.Context, tx *sql.Tx, id int64) error {
	if id == 0 {
		return nil
	}
	if err := warpHealthLock(ctx, tx); err != nil {
		return err
	}
	var blocked bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM warp_proxy_health WHERE proxy_id=$1 AND (claim_kind='repair' OR claim_state='unknown'))`, id).Scan(&blocked)
	if err != nil {
		return err
	}
	if blocked {
		return ErrWarpHealthBlocked
	}
	return nil
}
func SyncWarpHealthIntentTx(ctx context.Context, tx *sql.Tx, id int64, intent WarpHealthIntent) error {
	if err := CheckWarpHealthMutationTx(ctx, tx, id); err != nil {
		return err
	}
	p, err := getProxyTx(tx, id)
	if err != nil {
		return err
	}
	if p.Kind != ProxyKindWarp {
		return ErrProxyInvalidKind
	}
	pending, err := warpCreatePendingTx(tx, id)
	if err != nil {
		return err
	}
	// 明确 stop 与未完成创建优先，启用并不代表用户希望运行。
	if p.Status == ProxyStatusStopped || pending {
		intent.Desired = false
	}
	state := ProxyStatusUnknown
	if !intent.Desired {
		state = ProxyStatusStopped
	}
	if pending {
		state = "pending"
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO warp_proxy_health(proxy_id,desired,imported,material,profile_revision,state) VALUES($1,$2,true,$3,$4,$5)
ON CONFLICT(proxy_id) DO UPDATE SET desired=EXCLUDED.desired,imported=true,material=EXCLUDED.material,profile_revision=EXCLUDED.profile_revision,
revision=warp_proxy_health.revision+1,state=EXCLUDED.state,egress_ip='',last_error='',failures=0,recovery_attempts=0,next_probe_at=NULL,next_recovery_at=NULL,
		claim_token=NULL,claim_kind='',claim_state='',last_probe_token=NULL,owner_host='',owner_pid=0`, id, intent.Desired, intent.Material, intent.ProfileRevision, state)
	return err
}
func (d *DB) SyncWarpHealthIntent(ctx context.Context, id int64, intent WarpHealthIntent) error {
	return d.withWarpHealth(ctx, func(tx *sql.Tx) error { return SyncWarpHealthIntentTx(ctx, tx, id, intent) })
}

// 手动动作可显式启动 stopped 档案，也可在关闭自动维护时执行。
// 它仍需真实活动空闲、无 pending/repair/unknown；不得复用自动探测的准入推断。
func BeginWarpHealthManualTx(ctx context.Context, tx *sql.Tx, id int64, guard WarpHealthRepairGuard) (WarpHealthClaim, error) {
	if err := CheckWarpHealthMutationTx(ctx, tx, id); err != nil {
		return WarpHealthClaim{}, err
	}
	proxy, err := getProxyTx(tx, id)
	if err != nil {
		return WarpHealthClaim{}, err
	}
	if proxy.Kind != ProxyKindWarp {
		return WarpHealthClaim{}, ErrProxyInvalidKind
	}
	pending, err := warpCreatePendingTx(tx, id)
	if err != nil {
		return WarpHealthClaim{}, err
	}
	if pending {
		return WarpHealthClaim{}, ErrWarpHealthBlocked
	}
	if err = WarpHealthIdleTx(ctx, tx, id); err != nil {
		return WarpHealthClaim{}, err
	}
	if guard != nil {
		if err = guard(ctx, tx, id); err != nil {
			return WarpHealthClaim{}, err
		}
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO warp_proxy_health(proxy_id) VALUES($1) ON CONFLICT DO NOTHING`, id); err != nil {
		return WarpHealthClaim{}, err
	}
	s, err := healthTx(ctx, tx, id)
	if err != nil {
		return WarpHealthClaim{}, err
	}
	if s.Claim.Token != "" {
		return WarpHealthClaim{}, ErrWarpHealthBlocked
	}
	host, pid := warpHealthOwner()
	// 手动动作复用 repair 屏障，但不累加自动恢复次数或更改维护开关。
	claim := WarpHealthClaim{id, uuid.NewString(), s.Revision, "repair"}
	_, err = tx.ExecContext(ctx, `UPDATE warp_proxy_health SET claim_token=$2,claim_kind='repair',claim_state='active',state='repairing',egress_ip='',last_probe_token=NULL,owner_host=$3,owner_pid=$4 WHERE proxy_id=$1`, id, claim.Token, host, pid)
	return claim, err
}

// finish 仅复用当前事务更新代理连接和状态；不允许执行网络或再次借业务连接。
// 先验证原 token/revision，再完成代理状态、释放 claim、同步意图；任一步失败共同回滚。
func CompleteWarpHealthManualTx(ctx context.Context, tx *sql.Tx, claim WarpHealthClaim, intent WarpHealthIntent, finish func(*sql.Tx) error) error {
	if err := warpHealthLock(ctx, tx); err != nil {
		return err
	}
	s, err := healthTx(ctx, tx, claim.ProxyID)
	if err != nil {
		return err
	}
	if claim.Kind != "repair" || !matchHealthClaim(s, claim) {
		return ErrWarpHealthStale
	}
	if finish != nil {
		if err = finish(tx); err != nil {
			return err
		}
	}
	if _, err = tx.ExecContext(ctx, `UPDATE warp_proxy_health SET claim_token=NULL,claim_kind='',claim_state='',owner_host='',owner_pid=0 WHERE proxy_id=$1`, claim.ProxyID); err != nil {
		return err
	}
	return SyncWarpHealthIntentTx(ctx, tx, claim.ProxyID, intent)
}

func (e *WarpHealthExecution) BeginWarpHealthManual(ctx context.Context, id int64, guard WarpHealthRepairGuard) (WarpHealthClaim, error) {
	if e == nil || e.key != warpExecutionKey(id) {
		return WarpHealthClaim{}, ErrWarpHealthBlocked
	}
	var claim WarpHealthClaim
	err := e.withLifecycle(ctx, func(tx *sql.Tx) error {
		var err error
		claim, err = BeginWarpHealthManualTx(ctx, tx, id, guard)
		return err
	})
	return claim, err
}

func (e *WarpHealthExecution) CompleteWarpHealthManual(ctx context.Context, claim WarpHealthClaim, intent WarpHealthIntent, finish func(*sql.Tx) error) error {
	if e == nil || e.key != warpExecutionKey(claim.ProxyID) {
		return ErrWarpHealthBlocked
	}
	return e.withLifecycle(ctx, func(tx *sql.Tx) error { return CompleteWarpHealthManualTx(ctx, tx, claim, intent, finish) })
}

// 开关 CAS 不改变 Desired；关闭/重新开启均不得抢占 repair 或 unknown。
func SetWarpAutoRecoveryTx(ctx context.Context, tx *sql.Tx, id, expectedRevision int64, enabled bool) error {
	if err := CheckWarpHealthMutationTx(ctx, tx, id); err != nil {
		return err
	}
	s, err := healthTx(ctx, tx, id)
	if err != nil {
		return err
	}
	// JSON 数字必须在 JavaScript 可精确表示的安全整数范围内。
	if expectedRevision <= 0 || expectedRevision > 9007199254740991 || s.Revision != expectedRevision {
		return ErrWarpHealthStale
	}
	if s.Revision >= 9007199254740991 {
		return ErrWarpHealthBlocked
	}
	result, err := tx.ExecContext(ctx, `UPDATE warp_proxy_health SET auto_recover_enabled=$3,revision=revision+1,claim_token=NULL,claim_kind='',claim_state='',last_probe_token=NULL,owner_host='',owner_pid=0,
state=CASE WHEN state='checking' THEN 'unknown' ELSE state END WHERE proxy_id=$1 AND revision=$2`, id, expectedRevision, enabled)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err == nil && n != 1 {
		return ErrWarpHealthStale
	}
	return err
}

func healthEligibleTx(ctx context.Context, tx *sql.Tx, id int64) (WarpHealthState, error) {
	p, err := getProxyTx(tx, id)
	if err != nil {
		return WarpHealthState{}, err
	}
	s, err := healthTx(ctx, tx, id)
	if err != nil {
		return s, err
	}
	pending, err := warpCreatePendingTx(tx, id)
	if err != nil {
		return s, err
	}
	if p.Kind != ProxyKindWarp || !p.Enabled || p.Status == ProxyStatusStopped || !s.Desired || !s.Imported || s.Material == "" || pending {
		return s, ErrWarpHealthBlocked
	}
	return s, nil
}
func (d *DB) BeginWarpHealthProbe(ctx context.Context, id int64, now time.Time) (WarpHealthClaim, error) {
	var c WarpHealthClaim
	err := d.withWarpHealth(ctx, func(tx *sql.Tx) error {
		var err error
		c, err = beginWarpHealthProbeTx(ctx, tx, id, now)
		return err
	})
	return c, err
}

func beginWarpHealthProbeTx(ctx context.Context, tx *sql.Tx, id int64, now time.Time) (WarpHealthClaim, error) {
	s, err := healthEligibleTx(ctx, tx, id)
	if err != nil {
		return WarpHealthClaim{}, err
	}
	if s.Claim.Token != "" || (s.NextProbeAt != nil && now.Before(*s.NextProbeAt)) {
		return WarpHealthClaim{}, ErrWarpHealthBlocked
	}
	host, pid := warpHealthOwner()
	c := WarpHealthClaim{id, uuid.NewString(), s.Revision, "probe"}
	_, err = tx.ExecContext(ctx, `UPDATE warp_proxy_health SET claim_token=$2,claim_kind='probe',claim_state='active',state='checking',owner_host=$3,owner_pid=$4 WHERE proxy_id=$1`, id, c.Token, host, pid)
	return c, err
}
func matchHealthClaim(s WarpHealthState, c WarpHealthClaim) bool {
	return s.Revision == c.Revision && s.Claim.Token == c.Token && s.Claim.Kind == c.Kind && s.ClaimState == "active"
}
func (d *DB) CompleteWarpHealthProbe(ctx context.Context, c WarpHealthClaim, ip string, success bool, now time.Time, policy WarpHealthPolicy) (WarpHealthState, error) {
	var out WarpHealthState
	policy = policy.Normalized()
	err := d.withWarpHealth(ctx, func(tx *sql.Tx) error {
		var err error
		out, err = completeWarpHealthProbeTx(ctx, tx, c, ip, success, now, policy)
		return err
	})
	return out, err
}

func completeWarpHealthProbeTx(ctx context.Context, tx *sql.Tx, c WarpHealthClaim, ip string, success bool, now time.Time, policy WarpHealthPolicy) (WarpHealthState, error) {
	s, err := healthTx(ctx, tx, c.ProxyID)
	if err != nil {
		return s, err
	}
	if c.Kind != "probe" || !matchHealthClaim(s, c) {
		return s, ErrWarpHealthStale
	}
	if _, err = healthEligibleTx(ctx, tx, c.ProxyID); err != nil {
		return s, err
	}
	state, message, failures := "ready", "", 0
	if !success {
		state, message, ip, failures = "failed", "WARP 联网检测失败", "", s.Failures+1
	}
	_, err = tx.ExecContext(ctx, `UPDATE warp_proxy_health SET state=$2,egress_ip=$3,last_error=$4,failures=$5,last_checked_at=$6,next_probe_at=$7,
claim_token=NULL,claim_kind='',claim_state='',last_probe_token=$8,owner_host='',owner_pid=0 WHERE proxy_id=$1`, c.ProxyID, state, ip, message, failures, now, now.Add(policy.ProbeInterval), c.Token)
	if err != nil {
		return s, err
	}
	status := ProxyStatusHealthy
	if !success {
		status = ProxyStatusFailed
	}
	if _, err = tx.ExecContext(ctx, `UPDATE outbound_proxies SET status=$2,last_error=$3,last_checked_at=$4 WHERE id=$1`, c.ProxyID, status, message, now); err != nil {
		return s, err
	}
	return healthTx(ctx, tx, c.ProxyID)
}

func WarpHealthIdleTx(ctx context.Context, tx *sql.Tx, id int64) error {
	if err := warpHealthLock(ctx, tx); err != nil {
		return err
	}
	// task 静态活动/切换与 exclusive_release 均参与；对话与独立 LLM 由统一 token 表保护。
	var busy bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM warp_proxy_health_activity WHERE proxy_id=$1)
OR EXISTS(SELECT 1 FROM task_proxy_activity WHERE proxy_id=$1 AND (active_count>0 OR exclusive_release))
OR EXISTS(SELECT 1 FROM task_operation_activity a JOIN tasks t ON t.id=a.task_id WHERE (a.active_count>0 OR a.switch_token<>'') AND (a.target_id=$1 OR t.proxy_id=$1))`, id).Scan(&busy)
	if err != nil {
		return err
	}
	if busy {
		return ErrWarpHealthBlocked
	}
	return nil
}
func (d *DB) BeginWarpHealthRepair(ctx context.Context, c WarpHealthClaim, now time.Time, p WarpHealthPolicy) (WarpHealthClaim, error) {
	return d.BeginWarpHealthRepairWithGuard(ctx, c, now, p, nil)
}
func (d *DB) BeginWarpHealthRepairWithGuard(ctx context.Context, previous WarpHealthClaim, now time.Time, p WarpHealthPolicy, guard WarpHealthRepairGuard) (WarpHealthClaim, error) {
	var claim WarpHealthClaim
	p = p.Normalized()
	err := d.withWarpHealth(ctx, func(tx *sql.Tx) error {
		var err error
		claim, err = beginWarpHealthRepairTx(ctx, tx, previous, now, p, guard)
		return err
	})
	return claim, err
}

func beginWarpHealthRepairTx(ctx context.Context, tx *sql.Tx, previous WarpHealthClaim, now time.Time, p WarpHealthPolicy, guard WarpHealthRepairGuard) (WarpHealthClaim, error) {
	s, err := healthEligibleTx(ctx, tx, previous.ProxyID)
	if err != nil {
		return WarpHealthClaim{}, err
	}
	if s.Claim.Token != "" || s.Revision != previous.Revision || s.Failures < p.FailureThreshold || !s.AutoRecoverEnabled || (s.NextRecoveryAt != nil && now.Before(*s.NextRecoveryAt)) {
		return WarpHealthClaim{}, ErrWarpHealthBlocked
	}
	var token string
	if err = tx.QueryRowContext(ctx, `SELECT COALESCE(last_probe_token::text,'') FROM warp_proxy_health WHERE proxy_id=$1`, previous.ProxyID).Scan(&token); err != nil {
		return WarpHealthClaim{}, err
	}
	if token != previous.Token {
		return WarpHealthClaim{}, ErrWarpHealthStale
	}
	if err = WarpHealthIdleTx(ctx, tx, previous.ProxyID); err != nil {
		return WarpHealthClaim{}, err
	}
	if guard != nil {
		if err = guard(ctx, tx, previous.ProxyID); err != nil {
			return WarpHealthClaim{}, err
		}
	}
	host, pid := warpHealthOwner()
	claim := WarpHealthClaim{previous.ProxyID, uuid.NewString(), s.Revision, "repair"}
	_, err = tx.ExecContext(ctx, `UPDATE warp_proxy_health SET claim_token=$2,claim_kind='repair',claim_state='active',state='repairing',egress_ip='',last_probe_token=NULL,
recovery_attempts=recovery_attempts+1,next_recovery_at=$3,owner_host=$4,owner_pid=$5 WHERE proxy_id=$1`, claim.ProxyID, claim.Token, now.Add(p.RecoveryDelay(s.RecoveryAttempts+1)), host, pid)
	return claim, err
}
func (d *DB) CompleteWarpHealthRepair(ctx context.Context, c WarpHealthClaim, ip string, success bool, now time.Time, p WarpHealthPolicy) (WarpHealthState, error) {
	var out WarpHealthState
	p = p.Normalized()
	err := d.withWarpHealth(ctx, func(tx *sql.Tx) error {
		var err error
		out, err = completeWarpHealthRepairTx(ctx, tx, c, ip, success, now, p)
		return err
	})
	return out, err
}

func completeWarpHealthRepairTx(ctx context.Context, tx *sql.Tx, c WarpHealthClaim, ip string, success bool, now time.Time, p WarpHealthPolicy) (WarpHealthState, error) {
	s, err := healthTx(ctx, tx, c.ProxyID)
	if err != nil {
		return s, err
	}
	if c.Kind != "repair" || !matchHealthClaim(s, c) {
		return s, ErrWarpHealthStale
	}
	if _, err = healthEligibleTx(ctx, tx, c.ProxyID); err != nil {
		return s, err
	}
	state, message, failures := "ready", "", 0
	if !success {
		state, message, ip, failures = "failed", "WARP 安全重连未完成", "", s.Failures
	}
	_, err = tx.ExecContext(ctx, `UPDATE warp_proxy_health SET state=$2,egress_ip=$3,last_error=$4,failures=$5,last_checked_at=$6,next_probe_at=$7,
claim_token=NULL,claim_kind='',claim_state='',owner_host='',owner_pid=0 WHERE proxy_id=$1`, c.ProxyID, state, ip, message, failures, now, now.Add(p.ProbeInterval))
	if err != nil {
		return s, err
	}
	status := ProxyStatusHealthy
	if !success {
		status = ProxyStatusFailed
	}
	if _, err = tx.ExecContext(ctx, `UPDATE outbound_proxies SET status=$2,last_error=$3,last_checked_at=$4 WHERE id=$1`, c.ProxyID, status, message, now); err != nil {
		return s, err
	}
	return healthTx(ctx, tx, c.ProxyID)
}
func (d *DB) MarkWarpHealthDeferred(ctx context.Context, c WarpHealthClaim) error {
	return d.withWarpHealth(ctx, func(tx *sql.Tx) error {
		_, err := markWarpHealthDeferredTx(ctx, tx, c)
		return err
	})
}
func markWarpHealthDeferredTx(ctx context.Context, tx *sql.Tx, c WarpHealthClaim) (sql.Result, error) {
	return tx.ExecContext(ctx, `UPDATE warp_proxy_health SET state='deferred',egress_ip='',last_error='活动引用或恢复策略阻止重连' WHERE proxy_id=$1 AND revision=$2 AND last_probe_token=$3 AND claim_token IS NULL AND failures>=3`, c.ProxyID, c.Revision, c.Token)
}
func (d *DB) MarkWarpHealthUnknown(ctx context.Context, c WarpHealthClaim) error {
	return d.withWarpHealth(ctx, func(tx *sql.Tx) error {
		return markWarpHealthUnknownTx(ctx, tx, c)
	})
}
func markWarpHealthUnknownTx(ctx context.Context, tx *sql.Tx, c WarpHealthClaim) error {
	r, err := tx.ExecContext(ctx, `UPDATE warp_proxy_health SET claim_state='unknown',state='unknown',egress_ip='',last_error='旧执行退出未确认，需正式人工恢复' WHERE proxy_id=$1 AND revision=$2 AND claim_token=$3 AND claim_kind=$4`, c.ProxyID, c.Revision, c.Token, c.Kind)
	if err != nil {
		return err
	}
	n, err := r.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return ErrWarpHealthStale
	}
	_, err = tx.ExecContext(ctx, `UPDATE outbound_proxies SET status='failed',last_error='WARP 旧执行退出未确认' WHERE id=$1`, c.ProxyID)
	return err
}

// 执行锁为 session 级，专用连接保持到真实执行返回；不是基于时钟的租约。
type WarpHealthExecution struct {
	mu   sync.Mutex
	conn *sql.Conn
	pool *sql.DB
	key  int64
}

func warpExecutionKey(id int64) int64 { return int64(0x5752504800000000) ^ id }
func (d *DB) TryWarpHealthExecution(ctx context.Context, id int64) (*WarpHealthExecution, error) {
	// 仅短暂借业务连接读取实际配置；独立 session 不消耗业务池容量。
	// 不读取环境 DSN，避免实例配置变化后把锁拿到另一个数据库。
	source, err := d.Conn(ctx)
	if err != nil {
		return nil, err
	}
	var config *pgx.ConnConfig
	err = source.Raw(func(raw any) error {
		conn, ok := raw.(*stdlib.Conn)
		if !ok {
			return ErrWarpHealthBlocked
		}
		config = conn.Conn().Config().Copy()
		return nil
	})
	_ = source.Close()
	if err != nil {
		return nil, err
	}
	pool := sql.OpenDB(stdlib.GetConnector(*config))
	pool.SetMaxOpenConns(1)
	pool.SetMaxIdleConns(0)
	conn, err := pool.Conn(ctx)
	if err != nil {
		_ = pool.Close()
		return nil, err
	}
	key := warpExecutionKey(id)
	var ok bool
	if err = conn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock($1)`, key).Scan(&ok); err != nil || !ok {
		_ = conn.Close()
		_ = pool.Close()
		if err != nil {
			return nil, err
		}
		return nil, ErrWarpHealthBlocked
	}
	return &WarpHealthExecution{conn: conn, pool: pool, key: key}, nil
}
func (e *WarpHealthExecution) Close() error {
	if e == nil {
		return nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.conn == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := e.conn.ExecContext(ctx, `SELECT pg_advisory_unlock($1)`, e.key)
	if err != nil {
		_ = e.conn.Raw(func(any) error { return driver.ErrBadConn })
	}
	closeErr := e.conn.Close()
	poolErr := e.pool.Close()
	e.conn = nil
	return errors.Join(err, closeErr, poolErr)
}

func (e *WarpHealthExecution) withLifecycle(ctx context.Context, fn func(*sql.Tx) error) error {
	if e == nil {
		return ErrWarpHealthBlocked
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.conn == nil {
		return ErrWarpHealthBlocked
	}
	return withWarpHealthConn(ctx, e.conn, fn)
}

func (e *WarpHealthExecution) WarpHealth(ctx context.Context, id int64) (WarpHealthState, error) {
	if e == nil {
		return WarpHealthState{}, ErrWarpHealthBlocked
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.conn == nil {
		return WarpHealthState{}, ErrWarpHealthBlocked
	}
	return scanWarpHealth(e.conn.QueryRowContext(ctx, `SELECT `+healthColumns+` FROM warp_proxy_health WHERE proxy_id=$1`, id))
}

func (e *WarpHealthExecution) BeginWarpHealthProbe(ctx context.Context, id int64, now time.Time) (WarpHealthClaim, error) {
	var claim WarpHealthClaim
	err := e.withLifecycle(ctx, func(tx *sql.Tx) error {
		var err error
		claim, err = beginWarpHealthProbeTx(ctx, tx, id, now)
		return err
	})
	return claim, err
}

func (e *WarpHealthExecution) CompleteWarpHealthProbe(ctx context.Context, c WarpHealthClaim, ip string, success bool, now time.Time, p WarpHealthPolicy) (WarpHealthState, error) {
	var out WarpHealthState
	err := e.withLifecycle(ctx, func(tx *sql.Tx) error {
		var err error
		out, err = completeWarpHealthProbeTx(ctx, tx, c, ip, success, now, p.Normalized())
		return err
	})
	return out, err
}

func (e *WarpHealthExecution) BeginWarpHealthRepairWithGuard(ctx context.Context, previous WarpHealthClaim, now time.Time, p WarpHealthPolicy, guard WarpHealthRepairGuard) (WarpHealthClaim, error) {
	var claim WarpHealthClaim
	err := e.withLifecycle(ctx, func(tx *sql.Tx) error {
		var err error
		claim, err = beginWarpHealthRepairTx(ctx, tx, previous, now, p.Normalized(), guard)
		return err
	})
	return claim, err
}

func (e *WarpHealthExecution) CompleteWarpHealthRepair(ctx context.Context, c WarpHealthClaim, ip string, success bool, now time.Time, p WarpHealthPolicy) (WarpHealthState, error) {
	var out WarpHealthState
	err := e.withLifecycle(ctx, func(tx *sql.Tx) error {
		var err error
		out, err = completeWarpHealthRepairTx(ctx, tx, c, ip, success, now, p.Normalized())
		return err
	})
	return out, err
}

func (e *WarpHealthExecution) MarkWarpHealthDeferred(ctx context.Context, c WarpHealthClaim) error {
	return e.withLifecycle(ctx, func(tx *sql.Tx) error {
		_, err := markWarpHealthDeferredTx(ctx, tx, c)
		return err
	})
}

func (e *WarpHealthExecution) MarkWarpHealthUnknown(ctx context.Context, c WarpHealthClaim) error {
	return e.withLifecycle(ctx, func(tx *sql.Tx) error {
		return markWarpHealthUnknownTx(ctx, tx, c)
	})
}

func resolveWarpHealthUnknownTx(ctx context.Context, tx *sql.Tx, c WarpHealthClaim) error {
	s, err := healthTx(ctx, tx, c.ProxyID)
	if err != nil {
		return err
	}
	if s.ClaimState != "unknown" || s.Claim.Token != c.Token || s.Revision != c.Revision {
		return ErrWarpHealthStale
	}
	if err = WarpHealthIdleTx(ctx, tx, c.ProxyID); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE warp_proxy_health SET claim_token=NULL,claim_kind='',claim_state='',revision=revision+1,state='failed',egress_ip='',last_error='人工确认旧执行退出',last_probe_token=NULL,owner_host='',owner_pid=0 WHERE proxy_id=$1`, c.ProxyID)
	return err
}

func (e *WarpHealthExecution) ResolveWarpHealthUnknown(ctx context.Context, c WarpHealthClaim) error {
	if e == nil || e.key != warpExecutionKey(c.ProxyID) {
		return ErrWarpHealthBlocked
	}
	return e.withLifecycle(ctx, func(tx *sql.Tx) error {
		return resolveWarpHealthUnknownTx(ctx, tx, c)
	})
}

// 人工恢复在停止运行器前后都需复核真实活动；回调只能复用传入短事务。
func (e *WarpHealthExecution) CheckIdle(ctx context.Context, id int64, guard WarpHealthRepairGuard) error {
	if e == nil || e.key != warpExecutionKey(id) {
		return ErrWarpHealthBlocked
	}
	return e.withLifecycle(ctx, func(tx *sql.Tx) error {
		if err := WarpHealthIdleTx(ctx, tx, id); err != nil {
			return err
		}
		if guard != nil {
			return guard(ctx, tx, id)
		}
		return nil
	})
}

func (e *WarpHealthExecution) ResolveWarpHealthUnknownWithGuard(ctx context.Context, c WarpHealthClaim, guard WarpHealthRepairGuard) error {
	if e == nil || e.key != warpExecutionKey(c.ProxyID) {
		return ErrWarpHealthBlocked
	}
	return e.withLifecycle(ctx, func(tx *sql.Tx) error {
		if guard != nil {
			if err := guard(ctx, tx, c.ProxyID); err != nil {
				return err
			}
		}
		return resolveWarpHealthUnknownTx(ctx, tx, c)
	})
}

// 仅接受持有执行锁的服务端证明，外部 HTTP 布尔不能绕过 session 独占检查。
func (d *DB) ResolveWarpHealthUnknown(ctx context.Context, c WarpHealthClaim, execution *WarpHealthExecution) error {
	if execution == nil || execution.conn == nil || execution.key != warpExecutionKey(c.ProxyID) {
		return ErrWarpHealthBlocked
	}
	return execution.ResolveWarpHealthUnknown(ctx, c)
}

func (c WarpHealthClaim) String() string {
	return fmt.Sprintf("WARP#%d revision=%d kind=%s", c.ProxyID, c.Revision, c.Kind)
}
