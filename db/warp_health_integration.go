package db

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/google/uuid"
)

// CompleteWarpHealthProxyAction 在 claim 原子提交中回写代理档案与真实意图。
func (d *DB) CompleteWarpHealthProxyAction(ctx context.Context, execution *WarpHealthExecution, claim WarpHealthClaim, intent WarpHealthIntent, proxy *Proxy) error {
	if proxy == nil || proxy.ID != claim.ProxyID || proxy.Kind != ProxyKindWarp {
		return ErrWarpHealthBlocked
	}
	if proxy.LastCheckedAt == nil {
		now := time.Now()
		proxy.LastCheckedAt = &now
	}
	return execution.CompleteWarpHealthManual(ctx, claim, intent, func(tx *sql.Tx) error {
		urlValue, err := encryptProxySecret(proxy.URL)
		if err != nil {
			return err
		}
		password, err := encryptProxySecret(proxy.Password)
		if err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `UPDATE outbound_proxies SET url=$2,host=$3,port=$4,username=$5,password=$6,protocol=$7,status=$8,last_error=$9,last_checked_at=$10 WHERE id=$1 AND kind='warp'`, proxy.ID, urlValue, proxy.Host, proxy.Port, proxy.Username, password, proxy.Protocol, proxy.Status, proxy.LastError, proxy.LastCheckedAt)
		if err != nil {
			return err
		}
		n, err := result.RowsAffected()
		if err == nil && n != 1 {
			return errors.Join(ErrWarpHealthBlocked, sql.ErrNoRows)
		}
		return err
	})
}

// CompleteWarpHealthDelete 在 WARP 真实清理返回后提交墓碑和档案删除。
// 删除不再调用 SyncWarpHealthIntentTx，因为健康行会随代理档案级联删除；
// 事务失败时保留 claim，调用方随后只能把它提升为 unknown，禁止恢复旧运行。
func (d *DB) CompleteWarpHealthDelete(ctx context.Context, execution *WarpHealthExecution, claim WarpHealthClaim) error {
	if execution == nil || execution.key != warpExecutionKey(claim.ProxyID) {
		return ErrWarpHealthBlocked
	}
	return execution.withLifecycle(ctx, func(tx *sql.Tx) error {
		state, err := healthTx(ctx, tx, claim.ProxyID)
		if err != nil {
			return err
		}
		if claim.Kind != "repair" || !matchHealthClaim(state, claim) {
			return ErrWarpHealthStale
		}
		proxy, err := getProxyTx(tx, claim.ProxyID)
		if err != nil {
			return err
		}
		if proxy.Kind != ProxyKindWarp {
			return ErrProxyInvalidKind
		}
		if err = checkProxyDeleteReferencesTx(tx, claim.ProxyID); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE warp_proxy_create_requests SET state='deleted',proxy_id=NULL,updated_at=now() WHERE proxy_id=$1`, claim.ProxyID); err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `DELETE FROM outbound_proxies WHERE id=$1 AND kind='warp'`, claim.ProxyID)
		if err != nil {
			return err
		}
		n, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if n != 1 {
			return ErrProxyUnavailable
		}
		return nil
	})
}

// BeginWarpHealthDelete 在执行锁和短事务中先验证所有删除引用，再登记 claim。
func (d *DB) BeginWarpHealthDelete(ctx context.Context, execution *WarpHealthExecution, id int64, guard WarpHealthRepairGuard) (WarpHealthClaim, error) {
	if execution == nil || execution.key != warpExecutionKey(id) {
		return WarpHealthClaim{}, ErrWarpHealthBlocked
	}
	var claim WarpHealthClaim
	err := execution.withLifecycle(ctx, func(tx *sql.Tx) error {
		proxy, err := getProxyTx(tx, id)
		if err != nil || proxy.Kind != ProxyKindWarp {
			return ErrProxyInvalidKind
		}
		if err = checkProxyDeleteReferencesTx(tx, id); err != nil {
			return err
		}
		if err = WarpHealthIdleTx(ctx, tx, id); err != nil {
			return err
		}
		if guard != nil {
			if err = guard(ctx, tx, id); err != nil {
				return err
			}
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO warp_proxy_health(proxy_id) VALUES($1) ON CONFLICT DO NOTHING`, id); err != nil {
			return err
		}
		state, err := healthTx(ctx, tx, id)
		if err != nil {
			return err
		}
		if state.Claim.Token != "" {
			return ErrWarpHealthBlocked
		}
		host, pid := warpHealthOwner()
		claim = WarpHealthClaim{ProxyID: id, Token: uuid.NewString(), Revision: state.Revision, Kind: "repair"}
		_, err = tx.ExecContext(ctx, `UPDATE warp_proxy_health SET claim_token=$2,claim_kind='repair',claim_state='active',state='repairing',egress_ip='',owner_host=$3,owner_pid=$4 WHERE proxy_id=$1`, id, claim.Token, host, pid)
		return err
	})
	return claim, err
}

func (d *DB) SetWarpAutoRecovery(ctx context.Context, id, expectedRevision int64, enabled bool) error {
	return d.WithWarpHealthLifecycle(ctx, func(tx *sql.Tx) error {
		return SetWarpAutoRecoveryTx(ctx, tx, id, expectedRevision, enabled)
	})
}

// BeginWarpHealthCreate 持有新档案首次注册的独立执行锁；pending 创建意图是唯一允许的前置状态。
func (d *DB) BeginWarpHealthCreate(ctx context.Context, execution *WarpHealthExecution, id int64, guard WarpHealthRepairGuard) (WarpHealthClaim, error) {
	if execution == nil || execution.key != warpExecutionKey(id) {
		return WarpHealthClaim{}, ErrWarpHealthBlocked
	}
	var claim WarpHealthClaim
	err := execution.withLifecycle(ctx, func(tx *sql.Tx) error {
		if err := warpHealthLock(ctx, tx); err != nil {
			return err
		}
		proxy, err := getProxyTx(tx, id)
		if err != nil || proxy.Kind != ProxyKindWarp {
			return ErrProxyInvalidKind
		}
		pending, err := warpCreatePendingTx(tx, id)
		if err != nil || !pending {
			return errors.Join(ErrWarpHealthBlocked, err)
		}
		if err = WarpHealthIdleTx(ctx, tx, id); err != nil {
			return err
		}
		if guard != nil {
			if err = guard(ctx, tx, id); err != nil {
				return err
			}
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO warp_proxy_health(proxy_id) VALUES($1) ON CONFLICT DO NOTHING`, id); err != nil {
			return err
		}
		state, err := healthTx(ctx, tx, id)
		if err != nil {
			return err
		}
		if state.Claim.Token != "" {
			return ErrWarpHealthBlocked
		}
		host, pid := warpHealthOwner()
		claim = WarpHealthClaim{ProxyID: id, Token: uuid.NewString(), Revision: state.Revision, Kind: "repair"}
		_, err = tx.ExecContext(ctx, `UPDATE warp_proxy_health SET claim_token=$2,claim_kind='repair',claim_state='active',state='repairing',egress_ip='',owner_host=$3,owner_pid=$4 WHERE proxy_id=$1`, id, claim.Token, host, pid)
		return err
	})
	return claim, err
}

// CompleteWarpHealthCreate 令注册结果、幂等 intent、代理档案和健康意图原子提交。
func (d *DB) CompleteWarpHealthCreate(ctx context.Context, execution *WarpHealthExecution, claim WarpHealthClaim, key, executionToken string, connection *Proxy, failure string, intent WarpHealthIntent) (*Proxy, error) {
	if execution == nil || execution.key != warpExecutionKey(claim.ProxyID) {
		return nil, ErrWarpHealthBlocked
	}
	var out *Proxy
	err := execution.withLifecycle(ctx, func(tx *sql.Tx) error {
		state, err := healthTx(ctx, tx, claim.ProxyID)
		if err != nil {
			return err
		}
		if claim.Kind != "repair" || !matchHealthClaim(state, claim) {
			return ErrWarpHealthStale
		}
		var requestState string
		var requestID sql.NullInt64
		var requestToken sql.NullString
		if err = tx.QueryRowContext(ctx, `SELECT state,proxy_id,execution_token FROM warp_proxy_create_requests WHERE request_key=$1 FOR UPDATE`, key).Scan(&requestState, &requestID, &requestToken); err != nil {
			return err
		}
		if requestState != "pending" || !requestID.Valid || requestID.Int64 != claim.ProxyID || !requestToken.Valid || requestToken.String != executionToken {
			return ErrWarpCreateConflict
		}
		proxy, err := getProxyTx(tx, claim.ProxyID)
		if err != nil {
			return err
		}
		status, nextState := ProxyStatusFailed, "failed"
		if connection != nil {
			if connection.URL == "" || connection.Host == "" || connection.Port <= 0 || connection.Password == "" {
				return ErrProxyUnavailable
			}
			proxy.URL, proxy.Host, proxy.Port, proxy.Username, proxy.Password, proxy.Protocol = connection.URL, connection.Host, connection.Port, connection.Username, connection.Password, "socks5"
			status, nextState, failure = ProxyStatusHealthy, "succeeded", ""
		}
		urlValue, err := encryptProxySecret(proxy.URL)
		if err != nil {
			return err
		}
		password, err := encryptProxySecret(proxy.Password)
		if err != nil {
			return err
		}
		now := time.Now()
		out, err = scanProxy(tx.QueryRowContext(ctx, `UPDATE outbound_proxies SET url=$2,host=$3,port=$4,username=$5,password=$6,protocol=$7,status=$8,last_error=$9,last_checked_at=$10 WHERE id=$1
RETURNING id,name,kind,url,host,port,username,password,resin_platform,resin_admin_url,resin_admin_token,enabled,status,last_error,last_checked_at,created_at,updated_at,protocol`, proxy.ID, urlValue, proxy.Host, proxy.Port, proxy.Username, password, proxy.Protocol, status, failure, now))
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE warp_proxy_create_requests SET state=$2,last_error=$3,updated_at=now() WHERE request_key=$1 AND state='pending'`, key, nextState, failure); err != nil {
			return err
		}
		healthState := "unknown"
		if !intent.Desired && intent.Material != "" {
			healthState = "stopped"
		}
		result, err := tx.ExecContext(ctx, `UPDATE warp_proxy_health SET desired=$2,imported=true,material=$3,profile_revision=$4,revision=revision+1,state=$5,egress_ip='',last_error='',failures=0,recovery_attempts=0,last_checked_at=$6,next_probe_at=NULL,next_recovery_at=NULL,claim_token=NULL,claim_kind='',claim_state='',last_probe_token=NULL,owner_host='',owner_pid=0 WHERE proxy_id=$1 AND revision=$7 AND claim_token=$8`, claim.ProxyID, intent.Desired, intent.Material, intent.ProfileRevision, healthState, now, claim.Revision, claim.Token)
		if err != nil {
			return err
		}
		n, err := result.RowsAffected()
		if err == nil && n != 1 {
			return ErrWarpHealthStale
		}
		return err
	})
	return out, err
}
