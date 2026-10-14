package db

import (
	"database/sql"
	"errors"
)

// 人工恢复仍必须取得主体独占锁并核验真实活动为空，不以时间或状态夺权。
type ProxyPolicyRecovery struct {
	Policy  ProxyPolicy
	Token   string
	Request *ProxySwitchRequest
}

func (x *ProxyPolicyExecutor) PendingRecovery(expected int64) (ProxyPolicyRecovery, error) {
	var out ProxyPolicyRecovery
	err := x.x.d.withProxySwitchLifecycle(x.x.conn, func(tx *sql.Tx) error {
		p, e := proxyPolicyTx(tx, x.Subject)
		if e != nil {
			return e
		}
		out.Policy = p
		if p.State.Revision != expected {
			return ErrProxyRevisionConflict
		}
		if e = policyActivitiesIdleTx(tx, p); e != nil {
			return e
		}
		if p.SwitchToken != "" {
			out.Token = p.SwitchToken
			return nil
		}
		r, e := scanProxySwitch(tx.QueryRow(`SELECT `+proxySwitchCols+` FROM proxy_switch_requests r WHERE r.policy_id=$1 AND r.state IN ('pending','draining','applying','recovery_required')`, p.ID))
		if errors.Is(e, sql.ErrNoRows) {
			return ErrProxySwitchRecovery
		}
		if e != nil {
			return e
		}
		if r.State == "pending" || r.State == "draining" {
			return ErrProxyInUse
		}
		if r.BaseRevision != expected || r.ClaimToken == "" {
			return ErrProxySwitchRecovery
		}
		if r.State == "applying" {
			// 此处持有独占执行锁，证明原切换执行者已经退出，但不自动恢复旧运行。
			if _, e = tx.Exec(`UPDATE proxy_switch_requests SET state='recovery_required',error_code='proxy_switch_executor_lost',updated_at=now() WHERE id=$1`, r.ID); e != nil {
				return e
			}
			r.State = "recovery_required"
		}
		r.Subject = x.Subject
		out.Request = &r
		out.Token = r.ClaimToken
		return nil
	})
	return out, err
}

// 替换恢复只接受人工专属策略入口。重新选择与授权预算变更在一次短事务提交。
func (x *ProxyPolicyExecutor) ReplaceRecovery(recovery ProxyPolicyRecovery, patch ProxyPolicyPatch) (ProxyPolicy, error) {
	var out ProxyPolicy
	err := x.x.d.withProxySwitchLifecycle(x.x.conn, func(tx *sql.Tx) error {
		p, e := proxyPolicyTx(tx, x.Subject)
		if e != nil {
			return e
		}
		if p.State.Revision != recovery.Policy.State.Revision {
			return ErrProxyRevisionConflict
		}
		if e = policyActivitiesIdleTx(tx, p); e != nil {
			return e
		}
		if recovery.Request == nil {
			if p.SwitchToken == "" || p.SwitchToken != recovery.Token {
				return ErrProxySwitchRecovery
			}
		} else {
			var matches bool
			if e = tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM proxy_switch_requests WHERE id=$1 AND policy_id=$2 AND claim_token=$3 AND state='recovery_required')`, recovery.Request.ID, p.ID, recovery.Token).Scan(&matches); e != nil {
				return e
			}
			if !matches {
				return ErrProxySwitchRecovery
			}
		}
		target, e := prepareProxyPolicyTx(tx, p, patch)
		if e != nil {
			return e
		}
		if p.State.Revision >= 9007199254740991 {
			return ErrProxyInvalidMode
		}
		target.State.Revision = p.State.Revision + 1
		if e = saveProxyPolicyTx(tx, target); e != nil {
			return e
		}
		if recovery.Request != nil {
			if e = markSwitchFailedTx(tx, recovery.Request.ID, "proxy_replaced_by_manual_policy"); e != nil {
				return e
			}
		}
		if _, e = tx.Exec(`DELETE FROM proxy_policy_group_reservations WHERE policy_id=$1`, p.ID); e != nil {
			return e
		}
		if _, e = tx.Exec(`UPDATE proxy_subject_policies SET switch_token='',target_policy='{}',reserved_proxy_id=NULL WHERE id=$1`, p.ID); e != nil {
			return e
		}
		target.SwitchToken = ""
		out = target
		return nil
	})
	return out, err
}
