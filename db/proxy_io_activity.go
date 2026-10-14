package db

import (
	"context"
	"database/sql"

	"github.com/google/uuid"
)

func checkProxyActivityOwnersTx(tx *sql.Tx, p ProxyPolicy) error {
	var stale bool
	err := tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM proxy_subject_activity a WHERE a.policy_id=$1 AND NOT EXISTS(SELECT 1 FROM pg_locks l WHERE l.locktype='advisory' AND l.database=(SELECT oid FROM pg_database WHERE datname=current_database()) AND l.classid=(($2::bigint >> 32)&4294967295)::oid AND l.objid=($2::bigint&4294967295)::oid AND l.objsubid=1 AND l.granted AND l.mode='ShareLock' AND l.pid=a.backend_pid))`, p.ID, taskProxySessionKey(p.Subject.lockID())).Scan(&stale)
	if err == nil && stale {
		return ErrProxySwitchRecovery
	}
	return err
}

// 最终模型出口单独登记：inherit 渠道可能与工具不同，不能只用静态任务绑定保护。
func (d *DB) AcquireProxyIOOperation(s ProxySubject, revision int64, proxyID *int64) (string, error) {
	lease, _, err := d.acquireTaskOperationSession(s.lockID())
	if err != nil {
		return "", err
	}
	token := uuid.NewString()
	lease.txMu.Lock()
	err = d.withProxySwitchLifecycle(lease.conn, func(tx *sql.Tx) error {
		p, e := proxyPolicyTx(tx, s)
		if e != nil {
			return e
		}
		if p.State.Revision != revision {
			return ErrProxyRevisionConflict
		}
		if e = proxyPolicyBarrierTx(tx, p); e != nil {
			return e
		}
		if e = validateProxyPolicyTx(tx, p); e != nil {
			return e
		}
		if e = checkProxyActivityOwnersTx(tx, p); e != nil {
			return e
		}
		if p.State.Mode == TaskProxyModeGroup && (proxyID == nil || p.State.ID == nil || *proxyID != *p.State.ID) {
			return ErrProxyAuthorization
		}
		if proxyID != nil {
			if e = AcquireWarpHealthActivityTx(context.Background(), tx, *proxyID, token); e != nil {
				return e
			}
			proxy, e := getProxyTx(tx, *proxyID)
			if e != nil {
				return e
			}
			if e = usableProxy(proxy); e != nil {
				return e
			}
		}
		_, e = tx.Exec(`INSERT INTO proxy_subject_activity(token,policy_id,revision,proxy_id,backend_pid) VALUES($1,$2,$3,$4,pg_backend_pid())`, token, p.ID, revision, proxyID)
		return e
	})
	lease.txMu.Unlock()
	if err != nil {
		_ = d.releaseTaskOperationSession(s.lockID())
	}
	return token, err
}
func (d *DB) ReleaseProxyIOOperation(s ProxySubject, token string) error {
	k := taskOperationLeaseKey{database: d.DB, taskID: s.lockID()}
	taskOperationLeases.Lock()
	lease := taskOperationLeases.leases[k]
	taskOperationLeases.Unlock()
	if lease == nil {
		return ErrProxySwitchRecovery
	}
	lease.txMu.Lock()
	err := d.withProxySwitchLifecycle(lease.conn, func(tx *sql.Tx) error {
		_, col, e := s.table()
		if e != nil {
			return e
		}
		result, e := tx.Exec(`DELETE FROM proxy_subject_activity WHERE token=$1 AND policy_id=(SELECT id FROM proxy_subject_policies WHERE `+col+`=$2) AND backend_pid=pg_backend_pid()`, token, s.ID)
		if e != nil {
			return e
		}
		if e = ReleaseWarpHealthActivityTx(context.Background(), tx, token); e != nil {
			return e
		}
		n, e := result.RowsAffected()
		if e == nil && n != 1 {
			return ErrProxySwitchRecovery
		}
		return e
	})
	lease.txMu.Unlock()
	if err != nil {
		return err
	}
	return d.releaseTaskOperationSession(s.lockID())
}

func (d *DB) ProxyFailureEvidence(s ProxySubject) ([]ProxyFailureEvidence, error) {
	p, e := d.GetProxyPolicy(s)
	if e != nil {
		return nil, e
	}
	rows, e := d.Query(`SELECT kind,id FROM proxy_failure_evidence WHERE policy_id=$1 AND revision=$2 AND proxy_id=$3 AND created_at>now()-interval '5 minutes' ORDER BY created_at DESC LIMIT 10`, p.ID, p.State.Revision, p.State.ID)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []ProxyFailureEvidence{}
	for rows.Next() {
		var v ProxyFailureEvidence
		if e = rows.Scan(&v.Kind, &v.Reference); e != nil {
			return nil, e
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
