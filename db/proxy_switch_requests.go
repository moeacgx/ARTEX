package db

import (
	"database/sql"
	"encoding/json"
	"errors"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
)

type ProxyFailureEvidence struct {
	Kind      string `json:"kind"`
	Reference string `json:"reference"`
}
type ProxyFailoverInput struct {
	ExpectedRevision int64
	Reason           string
	Evidence         ProxyFailureEvidence
	TargetProxyID    *int64
}
type ProxySwitchRequest struct {
	ID                    string
	PolicyID              int64
	Subject               ProxySubject
	BaseRevision          int64
	AuthorizationRevision int64
	State                 string
	FromProxyID           *int64
	ToProxyID             *int64
	Reason                string
	ErrorCode             string
	CreatedAt             time.Time
	UpdatedAt             time.Time
	CompletedAt           *time.Time
	ResultRevision        *int64
	ClaimToken            string
	Snapshot              ProxySnapshot
	GroupRevisions        map[int64]int64
}

const proxySwitchCols = `r.id,r.policy_id,r.base_revision,r.authorization_revision,r.state,r.from_proxy_id,r.to_proxy_id,r.reason,r.error_code,r.created_at,r.updated_at,r.completed_at,r.result_revision,r.claim_token,r.target_snapshot,r.group_revisions`

func scanProxySwitch(row interface{ Scan(...any) error }) (ProxySwitchRequest, error) {
	var r ProxySwitchRequest
	var snapshot, groups []byte
	err := row.Scan(&r.ID, &r.PolicyID, &r.BaseRevision, &r.AuthorizationRevision, &r.State, &r.FromProxyID, &r.ToProxyID, &r.Reason, &r.ErrorCode, &r.CreatedAt, &r.UpdatedAt, &r.CompletedAt, &r.ResultRevision, &r.ClaimToken, &snapshot, &groups)
	if err != nil {
		return r, err
	}
	if err = json.Unmarshal(snapshot, &r.Snapshot); err != nil {
		return r, err
	}
	err = json.Unmarshal(groups, &r.GroupRevisions)
	return r, err
}
func groupRevisionsTx(tx *sql.Tx, p ProxyPolicy) (map[int64]int64, error) {
	out := map[int64]int64{}
	rows, err := tx.Query(`SELECT g.id,g.revision FROM proxy_policy_groups a JOIN proxy_groups g ON g.id=a.group_id WHERE a.policy_id=$1`, p.ID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id, revision int64
		if err = rows.Scan(&id, &revision); err != nil {
			return nil, err
		}
		out[id] = revision
	}
	return out, rows.Err()
}
func checkGroupRevisionsTx(tx *sql.Tx, p ProxyPolicy, expected map[int64]int64) error {
	actual, err := groupRevisionsTx(tx, p)
	if err != nil {
		return err
	}
	if len(actual) != len(expected) {
		return ErrProxyAuthorization
	}
	for id, v := range actual {
		if old, ok := expected[id]; !ok || old != v {
			return ErrProxyAuthorization
		}
	}
	return nil
}
func failoverAuthorized(p ProxyPolicy) error {
	if p.State.Mode != TaskProxyModeGroup || !p.AgentFailoverEnabled || len(p.AllowedGroupIDs) == 0 {
		return ErrProxyAuthorization
	}
	if p.SwitchesUsed >= p.FailoverMaxSwitches {
		return ErrProxyBudget
	}
	if p.CooldownUntil != nil && p.CooldownUntil.After(time.Now()) {
		return ErrProxyCooldown
	}
	return nil
}

// 证据只由正式运行器记录；403/验证码/429 和模型理由文本不能调用此入口证明故障。
func (d *DB) RecordProxyFailureEvidence(s ProxySubject, revision, proxyID int64, kind string) (string, error) {
	switch kind {
	case "connection_failure", "tunnel_failure", "process_failure", "health_confirmation":
	default:
		return "", ErrProxyEvidence
	}
	id := uuid.NewString()
	err := d.withProxyLifecycle(func(tx *sql.Tx) error {
		p, e := proxyPolicyTx(tx, s)
		if e != nil {
			return e
		}
		if p.State.Revision != revision || p.State.ID == nil || *p.State.ID != proxyID {
			return ErrProxyRevisionConflict
		}
		_, e = tx.Exec(`INSERT INTO proxy_failure_evidence(id,policy_id,revision,proxy_id,kind) VALUES($1,$2,$3,$4,$5)`, id, p.ID, revision, proxyID, kind)
		return e
	})
	return id, err
}
func (d *DB) RequestProxyFailover(s ProxySubject, in ProxyFailoverInput) (ProxySwitchRequest, error) {
	var out ProxySwitchRequest
	err := d.withProxyLifecycle(func(tx *sql.Tx) error {
		p, e := proxyPolicyTx(tx, s)
		if e != nil {
			return e
		}
		existing, e := scanProxySwitch(tx.QueryRow(`SELECT `+proxySwitchCols+` FROM proxy_switch_requests r WHERE r.policy_id=$1 AND r.state IN ('pending','draining','applying','recovery_required')`, p.ID))
		if e == nil {
			if existing.BaseRevision != in.ExpectedRevision {
				return ErrProxyRevisionConflict
			}
			out = existing
			out.Subject = s
			return nil
		}
		if !errors.Is(e, sql.ErrNoRows) {
			return e
		}
		if p.State.Revision != in.ExpectedRevision {
			return ErrProxyRevisionConflict
		}
		if p.SwitchToken != "" {
			return ErrProxySwitchRecovery
		}
		if e = failoverAuthorized(p); e != nil {
			return e
		}
		if utf8.RuneCountInString(in.Reason) < 1 || utf8.RuneCountInString(in.Reason) > 500 {
			return ErrProxyInvalidMode
		}
		var proof bool
		e = tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM proxy_failure_evidence WHERE id=$1 AND policy_id=$2 AND revision=$3 AND proxy_id=$4 AND kind=$5 AND created_at>now()-interval '5 minutes' AND (retry_after IS NULL OR retry_after<=now()))`, in.Evidence.Reference, p.ID, p.State.Revision, p.State.ID, in.Evidence.Kind).Scan(&proof)
		if e != nil {
			return e
		}
		if !proof {
			return ErrProxyEvidence
		}
		candidates, e := policyCandidatesTx(tx, p)
		if e != nil {
			return e
		}
		var target *Proxy
		for _, c := range candidates {
			if !c.Supported || usableProxy(c.Proxy) != nil || (p.State.ID != nil && c.Proxy.ID == *p.State.ID) {
				continue
			}
			if in.TargetProxyID != nil && c.Proxy.ID != *in.TargetProxyID {
				continue
			}
			target = c.Proxy
			break
		}
		if target == nil {
			return ErrProxyCandidatesExhausted
		}
		snapshot, e := target.SnapshotEncrypted()
		if e != nil {
			return e
		}
		raw, e := json.Marshal(snapshot)
		if e != nil {
			return e
		}
		revisions, e := groupRevisionsTx(tx, p)
		if e != nil {
			return e
		}
		groups, e := json.Marshal(revisions)
		if e != nil {
			return e
		}
		out, e = scanProxySwitch(tx.QueryRow(`INSERT INTO proxy_switch_requests AS r(id,policy_id,base_revision,authorization_revision,state,from_proxy_id,to_proxy_id,target_snapshot,group_revisions,evidence_id,reason) VALUES($1,$2,$3,$4,'pending',$5,$6,$7,$8,$9,$10) RETURNING `+proxySwitchCols, uuid.NewString(), p.ID, p.State.Revision, p.AuthorizationRevision, p.State.ID, target.ID, raw, groups, in.Evidence.Reference, in.Reason))
		out.Subject = s
		return e
	})
	return out, err
}
func (d *DB) GetProxySwitchRequest(s ProxySubject, id string) (ProxySwitchRequest, error) {
	var out ProxySwitchRequest
	err := d.withProxyLifecycle(func(tx *sql.Tx) error {
		p, e := proxyPolicyTx(tx, s)
		if e != nil {
			return e
		}
		out, e = scanProxySwitch(tx.QueryRow(`SELECT `+proxySwitchCols+` FROM proxy_switch_requests r WHERE r.policy_id=$1 AND r.id=$2`, p.ID, id))
		out.Subject = s
		return e
	})
	return out, err
}
func (d *DB) LatestProxySwitchRequest(s ProxySubject) (*ProxySwitchRequest, error) {
	var out ProxySwitchRequest
	err := d.withProxyLifecycle(func(tx *sql.Tx) error {
		p, e := proxyPolicyTx(tx, s)
		if e != nil {
			return e
		}
		out, e = scanProxySwitch(tx.QueryRow(`SELECT `+proxySwitchCols+` FROM proxy_switch_requests r WHERE r.policy_id=$1 ORDER BY r.created_at DESC,r.id DESC LIMIT 1`, p.ID))
		out.Subject = s
		return e
	})
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return &out, err
}
func (d *DB) PendingProxySwitchSubjects() ([]ProxySubject, error) {
	rows, e := d.Query(`SELECT p.task_id,p.conversation_id FROM proxy_switch_requests r JOIN proxy_subject_policies p ON p.id=r.policy_id WHERE r.state IN ('pending','draining','applying') ORDER BY r.created_at`)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []ProxySubject{}
	for rows.Next() {
		var task, conv *int64
		if e = rows.Scan(&task, &conv); e != nil {
			return nil, e
		}
		if task != nil {
			out = append(out, ProxySubject{Kind: "task", ID: *task})
		} else {
			out = append(out, ProxySubject{Kind: "conversation", ID: *conv})
		}
	}
	return out, rows.Err()
}

// 先持久标记退出阶段，再尝试独占锁；不能在工具自己的共享活动内等待。
func (d *DB) MarkProxyFailoverDraining(s ProxySubject) error {
	return d.withProxyLifecycle(func(tx *sql.Tx) error {
		p, e := proxyPolicyTx(tx, s)
		if e != nil {
			return e
		}
		_, e = tx.Exec(`UPDATE proxy_switch_requests SET state='draining',updated_at=now() WHERE policy_id=$1 AND state='pending'`, p.ID)
		return e
	})
}

func markSwitchFailedTx(tx *sql.Tx, id, code string) error {
	_, e := tx.Exec(`UPDATE proxy_switch_requests SET state='failed',error_code=$2,updated_at=now(),completed_at=now(),claim_token='' WHERE id=$1`, id, code)
	return e
}

// Claim 仅在真实共享会话锁全部退出后可持有，屏障不按时间过期或夺权。
func (x *ProxyPolicyExecutor) ClaimFailover() (ProxyPolicy, ProxySwitchRequest, error) {
	var p ProxyPolicy
	var r ProxySwitchRequest
	var rejected error
	err := x.x.d.withProxySwitchLifecycle(x.x.conn, func(tx *sql.Tx) error {
		var e error
		p, e = proxyPolicyTx(tx, x.Subject)
		if e != nil {
			return e
		}
		r, e = scanProxySwitch(tx.QueryRow(`SELECT `+proxySwitchCols+` FROM proxy_switch_requests r WHERE r.policy_id=$1 AND r.state IN ('pending','draining','applying')`, p.ID))
		if e != nil {
			return e
		}
		r.Subject = x.Subject
		if r.State == "applying" {
			_, e = tx.Exec(`UPDATE proxy_switch_requests SET state='recovery_required',error_code='proxy_switch_executor_lost',updated_at=now() WHERE id=$1`, r.ID)
			rejected = ErrProxySwitchRecovery
			return e
		}
		if p.State.Revision != r.BaseRevision || p.AuthorizationRevision != r.AuthorizationRevision || p.SwitchToken != "" {
			rejected = ErrProxyAuthorization
			return markSwitchFailedTx(tx, r.ID, "proxy_authorization_changed")
		}
		if e = failoverAuthorized(p); e != nil {
			rejected = e
			return markSwitchFailedTx(tx, r.ID, "proxy_failover_not_authorized")
		}
		if e = checkGroupRevisionsTx(tx, p, r.GroupRevisions); e != nil {
			rejected = e
			return markSwitchFailedTx(tx, r.ID, "proxy_group_changed")
		}
		target := p
		target.State.ID = r.ToProxyID
		target.State.Snapshot = r.Snapshot
		if e = validateProxyPolicyTx(tx, target); e != nil {
			rejected = e
			return markSwitchFailedTx(tx, r.ID, "proxy_target_unavailable")
		}
		var busy bool
		e = tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM proxy_subject_activity WHERE policy_id=$1) OR EXISTS(SELECT 1 FROM task_operation_activity WHERE task_id=$2 AND (active_count>0 OR operation_owners<>'{}' OR switch_token<>'')) OR EXISTS(SELECT 1 FROM task_proxy_activity WHERE task_id=$2)`, p.ID, func() any {
			if p.Subject.Kind == "task" {
				return p.Subject.ID
			}
			return nil
		}()).Scan(&busy)
		if e != nil {
			return e
		}
		if busy {
			return ErrProxyInUse
		}
		r.ClaimToken = uuid.NewString()
		r.State = "applying"
		if _, e = tx.Exec(`UPDATE proxy_switch_requests SET state='applying',claim_token=$2,updated_at=now() WHERE id=$1`, r.ID, r.ClaimToken); e != nil {
			return e
		}
		_, e = tx.Exec(`UPDATE proxy_subject_policies SET switches_used=switches_used+1,cooldown_until=now()+make_interval(secs=>failover_cooldown_seconds) WHERE id=$1`, p.ID)
		return e
	})
	if err == nil && rejected != nil {
		err = rejected
	}
	return p, r, err
}
func (x *ProxyPolicyExecutor) CompleteFailover(r ProxySwitchRequest) (ProxyPolicy, error) {
	var out ProxyPolicy
	err := x.x.d.withProxySwitchLifecycle(x.x.conn, func(tx *sql.Tx) error {
		p, e := proxyPolicyTx(tx, x.Subject)
		if e != nil {
			return e
		}
		current, e := scanProxySwitch(tx.QueryRow(`SELECT `+proxySwitchCols+` FROM proxy_switch_requests r WHERE r.id=$1 AND r.policy_id=$2`, r.ID, p.ID))
		if e != nil {
			return e
		}
		if (current.State != "applying" && current.State != "recovery_required") || current.ClaimToken != r.ClaimToken || r.ClaimToken == "" {
			return ErrProxySwitchRecovery
		}
		if p.State.Revision != r.BaseRevision || p.AuthorizationRevision != r.AuthorizationRevision || !p.AgentFailoverEnabled || p.State.Mode != TaskProxyModeGroup {
			return ErrProxyAuthorization
		}
		if e = checkGroupRevisionsTx(tx, p, r.GroupRevisions); e != nil {
			return e
		}
		p.State.ID = r.ToProxyID
		p.State.Snapshot = r.Snapshot
		if e = validateProxyPolicyTx(tx, p); e != nil {
			return e
		}
		if p.State.Revision >= 9007199254740991 {
			return ErrProxyInvalidMode
		}
		p.State.Revision++
		if e = saveProxyPolicyTx(tx, p); e != nil {
			return e
		}
		_, e = tx.Exec(`UPDATE proxy_switch_requests SET state='succeeded',claim_token='',updated_at=now(),completed_at=now(),result_revision=$2 WHERE id=$1`, r.ID, p.State.Revision)
		out = p
		return e
	})
	return out, err
}
func (x *ProxyPolicyExecutor) RequireFailoverRecovery(r ProxySwitchRequest, code string) error {
	return x.x.d.withProxySwitchLifecycle(x.x.conn, func(tx *sql.Tx) error {
		_, e := tx.Exec(`UPDATE proxy_switch_requests SET state='recovery_required',error_code=$3,updated_at=now() WHERE id=$1 AND claim_token=$2 AND state='applying'`, r.ID, r.ClaimToken, code)
		return e
	})
}
