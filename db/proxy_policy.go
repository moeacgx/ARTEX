package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"time"

	"github.com/google/uuid"
)

// ProxySubject 只能由正式任务或对话执行上下文构造，不能取自模型工具入参。
type ProxySubject struct {
	Kind string
	ID   int64
}

func (s ProxySubject) table() (string, string, error) {
	if s.ID <= 0 {
		return "", "", sql.ErrNoRows
	}
	switch s.Kind {
	case "task":
		return "tasks", "task_id", nil
	case "conversation":
		return "conversations", "conversation_id", nil
	default:
		return "", "", ErrProxyAuthorization
	}
}
func (s ProxySubject) lockID() int64 {
	if s.Kind == "conversation" {
		return -s.ID
	}
	return s.ID
}

type ProxyPolicy struct {
	ID                      int64
	Subject                 ProxySubject
	State                   TaskProxyState
	AllowedGroupIDs         []int64
	AgentFailoverEnabled    bool
	FailoverCooldownSeconds int
	FailoverMaxSwitches     int
	AuthorizationRevision   int64
	SwitchesUsed            int
	CooldownUntil           *time.Time
	ExitVerifiedAt          *time.Time
	SwitchToken             string
	GroupRevisions          map[int64]int64
}
type ProxyPolicyPatch struct {
	Mode                    TaskProxyMode
	ProxyID                 *int64
	AllowedGroupIDs         *[]int64
	AgentFailoverEnabled    *bool
	FailoverCooldownSeconds *int
	FailoverMaxSwitches     *int
}
type ProxyCandidate struct {
	Proxy             *Proxy
	GroupIDs          []int64
	Supported         bool
	UnsupportedReason string
}

func ensureProxyPolicyTx(tx *sql.Tx, s ProxySubject) (int64, error) {
	table, col, err := s.table()
	if err != nil {
		return 0, err
	}
	var exists bool
	condition := ""
	if table == "tasks" {
		condition = " AND deleted_at IS NULL"
	}
	if err = tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM `+table+` WHERE id=$1`+condition+`)`, s.ID).Scan(&exists); err != nil {
		return 0, err
	}
	if !exists {
		return 0, sql.ErrNoRows
	}
	var id int64
	err = tx.QueryRow(`INSERT INTO proxy_subject_policies(`+col+`) VALUES($1) ON CONFLICT(`+col+`) DO UPDATE SET `+col+`=EXCLUDED.`+col+` RETURNING id`, s.ID).Scan(&id)
	return id, err
}
func proxyPolicyTx(tx *sql.Tx, s ProxySubject) (ProxyPolicy, error) {
	p := ProxyPolicy{Subject: s, AllowedGroupIDs: []int64{}}
	table, col, err := s.table()
	if err != nil {
		return p, err
	}
	var raw []byte
	err = tx.QueryRow(`SELECT id,agent_failover_enabled,failover_cooldown_seconds,failover_max_switches,authorization_revision,switches_used,cooldown_until,exit_verified_at,switch_token FROM proxy_subject_policies WHERE `+col+`=$1`, s.ID).Scan(&p.ID, &p.AgentFailoverEnabled, &p.FailoverCooldownSeconds, &p.FailoverMaxSwitches, &p.AuthorizationRevision, &p.SwitchesUsed, &p.CooldownUntil, &p.ExitVerifiedAt, &p.SwitchToken)
	if errors.Is(err, sql.ErrNoRows) {
		p.ID, err = ensureProxyPolicyTx(tx, s)
		if err != nil {
			return p, err
		}
		return proxyPolicyTx(tx, s)
	}
	if err != nil {
		return p, err
	}
	err = tx.QueryRow(`SELECT proxy_mode,proxy_id,proxy_snapshot,proxy_revision FROM `+table+` WHERE id=$1`, s.ID).Scan(&p.State.Mode, &p.State.ID, &raw, &p.State.Revision)
	if err != nil {
		return p, err
	}
	if err = json.Unmarshal(raw, &p.State.Snapshot); err != nil {
		return p, err
	}
	rows, err := tx.Query(`SELECT group_id FROM proxy_policy_groups WHERE policy_id=$1 ORDER BY group_id`, p.ID)
	if err != nil {
		return p, err
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		if err = rows.Scan(&id); err != nil {
			return p, err
		}
		p.AllowedGroupIDs = append(p.AllowedGroupIDs, id)
	}
	return p, rows.Err()
}
func (d *DB) GetProxyPolicy(s ProxySubject) (ProxyPolicy, error) {
	var p ProxyPolicy
	err := d.withProxyLifecycle(func(tx *sql.Tx) error { var e error; p, e = proxyPolicyTx(tx, s); return e })
	return p, err
}

func policyCandidatesTx(tx *sql.Tx, p ProxyPolicy) ([]ProxyCandidate, error) {
	rows, err := tx.Query(`SELECT m.proxy_id,m.group_id FROM proxy_policy_groups a JOIN proxy_groups g ON g.id=a.group_id JOIN proxy_group_members m ON m.group_id=g.id WHERE a.policy_id=$1 AND g.enabled ORDER BY g.id,m.position`, p.ID)
	if err != nil {
		return nil, err
	}
	type pair struct{ proxy, group int64 }
	var pairs []pair
	for rows.Next() {
		var v pair
		if err = rows.Scan(&v.proxy, &v.group); err != nil {
			rows.Close()
			return nil, err
		}
		pairs = append(pairs, v)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	out := []ProxyCandidate{}
	idx := map[int64]int{}
	for _, v := range pairs {
		if i, ok := idx[v.proxy]; ok {
			out[i].GroupIDs = append(out[i].GroupIDs, v.group)
			continue
		}
		proxy, e := getProxyTx(tx, v.proxy)
		if e != nil {
			return nil, e
		}
		c := ProxyCandidate{Proxy: proxy, GroupIDs: []int64{v.group}, Supported: true}
		if proxy.Kind == ProxyKindResin {
			c.Supported = false
			c.UnsupportedReason = "group_resin_lease_scope_unsupported"
		} else if proxy.Kind == ProxyKindWarp {
			if e := CheckWarpHealthAdmissionTx(context.Background(), tx, proxy.ID); e != nil {
				c.Supported = false
				c.UnsupportedReason = "warp_health_unavailable"
			}
		}
		idx[v.proxy] = len(out)
		out = append(out, c)
	}
	return out, nil
}
func (d *DB) AllowedProxyCandidates(s ProxySubject) (ProxyPolicy, []ProxyCandidate, error) {
	var p ProxyPolicy
	var c []ProxyCandidate
	err := d.withProxyLifecycle(func(tx *sql.Tx) error {
		var e error
		p, e = proxyPolicyTx(tx, s)
		if e != nil {
			return e
		}
		c, e = policyCandidatesTx(tx, p)
		return e
	})
	return p, c, err
}

func validateProxyPolicyTx(tx *sql.Tx, p ProxyPolicy) error {
	if p.State.Mode == TaskProxyModeDirect {
		return nil
	}
	if p.State.ID == nil {
		if p.State.Mode == TaskProxyModeInherit && p.State.Snapshot.ID == 0 {
			return nil
		}
		return ErrProxyUnavailable
	}
	if p.State.Snapshot.ID != *p.State.ID {
		return ErrProxyUnavailable
	}
	proxy, err := getProxyTx(tx, *p.State.ID)
	if err != nil {
		return err
	}
	if err = usableProxy(proxy); err != nil {
		return err
	}
	if err = CheckWarpHealthAdmissionTx(context.Background(), tx, proxy.ID); err != nil {
		return err
	}
	if proxy.Kind == ProxyKindResin && (p.Subject.Kind == "conversation" || p.State.Mode == TaskProxyModeGroup) {
		return ErrProxyResinScope
	}
	if p.State.Mode == TaskProxyModeGroup {
		var allowed bool
		err = tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM proxy_policy_groups a JOIN proxy_groups g ON g.id=a.group_id JOIN proxy_group_members m ON m.group_id=g.id WHERE a.policy_id=$1 AND g.enabled AND m.proxy_id=$2)`, p.ID, *p.State.ID).Scan(&allowed)
		if err != nil {
			return err
		}
		if !allowed {
			return ErrProxyAuthorization
		}
	}
	return nil
}
func proxyPolicyBarrierTx(tx *sql.Tx, p ProxyPolicy) error {
	if p.SwitchToken != "" {
		return ErrProxySwitchRecovery
	}
	var pending bool
	err := tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM proxy_switch_requests WHERE policy_id=$1 AND state IN ('pending','draining','applying','recovery_required'))`, p.ID).Scan(&pending)
	if err != nil {
		return err
	}
	if pending {
		return ErrProxyInUse
	}
	return nil
}
func (d *DB) ValidateProxyPolicy(s ProxySubject) error {
	return d.withProxyLifecycle(func(tx *sql.Tx) error {
		p, e := proxyPolicyTx(tx, s)
		if e != nil {
			return e
		}
		if e = proxyPolicyBarrierTx(tx, p); e != nil {
			return e
		}
		return validateProxyPolicyTx(tx, p)
	})
}

// 组成员候选在同一事务中临时替换授权关系进行选择；失败会整体回滚。
func prepareProxyPolicyTx(tx *sql.Tx, old ProxyPolicy, patch ProxyPolicyPatch) (ProxyPolicy, error) {
	p := old
	mode, err := NormalizeTaskProxyMode(patch.Mode)
	if err != nil || patch.Mode == "" {
		return p, ErrProxyInvalidMode
	}
	p.State.Mode = mode
	if mode != TaskProxyModeFixed && patch.ProxyID != nil {
		return p, ErrProxyInvalidMode
	}
	if mode == TaskProxyModeFixed && (patch.ProxyID == nil || *patch.ProxyID <= 0) {
		return p, ErrProxyIDRequired
	}
	if patch.AllowedGroupIDs != nil {
		p.AllowedGroupIDs = append([]int64{}, (*patch.AllowedGroupIDs)...)
		sort.Slice(p.AllowedGroupIDs, func(i, j int) bool { return p.AllowedGroupIDs[i] < p.AllowedGroupIDs[j] })
		if len(p.AllowedGroupIDs) > 100 {
			return p, ErrProxyInvalidMode
		}
		for i, id := range p.AllowedGroupIDs {
			if id <= 0 || (i > 0 && id == p.AllowedGroupIDs[i-1]) {
				return p, ErrProxyInvalidMode
			}
			if _, e := proxyGroupTx(tx, id); e != nil {
				return p, e
			}
		}
	}
	if patch.AgentFailoverEnabled != nil {
		p.AgentFailoverEnabled = *patch.AgentFailoverEnabled
	}
	if patch.FailoverCooldownSeconds != nil {
		p.FailoverCooldownSeconds = *patch.FailoverCooldownSeconds
	}
	if patch.FailoverMaxSwitches != nil {
		p.FailoverMaxSwitches = *patch.FailoverMaxSwitches
	}
	if p.FailoverCooldownSeconds < 0 || p.FailoverCooldownSeconds > 86400 || p.FailoverMaxSwitches < 0 || p.FailoverMaxSwitches > 1000 {
		return p, ErrProxyInvalidMode
	}
	p.State.ID = nil
	p.State.Snapshot = ProxySnapshot{}
	switch mode {
	case TaskProxyModeFixed:
		p.State.ID = patch.ProxyID
	case TaskProxyModeInherit:
		p.State.ID, err = globalProxyIDTx(tx)
	case TaskProxyModeGroup:
		if len(p.AllowedGroupIDs) == 0 {
			return p, ErrProxyCandidatesExhausted
		}
		if err = setPolicyGroupsTx(tx, p.ID, p.AllowedGroupIDs); err != nil {
			return p, err
		}
		candidates, e := policyCandidatesTx(tx, p)
		if e != nil {
			return p, e
		}
		for _, c := range candidates {
			if c.Supported && usableProxy(c.Proxy) == nil {
				pid := c.Proxy.ID
				p.State.ID = &pid
				break
			}
		}
		if p.State.ID == nil {
			return p, ErrProxyCandidatesExhausted
		}
	}
	if err != nil {
		return p, err
	}
	if p.State.ID != nil {
		proxy, e := getProxyTx(tx, *p.State.ID)
		if e != nil {
			return p, e
		}
		p.State.Snapshot, e = proxy.SnapshotEncrypted()
		if e != nil {
			return p, e
		}
	}
	if err = validateProxyPolicyTx(tx, p); err != nil {
		return p, err
	}
	// 只有显式权限字段的实际变更建立新授权预算；切换/重启/普通编辑均不重置。
	if !reflect.DeepEqual(old.AllowedGroupIDs, p.AllowedGroupIDs) || old.AgentFailoverEnabled != p.AgentFailoverEnabled || old.FailoverCooldownSeconds != p.FailoverCooldownSeconds || old.FailoverMaxSwitches != p.FailoverMaxSwitches {
		if p.AuthorizationRevision >= 9007199254740991 {
			return p, ErrProxyInvalidMode
		}
		p.AuthorizationRevision++
		p.SwitchesUsed = 0
		p.CooldownUntil = nil
	}
	return p, nil
}
func setPolicyGroupsTx(tx *sql.Tx, id int64, groups []int64) error {
	if _, e := tx.Exec(`DELETE FROM proxy_policy_groups WHERE policy_id=$1`, id); e != nil {
		return e
	}
	for _, gid := range groups {
		if _, e := tx.Exec(`INSERT INTO proxy_policy_groups(policy_id,group_id) VALUES($1,$2)`, id, gid); e != nil {
			return e
		}
	}
	return nil
}
func saveProxyPolicyTx(tx *sql.Tx, p ProxyPolicy) error {
	table, _, err := p.Subject.table()
	if err != nil {
		return err
	}
	raw, err := json.Marshal(p.State.Snapshot)
	if err != nil {
		return err
	}
	if _, err = tx.Exec(`UPDATE `+table+` SET proxy_mode=$2,proxy_id=$3,proxy_snapshot=$4,proxy_revision=$5 WHERE id=$1`, p.Subject.ID, p.State.Mode, p.State.ID, raw, p.State.Revision); err != nil {
		return err
	}
	if err = setPolicyGroupsTx(tx, p.ID, p.AllowedGroupIDs); err != nil {
		return err
	}
	_, err = tx.Exec(`UPDATE proxy_subject_policies SET agent_failover_enabled=$2,failover_cooldown_seconds=$3,failover_max_switches=$4,authorization_revision=$5,switches_used=$6,cooldown_until=$7,exit_verified_at=$8 WHERE id=$1`, p.ID, p.AgentFailoverEnabled, p.FailoverCooldownSeconds, p.FailoverMaxSwitches, p.AuthorizationRevision, p.SwitchesUsed, p.CooldownUntil, p.ExitVerifiedAt)
	return err
}

type ProxyPolicyExecutor struct {
	x       *TaskProxySwitchExecutor
	Subject ProxySubject
}

func (d *DB) AcquireProxyPolicyExecutor(ctx context.Context, s ProxySubject) (*ProxyPolicyExecutor, error) {
	if _, _, e := s.table(); e != nil {
		return nil, e
	}
	x, e := d.AcquireTaskProxySwitchExecutor(ctx, s.lockID())
	if e != nil {
		return nil, e
	}
	return &ProxyPolicyExecutor{x: x, Subject: s}, nil
}
func (x *ProxyPolicyExecutor) Close() { x.x.Close() }
func policyIdleTx(tx *sql.Tx, p ProxyPolicy) error {
	if e := policyActivitiesIdleTx(tx, p); e != nil {
		return e
	}
	if p.Subject.Kind == "task" {
		var status string
		var queued, paused bool
		if e := tx.QueryRow(`SELECT status,queued,paused FROM tasks WHERE id=$1`, p.Subject.ID).Scan(&status, &queued, &paused); e != nil {
			return e
		}
		if queued || (!paused && status != "paused" && status != "done" && status != "failed" && status != "timeout") {
			return ErrProxyInUse
		}
	}
	return nil
}

func policyActivitiesIdleTx(tx *sql.Tx, p ProxyPolicy) error {
	var busy bool
	err := tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM proxy_subject_activity WHERE policy_id=$1)`, p.ID).Scan(&busy)
	if err != nil {
		return err
	}
	if busy {
		return ErrProxyInUse
	}
	if p.Subject.Kind == "task" {
		err = tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM task_operation_activity WHERE task_id=$1 AND (active_count>0 OR operation_owners<>'{}' OR switch_token<>'')) OR EXISTS(SELECT 1 FROM task_proxy_activity WHERE task_id=$1)`, p.Subject.ID).Scan(&busy)
		if err != nil {
			return err
		}
		if busy {
			return ErrProxyInUse
		}
	}
	return nil
}
func (x *ProxyPolicyExecutor) Begin(expected int64, patch ProxyPolicyPatch) (ProxyPolicy, string, error) {
	var old ProxyPolicy
	var token string
	err := x.x.d.withProxySwitchLifecycle(x.x.conn, func(tx *sql.Tx) error {
		var e error
		old, e = proxyPolicyTx(tx, x.Subject)
		if e != nil {
			return e
		}
		if old.State.Revision != expected {
			return ErrProxyRevisionConflict
		}
		if expected >= 9007199254740991 {
			return ErrProxyInvalidMode
		}
		if e = proxyPolicyBarrierTx(tx, old); e != nil {
			return e
		}
		if e = policyIdleTx(tx, old); e != nil {
			return e
		}
		target, e := prepareProxyPolicyTx(tx, old, patch)
		if e != nil {
			return e
		}
		target.State.Revision = expected + 1
		// 预选不能提前改已提交授权，目标的完整版本只作为持久意图保存。
		if e = setPolicyGroupsTx(tx, target.ID, target.AllowedGroupIDs); e != nil {
			return e
		}
		target.GroupRevisions, e = groupRevisionsTx(tx, target)
		if e != nil {
			return e
		}
		if e = setPolicyGroupsTx(tx, old.ID, old.AllowedGroupIDs); e != nil {
			return e
		}
		raw, e := json.Marshal(target)
		if e != nil {
			return e
		}
		token = uuid.NewString()
		if _, e = tx.Exec(`UPDATE proxy_subject_policies SET switch_token=$2,target_policy=$3,reserved_proxy_id=$4 WHERE id=$1`, old.ID, token, raw, target.State.ID); e != nil {
			return e
		}
		for _, gid := range target.AllowedGroupIDs {
			if _, e = tx.Exec(`INSERT INTO proxy_policy_group_reservations(policy_id,group_id) VALUES($1,$2)`, old.ID, gid); e != nil {
				return e
			}
		}
		return nil
	})
	return old, token, err
}
func (x *ProxyPolicyExecutor) Complete(expected int64, token string) (ProxyPolicy, error) {
	var p ProxyPolicy
	err := x.x.d.withProxySwitchLifecycle(x.x.conn, func(tx *sql.Tx) error {
		old, e := proxyPolicyTx(tx, x.Subject)
		if e != nil {
			return e
		}
		if old.State.Revision != expected {
			return ErrProxyRevisionConflict
		}
		if old.SwitchToken == "" || old.SwitchToken != token {
			return ErrProxySwitchRecovery
		}
		var raw []byte
		if e = tx.QueryRow(`SELECT target_policy FROM proxy_subject_policies WHERE id=$1`, old.ID).Scan(&raw); e != nil {
			return e
		}
		if e = json.Unmarshal(raw, &p); e != nil {
			return e
		}
		if p.ID != old.ID || p.Subject != old.Subject {
			return ErrProxySwitchRecovery
		}
		if e = setPolicyGroupsTx(tx, p.ID, p.AllowedGroupIDs); e != nil {
			return e
		}
		if e = checkGroupRevisionsTx(tx, p, p.GroupRevisions); e != nil {
			return e
		}
		if e = validateProxyPolicyTx(tx, p); e != nil {
			return e
		}
		if e = saveProxyPolicyTx(tx, p); e != nil {
			return e
		}
		if _, e = tx.Exec(`DELETE FROM proxy_policy_group_reservations WHERE policy_id=$1`, p.ID); e != nil {
			return e
		}
		_, e = tx.Exec(`UPDATE proxy_subject_policies SET switch_token='',target_policy='{}',reserved_proxy_id=NULL WHERE id=$1 AND switch_token=$2`, p.ID, token)
		p.SwitchToken = ""
		return e
	})
	return p, err
}

// 对话直连同样登记共享会话锁，取消不释放，真实资源清理完成后才解除原 token。
func (d *DB) AcquireConversationOperation(id int64) (ProxyPolicy, string, error) {
	s := ProxySubject{Kind: "conversation", ID: id}
	var p ProxyPolicy
	var token string
	lease, _, err := d.acquireTaskOperationSession(s.lockID())
	if err != nil {
		return p, "", err
	}
	lease.txMu.Lock()
	err = d.withProxySwitchLifecycle(lease.conn, func(tx *sql.Tx) error {
		var e error
		p, e = proxyPolicyTx(tx, s)
		if e != nil {
			return e
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
		token = uuid.NewString()
		if p.State.ID != nil {
			if e = AcquireWarpHealthActivityTx(context.Background(), tx, *p.State.ID, token); e != nil {
				return e
			}
		}
		_, e = tx.Exec(`INSERT INTO proxy_subject_activity(token,policy_id,revision,proxy_id,backend_pid) VALUES($1,$2,$3,$4,pg_backend_pid())`, token, p.ID, p.State.Revision, p.State.ID)
		return e
	})
	lease.txMu.Unlock()
	if err != nil {
		_ = d.releaseTaskOperationSession(s.lockID())
	}
	return p, token, err
}
func (d *DB) ReleaseConversationOperation(id int64, token string) error {
	s := ProxySubject{Kind: "conversation", ID: id}
	k := taskOperationLeaseKey{database: d.DB, taskID: s.lockID()}
	taskOperationLeases.Lock()
	lease := taskOperationLeases.leases[k]
	taskOperationLeases.Unlock()
	if lease == nil {
		return ErrProxySwitchRecovery
	}
	lease.txMu.Lock()
	err := d.withProxySwitchLifecycle(lease.conn, func(tx *sql.Tx) error {
		result, e := tx.Exec(`DELETE FROM proxy_subject_activity WHERE token=$1 AND policy_id=(SELECT id FROM proxy_subject_policies WHERE conversation_id=$2) AND backend_pid=pg_backend_pid()`, token, id)
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
