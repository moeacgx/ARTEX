package db

import (
	"database/sql"
	_ "embed"
	"errors"
	"strings"
	"unicode/utf8"
)

//go:embed proxy_policy_schema.sql
var proxyPolicySchema string

var (
	ErrProxyGroupRevisionConflict = errors.New("代理组版本已变化")
	ErrProxyGroupReferenced       = errors.New("代理组仍被主体授权引用")
	ErrProxyCandidatesExhausted   = errors.New("获准代理候选已耗尽")
	ErrProxyAuthorization         = errors.New("主体未授予代理故障切换权限")
	ErrProxyBudget                = errors.New("代理切换预算已耗尽")
	ErrProxyCooldown              = errors.New("代理切换正在冷却")
	ErrProxyEvidence              = errors.New("缺少可核验的基础设施故障证据")
	ErrProxyResinScope            = errors.New("此主体代理模式尚不支持独立 Resin 租约")
)

type ProxyGroup struct {
	ID       int64
	Name     string
	Enabled  bool
	Revision int64
	Members  []ProxyGroupMember
}
type ProxyGroupMember struct {
	ProxyID  int64
	Position int
	Proxy    *Proxy
}

func (d *DB) MigrateProxyPolicies() error {
	return d.withProxyLifecycle(func(tx *sql.Tx) error {
		_, err := tx.Exec(proxyPolicySchema)
		return err
	})
}

func proxyGroupTx(tx *sql.Tx, id int64) (*ProxyGroup, error) {
	g := &ProxyGroup{Members: []ProxyGroupMember{}}
	if err := tx.QueryRow(`SELECT id,name,enabled,revision FROM proxy_groups WHERE id=$1`, id).Scan(&g.ID, &g.Name, &g.Enabled, &g.Revision); err != nil {
		return nil, err
	}
	rows, err := tx.Query(`SELECT proxy_id,position FROM proxy_group_members WHERE group_id=$1 ORDER BY position`, id)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var m ProxyGroupMember
		if err = rows.Scan(&m.ProxyID, &m.Position); err != nil {
			rows.Close()
			return nil, err
		}
		g.Members = append(g.Members, m)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	// 先归还 rows 再查询档案，单连接也不产生嵌套借连接。
	for i := range g.Members {
		g.Members[i].Proxy, err = getProxyTx(tx, g.Members[i].ProxyID)
		if err != nil {
			return nil, err
		}
	}
	return g, nil
}

func (d *DB) GetProxyGroup(id int64) (*ProxyGroup, error) {
	var g *ProxyGroup
	err := d.withProxyLifecycle(func(tx *sql.Tx) error { var e error; g, e = proxyGroupTx(tx, id); return e })
	return g, err
}
func (d *DB) ListProxyGroups() ([]*ProxyGroup, error) {
	out := []*ProxyGroup{}
	err := d.withProxyLifecycle(func(tx *sql.Tx) error {
		rows, e := tx.Query(`SELECT id FROM proxy_groups ORDER BY id`)
		if e != nil {
			return e
		}
		var ids []int64
		for rows.Next() {
			var id int64
			if e = rows.Scan(&id); e != nil {
				rows.Close()
				return e
			}
			ids = append(ids, id)
		}
		e = rows.Err()
		rows.Close()
		if e != nil {
			return e
		}
		for _, id := range ids {
			g, e := proxyGroupTx(tx, id)
			if e != nil {
				return e
			}
			out = append(out, g)
		}
		return nil
	})
	return out, err
}

func validProxyGroupName(name string) bool {
	return name == strings.TrimSpace(name) && utf8.RuneCountInString(name) > 0 && utf8.RuneCountInString(name) <= 80
}
func (d *DB) CreateProxyGroup(name string, enabled bool) (*ProxyGroup, error) {
	if !validProxyGroupName(name) {
		return nil, ErrProxyInvalidMode
	}
	var out *ProxyGroup
	err := d.withProxyLifecycle(func(tx *sql.Tx) error {
		var id int64
		if e := tx.QueryRow(`INSERT INTO proxy_groups(name,enabled) VALUES($1,$2) RETURNING id`, name, enabled).Scan(&id); e != nil {
			return e
		}
		var e error
		out, e = proxyGroupTx(tx, id)
		return e
	})
	return out, err
}
func (d *DB) UpdateProxyGroup(id, expected int64, name string, enabled bool) (*ProxyGroup, error) {
	if !validProxyGroupName(name) {
		return nil, ErrProxyInvalidMode
	}
	var out *ProxyGroup
	err := d.withProxyLifecycle(func(tx *sql.Tx) error {
		g, e := proxyGroupTx(tx, id)
		if e != nil {
			return e
		}
		if g.Revision != expected {
			return ErrProxyGroupRevisionConflict
		}
		if expected >= 9007199254740991 {
			return ErrProxyInvalidMode
		}
		if _, e = tx.Exec(`UPDATE proxy_groups SET name=$2,enabled=$3,revision=revision+1 WHERE id=$1`, id, name, enabled); e != nil {
			return e
		}
		out, e = proxyGroupTx(tx, id)
		return e
	})
	return out, err
}
func (d *DB) DeleteProxyGroup(id, expected int64) error {
	return d.withProxyLifecycle(func(tx *sql.Tx) error {
		g, e := proxyGroupTx(tx, id)
		if e != nil {
			return e
		}
		if g.Revision != expected {
			return ErrProxyGroupRevisionConflict
		}
		var bound bool
		if e = tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM proxy_policy_groups WHERE group_id=$1) OR EXISTS(SELECT 1 FROM proxy_policy_group_reservations WHERE group_id=$1)`, id).Scan(&bound); e != nil {
			return e
		}
		if bound {
			return ErrProxyGroupReferenced
		}
		for _, m := range g.Members {
			if e = groupMemberRemovalTx(tx, id, m.ProxyID); e != nil {
				return e
			}
		}
		_, e = tx.Exec(`DELETE FROM proxy_groups WHERE id=$1`, id)
		return e
	})
}
func groupMemberRemovalTx(tx *sql.Tx, groupID, proxyID int64) error {
	var used bool
	err := tx.QueryRow(`SELECT
 EXISTS(SELECT 1 FROM proxy_subject_activity a JOIN proxy_policy_groups g ON g.policy_id=a.policy_id WHERE g.group_id=$1 AND a.proxy_id=$2)
 OR EXISTS(SELECT 1 FROM task_proxy_activity a JOIN proxy_subject_policies p ON p.task_id=a.task_id JOIN proxy_policy_groups g ON g.policy_id=p.id WHERE g.group_id=$1 AND a.proxy_id=$2)
 OR EXISTS(SELECT 1 FROM proxy_switch_requests r JOIN proxy_policy_groups g ON g.policy_id=r.policy_id WHERE g.group_id=$1 AND (r.to_proxy_id=$2 OR r.from_proxy_id=$2) AND r.state IN ('pending','draining','applying','recovery_required'))
 OR EXISTS(SELECT 1 FROM proxy_subject_policies p JOIN proxy_policy_group_reservations g ON g.policy_id=p.id WHERE g.group_id=$1 AND p.reserved_proxy_id=$2 AND p.switch_token<>'')`, groupID, proxyID).Scan(&used)
	if err == nil && used {
		return ErrProxyInUse
	}
	return err
}
func (d *DB) SetProxyGroupMembers(id, expected int64, ids []int64) (*ProxyGroup, error) {
	if len(ids) > 500 {
		return nil, ErrProxyInvalidMode
	}
	seen := map[int64]bool{}
	for _, pid := range ids {
		if pid <= 0 || seen[pid] {
			return nil, ErrProxyInvalidMode
		}
		seen[pid] = true
	}
	var out *ProxyGroup
	err := d.withProxyLifecycle(func(tx *sql.Tx) error {
		g, e := proxyGroupTx(tx, id)
		if e != nil {
			return e
		}
		if g.Revision != expected {
			return ErrProxyGroupRevisionConflict
		}
		if expected >= 9007199254740991 {
			return ErrProxyInvalidMode
		}
		for _, m := range g.Members {
			if !seen[m.ProxyID] {
				if e = groupMemberRemovalTx(tx, id, m.ProxyID); e != nil {
					return e
				}
			}
		}
		for _, pid := range ids {
			if _, e = getProxyTx(tx, pid); e != nil {
				return e
			}
		}
		if _, e = tx.Exec(`DELETE FROM proxy_group_members WHERE group_id=$1`, id); e != nil {
			return e
		}
		for i, pid := range ids {
			if _, e = tx.Exec(`INSERT INTO proxy_group_members(group_id,proxy_id,position) VALUES($1,$2,$3)`, id, pid, i); e != nil {
				return e
			}
		}
		if _, e = tx.Exec(`UPDATE proxy_groups SET revision=revision+1 WHERE id=$1`, id); e != nil {
			return e
		}
		out, e = proxyGroupTx(tx, id)
		return e
	})
	return out, err
}
