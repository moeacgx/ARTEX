package db

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync"
)

var (
	ErrPlannerRunBusy   = errors.New("旧 Planner 尚未确认退出")
	ErrPlannerRunPaused = errors.New("任务已暂停，不能启动普通 Planner")
)

func plannerRunSessionKey(id int64) int64 { return int64(0x504c4e0000000000) ^ id }

// PlannerRunLease 是跨 Manager 的会话锁，直到模型、工具和清理真实退出后才解除。
// 它与普通操作计数分离：普通任务操作可以并发，但 Planner 轮次不能重叠。
type PlannerRunLease struct {
	d    *DB
	conn *sql.Conn
	key  int64
	once sync.Once
	err  error
}

// AcquirePlannerRun 在现有生命周期事务锁下串行化 Planner 准入与暂停/继续变更，
// 随后持有独立会话锁直到真实退出。
func (d *DB) AcquirePlannerRun(ctx context.Context, id int64, allowPaused bool) (*PlannerRunLease, error) {
	conn, err := d.taskOperationLockConn(ctx)
	if err != nil {
		return nil, err
	}
	key := plannerRunSessionKey(id)
	locked := false
	err = d.withProxySwitchLifecycle(conn, func(tx *sql.Tx) error {
		var paused, present bool
		if err := tx.QueryRow(`SELECT paused,deleted_at IS NULL FROM tasks WHERE id=$1`, id).Scan(&paused, &present); err != nil {
			return err
		}
		if !present {
			return sql.ErrNoRows
		}
		if paused && !allowPaused {
			return ErrPlannerRunPaused
		}
		if err := tx.QueryRow(`SELECT pg_try_advisory_lock($1)`, key).Scan(&locked); err != nil {
			return err
		}
		if !locked {
			return ErrPlannerRunBusy
		}
		return nil
	})
	if err != nil {
		if locked {
			var unlocked bool
			if unlockErr := conn.QueryRowContext(context.Background(), `SELECT pg_advisory_unlock($1)`, key).Scan(&unlocked); unlockErr != nil || !unlocked {
				_ = conn.Raw(func(any) error { return driver.ErrBadConn })
			}
		}
		_ = conn.Close()
		return nil, err
	}
	return &PlannerRunLease{d: d, conn: conn, key: key}, nil
}

func (l *PlannerRunLease) Close() error {
	if l == nil {
		return nil
	}
	l.once.Do(func() {
		l.err = l.d.withProxySwitchLifecycle(l.conn, func(tx *sql.Tx) error {
			var unlocked bool
			if err := tx.QueryRow(`SELECT pg_advisory_unlock($1)`, l.key).Scan(&unlocked); err != nil {
				return err
			}
			if !unlocked {
				return ErrPlannerRunBusy
			}
			return nil
		})
		if l.err != nil {
			_ = l.conn.Raw(func(any) error { return driver.ErrBadConn })
		}
		l.err = errors.Join(l.err, l.conn.Close())
	})
	return l.err
}

// WithPlannerIdleTaskMutation 是正式继续准入门；事务级 advisory lock 与存活的
// Planner lease 冲突，并覆盖整个任务状态变更，消除跨 Manager 的先检查后解暂停竞态。
func (d *DB) WithPlannerIdleTaskMutation(id int64, fn func(*sql.Tx) error) error {
	return d.withProxyLifecycle(func(tx *sql.Tx) error {
		var switching bool
		if err := tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM task_operation_activity WHERE task_id=$1 AND switch_token<>'')`, id).Scan(&switching); err != nil {
			return err
		}
		if switching {
			return ErrProxyInUse
		}
		var locked bool
		if err := tx.QueryRow(`SELECT pg_try_advisory_xact_lock($1)`, plannerRunSessionKey(id)).Scan(&locked); err != nil {
			return err
		}
		if !locked {
			return ErrPlannerRunBusy
		}
		return fn(tx)
	})
}
