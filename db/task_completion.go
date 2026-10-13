package db

// CompleteTaskIfIdle 在探索根锁内重新核验目标及活动意图，拒绝陈旧规划轮的收尾。
func (d *DB) CompleteTaskIfIdle(id int64) (bool, error) {
	tx, err := d.Begin()
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var expID int64
	if err := tx.QueryRow(`SELECT exploration_id FROM tasks WHERE id=$1 FOR UPDATE`, id).Scan(&expID); err != nil {
		return false, err
	}
	var root int64
	if err := tx.QueryRow(`SELECT id FROM explorations WHERE id=$1 FOR UPDATE`, expID).Scan(&root); err != nil {
		return false, err
	}
	res, err := tx.Exec(`UPDATE tasks SET status='done',completed_at=COALESCE(completed_at,now())
		WHERE id=$1 AND status NOT IN ('done','failed','timeout') AND NOT paused AND NOT queued
		AND NOT EXISTS (SELECT 1 FROM exploration_nodes WHERE exploration_id=$2
		AND ((kind='goal' AND state='open') OR (kind='intent' AND state IN ('open','running'))))`, id, expID)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return n == 1, nil
}
