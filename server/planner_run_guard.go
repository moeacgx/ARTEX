package server

import (
	"context"
	"errors"
	"log"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/Autumn-27/artex/db"
)

var ErrPlannerStillExiting = errors.New("旧 Planner 尚未确认退出，请等待模型、工具与清理结束后继续")

// 本地计数只用于同 Engine 快速判断；跨 Manager 的准入真相是数据库会话 lease。
func (e *Engine) beginPlannerRun(taskID string, final ...bool) (func(), error) {
	allowPaused := len(final) > 0 && final[0]
	e.plannerRunMu.Lock()
	defer e.plannerRunMu.Unlock()
	if e.IsPaused(taskID) && !allowPaused {
		return nil, db.ErrPlannerRunPaused
	}
	value, _ := e.plannerRuns.LoadOrStore(taskID, new(atomic.Int64))
	counter := value.(*atomic.Int64)
	if !counter.CompareAndSwap(0, 1) {
		return nil, db.ErrPlannerRunBusy
	}
	var lease *db.PlannerRunLease
	if e.m != nil && e.m.pg != nil {
		id, err := strconv.ParseInt(taskID, 10, 64)
		if err != nil {
			counter.Add(-1)
			return nil, err
		}
		lease, err = e.m.pg.AcquirePlannerRun(context.Background(), id, allowPaused)
		if err != nil {
			counter.Add(-1)
			return nil, err
		}
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			if lease != nil {
				if err := lease.Close(); err != nil {
					log.Printf("[planner] task %s 跨 Manager 退出 lease 解除失败，保留本地保护: %v", taskID, err)
					return
				}
			}
			counter.Add(-1)
		})
	}, nil
}

func (e *Engine) plannerResumeError(taskID string) error {
	if value, ok := e.plannerRuns.Load(taskID); ok && value.(*atomic.Int64).Load() > 0 {
		return ErrPlannerStillExiting
	}
	return nil
}
