package server

import (
	"errors"
	"sync"
	"sync/atomic"
)

var ErrPlannerStillExiting = errors.New("旧 Planner 尚未确认退出，请等待模型、工具与清理结束后继续")

// 仅补同一 Engine 的旧 Planner 退出判定；持久操作占用仍由原准入路径负责。
func (e *Engine) beginPlannerRun(taskID string, final ...bool) func() {
	e.plannerRunMu.Lock()
	defer e.plannerRunMu.Unlock()
	if e.IsPaused(taskID) && (len(final) == 0 || !final[0]) {
		return nil
	}
	value, _ := e.plannerRuns.LoadOrStore(taskID, new(atomic.Int64))
	counter := value.(*atomic.Int64)
	if !counter.CompareAndSwap(0, 1) {
		return nil
	}
	var once sync.Once
	return func() { once.Do(func() { counter.Add(-1) }) }
}

func (e *Engine) plannerResumeError(taskID string) error {
	if value, ok := e.plannerRuns.Load(taskID); ok && value.(*atomic.Int64).Load() > 0 {
		return ErrPlannerStillExiting
	}
	return nil
}
