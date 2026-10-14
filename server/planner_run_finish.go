package server

import (
	"context"
	"errors"
	"log"

	"github.com/Autumn-27/artex/agent"
)

func finishPlannerRun(taskID string, plannerErr error, finishOperation, plannerExited func()) {
	finish := func(finalErr error) {
		if errors.Is(finalErr, agent.ErrPlannerCleanup) {
			log.Printf("[planner] task %s 清理失败，保留 Planner 与操作占用: %v", taskID, finalErr)
			return
		}
		plannerExited()
		finishOperation()
	}
	if !agent.HasPendingPlannerExit(plannerErr) {
		finish(plannerErr)
		return
	}
	go func() {
		finalErr := agent.AwaitPlannerExit(context.Background(), plannerErr)
		finish(finalErr)
	}()
}
