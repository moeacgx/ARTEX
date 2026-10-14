package server

import (
	"context"
	"errors"
	"log"

	"github.com/Autumn-27/artex/agent"
)

func finishPlannerRun(taskID string, plannerErr error, finishOperation, plannerExited func()) {
	finish := func(finalErr error) {
		finishOperation()
		if !errors.Is(finalErr, agent.ErrPlannerCleanup) {
			plannerExited()
		}
	}
	if !agent.HasPendingPlannerExit(plannerErr) {
		finish(plannerErr)
		return
	}
	go func() {
		finalErr := agent.AwaitPlannerExit(context.Background(), plannerErr)
		if finalErr != nil {
			log.Printf("[planner] task %s 延迟清理失败，保留操作占用: %v", taskID, finalErr)
		}
		finish(finalErr)
	}()
}
