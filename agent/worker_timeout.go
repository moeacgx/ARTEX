package agent

import (
	"context"
	"errors"
	"time"

	"github.com/Autumn-27/norma/harness"
)

const defaultWorkerSettlementTimeout = 2 * time.Minute

var errWorkerHardDeadline = errors.Join(errors.New("Worker 运行及收尾硬截止已到达"), harness.ErrHardDeadlineExceeded, context.DeadlineExceeded)

// 正常预算仍负责触发收尾；外层绝对截止也覆盖工具装配、模型等待及压缩。
// 显式不限保持不限，不能因为默认收尾宽限而悄悄增加运行上限。
func workerRunContext(ctx context.Context, budget time.Duration) (context.Context, context.CancelFunc) {
	if budget <= 0 {
		return context.WithCancel(ctx)
	}
	allowance := configuredTimeout(0, "ARTEX_WORKER_SETTLEMENT_TIMEOUT", defaultWorkerSettlementTimeout)
	const maxDuration = time.Duration(1<<63 - 1)
	hardBudget := maxDuration
	if budget <= maxDuration-allowance {
		hardBudget = budget + allowance
	}
	return context.WithTimeoutCause(ctx, hardBudget, errWorkerHardDeadline)
}

func workerRemainingBudget(start time.Time, budget time.Duration) time.Duration {
	if budget <= 0 {
		return 0
	}
	remaining := budget - time.Since(start)
	if remaining <= 0 {
		return time.Nanosecond
	}
	return remaining
}

func workerRemainingHardBudget(ctx context.Context, budget time.Duration) time.Duration {
	if budget <= 0 {
		return 0
	}
	deadline, _ := ctx.Deadline()
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return time.Nanosecond
	}
	return remaining
}

func workerTerminal(ctx context.Context, reason harness.TerminalReason, err error) (harness.TerminalReason, error) {
	if cause := context.Cause(ctx); cause != nil {
		err = errors.Join(cause, err)
		if errors.Is(cause, harness.ErrHardDeadlineExceeded) {
			reason = harness.ReasonTimeout
		}
	}
	return reason, err
}
