package agent

import (
	"context"
	"errors"
	"time"

	"github.com/Autumn-27/norma/harness"
)

const defaultPlannerRoundTimeout = 10 * time.Minute
const plannerCleanupTimeout = 10 * time.Second

var ErrPlannerCleanup = errors.New("Planner 清理未确认完成，保留任务操作占用")

type plannerExitPending struct{ done <-chan error }

func (e *plannerExitPending) Error() string { return ErrPlannerCleanup.Error() }
func (e *plannerExitPending) Unwrap() error { return ErrPlannerCleanup }
func (e *plannerExitPending) WaitPlannerExit(ctx context.Context) error {
	select {
	case finalErr, ok := <-e.done:
		if !ok {
			return nil
		}
		return finalErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

// PlannerExitWaiter 是跨包退出等待协议，服务端借此等待 errors.Join 树中的每个清理分支。
type PlannerExitWaiter interface {
	error
	WaitPlannerExit(context.Context) error
}

func HasPendingPlannerExit(err error) bool {
	if err == nil {
		return false
	}
	if _, ok := err.(PlannerExitWaiter); ok {
		return true
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		for _, child := range joined.Unwrap() {
			if HasPendingPlannerExit(child) {
				return true
			}
		}
		return false
	}
	if child := errors.Unwrap(err); child != nil {
		return HasPendingPlannerExit(child)
	}
	return false
}

func AwaitPlannerExit(ctx context.Context, err error) error {
	return errors.Join(awaitPlannerExitTree(ctx, err)...)
}

func awaitPlannerExitTree(ctx context.Context, err error) []error {
	if err == nil {
		return nil
	}
	if pending, ok := err.(PlannerExitWaiter); ok {
		return awaitPlannerExitTree(ctx, pending.WaitPlannerExit(ctx))
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		var resolved []error
		for _, child := range joined.Unwrap() {
			resolved = append(resolved, awaitPlannerExitTree(ctx, child)...)
		}
		return resolved
	}
	if child := errors.Unwrap(err); child != nil && HasPendingPlannerExit(child) {
		return awaitPlannerExitTree(ctx, child)
	}
	return []error{err}
}

// 即使压缩器或模型没有响应取消，也有界返回明确清理失败。
// 执行与其资源清理留在同一 goroutine，绝不因取消提前释放占用。
func waitPlannerRound(ctx context.Context, cleanupTimeout time.Duration, run func() (bool, string, error)) (bool, string, error) {
	type result struct {
		met    bool
		reason string
		err    error
	}
	done := make(chan result, 1)
	go func() {
		met, reason, err := run()
		done <- result{met, reason, err}
	}()
	select {
	case r := <-done:
		if ctx.Err() != nil {
			return false, r.reason, errors.Join(context.Cause(ctx), r.err)
		}
		return r.met, r.reason, r.err
	case <-ctx.Done():
	}
	timer := time.NewTimer(cleanupTimeout)
	defer timer.Stop()
	select {
	case r := <-done:
		return false, r.reason, errors.Join(context.Cause(ctx), r.err)
	case <-timer.C:
		late := make(chan error, 1)
		go func() { r := <-done; late <- AwaitPlannerExit(context.Background(), r.err) }()
		return false, "", errors.Join(context.Cause(ctx), &plannerExitPending{done: late})
	}
}

func plannerRoundDuration(deadlineUnix int64) (time.Duration, bool) {
	budget := configuredTimeout(0, "ARTEX_PLANNER_ROUND_TIMEOUT", defaultPlannerRoundTimeout)
	if deadlineUnix <= 0 {
		return budget, false
	}
	remaining := time.Until(time.Unix(deadlineUnix, 0))
	if remaining < budget {
		if remaining <= 0 {
			remaining = time.Nanosecond
		}
		return remaining, true
	}
	return budget, false
}

func reportPlannerCleanupFailure(ctx context.Context, err error) {
	if errors.Is(err, harness.ErrHardDeadlineCleanup) || errors.Is(err, ErrPlannerCleanup) {
		reportBrowserCleanupFailure(ctx)
	}
}

// 清理调用本身也必须有界；失败时只报告并保留屏障，不能提前归还操作引用。
func plannerCleanup(ctx context.Context, cleanup func() error) error {
	done := make(chan error, 1)
	go func() {
		err := cleanup()
		if err != nil {
			err = errors.Join(ErrPlannerCleanup, err)
			reportPlannerCleanupFailure(ctx, err)
		}
		done <- err
	}()
	timer := time.NewTimer(plannerCleanupTimeout)
	defer timer.Stop()
	var err error
	select {
	case err = <-done:
	case <-timer.C:
		return &plannerExitPending{done: done}
	}
	return err
}
