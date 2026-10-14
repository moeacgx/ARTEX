package harness

import (
	"context"
	"errors"
	"time"
)

var (
	ErrHardDeadlineExceeded = errors.New("规划整轮硬截止已到达")
	ErrHardDeadlineCleanup  = errors.New("规划工具未在清理时限内确认退出")
)

const defaultHardCleanupTimeout = 10 * time.Second

type toolCleanupPending struct{ done <-chan struct{} }

func (e *toolCleanupPending) Error() string { return ErrHardDeadlineCleanup.Error() }
func (e *toolCleanupPending) Unwrap() error { return ErrHardDeadlineCleanup }

// 清理超限后宿主仍可等待实际工具退出，再关闭工具共享的 Session 资源。
func WaitForToolExit(ctx context.Context, err error) error {
	var pending *toolCleanupPending
	if !errors.As(err, &pending) {
		return nil
	}
	select {
	case <-pending.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (l *loop) hardCleanupTimeout() time.Duration {
	if l.in.HardCleanupTimeout > 0 {
		return l.in.HardCleanupTimeout
	}
	return defaultHardCleanupTimeout
}

func (l *loop) finishCanceled(reason TerminalReason, extra error) {
	cause := context.Cause(l.ctx)
	if errors.Is(cause, ErrHardDeadlineExceeded) {
		reason = ReasonTimeout
	}
	l.finish(reason, errors.Join(cause, extra), "")
}

// 先关闭派发，再取消执行，最后等待真实工具 goroutine 退出。
// 超时只报告失败；宿主必须保留操作引用，不能把返回当成已经清理。
func (e *streamExec) stopDispatch() {
	e.mu.Lock()
	e.closed = true
	e.mu.Unlock()
	e.cancel()
}

func (e *streamExec) closeAndWait(timeout time.Duration) error {
	e.stopDispatch()
	done := make(chan struct{})
	go func() {
		e.wg.Wait()
		close(done)
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		return nil
	case <-timer.C:
		return &toolCleanupPending{done: done}
	}
}
