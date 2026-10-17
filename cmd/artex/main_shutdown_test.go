package main

import (
	"context"
	"errors"
	"testing"
	"time"
)

type shutdownSequence struct {
	calls int
	err   error
}

func (s *shutdownSequence) Shutdown(ctx context.Context) error {
	s.calls++
	if s.calls == 1 {
		<-ctx.Done()
		return ctx.Err()
	}
	return s.err
}

func TestWaitForServerShutdownContinuesAfterDeadline(t *testing.T) {
	s := new(shutdownSequence)
	if err := waitForServerShutdown(s, 5*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if s.calls != 2 {
		t.Fatalf("关停窗口超时被当成真实退出: calls=%d", s.calls)
	}
}

func TestWaitForServerShutdownPreservesCleanupFailure(t *testing.T) {
	failure := errors.New("出口回收未确认")
	s := &shutdownSequence{calls: 1, err: failure}
	if err := waitForServerShutdown(s, time.Second); !errors.Is(err, failure) {
		t.Fatalf("关停吞掉真实回收失败: %v", err)
	}
}

func TestWaitForServerShutdownPreservesCleanupDeadline(t *testing.T) {
	s := &shutdownSequence{calls: 1, err: context.DeadlineExceeded}
	if err := waitForServerShutdown(s, time.Second); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("关停吞掉出口自身超时: %v", err)
	}
}
