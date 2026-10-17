package server

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/Autumn-27/norma/harness"
)

func TestWorkerModelTimeoutDoesNotReplayIntent(t *testing.T) {
	for _, err := range []error{
		context.DeadlineExceeded,
		context.Canceled,
		fmt.Errorf("模型正文等待超时: %w", context.DeadlineExceeded),
		errors.Join(errors.New("模型执行中断"), harness.ErrHardDeadlineExceeded),
		harness.ErrHardDeadlineCleanup,
	} {
		if retryableWorkerModelError(harness.ReasonModelError, err) {
			t.Errorf("已发送的超时请求不能触发整条意图重放: %v", err)
		}
	}
	if !retryableWorkerModelError(harness.ReasonModelError, errors.New("temporary network error")) {
		t.Fatal("普通瞬时模型错误应保留既有重试")
	}
}
