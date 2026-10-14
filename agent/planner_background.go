package agent

import (
	"context"
	"sync"
)

type plannerBackgroundKey struct{}

// 轮内启动的冷节点压缩也属于本轮操作，不能脱离取消或退出判定。
type plannerBackground struct{ wg sync.WaitGroup }

func withPlannerBackground(ctx context.Context) (context.Context, *plannerBackground) {
	work := new(plannerBackground)
	return context.WithValue(ctx, plannerBackgroundKey{}, work), work
}

func compactionBackground(ctx context.Context) (context.Context, func()) {
	if work, ok := ctx.Value(plannerBackgroundKey{}).(*plannerBackground); ok {
		work.wg.Add(1)
		return ctx, work.wg.Done
	}
	return context.WithoutCancel(ctx), func() {}
}
