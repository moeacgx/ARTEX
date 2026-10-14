package agent

import (
	"context"
	"github.com/Autumn-27/norma/agentcore"
	"sync/atomic"
)

type proxyRunStopKey struct{}
type ProxyRunStop struct{ requested atomic.Bool }

func WithProxyRunStop(ctx context.Context) (context.Context, *ProxyRunStop) {
	stop := &ProxyRunStop{}
	return context.WithValue(ctx, proxyRunStopKey{}, stop), stop
}
func RequestProxyRunStop(ctx context.Context) bool {
	stop, ok := ctx.Value(proxyRunStopKey{}).(*ProxyRunStop)
	if !ok {
		return false
	}
	stop.requested.Store(true)
	return true
}
func ProxyRunStopRequested(ctx context.Context) bool {
	stop, ok := ctx.Value(proxyRunStopKey{}).(*ProxyRunStop)
	return ok && stop.requested.Load()
}

func closeProxySession(ctx context.Context, session *agentcore.Session) {
	if err := session.Close(); err != nil {
		ReportProxyCleanupFailure(ctx)
	}
}
