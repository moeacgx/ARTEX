package server

import (
	"context"
	"net/http"

	actool "github.com/Autumn-27/norma/tool"
)

// MCP 握手、调用和清理均报告真实连接错误，不把目标 HTTP 状态当作出口故障。
type proxyObservedTransport struct {
	base  *http.Transport
	ctx   context.Context
	proxy string
}

func (t proxyObservedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	response, err := t.base.RoundTrip(req)
	if err != nil {
		actool.ReportOperationNetworkFailure(t.ctx, t.proxy, err)
	}
	return response, err
}

func (t proxyObservedTransport) CloseIdleConnections() { t.base.CloseIdleConnections() }
