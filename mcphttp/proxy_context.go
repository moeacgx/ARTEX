package mcphttp

import (
	"context"
	"net/http"
)

type httpClientContextKey struct{}

// WithHTTPClient 保持本次 MCP 握手、调用和清理使用同一个主体出口。
func WithHTTPClient(ctx context.Context, client *http.Client) context.Context {
	return context.WithValue(ctx, httpClientContextKey{}, client)
}
func contextHTTPClient(ctx context.Context, fallback *http.Client) *http.Client {
	if client, ok := ctx.Value(httpClientContextKey{}).(*http.Client); ok && client != nil {
		return client
	}
	return fallback
}
