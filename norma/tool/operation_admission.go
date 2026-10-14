package tool

import "context"

type operationAdmissionKey struct{}
type operationNetworkFailureKey struct{}
type operationNetworkProxyKey struct{}

func WithOperationNetworkProxy(ctx context.Context, proxy string) context.Context {
	return context.WithValue(ctx, operationNetworkProxyKey{}, proxy)
}
func reportOperationHTTPFailure(ctx context.Context, err error) {
	if proxy, ok := ctx.Value(operationNetworkProxyKey{}).(string); ok {
		ReportOperationNetworkFailure(ctx, proxy, err)
	}
}

// WithOperationNetworkFailure 只接收网络客户端的真实错误，不解析模型生成的文本。
func WithOperationNetworkFailure(ctx context.Context, report func(context.Context, string, error)) context.Context {
	return context.WithValue(ctx, operationNetworkFailureKey{}, report)
}
func ReportOperationNetworkFailure(ctx context.Context, proxy string, err error) {
	if report, ok := ctx.Value(operationNetworkFailureKey{}).(func(context.Context, string, error)); ok && err != nil {
		report(ctx, proxy, err)
	}
}

// WithOperationAdmission 为所有工具（含 SDK 内置 WebFetch、Bash 和 MCP）设置正式准入。
func WithOperationAdmission(ctx context.Context, check func(context.Context, string) error) context.Context {
	return context.WithValue(ctx, operationAdmissionKey{}, check)
}
func CheckOperationAdmission(ctx context.Context, name string) error {
	if check, ok := ctx.Value(operationAdmissionKey{}).(func(context.Context, string) error); ok {
		return check(ctx, name)
	}
	return nil
}
