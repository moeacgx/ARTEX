package agent

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log"
	"os"
	"os/exec"
	"time"
)

// 等待本轮浏览器真实退出再归还任务活动引用，取消不会提前解除出口占用。
func browserRunEnvironment(env []string) []string {
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		panic("无法生成浏览器会话身份")
	}
	return append(env, "ARTEX_PLAYWRIGHT_RUN="+hex.EncodeToString(token[:]))
}

func closeRunBrowser(ctx context.Context, dir string, env []string) {
	tool, err := exec.LookPath("artex-playwright-cli")
	if err != nil {
		return
	}
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(cleanupCtx, tool, "cleanup")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), env...)
	if err := cmd.Run(); err != nil {
		log.Print("[browser] 本轮浏览器清理失败，需要核对残留会话")
		reportBrowserCleanupFailure(ctx)
	}
}

type browserCleanupKey struct{}

// WithBrowserCleanupFailure 由正式操作准入绑定；清理未确认时保留活动占用。
func WithBrowserCleanupFailure(ctx context.Context, report func()) context.Context {
	return context.WithValue(ctx, browserCleanupKey{}, report)
}
func reportBrowserCleanupFailure(ctx context.Context) {
	if report, ok := ctx.Value(browserCleanupKey{}).(func()); ok {
		report()
	}
}

// ReportProxyCleanupFailure 让 MCP 和浏览器共同遵守真实退出屏障。
func ReportProxyCleanupFailure(ctx context.Context) { reportBrowserCleanupFailure(ctx) }
