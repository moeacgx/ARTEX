// Command artex runs the ARTEX backend: the dual SQLite graph stores,
// the event-driven exploration engine, and the JSON HTTP API consumed by the
// shadcn/ui frontend.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"github.com/Autumn-27/artex/agent"
	"github.com/Autumn-27/artex/config"
	"github.com/Autumn-27/artex/db"
	browserruntime "github.com/Autumn-27/artex/runtime/playwright"
	"github.com/Autumn-27/artex/selfupdate"
	"github.com/Autumn-27/artex/server"
)

// version is the build version, injected at release time via
// -ldflags "-X main.version=<tag>". Defaults to "dev" for local builds.
var version = "dev"

const banner = `
    _    ____ _____ _______  __
   / \  |  _ \_   _| ____\ \/ /
  / _ \ | |_) || | |  _|  \  /
 / ___ \|  _ < | | | |___ /  \
/_/   \_\_| \_\|_| |_____/_/\_\
`

// printBanner writes the startup banner + version/runtime info to stdout.
func printBanner(addr string) {
	fmt.Print(banner)
	fmt.Println("  AI 自主渗透测试系统")
	fmt.Printf("  版本 %s  ·  %s/%s  ·  %s  ·  监听 %s\n\n",
		version, runtime.GOOS, runtime.GOARCH, runtime.Version(), addr)
}

// main only maps run's result onto the process exit code. The exit code is part
// of the update protocol — the supervising start script reads it to decide
// whether to relaunch us (see selfupdate.ExitRestart) — so the body has to live
// in a function that can *return* rather than os.Exit past its own defers.
func main() {
	os.Exit(run())
}

func run() int {
	var (
		addr          = flag.String("addr", ":8787", "HTTP listen address")
		dataDir       = flag.String("data", filepath.Join(config.BaseDir(), "data"), "data directory for SQLite stores (default: data/ next to the executable)")
		proxy         = flag.String("proxy", "127.0.0.1:8788", "traffic recording proxy address (empty to disable)")
		selectRuntime = flag.Bool("selfupdate-select", false, "选择容器数据卷中的程序（启动器使用）")
	)
	flag.Parse()
	if *selectRuntime {
		image, err := os.Executable()
		if err != nil {
			log.Print(err)
			return 1
		}
		bin, err := selfupdate.PrepareContainerRuntime(os.Getenv("ARTEX_SELFUPDATE_ROOT"), image, version)
		if err != nil {
			log.Printf("选择持久化程序失败: %v", err)
			return 1
		}
		fmt.Println(bin)
		return 0
	}

	// hand the build version to the server package so GET /api/health can report it
	// to the frontend top bar.
	server.BuildVersion = version

	printBanner(*addr)

	// capture backend logs into the in-memory sink (still to stderr) so the /logs
	// page can show a live log stream. Do this first, to catch startup logs too.
	server.StartLogCapture()

	// Self-update bootstrap: swap in a staged binary, or count a post-swap boot
	// attempt and roll back if the new build keeps dying. Must run before we open
	// the stores or bind a port — this may end with "exit and let the start script
	// relaunch me", and there is no point paying for either first.
	action, upState := selfupdate.Bootstrap()
	server.SetBootUpdateState(upState)
	if action == selfupdate.Restart {
		return selfupdate.ExitRestart
	}

	// surface which config file the binary reads (absolute, so `go run`'s relative
	// "config.json" — resolved against the CWD — is unambiguous).
	cfgPath := config.Path()
	if abs, e := filepath.Abs(cfgPath); e == nil {
		cfgPath = abs
	}
	if _, e := os.Stat(cfgPath); e == nil {
		log.Printf("[config] 配置文件: %s", cfgPath)
	} else {
		log.Printf("[config] 配置文件: %s (不存在 — 将仅尝试环境变量 ARTEX_PG_DSN)", cfgPath)
	}

	sigCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, shutdown := shutdownContext(sigCtx)
	defer shutdown(agent.AbortShutdown)
	toolRoot, err := browserruntime.Prepare(*dataDir)
	if err != nil {
		log.Printf("[browser] 工具包准备失败: %v", err)
		return 1
	}
	go func() {
		if err := browserruntime.Install(ctx, toolRoot); err != nil {
			log.Printf("[browser] %v", err)
		}
	}()

	// 代理密钥独立于 JWT；容器通过独立卷持久化，丢失/损坏时启动失败。
	secretPath := os.Getenv("ARTEX_PROXY_SECRETS_FILE")
	if secretPath == "" {
		secretPath = filepath.Join(*dataDir, "proxy-secrets", "key")
	}
	if err := db.ConfigureProxySecrets(secretPath); err != nil {
		log.Printf("代理密钥配置失败")
		return 1
	}
	if err := db.InitializeProxySecrets(); err != nil {
		log.Printf("代理密钥初始化失败: %v", err)
		return 1
	}
	mgr, err := server.NewManager(*dataDir, *proxy)
	if err != nil {
		log.Fatalf("open stores: %v", err)
	}
	defer mgr.Close()
	if err := mgr.InitProxyRuntime(ctx); err != nil {
		log.Printf("代理运行时初始化失败: %v", err)
		return 1
	}
	defer mgr.CloseProxyRuntime()

	skillDir := config.SkillDir()
	if abs, err := filepath.Abs(skillDir); err == nil {
		skillDir = abs
	}
	log.Printf("[config] skill 目录: %s", skillDir)
	srv := server.New(ctx, mgr, skillDir, *dataDir, config.BaseDir())
	httpSrv := &http.Server{
		Addr:              *addr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Printf("HTTP 监听失败: %v", err)
		return 1
	}
	defer listener.Close()
	// 成功监听后才开始稳定确认，初始化耗时不能提前消耗确认窗口。
	settle := time.AfterFunc(selfupdate.SettleDelay, selfupdate.Settle)
	defer settle.Stop()

	go func() {
		log.Printf("ARTEX %s backend listening on %s (data=%s, workers=%d)", version, *addr, *dataDir, mgr.Workers())
		if err := httpSrv.Serve(listener); err != nil && err != http.ErrServerClosed {
			log.Fatalf("serve: %v", err)
		}
	}()

	// Two ways out: a signal (normal stop → exit 0, the start script stops looping)
	// or a staged update / rollback (→ exit 75, the script relaunches us and the
	// bootstrap above installs the new build).
	code := 0
	select {
	case <-ctx.Done():
	case <-server.RestartRequested():
		code = selfupdate.ExitRestart
		shutdown(agent.AbortShutdown)
	}

	log.Println("shutting down...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP 关停未完成，关闭残留连接: %v", err)
		_ = httpSrv.Close()
	}
	cancel()
	if err := waitForServerShutdown(srv, 45*time.Second); err != nil {
		log.Printf("服务执行已退出，但出口回收未确认成功: %v", err)
		return 1
	}
	return code
}

// shutdownContext deliberately does not derive from signalCtx. If it did, the
// parent's plain context.Canceled could win the race before AbortShutdown was
// attached to the child, losing the diagnostic cause in every running Agent.
func shutdownContext(signalCtx context.Context) (context.Context, context.CancelCauseFunc) {
	ctx, shutdown := context.WithCancelCause(context.Background())
	go func() {
		select {
		case <-signalCtx.Done():
			shutdown(agent.AbortShutdown)
		case <-ctx.Done():
		}
	}()
	return ctx, shutdown
}

type serverShutdown interface {
	Shutdown(context.Context) error
}

func waitForServerShutdown(srv serverShutdown, window time.Duration) error {
	for {
		ctx, cancel := context.WithTimeout(context.Background(), window)
		err := srv.Shutdown(ctx)
		timedOut := ctx.Err() != nil && errors.Is(err, context.DeadlineExceeded)
		cancel()
		if !timedOut {
			return err
		}
		log.Println("服务仍在等待模型、工具或出口真实退出，保留资源，暂不重启")
	}
}
