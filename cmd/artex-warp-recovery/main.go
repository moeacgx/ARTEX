package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/Autumn-27/artex/db"
	"github.com/Autumn-27/artex/egress"
	"github.com/Autumn-27/artex/maintenance"
	"github.com/Autumn-27/artex/server"
)

type recoveryOptions struct {
	proxySecrets string
	warpDir      string
	request      server.WarpContainerExitRequest
	docker       maintenance.DockerExitOptions
}

func parseRecoveryOptions(args []string) (recoveryOptions, error) {
	var opts recoveryOptions
	flags := flag.NewFlagSet("artex-warp-recovery", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&opts.proxySecrets, "proxy-secrets", "", "现有代理密钥文件路径")
	flags.StringVar(&opts.warpDir, "warp-dir", "", "WARP 数据的实际宿主目录")
	flags.Int64Var(&opts.request.ProxyID, "proxy-id", 0, "待核验的 WARP 档案 ID")
	flags.Int64Var(&opts.request.ExpectedRevision, "expected-revision", 0, "预期健康版本")
	flags.StringVar(&opts.request.ExpectedClaim, "expected-claim", "", "预期 unknown claim token")
	flags.StringVar(&opts.request.ExpectedProcessSHA256, "expected-process-sha", "", "原 process.json 的 SHA256")
	flags.StringVar(&opts.docker.EvidencePath, "exit-evidence", "", "旧容器退出证据文件")
	flags.StringVar(&opts.docker.EvidenceSHA256, "expected-evidence-sha", "", "退出证据文件的 SHA256")
	flags.StringVar(&opts.docker.OldContainerID, "old-container-full-id", "", "旧容器完整 ID")
	flags.StringVar(&opts.docker.CurrentContainerID, "current-container-full-id", "", "当前容器完整 ID")
	if err := flags.Parse(args); err != nil {
		return opts, errors.New("维护参数无效；本命令不接受强制解除屏障选项")
	}
	if flags.NArg() != 0 || opts.proxySecrets == "" || opts.warpDir == "" || opts.request.ProxyID <= 0 || opts.request.ExpectedRevision <= 0 || opts.request.ExpectedClaim == "" || opts.request.ExpectedProcessSHA256 == "" || opts.docker.EvidencePath == "" || opts.docker.EvidenceSHA256 == "" || opts.docker.OldContainerID == "" || opts.docker.CurrentContainerID == "" {
		return opts, errors.New("必须指定现有密钥、WARP 目录、档案 ID、健康版本、claim、进程摘要、退出证据及两个完整容器 ID")
	}
	var err error
	if opts.warpDir, err = filepath.Abs(opts.warpDir); err != nil {
		return opts, errors.New("WARP 目录路径无效")
	}
	opts.docker.WarpDir = opts.warpDir
	return opts, nil
}

func runRecovery(ctx context.Context, args []string, output io.Writer) error {
	opts, err := parseRecoveryOptions(args)
	if err != nil {
		return err
	}
	dsn := os.Getenv("ARTEX_PG_DSN")
	if dsn == "" {
		return errors.New("仅从 ARTEX_PG_DSN 环境变量读取数据库连接，请先配置")
	}
	// 维护只读现有密钥和目录，拒绝因路径拼错创建新的密钥或档案。
	key, err := os.ReadFile(opts.proxySecrets)
	if err != nil || len(key) != 32 {
		return errors.New("现有代理密钥文件不存在或长度无效")
	}
	info, err := os.Stat(opts.warpDir)
	if err != nil || !info.IsDir() {
		return errors.New("WARP 宿主目录不存在")
	}
	if err := db.ConfigureProxySecrets(opts.proxySecrets); err != nil {
		return errors.New("代理密钥路径配置失败")
	}
	verifier, err := maintenance.DockerContainerExitVerifier(opts.docker)
	if err != nil {
		return err
	}
	// 只连接现有 schema，不使用会迁移和写入默认配置的 db.Open。
	conn, err := sql.Open("pgx", dsn)
	if err != nil {
		return errors.New("数据库连接参数无效")
	}
	defer conn.Close()
	if err := conn.PingContext(ctx); err != nil {
		return errors.New("数据库连接失败，未进行退出确认")
	}
	runtime, err := egress.NewWarpManager(opts.warpDir)
	if err != nil {
		return err
	}
	defer runtime.Close()
	if err := server.ConfirmWarpContainerProcessExit(ctx, &db.DB{DB: conn}, runtime, opts.request, verifier); err != nil {
		return err
	}
	_, err = fmt.Fprintf(output, "已核验并登记 WARP 档案 %d 的旧容器进程退出事实；数据库恢复屏障保留，请继续调用正常健康恢复接口。\n", opts.request.ProxyID)
	return err
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := runRecovery(ctx, os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "WARP 宿主退出确认失败：", err)
		os.Exit(1)
	}
}
