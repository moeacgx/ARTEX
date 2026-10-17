// Package maintenance 提供宿主机上的正式维护操作，不参与任务执行。
package maintenance

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type DockerExitOptions struct {
	OldContainerID, CurrentContainerID    string
	EvidencePath, EvidenceSHA256, WarpDir string
}

func DockerContainerExitVerifier(opts DockerExitOptions) (func(context.Context, string, int) error, error) {
	// 只能访问本机 Docker Unix socket，不接受远程地址或调用方提交的 Docker JSON。
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", "/var/run/docker.sock")
	}}
	return dockerContainerExitVerifier(opts, &http.Client{Transport: transport, Timeout: 15 * time.Second})
}

func dockerContainerExitVerifier(opts DockerExitOptions, client *http.Client) (func(context.Context, string, int) error, error) {
	for _, value := range []string{opts.OldContainerID, opts.CurrentContainerID, opts.EvidenceSHA256} {
		decoded, err := hex.DecodeString(value)
		if err != nil || len(decoded) != sha256.Size || strings.ToLower(value) != value {
			return nil, errors.New("必须指定完整容器 ID 和小写 SHA256")
		}
	}
	if opts.OldContainerID == opts.CurrentContainerID || !filepath.IsAbs(opts.WarpDir) || !filepath.IsAbs(opts.EvidencePath) {
		return nil, errors.New("新旧容器及证据路径无效")
	}
	return func(ctx context.Context, host string, pid int) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if host != opts.OldContainerID[:12] || pid <= 0 {
			return errors.New("旧进程记录与指定容器身份不匹配")
		}
		// 摘要由操作者从本次真实 stop 审计结果固定；仍需向宿主机验证，不能仅信 exited 声明。
		info, err := os.Lstat(opts.EvidencePath)
		if err != nil || !info.Mode().IsRegular() || info.Size() > 64*1024 {
			return errors.New("退出证据文件无效")
		}
		data, err := os.ReadFile(opts.EvidencePath)
		if err != nil {
			return errors.New("无法读取退出证据")
		}
		digest := sha256.Sum256(data)
		if hex.EncodeToString(digest[:]) != opts.EvidenceSHA256 {
			return errors.New("退出证据摘要不匹配")
		}
		var proof struct {
			ID        string `json:"id"`
			Status    string `json:"status"`
			PID       int    `json:"pid"`
			ExitCode  int    `json:"exit_code"`
			Finished  string `json:"finished"`
			OldPIDs   []int  `json:"old_pids"`
			Remaining []int  `json:"remaining_old_pids"`
		}
		if json.Unmarshal(data, &proof) != nil || proof.ID != opts.OldContainerID || proof.Status != "exited" || proof.PID != 0 || proof.ExitCode != 0 || len(proof.OldPIDs) == 0 || proof.Remaining == nil || len(proof.Remaining) != 0 {
			return errors.New("证据未确认旧容器正常完整退出")
		}
		finished, err := time.Parse(time.RFC3339Nano, proof.Finished)
		if err != nil || finished.IsZero() || finished.After(time.Now()) {
			return errors.New("退出证据时间无效")
		}
		current, status, err := inspectContainer(ctx, client, opts.CurrentContainerID)
		if err != nil || status != http.StatusOK || current.ID != opts.CurrentContainerID || !current.State.Running || current.State.PID <= 0 || current.Config.Labels["com.docker.compose.service"] != "artex" {
			return errors.New("无法核实当前 ARTEX 应用容器")
		}
		warpDir, err := filepath.EvalSymlinks(opts.WarpDir)
		if err != nil {
			return errors.New("WARP 宿主档案目录不存在")
		}
		mounted := false
		for _, mount := range current.Mounts {
			if mount.Type != "bind" || mount.Destination != "/app/data" {
				continue
			}
			expected, err := filepath.EvalSymlinks(filepath.Join(mount.Source, "egress", "warp"))
			if err == nil && filepath.Clean(expected) == filepath.Clean(warpDir) {
				mounted = true
			}
		}
		if !mounted {
			return errors.New("当前容器未挂载指定 WARP 档案目录")
		}
		old, status, err := inspectContainer(ctx, client, opts.OldContainerID)
		if err != nil {
			return err
		}
		switch status {
		case http.StatusNotFound:
			// 已移除容器只能与固定摘要的本次停止证据合用，不能将一次 404 当作退出声明。
		case http.StatusOK:
			if old.ID != opts.OldContainerID || old.State.Running || old.State.PID != 0 || old.State.Status != "exited" || old.State.ExitCode != 0 {
				return errors.New("旧容器仍运行或退出结果不明确")
			}
		default:
			return errors.New("Docker 未确认旧容器退出")
		}
		for _, oldPID := range proof.OldPIDs {
			if oldPID <= 0 {
				return errors.New("退出证据中的宿主进程无效")
			}
			exited, err := hostProcessExited(oldPID)
			if err != nil || !exited {
				return errors.New("旧宿主进程仍存在或无法确认退出")
			}
		}
		return ctx.Err()
	}, nil
}

type dockerContainer struct {
	ID    string `json:"Id"`
	State struct {
		Running  bool
		PID      int `json:"Pid"`
		Status   string
		ExitCode int
	}
	Config struct{ Labels map[string]string }
	Mounts []struct{ Type, Source, Destination string }
}

func inspectContainer(ctx context.Context, client *http.Client, id string) (dockerContainer, int, error) {
	var result dockerContainer
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://docker/containers/"+id+"/json", nil)
	if err != nil {
		return result, 0, err
	}
	response, err := client.Do(req)
	if err != nil {
		return result, 0, errors.New("无法连接宿主 Docker，拒绝确认退出")
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusOK {
		if err = json.NewDecoder(io.LimitReader(response.Body, 4<<20)).Decode(&result); err != nil {
			return result, 0, fmt.Errorf("Docker 容器响应无效: %w", err)
		}
	}
	return result, response.StatusCode, nil
}
