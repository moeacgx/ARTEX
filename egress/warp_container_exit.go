package egress

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// ConfirmContainerProcessExit 只接受宿主验证器给出的真实容器退出证明。
// 调用者还须持有健康执行租约并核对数据库闲置屏障；本方法仅原子更新原进程记录。
// 任何失败均不写记录，也不会终止进程或把原 Host/PID 替换成本机身份。
func (m *WarpManager) ConfirmContainerProcessExit(ctx context.Context, id, expectedProcessSHA256 string, verifier func(context.Context, string, int) error) error {
	unlock, err := m.beginProfile(id)
	if err != nil {
		return err
	}
	defer unlock()
	ctx, cancel := m.operationContext(ctx)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	if verifier == nil || m.process(id) != nil {
		return ErrWarpExitUnknown
	}
	expected, err := hex.DecodeString(expectedProcessSHA256)
	if err != nil || len(expected) != sha256.Size {
		return ErrWarpExitUnknown
	}
	path, err := m.path(id)
	if err != nil {
		return err
	}
	filename := filepath.Join(path, "process.json")
	data, err := os.ReadFile(filename)
	if err != nil {
		return ErrWarpExitUnknown
	}
	digest := sha256.Sum256(data)
	var record warpProcessRecord
	if !bytes.Equal(digest[:], expected) || json.Unmarshal(data, &record) != nil || !validWarpProcessRecord(record) || record.PID <= 0 {
		return ErrWarpExitUnknown
	}
	host, err := os.Hostname()
	if err != nil {
		return ErrWarpExitUnknown
	}
	if record.Host == host {
		exited, err := warpPIDExited(record.PID)
		if err != nil || !exited {
			return ErrWarpExitUnknown
		}
	}
	if err := verifier(ctx, record.Host, record.PID); err != nil {
		return fmt.Errorf("%w: 宿主退出证明校验失败: %w", ErrWarpExitUnknown, err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// 核验期间其他实例或外部操作可能更新文件；不覆盖任何新进程身份。
	current, err := os.ReadFile(filename)
	if err != nil {
		return ErrWarpExitUnknown
	}
	currentDigest := sha256.Sum256(current)
	if !bytes.Equal(currentDigest[:], expected) {
		return ErrWarpExitUnknown
	}
	record.Exited = true
	return privateWrite(filename, record)
}
