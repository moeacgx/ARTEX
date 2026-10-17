package server

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"

	"github.com/Autumn-27/artex/db"
	"github.com/google/uuid"
)

// WarpContainerExitRuntime 只允许登记经宿主验证的退出事实，不启动或停止进程。
type WarpContainerExitRuntime interface {
	ConfirmContainerProcessExit(context.Context, string, string, func(context.Context, string, int) error) error
}

type WarpContainerExitRequest struct {
	ProxyID               int64
	ExpectedRevision      int64
	ExpectedClaim         string
	ExpectedProcessSHA256 string
}

// ConfirmWarpContainerProcessExit 保留数据库 unknown 屏障及任务控制状态。
// 退出收据登记后，仍须通过正常健康恢复接口复核并解除旧 claim。
func ConfirmWarpContainerProcessExit(ctx context.Context, store *db.DB, runtime WarpContainerExitRuntime, req WarpContainerExitRequest, verifier func(context.Context, string, int) error) error {
	claim, claimErr := uuid.Parse(req.ExpectedClaim)
	digest, digestErr := hex.DecodeString(req.ExpectedProcessSHA256)
	if store == nil || runtime == nil || verifier == nil || req.ProxyID <= 0 || req.ExpectedRevision <= 0 || claimErr != nil || claim == uuid.Nil || digestErr != nil || len(digest) != 32 {
		return errors.New("容器退出确认参数不完整或无效")
	}
	execution, err := store.TryWarpHealthExecution(ctx, req.ProxyID)
	if err != nil {
		return err
	}
	defer execution.Close()
	m := &Manager{pg: store}
	guard := func(ctx context.Context, tx *sql.Tx, id int64) error {
		state, proxy, pending, err := db.WarpHealthViewTx(ctx, tx, id)
		if err != nil {
			return err
		}
		if proxy.Kind != db.ProxyKindWarp || pending || state.ClaimState != "unknown" {
			return db.ErrWarpHealthBlocked
		}
		if state.Revision != req.ExpectedRevision || state.Claim.Token != req.ExpectedClaim {
			return db.ErrWarpHealthStale
		}
		if _, running := warpHealthExecutions.Load(state.Claim.Token); running {
			return db.ErrWarpHealthBlocked
		}
		return m.warpHealthRepairGuard(ctx, tx, id)
	}
	if err := execution.CheckIdle(ctx, req.ProxyID, guard); err != nil {
		return err
	}
	return runtime.ConfirmContainerProcessExit(ctx, warpProfileID(req.ProxyID), req.ExpectedProcessSHA256, func(ctx context.Context, host string, pid int) error {
		if err := verifier(ctx, host, pid); err != nil {
			return err
		}
		// 宿主证据读取期间可能发生策略变更，写退出收据前必须复核同一 claim。
		return execution.CheckIdle(ctx, req.ProxyID, guard)
	})
}
