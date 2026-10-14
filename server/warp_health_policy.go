package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/Autumn-27/artex/db"
	"github.com/Autumn-27/artex/egress"
)

type warpHealthPolicyDTO struct {
	ProxyID              string `json:"proxy_id"`
	Revision             int64  `json:"revision"`
	AutoRecoverEnabled   bool   `json:"auto_recover_enabled"`
	ProbeIntervalSeconds int    `json:"probe_interval_seconds"`
	FailureThreshold     int    `json:"failure_threshold"`
	RecoveryMinSeconds   int    `json:"recovery_min_seconds"`
	RecoveryMaxSeconds   int    `json:"recovery_max_seconds"`
}

func warpPolicyDTO(state db.WarpHealthState) warpHealthPolicyDTO {
	return warpHealthPolicyDTO{ProxyID: i64s(state.ProxyID), Revision: state.Revision, AutoRecoverEnabled: state.AutoRecoverEnabled, ProbeIntervalSeconds: 60, FailureThreshold: 3, RecoveryMinSeconds: 300, RecoveryMaxSeconds: 3600}
}

func (s *Server) warpHealthPolicy(w http.ResponseWriter, r *http.Request) {
	id, err := parseProxyIDString(r.PathValue("id"))
	if err != nil {
		writeErr(w, 400, "WARP 档案 ID 无效")
		return
	}
	proxy, err := s.m.pg.GetProxy(id)
	if err != nil || proxy.Kind != db.ProxyKindWarp {
		writeErr(w, 404, "WARP 档案不存在")
		return
	}
	if r.Method == http.MethodPut {
		var input struct {
			Revision json.RawMessage `json:"expected_revision"`
			Enabled  *bool           `json:"auto_recover_enabled"`
		}
		if !decodeProxyHTTP(w, r, &input) {
			return
		}
		revision, err := parseProxyRevision(input.Revision)
		if err != nil || revision == 0 || input.Enabled == nil {
			writeErr(w, 400, "健康策略参数无效")
			return
		}
		err = s.m.pg.SetWarpAutoRecovery(r.Context(), id, revision, *input.Enabled)
		if err != nil {
			s.writeWarpHealthPolicyError(w, id, err)
			return
		}
	}
	state, err := s.m.pg.WarpHealth(r.Context(), id)
	if err != nil {
		s.writeWarpHealthPolicyError(w, id, err)
		return
	}
	writeJSON(w, 200, warpPolicyDTO(state))
}

func (s *Server) warpHealthRecovery(w http.ResponseWriter, r *http.Request) {
	id, err := parseProxyIDString(r.PathValue("id"))
	if err != nil {
		writeErr(w, 400, "WARP 档案 ID 无效")
		return
	}
	var body map[string]json.RawMessage
	if !decodeProxyHTTP(w, r, &body) {
		return
	}
	if len(body) != 0 {
		writeErr(w, 400, "恢复请求不接受解封声明")
		return
	}
	runtime, err := s.m.proxyRuntime()
	if err != nil || runtime.health == nil {
		writeErr(w, 503, "WARP 健康控制器不可用")
		return
	}
	if err := runtime.health.ResolveUnknown(r.Context(), id); err != nil {
		s.writeWarpHealthPolicyError(w, id, err)
		return
	}
	proxy, err := s.m.pg.GetProxy(id)
	if err != nil {
		s.writeWarpHealthPolicyError(w, id, err)
		return
	}
	writeJSON(w, 200, s.runtimeProxyDTO(proxy))
}

func (s *Server) writeWarpHealthPolicyError(w http.ResponseWriter, id int64, err error) {
	switch {
	case errors.Is(err, sql.ErrNoRows), errors.Is(err, db.ErrProxyUnavailable):
		writeJSON(w, 404, map[string]any{"error": "WARP 档案不存在", "code": "warp_health_not_found"})
	case errors.Is(err, db.ErrWarpHealthStale):
		response := map[string]any{"error": "健康策略版本已变化", "code": "warp_health_revision_conflict"}
		if state, readErr := s.m.pg.WarpHealth(context.Background(), id); readErr == nil {
			response["current_revision"] = state.Revision
		}
		writeJSON(w, 409, response)
	case errors.Is(err, db.ErrWarpHealthBlocked), errors.Is(err, db.ErrProxyInUse):
		writeJSON(w, 409, map[string]any{"error": "WARP 恢复正在执行或仍有活动", "code": "warp_health_recovery_busy"})
	case errors.Is(err, egress.ErrWarpExitUnknown):
		writeJSON(w, 409, map[string]any{"error": "旧 WARP 执行退出仍未确认", "code": "warp_health_recovery_required"})
	default:
		writeJSON(w, 500, map[string]any{"error": "WARP 健康操作失败", "code": "warp_health_operation_failed"})
	}
}
