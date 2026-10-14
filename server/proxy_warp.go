package server

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/Autumn-27/artex/db"
	"github.com/Autumn-27/artex/egress"
)

func warpProfileID(id int64) string { return "proxy_" + strconv.FormatInt(id, 10) }
func (s *Server) warpAction(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		writeErr(w, 400, "invalid proxy id")
		return
	}
	var req struct {
		AcceptTerms bool `json:"accept_terms"`
	}
	if r.ContentLength != 0 {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeErr(w, 400, "bad json")
			return
		}
	}
	p, err := s.performWarpAction(r.Context(), id, r.PathValue("action"), req.AcceptTerms)
	if err != nil {
		s.writeWarpFailure(w, id, err)
		return
	}
	writeJSON(w, 200, s.runtimeProxyDTO(p))
}

// 手动 action 持有档案执行锁，网络和进程等待均在短事务外。
func (s *Server) performWarpAction(ctx context.Context, id int64, action string, acceptTerms bool) (*db.Proxy, error) {
	runtime, err := s.m.proxyRuntime()
	if err != nil {
		return nil, err
	}
	if action != "register" && action != "start" && action != "stop" && action != "recover" {
		return nil, errors.New("未知 WARP 操作")
	}
	if action == "register" && !acceptTerms {
		return nil, errors.New("请先阅读并接受 Cloudflare WARP 条款")
	}
	execution, err := s.m.pg.TryWarpHealthExecution(ctx, id)
	if err != nil {
		return nil, err
	}
	defer execution.Close()
	claim, err := execution.BeginWarpHealthManual(ctx, id, s.m.warpHealthRepairGuard)
	if err != nil {
		return nil, err
	}
	p, err := s.m.pg.GetProxyContext(ctx, id)
	if err != nil || p == nil || p.Kind != db.ProxyKindWarp {
		markWarpActionUnknown(execution, claim)
		return nil, db.ErrProxyUnavailable
	}
	// 取消先持久标 unknown；仍同步等待原动作真实退出才释放执行 session。
	marked := make(chan struct{})
	var once sync.Once
	stopMark := context.AfterFunc(ctx, func() {
		markWarpActionUnknown(execution, claim)
		once.Do(func() { close(marked) })
	})
	var profile egress.WarpProfile
	var operationErr error
	switch action {
	case "register":
		profile, operationErr = runtime.warp.Create(ctx, warpProfileID(id), p.Name, acceptTerms)
	case "start":
		profile, operationErr = runtime.warp.Start(ctx, warpProfileID(id))
	case "stop":
		profile, operationErr = runtime.warp.Stop(ctx, warpProfileID(id))
	case "recover":
		profile, operationErr = runtime.warp.Resume(ctx, warpProfileID(id))
	}
	stopped := stopMark()
	if !stopped {
		<-marked
	}
	if ctx.Err() != nil || !stopped || errors.Is(operationErr, egress.ErrWarpExitUnknown) {
		if stopped {
			markWarpActionUnknown(execution, claim)
		}
		return nil, errors.Join(db.ErrWarpHealthBlocked, operationErr)
	}
	if current, readErr := runtime.warp.Get(warpProfileID(id)); readErr == nil {
		profile = current
	} else if operationErr == nil {
		operationErr = readErr
	}
	p.Status, p.LastError = db.ProxyStatusStopped, ""
	if profile.Phase == "ready" && profile.Desired {
		connection, connectionErr := runtime.warp.Connection(warpProfileID(id))
		if connectionErr != nil {
			operationErr = errors.Join(operationErr, connectionErr)
		} else {
			p.Host, p.Port, p.Username, p.Password, p.Protocol = connection.Host, connection.Port, connection.Username, connection.Password, "socks5"
			p.URL = (&url.URL{Scheme: "socks5", Host: net.JoinHostPort(p.Host, strconv.Itoa(p.Port)), User: url.UserPassword(p.Username, p.Password)}).String()
			p.Status = db.ProxyStatusHealthy
		}
	}
	if operationErr == nil && action != "stop" && p.Status != db.ProxyStatusHealthy {
		operationErr = errors.New("WARP 尚未就绪")
	}
	if operationErr != nil && p.Status != db.ProxyStatusHealthy {
		p.Status, p.LastError = db.ProxyStatusFailed, safeProxyRuntimeError(operationErr)
	}
	profile, material, materialErr := runtime.warp.HealthMaterial(warpProfileID(id))
	if materialErr != nil {
		if action != "stop" || profile.Desired {
			markWarpActionUnknown(execution, claim)
			return nil, errors.Join(db.ErrWarpHealthBlocked, materialErr)
		}
		material, profile.Desired = "", false
	}
	if err = s.m.pg.CompleteWarpHealthProxyAction(ctx, execution, claim, db.WarpHealthIntent{Desired: profile.Desired, Material: material, ProfileRevision: profile.Revision}, p); err != nil {
		markWarpActionUnknown(execution, claim)
		return nil, db.ErrWarpHealthBlocked
	}
	if err = execution.Close(); err != nil {
		return nil, db.ErrWarpHealthBlocked
	}
	if profile.Desired && runtime.health != nil {
		_ = runtime.health.RunOnce(ctx, id)
		if fresh, readErr := s.m.pg.GetProxyContext(ctx, id); readErr == nil {
			p = fresh
		}
	}
	return p, operationErr
}

func markWarpActionUnknown(execution *db.WarpHealthExecution, claim db.WarpHealthClaim) {
	cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = execution.MarkWarpHealthUnknown(cleanup, claim)
}

func (s *Server) writeWarpFailure(w http.ResponseWriter, id int64, err error) {
	code := http.StatusBadGateway
	if errors.Is(err, db.ErrProxyInUse) || errors.Is(err, db.ErrWarpHealthBlocked) {
		code = http.StatusConflict
	}
	p, _ := s.m.pg.GetProxy(id)
	writeJSON(w, code, map[string]any{"error": "WARP 操作失败：" + safeProxyRuntimeError(err), "proxy_id": id, "proxy": s.runtimeProxyDTO(p)})
}

func safeProxyRuntimeError(err error) string {
	if errors.Is(err, db.ErrProxyInUse) {
		return "代理被任务或全局默认引用"
	}
	if errors.Is(err, context.Canceled) {
		return "操作已取消"
	}
	return "运行器未完成操作，请检查档案状态；不确定注册禁止重试"
}
