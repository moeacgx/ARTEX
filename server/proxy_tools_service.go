package server

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/Autumn-27/artex/db"
	"github.com/Autumn-27/artex/egress"
	"github.com/google/uuid"
)

const proxyToolInputLimit = 128 << 10

// 所有回显采用白名单，既不返回原始错误，也不序列化数据库代理实体。
type proxyToolView struct {
	ID      string       `json:"proxy_id"`
	Name    string       `json:"name"`
	Kind    db.ProxyKind `json:"kind"`
	Status  string       `json:"status"`
	Address string       `json:"address"`
}

func proxyToolDTO(p *db.Proxy) *proxyToolView {
	if p == nil {
		return nil
	}
	name := p.Name
	if strings.Contains(name, "://") || strings.Contains(name, "@") {
		name = "[已脱敏]"
	}
	for _, secret := range []string{p.Password, p.ResinAdminToken, p.Username} {
		if secret != "" {
			name = strings.ReplaceAll(name, secret, "[已脱敏]")
		}
	}
	name = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return -1
		}
		return r
	}, name)
	if len(name) > 160 {
		name = "[名称过长]"
	}
	status := p.Status
	if !p.Enabled {
		status = "disabled"
	}
	address := "[地址已隐藏]"
	if validProxyHost(p.Host) {
		protocol := p.Protocol
		if protocol == "" {
			protocol = string(p.Kind)
		}
		port := p.Port
		if port == 0 && protocol == "http" {
			port = 80
		}
		if port == 0 && protocol == "https" {
			port = 443
		}
		switch protocol {
		case "http", "https", "socks5":
			address = protocol + "://" + net.JoinHostPort(p.Host, strconv.Itoa(port))
		}
	}
	return &proxyToolView{ID: i64s(p.ID), Name: name, Kind: p.Kind, Status: status, Address: address}
}

type proxyAddItem struct {
	ItemKey         string       `json:"item_key"`
	Name            string       `json:"name"`
	Kind            db.ProxyKind `json:"kind"`
	URL             *string      `json:"url"`
	Protocol        string       `json:"protocol"`
	Host            string       `json:"host"`
	Port            int          `json:"port"`
	Password        *string      `json:"password"`
	ResinPlatform   string       `json:"resin_platform"`
	ResinAdminURL   string       `json:"resin_admin_url"`
	ResinAdminToken *string      `json:"resin_admin_token"`
	Enabled         *bool        `json:"enabled"`
}

func (a proxyAddItem) request() proxyRequest {
	return proxyRequest{Name: a.Name, Kind: a.Kind, URL: a.URL, Protocol: a.Protocol, Host: a.Host, Port: a.Port, Password: a.Password, ResinPlatform: a.ResinPlatform, ResinAdminURL: a.ResinAdminURL, ResinAdminToken: a.ResinAdminToken, Enabled: a.Enabled}
}

type proxyAddInput struct {
	Items []proxyAddItem `json:"items"`
}
type proxyItemResult struct {
	ItemKey         string         `json:"item_key,omitempty"`
	Proxy           *proxyToolView `json:"proxy,omitempty"`
	OperationStatus string         `json:"operation_status"`
	ErrorCode       string         `json:"error_code,omitempty"`
	Replayed        bool           `json:"replayed,omitempty"`
}

func validProxyHost(host string) bool {
	if host == "" || len(host) > 253 || strings.TrimSpace(host) != host {
		return false
	}
	if net.ParseIP(host) != nil {
		return true
	}
	for _, r := range host {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '.' || r == '-') {
			return false
		}
	}
	for _, label := range strings.Split(host, ".") {
		if label == "" || len(label) > 63 || strings.HasPrefix(label, "-") || strings.HasSuffix(label, "-") {
			return false
		}
	}
	return true
}

// HTTP 单条创建和工具批量添加共用校验。普通代理仅接受 URL，避免双凭据来源。
func validatedProxyCreate(req proxyRequest) (*db.Proxy, error) {
	bad := errors.New("代理创建参数无效或存在冲突")
	if req.AcceptTerms || req.ClearPassword || req.ClearAdminToken {
		return nil, bad
	}
	p := proxyFromRequest(req)
	if p.Name == "" || len(p.Name) > 80 || strings.IndexFunc(p.Name, unicode.IsControl) >= 0 {
		return nil, bad
	}
	switch p.Kind {
	case db.ProxyKindHTTP, db.ProxyKindHTTPS, db.ProxyKindSOCKS5:
		if req.URL == nil || len(*req.URL) > 4096 || req.Host != "" || req.Port != 0 || req.Username != "" || req.Password != nil || req.ResinPlatform != "" || req.ResinAdminURL != "" || req.ResinAdminToken != nil {
			return nil, bad
		}
		u, err := url.Parse(p.URL)
		if err != nil || !validProxyHost(u.Hostname()) || u.ForceQuery || u.Opaque != "" {
			return nil, bad
		}
		if strings.HasSuffix(u.Host, ":") {
			return nil, bad
		}
		if u.Port() != "" {
			n, err := strconv.Atoi(u.Port())
			if err != nil || n < 1 || n > 65535 {
				return nil, bad
			}
		} else if p.Kind == db.ProxyKindSOCKS5 {
			return nil, bad
		}
	case db.ProxyKindResin:
		if req.URL != nil || req.Username != "" || !validProxyHost(p.Host) || p.Port < 1 || p.Port > 65535 || len(p.Password) > 4096 || len(p.ResinAdminToken) > 4096 || len(p.ResinAdminURL) > 2048 {
			return nil, bad
		}
		if req.Protocol != "http" && req.Protocol != "socks5" {
			return nil, bad
		}
		// Resin 用点号分隔平台和粘性身份，拒绝能改变账号结构的字符。
		if len(p.ResinPlatform) > 80 {
			return nil, bad
		}
		for _, r := range p.ResinPlatform {
			if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-') {
				return nil, bad
			}
		}
		if p.ResinAdminURL != "" {
			if !validProxyProbeTarget(p.ResinAdminURL) {
				return nil, bad
			}
		} else if p.ResinAdminToken != "" {
			return nil, bad
		}
	default:
		return nil, bad
	}
	if err := db.ValidateProxy(p); err != nil {
		return nil, bad
	}
	return p, nil
}

func (s *Server) addProxyItems(ctx context.Context, a proxyAddInput) []proxyItemResult {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	out := make([]proxyItemResult, len(a.Items))
	for i, item := range a.Items {
		result := proxyItemResult{ItemKey: item.ItemKey, OperationStatus: "failed"}
		key, err := uuid.Parse(item.ItemKey)
		if err != nil || key == uuid.Nil || key.String() != item.ItemKey {
			result.ItemKey = ""
			result.ErrorCode = "invalid_item_key"
			out[i] = result
			continue
		}
		p, err := validatedProxyCreate(item.request())
		if err != nil {
			result.ErrorCode = "invalid_proxy"
			out[i] = result
			continue
		}
		created, replayed, err := s.m.pg.CreateProxyIntent(ctx, item.ItemKey, p)
		switch {
		case errors.Is(err, db.ErrProxyIntentConflict):
			result.ErrorCode = "item_key_conflict"
		case errors.Is(err, db.ErrProxyIntentGone):
			result.ErrorCode = "proxy_deleted"
		case err != nil:
			result.ErrorCode = "storage_failed"
		default:
			result.OperationStatus = "succeeded"
			result.Proxy = proxyToolDTO(created)
			result.Replayed = replayed
		}
		out[i] = result
	}
	return out
}

type proxyTestInput struct {
	ProxyIDs       []int64 `json:"proxy_ids"`
	Concurrency    *int    `json:"concurrency"`
	TimeoutSeconds *int    `json:"timeout_seconds"`
	TargetURL      string  `json:"target_url"`
}

func validProxyProbeTarget(raw string) bool {
	if raw == "" {
		return true
	}
	if len(raw) > 2048 {
		return false
	}
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || !validProxyHost(u.Hostname()) || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Opaque != "" {
		return false
	}
	if strings.HasSuffix(u.Host, ":") {
		return false
	}
	if u.Port() != "" {
		n, err := strconv.Atoi(u.Port())
		if err != nil || n < 1 || n > 65535 {
			return false
		}
	}
	return true
}

// probeProxyCenter 不依赖当前任务、全局出口或环境代理；只检测所选档案。
func (s *Server) probeProxyCenter(ctx context.Context, id int64, target string) (*db.Proxy, string) {
	p, err := s.m.pg.GetProxyContext(ctx, id)
	if err != nil {
		return nil, "proxy_unavailable"
	}
	if !p.Enabled {
		return p, "proxy_disabled"
	}
	// 工具不隐式启动 WARP；其专属管理接口仍保留原生命周期行为。
	if p.Kind == db.ProxyKindWarp {
		return p, "unsupported_proxy_kind"
	}
	select {
	case proxyProbeSlots <- struct{}{}:
		defer func() { <-proxyProbeSlots }()
	case <-ctx.Done():
		return p, "probe_timeout"
	}
	if p.Kind == db.ProxyKindResin {
		err = (&resinAdapter{target: target}).probe(ctx, p)
	} else {
		err = egress.ProbeProxy(ctx, p.URL, target, false)
	}
	status, message := db.ProxyStatusHealthy, ""
	code := ""
	if err != nil {
		status = db.ProxyStatusFailed
		message = "代理检测失败"
		code = "probe_failed"
	}
	now := time.Now()
	// 网络超时仍需持久化失败；只延长状态写入，最多 2 秒，不重试网络。
	saveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	if err := s.m.pg.SaveProxyProbe(saveCtx, p, status, message, now); err != nil {
		return p, "status_not_saved"
	}
	p.Status, p.LastError, p.LastCheckedAt = status, message, &now
	return p, code
}

// 所有 HTTP/工具检测共用进程级上限，防止并行批次放大网络并发。
var proxyProbeSlots = make(chan struct{}, 4)

func (s *Server) testProxyItems(ctx context.Context, a proxyTestInput) []proxyItemResult {
	concurrency, seconds := 2, 10
	if a.Concurrency != nil {
		concurrency = *a.Concurrency
	}
	if a.TimeoutSeconds != nil {
		seconds = *a.TimeoutSeconds
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	out := make([]proxyItemResult, len(a.ProxyIDs))
	jobs := make(chan int)
	var wg sync.WaitGroup
	for n := 0; n < concurrency; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				probeCtx, stop := context.WithTimeout(ctx, time.Duration(seconds)*time.Second)
				p, code := s.probeProxyCenter(probeCtx, a.ProxyIDs[i], a.TargetURL)
				stop()
				state := "succeeded"
				if code != "" {
					state = "failed"
				}
				view := proxyToolDTO(p)
				if view == nil {
					view = &proxyToolView{ID: i64s(a.ProxyIDs[i]), Status: "unavailable"}
				}
				out[i] = proxyItemResult{Proxy: view, OperationStatus: state, ErrorCode: code}
			}
		}()
	}
	for i := range a.ProxyIDs {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	return out
}

func (s *Server) batchCreateProxies(w http.ResponseWriter, r *http.Request) {
	raw, err := io.ReadAll(io.LimitReader(r.Body, proxyToolInputLimit+1))
	var a proxyAddInput
	if err != nil || decodeProxyToolInput(raw, proxyAddSchema(), &a) != nil {
		writeErr(w, 400, "代理批量参数无效")
		return
	}
	writeJSON(w, 200, map[string]any{"items": s.addProxyItems(r.Context(), a)})
}
