package egress

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

var (
	ErrWarpExitUnknown = errors.New("WARP 进程退出未确认，需人工核对")
	ErrWarpStaleProbe  = errors.New("WARP 档案版本已改变，丢弃迟到探测")
)

// 跨宿主无法由本机核实，保持屏障交运维确认；PID 仍存在也不会推断其旧执行已退出。
func ConfirmWarpOwnerExited(host string, pid int) error {
	local, err := os.Hostname()
	if err != nil || host != local || pid <= 0 {
		return ErrWarpExitUnknown
	}
	exited, err := warpPIDExited(pid)
	if err != nil || !exited {
		return ErrWarpExitUnknown
	}
	return nil
}

func (m *WarpManager) profileLock(id string) *sync.Mutex {
	m.mu.Lock()
	defer m.mu.Unlock()
	lock := m.locks[id]
	if lock == nil {
		lock = &sync.Mutex{}
		m.locks[id] = lock
	}
	return lock
}
func (m *WarpManager) beginProfile(id string) (func(), error) {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil, errors.New("WARP 管理器已关闭")
	}
	m.operations.Add(1)
	m.mu.Unlock()
	lock := m.profileLock(id)
	lock.Lock()
	return func() { lock.Unlock(); m.operations.Done() }, nil
}
func (m *WarpManager) operationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	child, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(m.ctx, cancel)
	return child, func() { stop(); cancel() }
}
func (m *WarpManager) process(id string) *warpProcess {
	m.mu.Lock()
	defer m.mu.Unlock()
	proc := m.processes[id]
	if proc != nil {
		select {
		case <-proc.done:
			return nil
		default:
		}
	}
	return proc
}

// 启动监听与 TLS 检测均在档案 mutex 外等待，真实操作计数仍由外层持有。
func (m *WarpManager) waitStartedProbe(ctx context.Context, p WarpProfile, proc *warpProcess, auth warpAuth) (string, error) {
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	for {
		conn, err := (&net.Dialer{Timeout: 150 * time.Millisecond}).DialContext(ctx, "tcp", net.JoinHostPort("127.0.0.1", fmt.Sprint(p.Port)))
		if err == nil {
			_ = conn.Close()
			break
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-proc.done:
			return "", errors.New("wireproxy 启动后退出")
		case <-deadline.C:
			return "", errors.New("WARP 本地监听超时")
		case <-time.After(100 * time.Millisecond):
		}
	}
	return m.probe(ctx, WarpConnection{"127.0.0.1", p.Port, auth.Username, auth.Password})
}

type warpProcessRecord struct {
	Host   string `json:"host"`
	PID    int    `json:"pid"`
	Exited bool   `json:"exited"`
}

func writeWarpProcessRecord(path string, pid int, exited bool) error {
	host, err := os.Hostname()
	if err != nil {
		return err
	}
	return privateWrite(filepath.Join(path, "process.json"), warpProcessRecord{host, pid, exited})
}
func (m *WarpManager) confirmPriorProcess(path string) error {
	data, err := os.ReadFile(filepath.Join(path, "process.json"))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return ErrWarpExitUnknown
	}
	var record warpProcessRecord
	host, err := os.Hostname()
	if err != nil || json.Unmarshal(data, &record) != nil || record.Host != host {
		return ErrWarpExitUnknown
	}
	if record.Exited {
		return nil
	}
	if record.PID <= 0 {
		return ErrWarpExitUnknown
	}
	exited, err := warpPIDExited(record.PID)
	if err != nil || !exited {
		return ErrWarpExitUnknown
	}
	return writeWarpProcessRecord(path, record.PID, true)
}

// ConfirmStopped 不改变 Desired。只终止本 Manager 确认拥有的进程；其他实例遗留 PID 必须已经退出。
// 不会按 PID 盲杀进程，也不会用本地端口关闭代替进程退出证明。
func (m *WarpManager) ConfirmStopped(ctx context.Context, id string) error {
	unlock, err := m.beginProfile(id)
	if err != nil {
		return err
	}
	defer unlock()
	if err = ctx.Err(); err != nil {
		return err
	}
	path, err := m.path(id)
	if err != nil {
		return err
	}
	if err = m.stopProcessLocked(id); err != nil {
		return err
	}
	if err = m.confirmPriorProcess(path); err != nil {
		return err
	}
	p, err := readProfile(path)
	if err != nil {
		return err
	}
	if p.Phase == "unknown" && p.Registered && p.Generated {
		p.Phase, p.EgressIP, p.LastError = "failed", "", "旧运行进程已确认退出"
		return m.save(path, p)
	}
	return nil
}

// HealthMaterial 只返回摘要和明确意图，不把注册私钥或认证材料交给健康调度器。
func (m *WarpManager) HealthMaterial(id string) (WarpProfile, string, error) {
	lock := m.profileLock(id)
	lock.Lock()
	defer lock.Unlock()
	path, err := m.path(id)
	if err != nil {
		return WarpProfile{}, "", err
	}
	p, err := readProfile(path)
	if err != nil {
		return p, "", err
	}
	if !p.RegistrationAttempted || !p.Registered || !p.Generated || p.Phase == "registering" || p.Phase == "generating" || p.Phase == "unknown" {
		return p, "", errors.New("WARP 注册材料未确定完整")
	}
	var auth warpAuth
	if err = readAuth(path, &auth); err != nil {
		return p, "", errors.New("WARP 认证材料缺失")
	}
	if p.Port < 1024 || p.Port > 65535 {
		return p, "", errors.New("WARP 本地端口无效")
	}
	digest := sha256.New()
	for _, name := range []string{"wgcf-account.toml", "generated.conf", "wireproxy.conf", "auth.json"} {
		data, err := os.ReadFile(filepath.Join(path, name))
		if err != nil || len(data) == 0 || len(data) > 1<<20 {
			return p, "", errors.New("WARP 注册材料缺失或无效")
		}
		text := string(data)
		if name == "wgcf-account.toml" && (!strings.Contains(text, "private_key") || !strings.Contains(text, "access_token") || !strings.Contains(text, "device_id")) {
			return p, "", errors.New("WARP 注册材料结构无效")
		}
		if strings.HasSuffix(name, ".conf") && (!strings.Contains(text, "[Interface]") || !strings.Contains(text, "[Peer]")) {
			return p, "", errors.New("WARP 配置结构无效")
		}
		if name == "wireproxy.conf" && (!strings.Contains(text, "BindAddress = 127.0.0.1:"+fmt.Sprint(p.Port)) || !strings.Contains(text, "Username = "+auth.Username) || !strings.Contains(text, "Password = "+auth.Password)) {
			return p, "", errors.New("WARP 代理认证配置不匹配")
		}
		digest.Write([]byte(name))
		digest.Write([]byte{0})
		digest.Write(data)
		digest.Write([]byte{0})
	}
	return p, hex.EncodeToString(digest.Sum(nil)), nil
}

// Reconnect 只能由已经提交的持久 repair claim 调用，绝不走 Create/register/generate。
func (m *WarpManager) Reconnect(ctx context.Context, id, material string) (WarpProfile, error) {
	p, got, err := m.HealthMaterial(id)
	if err != nil || material == "" || got != material || !p.Desired {
		return p, errors.New("WARP 意图或材料已变化，拒绝重连")
	}
	unlock, err := m.beginProfile(id)
	if err != nil {
		return p, err
	}
	defer unlock()
	ctx, cancel := m.operationContext(ctx)
	defer cancel()
	path, _ := m.path(id)
	current, err := readProfile(path)
	if err != nil {
		return current, err
	}
	if current.Revision != p.Revision || !current.Desired {
		return current, ErrWarpStaleProbe
	}
	if err = m.stopProcessLocked(id); err != nil {
		current.Phase, current.EgressIP, current.LastError = "unknown", "", err.Error()
		_ = m.save(path, current)
		return current, err
	}
	return m.startLocked(ctx, path, current)
}

func ValidWarpPublicIP(raw string) bool {
	ip := net.ParseIP(raw)
	if ip == nil || !ip.IsGlobalUnicast() || ip.IsPrivate() {
		return false
	}
	// 文档、基准、运营商共享地址不作为可验证公网出口。
	for _, cidr := range []string{"100.64.0.0/10", "192.0.0.0/24", "192.0.2.0/24", "198.18.0.0/15", "198.51.100.0/24", "203.0.113.0/24", "2001:db8::/32"} {
		_, block, _ := net.ParseCIDR(cidr)
		if block.Contains(ip) {
			return false
		}
	}
	return true
}

func probeWarpEgress(ctx context.Context, c WarpConnection) (string, error) {
	return probeWarpTrace(ctx, c, "https://cloudflare.com/cdn-cgi/trace", nil)
}

// 测试可注入本地 TLS 信任根；正式路径始终校验证书，经带认证的 SOCKS，不回退直连。
func probeWarpTrace(ctx context.Context, c WarpConnection, target string, trust *tls.Config) (string, error) {
	dest, err := url.Parse(target)
	if err != nil || dest.Scheme != "https" || dest.Host == "" || c.Username == "" || c.Password == "" {
		return "", errors.New("WARP 检测参数无效")
	}
	u := &url.URL{Scheme: "socks5", Host: net.JoinHostPort(c.Host, fmt.Sprint(c.Port)), User: url.UserPassword(c.Username, c.Password)}
	tr := &http.Transport{Proxy: http.ProxyURL(u), TLSClientConfig: trust, DialContext: (&net.Dialer{Timeout: 5 * time.Second}).DialContext, TLSHandshakeTimeout: 5 * time.Second, ResponseHeaderTimeout: 8 * time.Second}
	defer tr.CloseIdleConnections()
	client := &http.Client{Transport: tr, Timeout: 12 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return "", errors.New("WARP 检测参数无效")
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", errors.New("WARP 出口检测连接失败")
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return "", errors.New("WARP 出口检测响应异常")
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4097))
	if err != nil || len(body) > 4096 {
		return "", errors.New("WARP 出口检测读取失败")
	}
	values := map[string]string{}
	for _, line := range strings.Split(string(body), "\n") {
		if k, v, ok := strings.Cut(strings.TrimSpace(line), "="); ok {
			if _, dup := values[k]; dup {
				return "", errors.New("WARP 检测字段重复")
			}
			values[k] = v
		}
	}
	if !ValidWarpPublicIP(values["ip"]) || (values["warp"] != "on" && values["warp"] != "plus") {
		return "", errors.New("WARP 出口未验证公网 IP 和启用状态")
	}
	return net.ParseIP(values["ip"]).String(), nil
}
