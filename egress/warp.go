package egress

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

var warpIDPattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{0,47}$`)

type WarpProfile struct {
	Revision              uint64 `json:"revision"`
	ID                    string `json:"id"`
	Name                  string `json:"name"`
	Phase                 string `json:"phase"`
	Desired               bool   `json:"desired"`
	Port                  int    `json:"port"`
	EgressIP              string `json:"egress_ip,omitempty"`
	LastError             string `json:"last_error,omitempty"`
	RegistrationAttempted bool   `json:"registration_attempted"`
	Registered            bool   `json:"registered"`
	Generated             bool   `json:"generated"`
}

type WarpConnection struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Username string `json:"-"`
	Password string `json:"-"`
}

type warpAuth struct {
	Username string `json:"username"`
	Password string `json:"password"`
}
type warpProcess struct {
	cmd      *exec.Cmd
	done     chan struct{}
	stopping bool
}

type WarpManager struct {
	dir        string
	mu         sync.Mutex
	processes  map[string]*warpProcess
	locks      map[string]*sync.Mutex
	probing    map[string]bool
	closed     bool
	operations sync.WaitGroup
	watchers   sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc
	command    func(string) *exec.Cmd
	run        func(context.Context, string, []string, string) error
	probe      func(context.Context, WarpConnection) (string, error)
}

func NewWarpManager(dir string) (*WarpManager, error) {
	if dir == "" {
		return nil, errors.New("WARP 数据目录不能为空")
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	if err := os.Chmod(dir, 0700); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &WarpManager{dir: dir, processes: make(map[string]*warpProcess), locks: make(map[string]*sync.Mutex), probing: make(map[string]bool), ctx: ctx, cancel: cancel, run: runWarpCommand, probe: probeWarpEgress, command: func(path string) *exec.Cmd {
		return exec.Command("wireproxy", "-c", filepath.Join(path, "wireproxy.conf"))
	}}, nil
}

func runWarpCommand(ctx context.Context, exe string, args []string, dir string) error {
	// 子进程先设置私有 umask，避免注册材料曾短暂以 0644 出现。
	argv := append([]string{"-c", `umask 077; exec "$@"`, "sh", exe}, args...)
	cmd := exec.CommandContext(ctx, "sh", argv...)
	cmd.Dir = dir
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if err := cmd.Run(); err != nil {
		return errors.New("WARP 工具命令失败，原始输出已隐藏")
	}
	return nil
}

func (m *WarpManager) path(id string) (string, error) {
	if !warpIDPattern.MatchString(id) {
		return "", errors.New("WARP 档案编号无效")
	}
	return filepath.Join(m.dir, id), nil
}

func privateWrite(path string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err = os.WriteFile(tmp, data, 0600); err != nil {
		return err
	}
	if err = os.Chmod(tmp, 0600); err != nil {
		return err
	}
	if err = os.Rename(tmp, path); err != nil {
		return err
	}
	return nil
}

func readProfile(path string) (WarpProfile, error) {
	var p WarpProfile
	data, err := os.ReadFile(filepath.Join(path, "meta.json"))
	if err != nil {
		return p, err
	}
	if err = json.Unmarshal(data, &p); err != nil {
		return p, errors.New("WARP 档案状态无效")
	}
	if !warpIDPattern.MatchString(p.ID) || filepath.Base(path) != p.ID {
		return p, errors.New("WARP 档案编号不匹配")
	}
	return p, nil
}

func (m *WarpManager) save(path string, p WarpProfile) error {
	if old, err := readProfile(path); err == nil {
		p.Revision = old.Revision + 1
	} else {
		p.Revision++
	}
	return privateWrite(filepath.Join(path, "meta.json"), p)
}

func (m *WarpManager) Get(id string) (WarpProfile, error) {
	path, err := m.path(id)
	if err != nil {
		return WarpProfile{}, err
	}
	p, err := readProfile(path)
	return m.liveProfileLocked(p), err
}

// 磁盘的 ready 只是上次探测结果；本进程没有活跃运行器时不能宣称在线。
func (m *WarpManager) liveProfileLocked(p WarpProfile) WarpProfile {
	if p.Phase != "ready" {
		p.EgressIP = ""
		return p
	}
	if !m.runningLocked(p.ID) || !p.Desired || !ValidWarpPublicIP(p.EgressIP) {
		p.Phase, p.EgressIP, p.LastError = "failed", "", "WARP 运行器未在线或出口未验证"
	}
	return p
}

func (m *WarpManager) runningLocked(id string) bool {
	proc := m.process(id)
	if proc == nil {
		return false
	}
	select {
	case <-proc.done:
		return false
	default:
		return true
	}
}

func (m *WarpManager) List() ([]WarpProfile, error) {
	entries, err := os.ReadDir(m.dir)
	if err != nil {
		return nil, err
	}
	list := make([]WarpProfile, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() || !warpIDPattern.MatchString(entry.Name()) {
			continue
		}
		p, err := readProfile(filepath.Join(m.dir, entry.Name()))
		if err != nil {
			return nil, err
		}
		list = append(list, m.liveProfileLocked(p))
	}
	return list, nil
}

func secret() (string, error) {
	var buf [24]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf[:]), nil
}

func (m *WarpManager) allocatePort() (int, error) {
	used := map[int]bool{}
	entries, err := os.ReadDir(m.dir)
	if err != nil {
		return 0, err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			if p, e := readProfile(filepath.Join(m.dir, entry.Name())); e == nil {
				used[p.Port] = true
			}
		}
	}
	for i := 0; i < 32; i++ {
		listener, e := net.Listen("tcp", "127.0.0.1:0")
		if e != nil {
			return 0, e
		}
		port := listener.Addr().(*net.TCPAddr).Port
		listener.Close()
		if port >= 1024 && !used[port] {
			return port, nil
		}
	}
	return 0, errors.New("无法分配 WARP 本地端口")
}

// Create 在执行任何外部注册命令前持久化注册意图。重复请求只返回原档案，绝不重新注册。
func (m *WarpManager) Create(ctx context.Context, id, name string, acceptTerms bool) (WarpProfile, error) {
	unlock, err := m.beginProfile(id)
	if err != nil {
		return WarpProfile{}, err
	}
	defer unlock()
	ctx, cancel := m.operationContext(ctx)
	defer cancel()
	path, err := m.path(id)
	if err != nil {
		return WarpProfile{}, err
	}
	if !acceptTerms {
		return WarpProfile{}, errors.New("请先阅读并接受 Cloudflare WARP 条款")
	}
	if _, e := os.Stat(path); e == nil {
		return readProfile(path)
	} else if !os.IsNotExist(e) {
		return WarpProfile{}, e
	}
	if strings.TrimSpace(name) == "" {
		name = id
	}
	if len(name) > 80 {
		return WarpProfile{}, errors.New("WARP 名称过长")
	}
	port, err := m.allocatePort()
	if err != nil {
		return WarpProfile{}, err
	}
	username, err := secret()
	if err != nil {
		return WarpProfile{}, err
	}
	password, err := secret()
	if err != nil {
		return WarpProfile{}, err
	}
	if err = os.Mkdir(path, 0700); err != nil {
		return WarpProfile{}, err
	}
	if err = privateWrite(filepath.Join(path, "auth.json"), warpAuth{username, password}); err != nil {
		_ = os.RemoveAll(path)
		return WarpProfile{}, err
	}
	p := WarpProfile{ID: id, Name: name, Phase: "registering", Desired: true, Port: port, RegistrationAttempted: true}
	if err = m.save(path, p); err != nil {
		_ = os.RemoveAll(path)
		return WarpProfile{}, err
	}
	return m.finish(ctx, path, p)
}

func (m *WarpManager) finish(ctx context.Context, path string, p WarpProfile) (WarpProfile, error) {
	fail := func(message string, e error) (WarpProfile, error) {
		p.Phase = "failed"
		p.EgressIP = ""
		p.LastError = message
		_ = m.save(path, p)
		return p, e
	}
	if !p.Registered {
		// 不明确的注册结果只能人工核对；Resume 不能重新提交注册。
		if p.Phase != "registering" {
			return fail("注册结果不明确，需人工核对", errors.New("WARP 注册结果不明确，拒绝重复注册"))
		}
		accountFile := filepath.Join(path, "wgcf-account.toml")
		if err := m.run(ctx, "wgcf", []string{"--config", accountFile, "register", "--accept-tos"}, path); err != nil {
			if _, statErr := os.Stat(accountFile); statErr == nil {
				_ = os.Chmod(accountFile, 0600)
			}
			return fail("注册结果不明确，需人工核对", err)
		}
		if err := os.Chmod(accountFile, 0600); err != nil {
			return fail("注册材料权限设置失败", err)
		}
		p.Registered = true
		p.Phase = "generating"
		if err := m.save(path, p); err != nil {
			return p, err
		}
	}
	if !p.Generated {
		generated := filepath.Join(path, "generated.conf")
		if err := m.run(ctx, "wgcf", []string{"--config", filepath.Join(path, "wgcf-account.toml"), "generate", "--profile", generated}, path); err != nil {
			return fail("WARP 配置生成失败", err)
		}
		if err := os.Chmod(generated, 0600); err != nil {
			return fail("WARP 配置权限设置失败", err)
		}
		data, err := os.ReadFile(generated)
		if err != nil {
			return fail("WARP 配置读取失败", err)
		}
		if !strings.Contains(string(data), "[Interface]") || !strings.Contains(string(data), "[Peer]") {
			return fail("WARP 配置结构无效", errors.New("WARP 配置结构无效"))
		}
		var auth warpAuth
		if err := readAuth(path, &auth); err != nil {
			return fail("WARP 认证材料无效", err)
		}
		body := strings.TrimSpace(string(data)) + fmt.Sprintf("\n\n[Socks5]\nBindAddress = 127.0.0.1:%d\nUsername = %s\nPassword = %s\n", p.Port, auth.Username, auth.Password)
		if err := os.WriteFile(filepath.Join(path, "wireproxy.conf"), []byte(body), 0600); err != nil {
			return fail("WARP 代理配置写入失败", err)
		}
		if err := os.Chmod(filepath.Join(path, "wireproxy.conf"), 0600); err != nil {
			return fail("WARP 代理配置权限设置失败", err)
		}
		if err := m.run(ctx, "wireproxy", []string{"-n", "-c", filepath.Join(path, "wireproxy.conf")}, path); err != nil {
			return fail("WARP 代理配置验证失败", err)
		}
		p.Generated = true
		p.Phase = "starting"
		if err := m.save(path, p); err != nil {
			return p, err
		}
	}
	return m.startLocked(ctx, path, p)
}

func readAuth(path string, auth *warpAuth) error {
	data, err := os.ReadFile(filepath.Join(path, "auth.json"))
	if err != nil {
		return err
	}
	if err = json.Unmarshal(data, auth); err != nil {
		return err
	}
	if auth.Username == "" || auth.Password == "" {
		return errors.New("认证材料缺失")
	}
	return nil
}

func (m *WarpManager) Connection(id string) (WarpConnection, error) {
	path, err := m.path(id)
	if err != nil {
		return WarpConnection{}, err
	}
	p, err := readProfile(path)
	if err != nil {
		return WarpConnection{}, err
	}
	if p.Phase != "ready" || !p.Desired || p.EgressIP == "" {
		return WarpConnection{}, errors.New("WARP 出口尚未验证可用")
	}
	if m.liveProfileLocked(p).Phase != "ready" {
		return WarpConnection{}, errors.New("WARP 运行器未在线")
	}
	var auth warpAuth
	if err := readAuth(path, &auth); err != nil {
		return WarpConnection{}, err
	}
	return WarpConnection{Host: "127.0.0.1", Port: p.Port, Username: auth.Username, Password: auth.Password}, nil
}

func (m *WarpManager) startLocked(ctx context.Context, path string, p WarpProfile) (WarpProfile, error) {
	if err := ctx.Err(); err != nil {
		return p, err
	}
	if running := m.process(p.ID); running != nil {
		if current := m.liveProfileLocked(p); current.Phase == "ready" {
			return current, nil
		}
		return p, errors.New("WARP 出口已有运行进程")
	}
	if err := m.confirmPriorProcess(path); err != nil {
		return p, err
	}
	p.Desired = true
	p.Phase = "starting"
	p.LastError = ""
	p.EgressIP = ""
	if err := m.save(path, p); err != nil {
		return p, err
	}
	var auth warpAuth
	if err := readAuth(path, &auth); err != nil {
		p.Phase, p.LastError = "failed", "WARP 认证材料无效"
		_ = m.save(path, p)
		return p, errors.New(p.LastError)
	}
	// 记录本次启动版本；锁外等待期间明确管理意图可以使其失效。
	p, err := readProfile(path)
	if err != nil {
		return p, err
	}
	cmd := m.command(path)
	cmd.Dir = path
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if err := writeWarpProcessRecord(path, 0, false); err != nil {
		return p, err
	}
	if err := cmd.Start(); err != nil {
		_ = writeWarpProcessRecord(path, 0, true)
		p.Phase = "failed"
		p.LastError = "wireproxy 启动失败"
		_ = m.save(path, p)
		return p, errors.New(p.LastError)
	}
	proc := &warpProcess{cmd: cmd, done: make(chan struct{})}
	m.mu.Lock()
	m.processes[p.ID] = proc
	m.mu.Unlock()
	m.watchers.Add(1)
	go m.watch(path, p.ID, proc)
	if err := writeWarpProcessRecord(path, cmd.Process.Pid, false); err != nil {
		_ = m.stopProcessLocked(p.ID)
		return p, ErrWarpExitUnknown
	}
	lock := m.profileLock(p.ID)
	lock.Unlock()
	ip, probeErr := m.waitStartedProbe(ctx, p, proc, auth)
	lock.Lock()
	current, err := readProfile(path)
	if err != nil {
		return current, err
	}
	if current.Revision != p.Revision || !current.Desired || m.process(p.ID) != proc {
		return m.liveProfileLocked(current), ErrWarpStaleProbe
	}
	p = current
	if probeErr != nil || ctx.Err() != nil || !ValidWarpPublicIP(ip) || !m.runningLocked(p.ID) {
		if stopErr := m.stopProcessLocked(p.ID); stopErr != nil {
			return p, stopErr
		}
		p.Phase = "failed"
		p.LastError = "WARP 联网检测失败"
		_ = m.save(path, p)
		return p, errors.New(p.LastError)
	}
	p.Phase = "ready"
	p.EgressIP = ip
	if err := m.save(path, p); err != nil {
		return p, err
	}
	return readProfile(path)
}

func (m *WarpManager) watch(path, id string, proc *warpProcess) {
	defer m.watchers.Done()
	_ = proc.cmd.Wait()
	close(proc.done)
	lock := m.profileLock(id)
	lock.Lock()
	defer lock.Unlock()
	m.mu.Lock()
	if m.processes[id] != proc {
		m.mu.Unlock()
		return
	}
	delete(m.processes, id)
	stopping := proc.stopping
	m.mu.Unlock()
	_ = writeWarpProcessRecord(path, proc.cmd.Process.Pid, true)
	if stopping {
		return
	}
	p, err := readProfile(path)
	if err != nil {
		return
	}
	p.Phase, p.EgressIP, p.LastError = "failed", "", "wireproxy 进程已退出"
	_ = m.save(path, p)
}

// 超时仅表示退出未确认，保留进程记录，禁止另起重叠运行器。
func (m *WarpManager) stopProcessLocked(id string) error {
	m.mu.Lock()
	proc := m.processes[id]
	if proc == nil {
		m.mu.Unlock()
		return nil
	}
	proc.stopping = true
	m.mu.Unlock()
	if proc.cmd != nil && proc.cmd.Process != nil {
		_ = proc.cmd.Process.Kill()
	}
	select {
	case <-proc.done:
		m.mu.Lock()
		if m.processes[id] == proc {
			delete(m.processes, id)
		}
		m.mu.Unlock()
		if proc.cmd != nil && proc.cmd.Process != nil {
			path, _ := m.path(id)
			_ = writeWarpProcessRecord(path, proc.cmd.Process.Pid, true)
		}
		return nil
	case <-time.After(3 * time.Second):
		return ErrWarpExitUnknown
	}
}

func (m *WarpManager) Start(ctx context.Context, id string) (WarpProfile, error) {
	unlock, err := m.beginProfile(id)
	if err != nil {
		return WarpProfile{}, err
	}
	defer unlock()
	ctx, cancel := m.operationContext(ctx)
	defer cancel()
	path, err := m.path(id)
	if err != nil {
		return WarpProfile{}, err
	}
	p, err := readProfile(path)
	if err != nil {
		return p, err
	}
	if !p.Registered || !p.Generated {
		return p, errors.New("WARP 档案尚未完成创建，请恢复")
	}
	return m.startLocked(ctx, path, p)
}

// Probe 刷新当前出口 IP；停止、崩溃或失败的运行器不会仅凭旧地址恢复为 healthy。
func (m *WarpManager) Probe(ctx context.Context, id string) (WarpProfile, error) {
	unlock, err := m.beginProfile(id)
	if err != nil {
		return WarpProfile{}, err
	}
	ctx, cancel := m.operationContext(ctx)
	defer cancel()
	path, err := m.path(id)
	if err != nil {
		unlock()
		return WarpProfile{}, err
	}
	p, err := readProfile(path)
	if err != nil {
		unlock()
		return p, err
	}
	p = m.liveProfileLocked(p)
	m.mu.Lock()
	busy := m.probing[id]
	if !busy {
		m.probing[id] = true
	}
	m.mu.Unlock()
	if busy {
		unlock()
		return p, errors.New("WARP 正在探测")
	}
	defer func() { m.mu.Lock(); delete(m.probing, id); m.mu.Unlock() }()
	proc := m.process(id)
	if (p.Phase != "ready" && p.Phase != "failed") || !p.Desired || !p.Registered || !p.Generated || !m.runningLocked(id) {
		unlock()
		return p, errors.New("WARP 出口尚未就绪")
	}
	var auth warpAuth
	if err = readAuth(path, &auth); err != nil {
		unlock()
		return p, err
	}
	// 探测不改变进程，释放档案锁使 stop 可先递增 revision。
	// operations 继续覆盖真实网络退出，释放锁不代表执行结束。
	m.operations.Add(1)
	unlock()
	defer m.operations.Done()
	ip, probeErr := m.probe(ctx, WarpConnection{"127.0.0.1", p.Port, auth.Username, auth.Password})
	lock := m.profileLock(id)
	lock.Lock()
	defer lock.Unlock()
	current, err := readProfile(path)
	if err != nil {
		return current, err
	}
	if current.Revision != p.Revision || m.process(id) != proc || !current.Desired {
		return m.liveProfileLocked(current), ErrWarpStaleProbe
	}
	if probeErr != nil || ctx.Err() != nil || !ValidWarpPublicIP(ip) || !m.runningLocked(id) {
		current.Phase, current.EgressIP, current.LastError = "failed", "", "WARP 联网检测失败"
	} else {
		current.Phase, current.EgressIP, current.LastError = "ready", ip, ""
	}
	if err = m.save(path, current); err != nil {
		return current, err
	}
	if current.Phase != "ready" {
		return current, errors.New(current.LastError)
	}
	return current, nil
}

func (m *WarpManager) Resume(ctx context.Context, id string) (WarpProfile, error) {
	unlock, err := m.beginProfile(id)
	if err != nil {
		return WarpProfile{}, err
	}
	defer unlock()
	ctx, cancel := m.operationContext(ctx)
	defer cancel()
	path, err := m.path(id)
	if err != nil {
		return WarpProfile{}, err
	}
	p, err := readProfile(path)
	if err != nil {
		return p, err
	}
	if m.liveProfileLocked(p).Phase == "ready" {
		return p, nil
	}
	if p.RegistrationAttempted && !p.Registered {
		return p, errors.New("WARP 注册结果不明确，拒绝重复注册")
	}
	return m.finish(ctx, path, p)
}

func (m *WarpManager) Stop(ctx context.Context, id string) (WarpProfile, error) {
	unlock, err := m.beginProfile(id)
	if err != nil {
		return WarpProfile{}, err
	}
	defer unlock()
	ctx, cancel := m.operationContext(ctx)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return WarpProfile{}, err
	}
	path, err := m.path(id)
	if err != nil {
		return WarpProfile{}, err
	}
	p, err := readProfile(path)
	if err != nil {
		return p, err
	}
	p.Desired = false
	p.Phase = "stopped"
	p.EgressIP = ""
	p.LastError = ""
	if err = m.save(path, p); err != nil {
		return p, err
	}
	if err = m.stopProcessLocked(id); err != nil {
		p.Phase, p.LastError = "unknown", err.Error()
		_ = m.save(path, p)
		return p, err
	}
	return p, nil
}

func (m *WarpManager) Delete(ctx context.Context, id string) error {
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
	path, err := m.path(id)
	if err != nil {
		return err
	}
	p, err := readProfile(path)
	if err != nil {
		return err
	}
	// 管理端已检查引用；删除直接停止出口，先保存停止意图以免中断后自动恢复。
	p.Desired, p.Phase, p.EgressIP, p.LastError = false, "stopped", "", ""
	if err := m.save(path, p); err != nil {
		return err
	}
	if err = m.stopProcessLocked(id); err != nil {
		return err
	}
	return os.RemoveAll(path)
}

func (m *WarpManager) Restore(ctx context.Context) error {
	list, err := m.List()
	if err != nil {
		return err
	}
	for _, p := range list {
		if p.Desired && p.Registered && p.Generated {
			if _, err := m.Start(ctx, p.ID); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *WarpManager) Close() error {
	m.mu.Lock()
	m.closed = true
	m.cancel()
	m.mu.Unlock()
	m.operations.Wait()
	m.mu.Lock()
	ids := make([]string, 0, len(m.processes))
	for id := range m.processes {
		ids = append(ids, id)
	}
	m.mu.Unlock()
	var result error
	for _, id := range ids {
		lock := m.profileLock(id)
		lock.Lock()
		if err := m.stopProcessLocked(id); err != nil {
			result = err
		}
		lock.Unlock()
	}
	if result == nil {
		m.watchers.Wait()
	}
	return result
}
