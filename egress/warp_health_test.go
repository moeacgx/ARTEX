package egress

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func healthProfile(t *testing.T, m *WarpManager, id string) WarpProfile {
	t.Helper()
	path := filepath.Join(m.dir, id)
	if err := os.MkdirAll(path, 0700); err != nil {
		t.Fatal(err)
	}
	p := WarpProfile{ID: id, Phase: "ready", Desired: true, Port: 12345, EgressIP: "8.8.8.8", RegistrationAttempted: true, Registered: true, Generated: true}
	if err := m.save(path, p); err != nil {
		t.Fatal(err)
	}
	if err := privateWrite(filepath.Join(path, "auth.json"), warpAuth{"user", "pass"}); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestWarpHealthOtherProfileDoesNotWaitForNetwork(t *testing.T) {
	m, _ := NewWarpManager(t.TempDir())
	healthProfile(t, m, "a")
	healthProfile(t, m, "b")
	m.processes["a"] = &warpProcess{done: make(chan struct{})}
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	m.probe = func(context.Context, WarpConnection) (string, error) {
		close(entered)
		<-release
		return "8.8.8.8", nil
	}
	go func() { defer close(done); m.Probe(context.Background(), "a") }()
	<-entered
	got := make(chan struct{})
	go func() { m.Get("b"); close(got) }()
	select {
	case <-got:
	case <-time.After(200 * time.Millisecond):
		close(release)
		<-done
		t.Fatal("另一档案被网络探测的全局锁阻塞")
	}
	close(release)
	<-done
}

func TestWarpHealthLateProbeDoesNotResurrectStoppedProfile(t *testing.T) {
	m, _ := NewWarpManager(t.TempDir())
	healthProfile(t, m, "a")
	proc := &warpProcess{done: make(chan struct{})}
	m.processes["a"] = proc
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	m.probe = func(context.Context, WarpConnection) (string, error) {
		close(entered)
		<-release
		return "8.8.8.8", nil
	}
	go func() { defer close(done); m.Probe(context.Background(), "a") }()
	<-entered
	close(proc.done)
	stop := make(chan struct{})
	go func() { m.Stop(context.Background(), "a"); close(stop) }()
	select {
	case <-stop:
	case <-time.After(200 * time.Millisecond):
		close(release)
		<-done
		<-stop
		t.Fatal("明确停止无法先于迟到探测生效")
	}
	close(release)
	<-done
	p, _ := m.Get("a")
	if p.Desired || p.Phase != "stopped" || p.EgressIP != "" {
		t.Fatalf("迟到探测复活档案: %+v", p)
	}
}

func TestWarpHealthProbeFailureClearsIPWithoutStopping(t *testing.T) {
	m, _ := NewWarpManager(t.TempDir())
	healthProfile(t, m, "a")
	m.processes["a"] = &warpProcess{done: make(chan struct{})}
	m.probe = func(context.Context, WarpConnection) (string, error) { return "", errors.New("offline") }
	p, err := m.Probe(context.Background(), "a")
	if err == nil || p.Phase != "failed" || p.EgressIP != "" || !m.runningLocked("a") {
		t.Fatalf("探测必须清空有效IP且保留进程: %+v %v", p, err)
	}
}

// 合成 SOCKS 只允许转发到本次本地 TLS 服务，拒绝任何公网地址。
func healthSOCKSConnection(conn net.Conn, target, username, password string) {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	read := func(n int) ([]byte, error) { b := make([]byte, n); _, err := io.ReadFull(conn, b); return b, err }
	head, err := read(2)
	if err != nil || head[0] != 5 {
		return
	}
	methods, err := read(int(head[1]))
	if err != nil {
		return
	}
	hasAuth := false
	for _, method := range methods {
		if method == 2 {
			hasAuth = true
		}
	}
	if !hasAuth {
		_, _ = conn.Write([]byte{5, 255})
		return
	}
	_, _ = conn.Write([]byte{5, 2})
	auth, err := read(2)
	if err != nil || auth[0] != 1 {
		return
	}
	u, err := read(int(auth[1]))
	if err != nil {
		return
	}
	length, err := read(1)
	if err != nil {
		return
	}
	p, err := read(int(length[0]))
	if err != nil {
		return
	}
	if string(u) != username || string(p) != password {
		_, _ = conn.Write([]byte{1, 1})
		return
	}
	_, _ = conn.Write([]byte{1, 0})
	head, err = read(4)
	if err != nil || head[0] != 5 || head[1] != 1 {
		return
	}
	var host string
	switch head[3] {
	case 1:
		b, e := read(4)
		if e != nil {
			return
		}
		host = net.IP(b).String()
	case 4:
		b, e := read(16)
		if e != nil {
			return
		}
		host = net.IP(b).String()
	case 3:
		b, e := read(1)
		if e != nil {
			return
		}
		b, e = read(int(b[0]))
		if e != nil {
			return
		}
		host = string(b)
	default:
		return
	}
	port, err := read(2)
	if err != nil {
		return
	}
	address := net.JoinHostPort(host, strconv.Itoa(int(binary.BigEndian.Uint16(port))))
	if address != target {
		_, _ = conn.Write([]byte{5, 2, 0, 1, 0, 0, 0, 0, 0, 0})
		return
	}
	upstream, err := net.DialTimeout("tcp", target, time.Second)
	if err != nil {
		return
	}
	defer upstream.Close()
	_, _ = conn.Write([]byte{5, 0, 0, 1, 127, 0, 0, 1, 0, 0})
	done := make(chan struct{})
	go func() { _, _ = io.Copy(upstream, conn); close(done) }()
	_, _ = io.Copy(conn, upstream)
	_ = conn.Close()
	_ = upstream.Close()
	<-done
}

func healthSOCKS(t *testing.T, target string) WarpConnection {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, e := listener.Accept()
			if e != nil {
				return
			}
			go healthSOCKSConnection(conn, target, "test-user", "test-password")
		}
	}()
	return WarpConnection{Host: "127.0.0.1", Port: listener.Addr().(*net.TCPAddr).Port, Username: "test-user", Password: "test-password"}
}

func healthTLSTrace(t *testing.T, body *atomic.Value) (*httptest.Server, *tls.Config) {
	t.Helper()
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/redirect" {
			http.Redirect(w, r, "/trace", http.StatusFound)
			return
		}
		fmt.Fprint(w, body.Load().(string))
	}))
	t.Cleanup(server.Close)
	pool := x509.NewCertPool()
	pool.AddCert(server.Certificate())
	return server, &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
}

func TestWarpHealthAuthenticatedTLSRequiresPublicIPAndWarp(t *testing.T) {
	var body atomic.Value
	body.Store("ip=8.8.8.8\nwarp=on\n")
	server, trust := healthTLSTrace(t, &body)
	connection := healthSOCKS(t, server.Listener.Addr().String())
	for _, tc := range []struct{ name, trace, want string }{
		{"on", "ip=8.8.8.8\nwarp=on\n", "8.8.8.8"},
		{"plus", "ip=2606:4700:4700::1111\nwarp=plus\n", "2606:4700:4700::1111"},
		{"off", "ip=8.8.8.8\nwarp=off\n", ""},
		{"private", "ip=10.0.0.1\nwarp=on\n", ""},
		{"documentation", "ip=203.0.113.1\nwarp=on\n", ""},
		{"shared", "ip=100.64.0.1\nwarp=on\n", ""},
		{"duplicate", "ip=8.8.8.8\nip=1.1.1.1\nwarp=on\n", ""},
		{"too-long", "ip=8.8.8.8\nwarp=on\nextra=" + strings.Repeat("x", 4096), ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body.Store(tc.trace)
			ip, err := probeWarpTrace(t.Context(), connection, server.URL+"/trace", trust)
			if ip != tc.want || (tc.want != "" && err != nil) || (tc.want == "" && err == nil) {
				t.Fatalf("trace result=%q %v want=%q", ip, err, tc.want)
			}
		})
	}
	body.Store("ip=8.8.8.8\nwarp=on\n")
	bad := connection
	bad.Password = "wrong-secret"
	if ip, err := probeWarpTrace(t.Context(), bad, server.URL+"/trace", trust); err == nil || ip != "" || strings.Contains(err.Error(), bad.Password) {
		t.Fatalf("认证失败未封闭/脱敏: %q %v", ip, err)
	}
	if _, err := probeWarpTrace(t.Context(), connection, server.URL+"/trace", nil); err == nil {
		t.Fatal("未校验证书")
	}
	if _, err := probeWarpTrace(t.Context(), connection, server.URL+"/redirect", trust); err == nil {
		t.Fatal("接受重定向")
	}
}

func fullHealthProfile(t *testing.T, m *WarpManager, id string, port int) WarpProfile {
	t.Helper()
	p := healthProfile(t, m, id)
	p.Port = port
	path := filepath.Join(m.dir, id)
	if err := m.save(path, p); err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string]string{
		"wgcf-account.toml": "private_key = 'test-private'\naccess_token = 'test-token'\ndevice_id = 'test-device'\n",
		"generated.conf":    "[Interface]\nPrivateKey = test-private\n[Peer]\nPublicKey = test-public\n",
		"wireproxy.conf":    fmt.Sprintf("[Interface]\nPrivateKey = test-private\n[Peer]\nPublicKey = test-public\n[Socks5]\nBindAddress = 127.0.0.1:%d\nUsername = user\nPassword = pass\n", port),
	} {
		if err := os.WriteFile(filepath.Join(path, name), []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
	}
	return p
}

// 真实子进程只监听回环 SOCKS，所有网络都指向指定本地 TLS 服务。
func TestWarpHealthLocalProcess(t *testing.T) {
	if os.Getenv("ARTEX_WARP_HEALTH_CHILD") != "1" {
		return
	}
	port := os.Getenv("ARTEX_WARP_HEALTH_CHILD_PORT")
	target := os.Getenv("ARTEX_WARP_HEALTH_CHILD_TARGET")
	listener, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", port))
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		for {
			conn, e := listener.Accept()
			if e != nil {
				return
			}
			go healthSOCKSConnection(conn, target, "user", "pass")
		}
	}()
	<-time.After(40 * time.Second)
}

func realHealthManager(t *testing.T) (*WarpManager, *atomic.Value) {
	t.Helper()
	body := &atomic.Value{}
	body.Store("ip=8.8.8.8\nwarp=on\n")
	server, trust := healthTLSTrace(t, body)
	m, err := NewWarpManager(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	port, err := m.allocatePort()
	if err != nil {
		t.Fatal(err)
	}
	fullHealthProfile(t, m, "real", port)
	m.run = func(context.Context, string, []string, string) error {
		t.Error("健康恢复重新注册/生成")
		return errors.New("禁止")
	}
	m.command = func(string) *exec.Cmd {
		cmd := exec.Command(os.Args[0], "-test.run=^TestWarpHealthLocalProcess$", "-test.timeout=45s")
		cmd.Env = append(os.Environ(), "ARTEX_WARP_HEALTH_CHILD=1", "ARTEX_WARP_HEALTH_CHILD_PORT="+strconv.Itoa(port), "ARTEX_WARP_HEALTH_CHILD_TARGET="+server.Listener.Addr().String())
		return cmd
	}
	m.probe = func(ctx context.Context, c WarpConnection) (string, error) {
		return probeWarpTrace(ctx, c, server.URL+"/trace", trust)
	}
	t.Cleanup(func() {
		if err := m.Close(); err != nil {
			t.Error(err)
		}
	})
	return m, body
}

func TestWarpHealthRealReconnectReusesMaterialAndWaitsProcess(t *testing.T) {
	m, body := realHealthManager(t)
	p, material, err := m.HealthMaterial("real")
	if err != nil || material == "" {
		t.Fatalf("材料读取: %+v %v", p, err)
	}
	if _, err = m.Start(t.Context(), "real"); err != nil {
		t.Fatal(err)
	}
	old := m.process("real")
	if old == nil {
		t.Fatal("未启动真实进程")
	}
	body.Store("ip=8.8.8.8\nwarp=off\n")
	if p, err = m.Probe(t.Context(), "real"); err == nil || p.EgressIP != "" || m.process("real") != old {
		t.Fatalf("失败探测影响运行进程: %+v %v", p, err)
	}
	body.Store("ip=8.8.8.8\nwarp=plus\n")
	p, err = m.Reconnect(t.Context(), "real", material)
	if err != nil || p.Phase != "ready" || p.EgressIP != "8.8.8.8" {
		t.Fatalf("同IP恢复: %+v %v", p, err)
	}
	select {
	case <-old.done:
	default:
		t.Fatal("替代进程先于旧进程 Wait 启动")
	}
	if _, got, e := m.HealthMaterial("real"); e != nil || got != material {
		t.Fatal("恢复更换注册材料")
	}
	if err = m.ConfirmStopped(t.Context(), "real"); err != nil {
		t.Fatal(err)
	}
	if m.process("real") != nil {
		t.Fatal("ConfirmStopped 未等待真实退出")
	}
	p, _ = m.Get("real")
	if !p.Desired || p.EgressIP != "" {
		t.Fatalf("退出确认误改 Desired/暴露 IP: %+v", p)
	}
}

func TestWarpHealthStartNetworkDoesNotHoldProfileMutex(t *testing.T) {
	m, _ := realHealthManager(t)
	entered, release := make(chan struct{}), make(chan struct{})
	realProbe := m.probe
	m.probe = func(ctx context.Context, c WarpConnection) (string, error) {
		close(entered)
		<-release
		return realProbe(ctx, c)
	}
	done := make(chan error, 1)
	go func() { _, err := m.Start(t.Context(), "real"); done <- err }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("没有开始检测")
	}
	inspected := make(chan struct{})
	go func() { _, _, _ = m.HealthMaterial("real"); close(inspected) }()
	select {
	case <-inspected:
		close(release)
	case <-time.After(200 * time.Millisecond):
		close(release)
		<-done
		<-inspected
		t.Fatal("网络检测持有档案 mutex")
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestWarpHealthUnknownRegistrationAndPriorProcessStayBlocked(t *testing.T) {
	m, err := NewWarpManager(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	p := fullHealthProfile(t, m, "a", 12345)
	path := filepath.Join(m.dir, "a")
	p.Registered = false
	if err = m.save(path, p); err != nil {
		t.Fatal(err)
	}
	if _, material, err := m.HealthMaterial("a"); err == nil || material != "" {
		t.Fatal("未知注册误报完整材料")
	}
	if _, err = m.Reconnect(t.Context(), "a", "fabricated"); err == nil {
		t.Fatal("未知注册重连")
	}
	p.Registered = true
	if err = m.save(path, p); err != nil {
		t.Fatal(err)
	}
	if err = writeWarpProcessRecord(path, os.Getpid(), false); err != nil {
		t.Fatal(err)
	}
	if err = m.ConfirmStopped(t.Context(), "a"); !errors.Is(err, ErrWarpExitUnknown) {
		t.Fatalf("PID 存活视为退出: %v", err)
	}
	data, _ := os.ReadFile(filepath.Join(path, "process.json"))
	var record warpProcessRecord
	_ = json.Unmarshal(data, &record)
	if record.Exited {
		t.Fatal("未知进程记录自动解封")
	}
}

func TestWarpHealthConfirmedExitRepairsUnknownPhase(t *testing.T) {
	m, err := NewWarpManager(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	p := fullHealthProfile(t, m, "a", 12345)
	p.Phase = "unknown"
	path := filepath.Join(m.dir, "a")
	if err = m.save(path, p); err != nil {
		t.Fatal(err)
	}
	if err = writeWarpProcessRecord(path, 0, true); err != nil {
		t.Fatal(err)
	}
	if err = m.ConfirmStopped(t.Context(), "a"); err != nil {
		t.Fatal(err)
	}
	p, material, err := m.HealthMaterial("a")
	if err != nil || material == "" || !p.Desired || p.Phase != "failed" {
		t.Fatalf("真实退出后仍因旧 unknown 无法恢复: %+v %v", p, err)
	}
}

func TestWarpHealthOtherManagerRequiresPriorProcessExit(t *testing.T) {
	m, _ := realHealthManager(t)
	if _, err := m.Start(t.Context(), "real"); err != nil {
		t.Fatal(err)
	}
	other, err := NewWarpManager(m.dir)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if err = other.ConfirmStopped(t.Context(), "real"); !errors.Is(err, ErrWarpExitUnknown) {
		t.Fatalf("另一 Manager 未 Wait 就确认存活 PID 退出: %v", err)
	}
	if err = m.ConfirmStopped(t.Context(), "real"); err != nil {
		t.Fatal(err)
	}
	if err = other.ConfirmStopped(t.Context(), "real"); err != nil {
		t.Fatal(err)
	}
	p, material, err := other.HealthMaterial("real")
	if err != nil || material == "" || !p.Desired {
		t.Fatalf("真实退出后无法复用材料: %+v %v", p, err)
	}
}

func TestWarpHealthMissingExplicitDesiredDoesNotStart(t *testing.T) {
	m, err := NewWarpManager(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	p := fullHealthProfile(t, m, "a", 12345)
	path := filepath.Join(m.dir, "a")
	data, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err = json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	delete(fields, "desired")
	if err = privateWrite(filepath.Join(path, "meta.json"), fields); err != nil {
		t.Fatal(err)
	}
	p, material, err := m.HealthMaterial("a")
	if err != nil || material == "" || p.Desired {
		t.Fatalf("旧 ready 被推断为 Desired: %+v %v", p, err)
	}
	if _, err = m.Reconnect(t.Context(), "a", material); err == nil {
		t.Fatal("没有明确 Desired 仍启动")
	}
}
