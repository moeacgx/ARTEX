package server

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Autumn-27/artex/db"
	"github.com/google/uuid"
)

// 合成工具只处理本地文件和回环连接，绝不转发 SOCKS 请求到外网。
func TestWarpToolProcess(t *testing.T) {
	if os.Getenv("ARTEX_WARP_TOOL_CHILD") != "1" {
		return
	}
	args := os.Args
	for len(args) > 0 && args[0] != "--" {
		args = args[1:]
	}
	if len(args) < 2 {
		os.Exit(90)
	}
	tool, args := args[1], args[2:]
	mode := os.Getenv("ARTEX_WARP_FIXTURE_MODE")
	logCall := func(action string) {
		f, err := os.OpenFile(os.Getenv("ARTEX_WARP_CALL_LOG"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			os.Exit(91)
		}
		fmt.Fprintln(f, action)
		f.Close()
	}
	if tool == "wgcf" {
		if len(args) == 4 && args[2] == "register" && args[3] == "--accept-tos" {
			logCall("register")
			if mode == "register-block" {
				gate := os.Getenv("ARTEX_WARP_REGISTER_GATE")
				for {
					if _, err := os.Stat(gate); os.IsNotExist(err) {
						break
					}
					time.Sleep(10 * time.Millisecond)
				}
			}
			if mode == "register-fail" {
				fmt.Fprintln(os.Stderr, "synthetic-private-token")
				os.Exit(42)
			}
			if err := os.WriteFile(args[1], []byte("private_key = synthetic\naccess_token = synthetic\ndevice_id = synthetic\n"), 0600); err != nil {
				os.Exit(92)
			}
		} else if len(args) == 5 && args[2] == "generate" && args[3] == "--profile" {
			logCall("generate")
			if mode == "generate-fail" {
				os.Exit(43)
			}
			if err := os.WriteFile(args[4], []byte("[Interface]\nPrivateKey = synthetic\n[Peer]\nPublicKey = synthetic\n"), 0600); err != nil {
				os.Exit(93)
			}
		} else {
			os.Exit(94)
		}
		os.Exit(0)
	}
	if len(args) == 3 && args[0] == "-n" {
		logCall("validate")
		os.Exit(0)
	}
	if len(args) != 2 || args[0] != "-c" {
		os.Exit(95)
	}
	logCall("start")
	data, err := os.ReadFile(args[1])
	if err != nil {
		os.Exit(96)
	}
	values := map[string]string{}
	for _, line := range strings.Split(string(data), "\n") {
		if k, v, ok := strings.Cut(line, "="); ok {
			values[strings.TrimSpace(k)] = strings.TrimSpace(v)
		}
	}
	cert, err := tls.LoadX509KeyPair(os.Getenv("ARTEX_WARP_TEST_CERT"), os.Getenv("ARTEX_WARP_TEST_KEY"))
	if err != nil {
		os.Exit(97)
	}
	listener, err := net.Listen("tcp", values["BindAddress"])
	if err != nil {
		os.Exit(98)
	}
	go func() {
		for {
			if _, err := os.Stat(filepath.Join(filepath.Dir(args[1]), "synthetic-exit")); err == nil {
				os.Exit(44)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()
	for {
		conn, err := listener.Accept()
		if err != nil {
			os.Exit(99)
		}
		go func() {
			defer conn.Close()
			conn.SetDeadline(time.Now().Add(5 * time.Second))
			read := func(n int) ([]byte, error) { b := make([]byte, n); _, e := io.ReadFull(conn, b); return b, e }
			header, err := read(2)
			if err != nil || header[0] != 5 {
				return
			}
			if _, err = read(int(header[1])); err != nil {
				return
			}
			conn.Write([]byte{5, 2})
			auth, err := read(2)
			if err != nil {
				return
			}
			user, err := read(int(auth[1]))
			if err != nil {
				return
			}
			n, err := read(1)
			if err != nil {
				return
			}
			pass, err := read(int(n[0]))
			if err != nil {
				return
			}
			if string(user) != values["Username"] || string(pass) != values["Password"] {
				conn.Write([]byte{1, 1})
				return
			}
			conn.Write([]byte{1, 0})
			request, err := read(4)
			if err != nil || request[3] != 3 {
				return
			}
			n, err = read(1)
			if err != nil {
				return
			}
			host, err := read(int(n[0]))
			if err != nil {
				return
			}
			port, err := read(2)
			if err != nil {
				return
			}
			if (string(host) != "cloudflare.com" && string(host) != "www.cloudflare.com") || binary.BigEndian.Uint16(port) != 443 {
				return
			}
			conn.Write([]byte{5, 0, 0, 1, 127, 0, 0, 1, 0, 0})
			secured := tls.Server(conn, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
			if err := secured.Handshake(); err != nil {
				return
			}
			req, err := http.ReadRequest(bufio.NewReader(secured))
			if err != nil || req.URL.Path != "/cdn-cgi/trace" {
				return
			}
			logCall("probe")
			warp := "on"
			_, failProbe := os.Stat(filepath.Join(filepath.Dir(args[1]), "synthetic-bad-probe"))
			if mode == "probe-fail" || failProbe == nil {
				warp = "off"
			}
			body := "ip=8.8.8.8\nwarp=" + warp + "\n"
			fmt.Fprintf(secured, "HTTP/1.1 200 OK\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s", len(body), body)
		}()
	}
}

func warpFixtureSubprocess(t *testing.T) bool {
	t.Helper()
	if os.Getenv("ARTEX_WARP_API_CHILD") == "1" {
		return false
	}
	dir := t.TempDir()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "仅本地合成WARP"}, DNSNames: []string{"cloudflare.com", "www.cloudflare.com"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certPath, keyPath := filepath.Join(dir, "ca.pem"), filepath.Join(dir, "key.pem")
	for path, data := range map[string][]byte{certPath: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), keyPath: pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})} {
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, tool := range []string{"wgcf", "wireproxy"} {
		script := "#!/bin/sh\nARTEX_PG_DSN='postgres://artex_test@127.0.0.1:1/unused?sslmode=disable&connect_timeout=1' ARTEX_WARP_TOOL_CHILD=1 exec \"$ARTEX_WARP_TEST_BINARY\" -test.run='^TestWarpToolProcess$' -test.timeout=45s -- " + tool + " \"$@\"\n"
		if err := os.WriteFile(filepath.Join(dir, tool), []byte(script), 0700); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, exe, "-test.run=^"+t.Name()+"$", "-test.v", "-test.timeout=45s")
	cmd.Env = append(os.Environ(), "ARTEX_WARP_API_CHILD=1", "ARTEX_WARP_TEST_BINARY="+exe, "ARTEX_WARP_TEST_CERT="+certPath, "ARTEX_WARP_TEST_KEY="+keyPath, "SSL_CERT_FILE="+certPath, "SSL_CERT_DIR="+dir, "PATH="+dir+":"+os.Getenv("PATH"))
	// 父进程已持有整套测试的 advisory lock；子进程跳过同锁后恢复同一个隔离 DSN。
	cmd.Env = append(cmd.Env, "ARTEX_WARP_CHILD_DSN="+os.Getenv("ARTEX_PG_DSN"), "ARTEX_PG_DSN=postgres://artex_test@127.0.0.1:1/unused?sslmode=disable&connect_timeout=1")
	out, err := cmd.CombinedOutput()
	t.Log(string(out))
	if err != nil {
		t.Fatalf("合成工具隔离进程失败: %v", err)
	}
	return true
}

func TestProxyPGWarpOneClick(t *testing.T) {
	if warpFixtureSubprocess(t) {
		return
	}
	t.Setenv("ARTEX_PG_DSN", os.Getenv("ARTEX_WARP_CHILD_DSN"))
	for _, mode := range []string{"ready", "register-fail", "generate-fail", "probe-fail"} {
		t.Run(mode, func(t *testing.T) {
			s, mux := proxyIntegration(t)
			mux.HandleFunc("GET /api/proxies", s.listProxies)
			logPath := filepath.Join(t.TempDir(), "calls.log")
			t.Setenv("ARTEX_WARP_CALL_LOG", logPath)
			t.Setenv("ARTEX_WARP_FIXTURE_MODE", mode)
			code, out, raw := warpCreateHTTP(t, mux, uuid.NewString(), `{"name":"一次确认WARP","kind":"warp","accept_terms":true}`, false)
			want := 201
			if mode != "ready" {
				want = 502
			}
			row := out
			if nested, ok := out["proxy"].(map[string]any); ok {
				row = nested
			}
			id, ok := row["id"].(float64)
			if !ok {
				t.Fatalf("未保留已建proxy DTO: %d %s", code, raw)
			}
			t.Cleanup(func() {
				if rt, err := s.m.proxyRuntime(); err == nil {
					_, _ = rt.warp.Stop(t.Context(), warpProfileID(int64(id)))
				}
				_ = s.m.pg.DeleteProxy(int64(id))
			})
			if code != want {
				t.Fatalf("创建未完成运行链或失败状态不准确: %d %s", code, raw)
			}
			path := fmt.Sprintf("/api/proxies/%d", int64(id))
			if mode != "ready" {
				if out["proxy_id"] != id || row["warp_phase"] != "failed" || row["status"] != "failed" || row["egress_ip"] != nil || strings.Contains(raw, "synthetic-private-token") {
					t.Fatalf("失败包泄密或丢失状态: %s", raw)
				}
				if mode == "register-fail" {
					for _, action := range []string{"register", "recover"} {
						c, _, r := proxyAPI(t, mux, "POST", path+"/warp/"+action, `{"accept_terms":true}`)
						if c != 409 {
							t.Fatalf("不确定注册未保持人工恢复屏障: %d %s", c, r)
						}
					}
				}
			} else {
				if row["status"] != "healthy" || row["warp_phase"] != "ready" || row["egress_ip"] != "8.8.8.8" {
					t.Fatalf("未返回真实探测IP: %s", raw)
				}
				for _, action := range []string{"register", "recover"} {
					c, _, r := proxyAPI(t, mux, "POST", path+"/warp/"+action, `{"accept_terms":true}`)
					if c != 200 {
						t.Fatalf("已完成注册不幂等: %d %s", c, r)
					}
				}
				c, got, r := proxyAPI(t, mux, "GET", path, "")
				if c != 200 || got["egress_ip"] != row["egress_ip"] {
					t.Fatalf("GET出口不一致: %d %s", c, r)
				}
				c, listed, r := proxyAPI(t, mux, "GET", "/api/proxies", "")
				if c != 200 {
					t.Fatal(r)
				}
				found := false
				for _, v := range listed["proxies"].([]any) {
					p := v.(map[string]any)
					if p["id"] == id {
						found = p["egress_ip"] == row["egress_ip"]
					}
				}
				if !found {
					t.Fatal("列表缺少真实出口IP")
				}
				proxyID := int64(id)
				selected, err := s.m.ResolveLLMProxySelection(t.Context(), nil, "", "fixed", &proxyID)
				if err != nil || !strings.HasPrefix(selected, "socks5://") {
					t.Fatalf("渠道WARP引用未解析可信出口: %v", err)
				}
				task := createRuntimeTask(t, s, db.TaskProxyModeFixed, &proxyID)
				taskID, _ := strconv.ParseInt(task.ID, 10, 64)
				activity, err := s.m.pg.AcquireTaskOperation(taskID)
				if err != nil {
					t.Fatal(err)
				}
				activityReleased := false
				defer func() {
					if !activityReleased {
						_ = s.m.pg.ReleaseTaskOperationToken(taskID, activity.OperationToken)
					}
				}()
				badProbe := filepath.Join(os.Getenv("ARTEX_WARP_DIR"), warpProfileID(proxyID), "synthetic-bad-probe")
				if err := os.WriteFile(badProbe, []byte("fail"), 0600); err != nil {
					t.Fatal(err)
				}
				c, _, r = proxyAPI(t, mux, "POST", path+"/test", "")
				if c != 502 {
					t.Fatalf("联网检测失败仍返回成功: %d %s", c, r)
				}
				conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", int(row["port"].(float64))), time.Second)
				if err != nil {
					t.Fatalf("健康探测绕过引用屏障停止了任务出口: %v", err)
				}
				conn.Close()
				if err := os.Remove(badProbe); err != nil {
					t.Fatal(err)
				}
				c, _, r = proxyAPI(t, mux, "POST", path+"/warp/stop", "")
				if c != 409 {
					t.Fatalf("WARP action绕过任务引用屏障: %d %s", c, r)
				}
				if err := s.m.pg.ReleaseTaskOperationToken(taskID, activity.OperationToken); err != nil {
					t.Fatal(err)
				}
				activityReleased = true
				if _, err := s.m.pg.Exec(`UPDATE tasks SET status='done' WHERE id=$1`, task.ID); err != nil {
					t.Fatal(err)
				}
				c, got, r = proxyAPI(t, mux, "POST", path+"/warp/stop", "")
				if c != 200 || got["warp_phase"] != "stopped" || got["egress_ip"] != nil {
					t.Fatalf("停止仍显示在线IP: %d %s", c, r)
				}
				if _, err := s.m.ResolveLLMProxySelection(t.Context(), nil, "", "fixed", &proxyID); err == nil {
					t.Fatal("停止WARP渠道引用静默直连")
				}
			}
			calls, err := os.ReadFile(logPath)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Count(string(calls), "register\n") != 1 {
				t.Fatalf("重复注册: %s", calls)
			}
			if mode == "ready" && (strings.Count(string(calls), "start\n") != 1 || strings.Count(string(calls), "probe\n") != 5) {
				t.Fatalf("重复启动或漏探测: %s", calls)
			}
			if mode == "ready" {
				c, got, r := proxyAPI(t, mux, "POST", path+"/warp/start", "")
				if c != 200 || got["egress_ip"] != "8.8.8.8" {
					t.Fatalf("同ID启动未返回出口: %d %s", c, r)
				}
				c, got, r = proxyAPI(t, mux, "POST", path+"/test", "")
				if c != 200 || got["egress_ip"] != "8.8.8.8" {
					t.Fatalf("出口重新探测失败: %d %s", c, r)
				}
				if err := os.WriteFile(filepath.Join(os.Getenv("ARTEX_WARP_DIR"), warpProfileID(int64(id)), "synthetic-exit"), []byte("exit"), 0600); err != nil {
					t.Fatal(err)
				}
				deadline := time.Now().Add(2 * time.Second)
				for {
					c, got, r = proxyAPI(t, mux, "GET", path, "")
					if c == 200 && got["status"] == "failed" && got["egress_ip"] == nil {
						break
					}
					if time.Now().After(deadline) {
						t.Fatalf("运行器退出仍虚报online: %d %s", c, r)
					}
					time.Sleep(20 * time.Millisecond)
				}
			}
		})
	}
}

func TestProxyPGWarpRuntimeRecovery(t *testing.T) {
	if warpFixtureSubprocess(t) {
		return
	}
	t.Setenv("ARTEX_PG_DSN", os.Getenv("ARTEX_WARP_CHILD_DSN"))
	for _, scenario := range []string{"probe-recovery", "repeat-start", "rejected-operation"} {
		t.Run(scenario, func(t *testing.T) {
			s, mux := proxyIntegration(t)
			logPath := filepath.Join(t.TempDir(), "calls.log")
			t.Setenv("ARTEX_WARP_CALL_LOG", logPath)
			t.Setenv("ARTEX_WARP_FIXTURE_MODE", "ready")
			code, out, raw := warpCreateHTTP(t, mux, uuid.NewString(), `{"name":"运行恢复回归","kind":"warp","accept_terms":true}`, false)
			if code != 201 {
				t.Fatalf("创建失败: %d %s", code, raw)
			}
			id := int64(out["id"].(float64))
			path := fmt.Sprintf("/api/proxies/%d", id)
			t.Cleanup(func() {
				if rt, err := s.m.proxyRuntime(); err == nil {
					_, _ = rt.warp.Stop(t.Context(), warpProfileID(id))
				}
				_ = s.m.pg.DeleteProxy(id)
			})
			if scenario == "probe-recovery" {
				task := createRuntimeTask(t, s, db.TaskProxyModeFixed, &id)
				taskID, _ := strconv.ParseInt(task.ID, 10, 64)
				activity, err := s.m.pg.AcquireTaskOperation(taskID)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = s.m.pg.ReleaseTaskOperationToken(taskID, activity.OperationToken) }()
				marker := filepath.Join(os.Getenv("ARTEX_WARP_DIR"), warpProfileID(id), "synthetic-bad-probe")
				if err := os.WriteFile(marker, []byte("fail"), 0600); err != nil {
					t.Fatal(err)
				}
				code, _, raw = proxyAPI(t, mux, "POST", path+"/test", "")
				if code != 502 {
					t.Fatalf("失败trace未返回502: %d %s", code, raw)
				}
				if err := os.Remove(marker); err != nil {
					t.Fatal(err)
				}
				code, out, raw = proxyAPI(t, mux, "POST", path+"/test", "")
				if code != 200 || out["status"] != "healthy" || out["egress_ip"] != "8.8.8.8" {
					t.Fatalf("存活出口不能原地复探恢复: %d %s", code, raw)
				}
				code, _, raw = proxyAPI(t, mux, "POST", path+"/warp/stop", "")
				if code != 409 {
					t.Fatalf("恢复后引用屏障丢失: %d %s", code, raw)
				}
			} else if scenario == "repeat-start" {
				for i := 0; i < 2; i++ {
					code, out, raw = proxyAPI(t, mux, "POST", path+"/warp/start", "")
					if code != 200 || out["status"] != "healthy" || out["egress_ip"] != "8.8.8.8" {
						t.Fatalf("重复start毒化健康出口: %d %s", code, raw)
					}
				}
			} else {
				code, _, raw = proxyAPI(t, mux, "POST", path+"/warp/register", `{"accept_terms":false}`)
				if code < 400 {
					t.Fatalf("未接受条款操作应被拒绝: %d %s", code, raw)
				}
				code, out, raw = proxyAPI(t, mux, "GET", path, "")
				if code != 200 || out["status"] != "healthy" || out["egress_ip"] != "8.8.8.8" {
					t.Fatalf("被拒绝的操作毒化实际健康状态: %d %s", code, raw)
				}
			}
			if got, err := s.m.ResolveLLMProxySelection(t.Context(), nil, "", "fixed", &id); err != nil || !strings.HasPrefix(got, "socks5://") {
				t.Fatalf("正常出口不再可解析: %v", err)
			}
			calls, err := os.ReadFile(logPath)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Count(string(calls), "register\n") != 1 || strings.Count(string(calls), "start\n") != 1 {
				t.Fatalf("恢复/重放重复注册或启动: %s", calls)
			}
		})
	}
}

func TestProxyPGWarpActionSingleConnection(t *testing.T) {
	if warpFixtureSubprocess(t) {
		return
	}
	t.Setenv("ARTEX_PG_DSN", os.Getenv("ARTEX_WARP_CHILD_DSN"))
	s, mux := proxyIntegration(t)
	t.Setenv("ARTEX_WARP_CALL_LOG", filepath.Join(t.TempDir(), "calls.log"))
	t.Setenv("ARTEX_WARP_FIXTURE_MODE", "ready")
	code, out, raw := warpCreateHTTP(t, mux, uuid.NewString(), `{"name":"单连接WARP操作","kind":"warp","accept_terms":true}`, false)
	if code != 201 {
		t.Fatalf("创建失败: %d %s", code, raw)
	}
	id := int64(out["id"].(float64))
	t.Cleanup(func() {
		if err := s.m.pg.DeleteProxy(id); err != nil {
			t.Errorf("清理单连接测试代理: %v", err)
		}
	})
	s.m.pg.SetMaxOpenConns(1)
	t.Cleanup(func() { s.m.pg.SetMaxOpenConns(2) })
	// 超时仅放宽本测试池以解除旧实现死锁，等待请求退出后再清理夹具。
	for _, action := range []string{"stop", "start"} {
		t.Run(action, func(t *testing.T) {
			srv := httptest.NewServer(mux)
			defer srv.Close()
			type result struct {
				response *http.Response
				err      error
			}
			done := make(chan result, 1)
			before := s.m.pg.Stats().WaitCount
			go func() {
				resp, err := srv.Client().Post(fmt.Sprintf("%s/api/proxies/%d/warp/%s", srv.URL, id, action), "application/json", nil)
				done <- result{resp, err}
			}()
			var got result
			select {
			case got = <-done:
			case <-time.After(2 * time.Second):
				waits := s.m.pg.Stats().WaitCount - before
				s.m.pg.SetMaxOpenConns(2)
				wakeCtx, cancel := context.WithTimeout(t.Context(), time.Second)
				wakeErr := s.m.pg.PingContext(wakeCtx)
				cancel()
				if wakeErr != nil {
					t.Errorf("唤醒测试连接池失败: %v", wakeErr)
				}
				got = <-done
				t.Errorf("单连接WARP action死锁，事务内重复借连接: action=%s waits=%d", action, waits)
			}
			if got.err != nil {
				t.Fatal(got.err)
			}
			defer got.response.Body.Close()
			data, err := io.ReadAll(got.response.Body)
			if err != nil || got.response.StatusCode != 200 {
				t.Fatalf("正式WARP action失败: %d %s %v", got.response.StatusCode, data, err)
			}
			var dto ProxyDTO
			if err := json.Unmarshal(data, &dto); err != nil {
				t.Fatal(err)
			}
			if dto.ID != id || (action == "stop" && (dto.Status != "stopped" || dto.EgressIP != "")) || (action == "start" && (dto.Status != "healthy" || dto.EgressIP != "8.8.8.8")) {
				t.Fatalf("操作DTO状态错误: %s", data)
			}
			if s.m.pg.Stats().WaitCount != before {
				t.Error("事务内仍额外申请数据库连接")
			}
		})
		if t.Failed() {
			break
		}
	}
}

func TestProxyPGWarpChannelDeletePreservesProfile(t *testing.T) {
	if warpFixtureSubprocess(t) {
		return
	}
	t.Setenv("ARTEX_PG_DSN", os.Getenv("ARTEX_WARP_CHILD_DSN"))
	s, mux := proxyIntegration(t)
	mux.HandleFunc("DELETE /api/proxies/{id}", s.deleteProxy)
	logPath := filepath.Join(t.TempDir(), "calls.log")
	t.Setenv("ARTEX_WARP_CALL_LOG", logPath)
	t.Setenv("ARTEX_WARP_FIXTURE_MODE", "ready")
	code, out, raw := warpCreateHTTP(t, mux, uuid.NewString(), `{"name":"渠道引用删除保护","kind":"warp","accept_terms":true}`, false)
	if code != 201 {
		t.Fatalf("创建失败: %d %s", code, raw)
	}
	id := int64(out["id"].(float64))
	path := fmt.Sprintf("/api/proxies/%d", id)
	t.Cleanup(func() {
		if rt, err := s.m.proxyRuntime(); err == nil {
			_, _ = rt.warp.Stop(t.Context(), warpProfileID(id))
		}
		_ = s.m.pg.DeleteProxy(id)
	})
	profileID, err := s.m.pg.SaveProfile(&db.LLMProfile{Name: "WARP渠道引用", Format: "openai", Model: "synthetic", ProxyMode: "fixed", ProxyID: &id})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = s.m.pg.Exec(`DELETE FROM llm_profiles WHERE id=$1`, profileID) })
	code, _, raw = proxyAPI(t, mux, "POST", path+"/warp/stop", "")
	if code != 200 {
		t.Fatalf("静态渠道不应阻止停止: %d %s", code, raw)
	}
	code, _, raw = proxyAPI(t, mux, "DELETE", path, "")
	if code != 409 {
		t.Errorf("渠道引用删除应先拒绝409: %d %s", code, raw)
	}
	dir := filepath.Join(os.Getenv("ARTEX_WARP_DIR"), warpProfileID(id))
	for _, name := range []string{"meta.json", "auth.json", "wgcf-account.toml", "wireproxy.conf"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Errorf("删除拒绝后档案丢失 %s: %v", name, err)
		}
	}
	if _, err := s.m.pg.GetProxy(id); err != nil {
		t.Fatalf("删除拒绝后DB记录丢失: %v", err)
	}
	if t.Failed() {
		return
	}
	if _, err := s.m.pg.SaveProfile(&db.LLMProfile{ID: profileID, Name: "WARP渠道引用", Format: "openai", ProxyMode: "direct"}); err != nil {
		t.Fatal(err)
	}
	for _, action := range []string{"start", "recover"} {
		code, out, raw = proxyAPI(t, mux, "POST", path+"/warp/"+action, "")
		if code != 200 || out["id"] != float64(id) || out["egress_ip"] != "8.8.8.8" {
			t.Fatalf("解绑后原ID不能恢复: %s %d %s", action, code, raw)
		}
	}
	calls, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(calls), "register\n") != 1 || strings.Count(string(calls), "start\n") != 2 {
		t.Fatalf("恢复时重复注册或启动: %s", calls)
	}
	code, _, raw = proxyAPI(t, mux, "POST", path+"/warp/stop", "")
	if code != 200 {
		t.Fatal(raw)
	}
	code, _, raw = proxyAPI(t, mux, "DELETE", path, "")
	if code != 200 {
		t.Fatalf("解绑后删除失败: %d %s", code, raw)
	}
}

func TestProxyPGWarpDeleteRunning(t *testing.T) {
	if warpFixtureSubprocess(t) {
		return
	}
	t.Setenv("ARTEX_PG_DSN", os.Getenv("ARTEX_WARP_CHILD_DSN"))
	s, mux := proxyIntegration(t)
	mux.HandleFunc("DELETE /api/proxies/{id}", s.deleteProxy)
	t.Setenv("ARTEX_WARP_CALL_LOG", filepath.Join(t.TempDir(), "calls.log"))
	t.Setenv("ARTEX_WARP_FIXTURE_MODE", "ready")
	key := uuid.NewString()
	body := `{"name":"直接删除运行出口","kind":"warp","accept_terms":true}`
	code, out, raw := warpCreateHTTP(t, mux, key, body, false)
	if code != 201 {
		t.Fatalf("创建失败: %d %s", code, raw)
	}
	id := int64(out["id"].(float64))
	path := fmt.Sprintf("/api/proxies/%d", id)
	rt, err := s.m.proxyRuntime()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.m.pg.SetGlobalProxyID(nil); _ = s.m.pg.DeleteProxy(id) })
	conn, err := rt.warp.Connection(warpProfileID(id))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.m.pg.SetGlobalProxyID(&id); err != nil {
		t.Fatal(err)
	}
	code, _, raw = proxyAPI(t, mux, "DELETE", path, "")
	if code != 409 {
		t.Fatalf("全局引用应阻止删除: %d %s", code, raw)
	}
	if _, err := rt.warp.Connection(warpProfileID(id)); err != nil {
		t.Fatalf("被拒绝的删除停止了出口: %v", err)
	}
	if err := s.m.pg.SetGlobalProxyID(nil); err != nil {
		t.Fatal(err)
	}
	code, _, raw = proxyAPI(t, mux, "DELETE", path, "")
	if code != 200 {
		t.Fatalf("运行中的出口应直接删除，无需先停止: %d %s", code, raw)
	}
	if _, err := s.m.pg.GetProxy(id); err == nil {
		t.Fatal("删除后仍存在代理行")
	}
	if _, err := os.Stat(filepath.Join(os.Getenv("ARTEX_WARP_DIR"), warpProfileID(id))); !os.IsNotExist(err) {
		t.Fatalf("删除后仍存在档案: %v", err)
	}
	if c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", conn.Port), time.Second); err == nil {
		c.Close()
		t.Fatal("删除后运行器仍监听端口")
	}
	code, _, raw = warpCreateHTTP(t, mux, key, body, false)
	if code != 410 {
		t.Fatalf("删除后重放应保留墓碑: %d %s", code, raw)
	}
}

func TestProxyPGChannelBindWaitsForDeleteCleanup(t *testing.T) {
	s, _ := proxyIntegration(t)
	p := createRuntimeProxy(t, s, "http://127.0.0.1:19981")
	entered, unblock := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(unblock) })
	deleted := make(chan error, 1)
	go func() {
		deleted <- s.m.pg.DeleteProxyWithCleanup(p.ID, func(*db.Proxy) error { close(entered); <-unblock; return nil })
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("删除未进入cleanup")
	}
	type saved struct {
		id  int64
		err error
	}
	bound := make(chan saved, 1)
	go func() {
		id, err := s.m.pg.SaveProfile(&db.LLMProfile{Name: "并发绑定删除", Format: "openai", Model: "synthetic", ProxyMode: "fixed", ProxyID: &p.ID})
		bound <- saved{id, err}
	}()
	select {
	case result := <-bound:
		if result.id > 0 {
			_, _ = s.m.pg.Exec(`DELETE FROM llm_profiles WHERE id=$1`, result.id)
		}
		t.Fatalf("绑定未等待删除生命周期锁: %v", result.err)
	case <-time.After(150 * time.Millisecond):
	}
	once.Do(func() { close(unblock) })
	if err := <-deleted; err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-bound:
		if result.id > 0 {
			_, _ = s.m.pg.Exec(`DELETE FROM llm_profiles WHERE id=$1`, result.id)
		}
		if result.err == nil {
			t.Fatal("删除提交后仍绑定已删除出口")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("删除结束后绑定未解除阻塞")
	}
}
