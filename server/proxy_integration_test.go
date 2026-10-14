package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Autumn-27/artex/agent"
	"github.com/Autumn-27/artex/db"
	"github.com/Autumn-27/artex/traffic"
	actool "github.com/Autumn-27/norma/tool"
)

func proxyIntegration(t *testing.T) (*Server, *http.ServeMux) {
	t.Helper()
	dsn := os.Getenv("ARTEX_PG_DSN")
	if !proxyIntegrationDSNAllowed(dsn) {
		t.Fatal("代理集成测试仅允许 127.0.0.1:25432 的 artex_proxy_test 或 artex_proxy_verify_test DSN")
	}
	pg, err := db.Open(dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pg.Close() })
	dir := t.TempDir()
	t.Setenv("ARTEX_WARP_DIR", filepath.Join(dir, "warp"))
	if err = db.ConfigureProxySecrets(filepath.Join(dir, "key")); err != nil {
		t.Fatal(err)
	}
	if err = db.InitializeProxySecrets(); err != nil {
		t.Fatal(err)
	}
	m := &Manager{pg: pg, dir: dir}
	if err = m.InitProxyRuntime(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.CloseProxyRuntime() })
	s := &Server{m: m}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/proxies", s.createProxy)
	mux.HandleFunc("PUT /api/proxies/{id}", s.updateProxy)
	mux.HandleFunc("POST /api/proxies/{id}/test", s.testProxy)
	mux.HandleFunc("POST /api/proxies/global", s.setGlobalProxy)
	s.RegisterProxyActionRoutes(mux)
	return s, mux
}

func proxyIntegrationDSNAllowed(dsn string) bool {
	u, err := url.Parse(dsn)
	// pgx 的查询参数能够覆盖主机、端口或库名，只接受已授权的本地测试参数。
	return err == nil && u.Host == "127.0.0.1:25432" &&
		(u.Scheme == "postgres" || u.Scheme == "postgresql") &&
		(u.Path == "/artex_proxy_test" || u.Path == "/artex_proxy_verify_test" || u.Path == "/artex_proxy_id_string_20261015") &&
		u.RawQuery == "sslmode=disable" && u.Fragment == ""
}

func TestProxyIntegrationDSNGuard(t *testing.T) {
	for _, database := range []string{"artex_proxy_test", "artex_proxy_verify_test"} {
		dsn := "postgres://artex_test@127.0.0.1:25432/" + database + "?sslmode=disable"
		if !proxyIntegrationDSNAllowed(dsn) {
			t.Fatalf("明确测试库被拒绝: %s", database)
		}
	}
	for _, dsn := range []string{
		"postgres://artex_test@127.0.0.1:25432/artex_models_verify_test?sslmode=disable",
		"postgres://artex_test@127.0.0.1:5432/artex_proxy_test?sslmode=disable",
		"postgres://artex_test@remote.invalid:25432/artex_proxy_test?sslmode=disable",
		"postgres://artex_test@127.0.0.1:25432/artex_proxy_test?sslmode=disable&host=remote.invalid",
		"postgres://artex_test@127.0.0.1:25432/artex_proxy_test?sslmode=disable&port=5432",
		"postgres://artex_test@127.0.0.1:25432/artex_proxy_test?sslmode=disable&dbname=production",
		"host=127.0.0.1 port=25432 dbname=artex_proxy_test sslmode=disable",
		"",
	} {
		if proxyIntegrationDSNAllowed(dsn) {
			t.Fatal("越界 DSN 被接受")
		}
	}
}

func proxyAPI(t *testing.T, handler http.Handler, method, path, body string) (int, map[string]any, string) {
	t.Helper()
	srv := httptest.NewServer(handler)
	defer srv.Close()
	req, err := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	var out map[string]any
	if err = json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, out, string(b)
}

func createRuntimeProxy(t *testing.T, s *Server, raw string) *db.Proxy {
	t.Helper()
	p, err := s.m.pg.CreateProxy(&db.Proxy{Name: "代理集成测试", Kind: db.ProxyKindHTTP, URL: raw, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = s.m.pg.Exec(`DELETE FROM outbound_proxies WHERE id=$1`, p.ID) })
	return p
}
func createRuntimeTask(t *testing.T, s *Server, mode db.TaskProxyMode, id *int64) *Task {
	t.Helper()
	row, err := s.m.pg.CreateTaskWithOptions("代理隔离测试", "验证出口", db.TaskCreateOptions{ProxyMode: mode, ProxyID: id})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = s.m.pg.Exec(`DELETE FROM tasks WHERE id=$1`, row.ID)
		_, _ = s.m.pg.Exec(`DELETE FROM explorations WHERE id=$1`, row.ExplorationID)
	})
	return &Task{ID: strconv.FormatInt(row.ID, 10), ProxyMode: row.ProxyMode, ProxyID: row.ProxyID, ProxySnapshot: row.ProxySnapshot}
}

func TestProxyPGHTTPUpdateContract(t *testing.T) {
	s, mux := proxyIntegration(t)
	code, out, body := proxyAPI(t, mux, "POST", "/api/proxies", `{"name":"test","kind":"http","url":"http://alice:url-secret@localhost:18080","password":"explicit-secret"}`)
	if code != 201 {
		t.Fatalf("创建: %d %s", code, body)
	}
	id, err := strconv.ParseInt(out["id"].(string), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = s.m.pg.Exec(`DELETE FROM outbound_proxies WHERE id=$1`, id) })
	path := fmt.Sprintf("/api/proxies/%d", id)
	if out["protocol"] != "http" || out["has_password"] != true || strings.Contains(body, "explicit-secret") || strings.Contains(body, "url-secret") {
		t.Fatal("DTO 未脱敏或缺字段")
	}
	code, _, body = proxyAPI(t, mux, "PUT", path, `{"name":"renamed","kind":"http","enabled":true}`)
	if code != 200 {
		t.Fatalf("省略: %d %s", code, body)
	}
	p, err := s.m.pg.GetProxy(id)
	if err != nil {
		t.Fatal(err)
	}
	u, _ := url.Parse(p.URL)
	pw, _ := u.User.Password()
	if pw != "explicit-secret" || p.Password != pw {
		t.Fatal("省略未保留凭据")
	}
	code, _, body = proxyAPI(t, mux, "PUT", path, `{"name":"renamed","kind":"http","password":"new-secret"}`)
	if code != 200 {
		t.Fatalf("替换: %d %s", code, body)
	}
	p, _ = s.m.pg.GetProxy(id)
	u, _ = url.Parse(p.URL)
	pw, _ = u.User.Password()
	if pw != "new-secret" {
		t.Fatal("password 未覆盖 URL")
	}
	code, out, body = proxyAPI(t, mux, "PUT", path, `{"name":"renamed","kind":"http","clear_password":true}`)
	if code != 200 || out["has_password"] != false {
		t.Fatalf("清除: %d %s", code, body)
	}
	p, _ = s.m.pg.GetProxy(id)
	u, _ = url.Parse(p.URL)
	pw, _ = u.User.Password()
	if p.Password != "" || pw != "" {
		t.Fatal("清除后残留 URL 密码")
	}
	code, _, _ = proxyAPI(t, mux, "PUT", path, `{"name":"renamed","kind":"http","url":"http://alice:***@localhost:18080"}`)
	if code != 400 {
		t.Fatal("接受了掩码凭据")
	}
	var stored string
	if err = s.m.pg.QueryRow(`SELECT url FROM outbound_proxies WHERE id=$1`, id).Scan(&stored); err != nil || !strings.HasPrefix(stored, "v1:") {
		t.Fatal("数据库未加密URL")
	}
	code, _, body = proxyAPI(t, mux, "POST", path+"/test", "")
	if code != 502 {
		t.Fatalf("故障代理虚报健康: %d %s", code, body)
	}
	if strings.Contains(body, "secret") {
		t.Fatal("检测错误泄漏凭据")
	}
	code, _, body = proxyAPI(t, mux, "POST", path+"/warp/stop", "")
	if code == 200 {
		t.Fatal("普通代理被当作 WARP")
	}
}

func TestProxyPGConcurrentTaskEgressAndRecording(t *testing.T) {
	s, _ := proxyIntegration(t)
	var direct atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { direct.Add(1); fmt.Fprint(w, "DIRECT") }))
	defer target.Close()
	for _, capture := range []bool{false, true} {
		t.Run(fmt.Sprint("录制=", capture), func(t *testing.T) {
			proxy := func(label string) (*httptest.Server, func()) {
				var conns sync.Map
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Header.Get("Proxy-Authorization") == "" {
						t.Error("未带上游凭据")
					}
					if r.Method == "CONNECT" {
						conn, rw, err := w.(http.Hijacker).Hijack()
						if err != nil {
							t.Error(err)
							return
						}
						conns.Store(conn, true)
						defer func() { conns.Delete(conn); conn.Close() }()
						fmt.Fprint(rw, "HTTP/1.1 200 Connection Established\r\n\r\n")
						rw.Flush()
						for {
							req, err := http.ReadRequest(rw.Reader)
							if err != nil {
								return
							}
							req.Body.Close()
							fmt.Fprintf(rw, "HTTP/1.1 200 OK\r\nContent-Length: %d\r\n\r\n%s", len([]byte(label)), label)
							if err = rw.Flush(); err != nil {
								return
							}
						}
					}
					fmt.Fprint(w, label)
				}))
				return server, func() { conns.Range(func(k, v any) bool { k.(net.Conn).Close(); return true }); server.Close() }
			}
			pa, closeA := proxy("出口A")
			pb, closeB := proxy("出口B")
			defer closeA()
			defer closeB()
			withAuth := func(raw string) string {
				u, _ := url.Parse(raw)
				u.User = url.UserPassword("user", "secret")
				return u.String()
			}
			a, b := createRuntimeProxy(t, s, withAuth(pa.URL)), createRuntimeProxy(t, s, withAuth(pb.URL))
			ta, tb := createRuntimeTask(t, s, db.TaskProxyModeFixed, &a.ID), createRuntimeTask(t, s, db.TaskProxyModeFixed, &b.ID)
			if !strings.HasPrefix(ta.ProxySnapshot.URL, "v1:") {
				t.Fatal("任务快照未加密")
			}
			if capture {
				tr, err := traffic.Open(t.TempDir(), "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				s.m.traffic = tr
				s.m.trafficOn = true
				t.Cleanup(func() { s.m.CloseProxyRuntime(); tr.Close(); s.m.traffic = nil; s.m.trafficOn = false })
			}
			var wg sync.WaitGroup
			for i := 0; i < 8; i++ {
				for _, pair := range []struct {
					task *Task
					want string
				}{{ta, "出口A"}, {tb, "出口B"}} {
					wg.Add(1)
					go func(task *Task, want string) {
						defer wg.Done()
						id, _ := strconv.ParseInt(task.ID, 10, 64)
						ctx := agent.WithRunInfo(t.Context(), agent.RunInfo{TaskID: id})
						raw, _ := json.Marshal(httpExec{URL: target.URL, Proxy: "http://127.0.0.1:1"})
						res, err := s.runHTTPTool(ctx, raw, nil, &actool.ToolContext{})
						if err != nil || res.IsError || !strings.Contains(res.Flatten(), want) {
							t.Errorf("串线/失败: %s %v", res.Flatten(), err)
						}
					}(pair.task, pair.want)
				}
			}
			wg.Wait()
			// 调用真实 Bash/WebFetch 工具入口，验证环境绕过列表不能改变任务出口。
			t.Setenv("NO_PROXY", "*")
			for _, pair := range []struct {
				task *Task
				want string
			}{{ta, "出口A"}, {tb, "出口B"}} {
				id, _ := strconv.ParseInt(pair.task.ID, 10, 64)
				ctx := agent.WithRunInfo(t.Context(), agent.RunInfo{TaskID: id})
				curl := "curl"
				if runtime.GOOS == "windows" {
					curl = "curl.exe"
				}
				raw, _ := json.Marshal(commandExec{Command: curl + " -fsS --max-time 3 '" + target.URL + "'"})
				res, err := s.runCommandTool(ctx, raw, nil, &actool.ToolContext{WorkingDir: t.TempDir()})
				if err != nil || res.IsError || !strings.Contains(res.Flatten(), pair.want) {
					t.Fatalf("Bash出口失败: %s %v", res.Flatten(), err)
				}
				addr, ca, err := s.m.TaskToolProxy(ctx, pair.task)
				if err != nil {
					t.Fatal(err)
				}
				input, _ := json.Marshal(map[string]any{"url": target.URL})
				res, err = actool.NewWebFetch(actool.WebFetchConfig{Proxy: addr, CACert: ca, StrictProxy: true}).Call(ctx, input, nil)
				if err != nil || res.IsError || !strings.Contains(res.Flatten(), pair.want) {
					t.Fatalf("WebFetch出口失败: %s %v", res.Flatten(), err)
				}
			}
			if direct.Load() != 0 {
				t.Fatal("任务流量直连")
			}
			closeA()
			id, _ := strconv.ParseInt(ta.ID, 10, 64)
			ctx := agent.WithRunInfo(t.Context(), agent.RunInfo{TaskID: id})
			raw, _ := json.Marshal(httpExec{URL: target.URL})
			res, _ := s.runHTTPTool(ctx, raw, nil, nil)
			if !res.IsError && !strings.Contains(res.Flatten(), `502`) {
				t.Fatal("故障上游未失败")
			}
			if direct.Load() != 0 {
				t.Fatal("失败回退直连")
			}
			if capture {
				var n int
				if err := s.m.traffic.DB().QueryRow(`SELECT count(*) FROM exchanges`).Scan(&n); err != nil || n < 16 {
					t.Fatalf("流量录制丢失 n=%d err=%v", n, err)
				}
			}
			if err := s.m.pg.DeleteProxy(a.ID); err == nil {
				t.Fatal("活动任务代理可被删除")
			}
			if _, err := s.m.pg.UpdateProxyWithOptions(a, db.ProxyUpdateOptions{ClearPassword: true}); err == nil {
				t.Fatal("活动任务代理可被修改")
			}
		})
	}
}

func TestProxyPGLegacyMigrationAndWarpDTO(t *testing.T) {
	s, mux := proxyIntegration(t)
	if _, err := s.m.pg.Exec(`DELETE FROM settings WHERE key IN ('global_proxy','global_proxy_id')`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = s.m.pg.Exec(`DELETE FROM settings WHERE key IN ('global_proxy','global_proxy_id')`) })
	if err := s.m.pg.SetSetting("global_proxy", "http://broken:***@localhost:1080"); err != nil {
		t.Fatal(err)
	}
	if err := s.m.pg.MigrateLegacyProxy(); err == nil {
		t.Fatal("非法旧代理迁移错误被吞")
	}
	if err := s.m.pg.SetSetting("global_proxy", "http://legacy:secret@localhost:1080"); err != nil {
		t.Fatal(err)
	}
	if err := s.m.pg.MigrateLegacyProxy(); err != nil {
		t.Fatal(err)
	}
	id, err := s.m.pg.GlobalProxyID()
	if err != nil || id == nil {
		t.Fatal("迁移没有设置默认")
	}
	t.Cleanup(func() { _, _ = s.m.pg.Exec(`DELETE FROM outbound_proxies WHERE id=$1`, *id) })
	if err = s.m.pg.MigrateLegacyProxy(); err != nil {
		t.Fatal(err)
	}
	again, _ := s.m.pg.GlobalProxyID()
	if again == nil || *again != *id {
		t.Fatal("迁移重复注册代理")
	}
	var n int
	if err = s.m.pg.QueryRow(`SELECT count(*) FROM settings WHERE key='global_proxy'`).Scan(&n); err != nil || n != 0 {
		t.Fatal("旧明文仍存在")
	}
	p, err := s.m.pg.CreateProxy(&db.Proxy{Name: "WARP DTO测试", Kind: db.ProxyKindWarp, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = s.m.pg.Exec(`DELETE FROM outbound_proxies WHERE id=$1`, p.ID) })
	dir := filepath.Join(os.Getenv("ARTEX_WARP_DIR"), warpProfileID(p.ID))
	if err = os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	meta := fmt.Sprintf(`{"id":%q,"name":"test","phase":"stopped","desired":false,"port":1080}`, warpProfileID(p.ID))
	if err = os.WriteFile(filepath.Join(dir, "meta.json"), []byte(meta), 0600); err != nil {
		t.Fatal(err)
	}
	code, out, body := proxyAPI(t, mux, "POST", fmt.Sprintf("/api/proxies/%d/warp/stop", p.ID), "")
	if code != 200 || out["id"] != i64s(p.ID) || out["status"] != "stopped" || out["protocol"] != "socks5" {
		t.Fatalf("WARP未返回ProxyDTO: %d %s", code, body)
	}
	code, _, _ = proxyAPI(t, mux, "POST", fmt.Sprintf("/api/proxies/%d/test", p.ID), "")
	if code != 502 {
		t.Fatal("停止WARP虚报healthy")
	}
}

func TestProxyPGResinCredentialsAndUncertainRelease(t *testing.T) {
	s, mux := proxyIntegration(t)
	var deletes atomic.Int64
	admin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/healthz" && r.Header.Get("Authorization") != "Bearer token-secret" {
			http.Error(w, "token-secret", 401)
			return
		}
		switch r.URL.Path {
		case "/healthz":
			w.WriteHeader(200)
		case "/api/v1/system/info":
			fmt.Fprint(w, `{"auth_version":"V1"}`)
		case "/api/v1/platforms":
			fmt.Fprint(w, `{"items":[{"id":"p","name":"Default"}]}`)
		default:
			deletes.Add(1)
			http.Error(w, "token-secret", 500)
		}
	}))
	defer admin.Close()
	input := fmt.Sprintf(`{"name":"resin","kind":"resin","protocol":"http","host":"localhost","port":1080,"password":"proxy-secret","resin_admin_url":%q,"resin_admin_token":"token-secret"}`, admin.URL)
	code, out, body := proxyAPI(t, mux, "POST", "/api/proxies", input)
	if code != 201 {
		t.Fatal(body)
	}
	id, err := strconv.ParseInt(out["id"].(string), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = s.m.pg.Exec(`DELETE FROM outbound_proxies WHERE id=$1`, id) })
	path := fmt.Sprintf("/api/proxies/%d", id)
	code, out, body = proxyAPI(t, mux, "PUT", path, fmt.Sprintf(`{"name":"resin","kind":"resin","protocol":"http","host":"localhost","port":1080,"resin_admin_url":%q}`, admin.URL))
	if code != 200 || out["has_password"] != true || out["has_resin_admin_token"] != true || strings.Contains(body, "secret") {
		t.Fatalf("Resin省略未保留: %s", body)
	}
	task := createRuntimeTask(t, s, db.TaskProxyModeFixed, &id)
	if _, _, err := s.m.TaskToolProxy(t.Context(), task); err != nil {
		t.Fatal(err)
	}
	if err := s.m.ReleaseTaskProxy(t.Context(), task); err == nil || strings.Contains(err.Error(), "secret") {
		t.Fatal("回收失败被吞或错误泄密")
	}
	if err := s.m.ReleaseTaskProxy(t.Context(), task); err == nil || deletes.Load() != 1 {
		t.Fatal("不明确回收被自动重复")
	}
	if _, err := s.m.pg.Exec(`UPDATE tasks SET status='done' WHERE id=$1`, task.ID); err != nil {
		t.Fatal(err)
	}
	code, out, body = proxyAPI(t, mux, "PUT", path, fmt.Sprintf(`{"name":"resin","kind":"resin","protocol":"http","host":"localhost","port":1080,"resin_admin_url":%q,"clear_password":true,"clear_resin_admin_token":true}`, admin.URL))
	if code != 200 || out["has_password"] != false || out["has_resin_admin_token"] != false {
		t.Fatalf("Resin清除失败: %s", body)
	}
	code, _, body = proxyAPI(t, mux, "POST", path+"/test", "")
	if code != 502 || strings.Contains(body, "secret") {
		t.Fatal("控制面认证失败未闭锁或未脱敏")
	}
}

func TestProxyPGSnapshotAndLLMPriority(t *testing.T) {
	s, mux := proxyIntegration(t)
	a := createRuntimeProxy(t, s, "http://127.0.0.1:19001")
	b := createRuntimeProxy(t, s, "http://127.0.0.1:19002")
	t.Cleanup(func() { s.m.pg.SetGlobalProxyID(nil) })
	code, _, body := proxyAPI(t, mux, "POST", "/api/proxies/global", fmt.Sprintf(`{"proxy_id":%d}`, a.ID))
	if code != 200 {
		t.Fatal(body)
	}
	task := createRuntimeTask(t, s, db.TaskProxyModeInherit, nil)
	if err := s.m.pg.SetGlobalProxyID(&b.ID); err != nil {
		t.Fatal(err)
	}
	got, _, err := s.m.TaskToolProxy(t.Context(), task)
	if err != nil || got != a.URL {
		t.Fatalf("全局变更影响快照: %s %v", got, err)
	}
	if got, err = s.m.ResolveLLMProxy(t.Context(), task, "http://channel.test:1080"); err != nil || got != "http://channel.test:1080" {
		t.Fatal("渠道优先级错误")
	}
	task.ProxyMode = db.TaskProxyModeFixed
	if got, err = s.m.ResolveLLMProxy(t.Context(), task, "http://channel.test:1080"); err != nil || got != a.URL {
		t.Fatal("固定未覆盖渠道")
	}
	task.ProxyMode = db.TaskProxyModeDirect
	if got, err = s.m.ResolveLLMProxy(t.Context(), task, "http://channel.test:1080"); err != nil || got != "" {
		t.Fatal("直连未覆盖渠道")
	}
	if got, err = s.m.ResolveLLMProxy(t.Context(), nil, ""); err != nil || got != b.URL {
		t.Fatal("非任务LLM未继承当前全局")
	}
	var n int
	if err = s.m.pg.QueryRow(`SELECT count(*) FROM settings WHERE key='global_proxy'`).Scan(&n); err != nil || n != 0 {
		t.Fatal("全局接口仍持久明文旧设置")
	}
}

func TestProxyPGBrowserCloneAndResinRelease(t *testing.T) {
	s, mux := proxyIntegration(t)
	var deletes atomic.Int64
	admin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/healthz" && r.Header.Get("Authorization") != "Bearer admin-secret" {
			w.WriteHeader(401)
			return
		}
		switch {
		case r.URL.Path == "/healthz":
			w.WriteHeader(200)
		case r.URL.Path == "/api/v1/system/info":
			fmt.Fprint(w, `{"auth_version":"V1"}`)
		case r.URL.Path == "/api/v1/platforms":
			fmt.Fprint(w, `{"items":[{"id":"platform-id","name":"Default"}]}`)
		case r.Method == "DELETE":
			if !strings.Contains(r.URL.Path, "artex_task_") {
				t.Error("释放使用了非任务稳定账号")
			}
			deletes.Add(1)
			w.WriteHeader(204)
		default:
			w.WriteHeader(404)
		}
	}))
	defer admin.Close()
	p, err := s.m.pg.CreateProxy(&db.Proxy{Name: "Resin测试", Kind: db.ProxyKindResin, Protocol: "http", Host: "127.0.0.1", Port: 19008, Password: "proxy-secret", ResinAdminURL: admin.URL, ResinAdminToken: "admin-secret", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = s.m.pg.Exec(`DELETE FROM outbound_proxies WHERE id=$1`, p.ID) })
	task := createRuntimeTask(t, s, db.TaskProxyModeFixed, &p.ID)
	addr, _, err := s.m.TaskToolProxy(t.Context(), task)
	if err != nil || !strings.HasPrefix(addr, "http://Default.artex_task_") {
		t.Fatalf("Resin协议/账号错误: %s %v", addr, err)
	}
	original := &db.MCPServer{Name: "browser", Transport: "stdio", Command: "playwright-mcp", Args: json.RawMessage(`["--proxy-server","http://shared:8080","--headless"]`), Env: json.RawMessage(`{"NO_PROXY":"*"}`)}
	id, _ := strconv.ParseInt(task.ID, 10, 64)
	ctx := agent.WithRunInfo(t.Context(), agent.RunInfo{TaskID: id})
	copy, err := s.m.TaskMCPConfig(ctx, original)
	if err != nil {
		t.Fatal(err)
	}
	if string(original.Args) != `["--proxy-server","http://shared:8080","--headless"]` || strings.Contains(string(copy.Args), "shared") || jsonStrMap(copy.Env)["NO_PROXY"] != "" {
		t.Fatal("浏览器克隆污染或绕过泄漏")
	}
	args := jsonStrSlice(copy.Args)
	configBytes, err := os.ReadFile(args[len(args)-1])
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(configBytes), "Default.artex_task_") || strings.Contains(string(configBytes), "shared") {
		t.Fatal("浏览器未使用任务账号")
	}
	code, _, _ := proxyAPI(t, mux, "POST", fmt.Sprintf("/api/proxies/%d/resin/release", p.ID), fmt.Sprintf(`{"task_id":"%s"}`, task.ID))
	if code != 409 {
		t.Fatal("运行任务租约可被回收")
	}
	if _, err = s.m.pg.Exec(`UPDATE tasks SET status='done' WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
	code, out, body := proxyAPI(t, mux, "POST", fmt.Sprintf("/api/proxies/%d/resin/release", p.ID), "")
	if code != 200 || out["id"] != i64s(p.ID) || out["protocol"] != "http" || strings.Contains(body, "secret") {
		t.Fatalf("回收DTO: %d %s", code, body)
	}
	if err = s.m.ReleaseTaskProxy(context.Background(), task); err != nil || deletes.Load() != 1 {
		t.Fatal("回收不幂等")
	}
	if _, _, err = s.m.TaskToolProxy(t.Context(), task); err == nil {
		t.Fatal("结束租约自动重新使用")
	}
}
