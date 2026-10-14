package server

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Autumn-27/artex/db"
	"github.com/google/uuid"
)

func warpCreateHTTP(t *testing.T, handler http.Handler, key, body string, discard bool) (int, map[string]any, string) {
	t.Helper()
	srv := httptest.NewServer(handler)
	defer srv.Close()
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/api/proxies", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if discard {
		return resp.StatusCode, nil, ""
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, out, string(data)
}

func proxyResponseID(t *testing.T, out map[string]any, key string) int64 {
	t.Helper()
	raw, ok := out[key].(string)
	if !ok {
		t.Fatalf("响应 %s 不是十进制字符串: %#v", key, out[key])
	}
	id, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || id <= 0 {
		t.Fatalf("响应 %s 不是有效代理编号: %#v", key, out[key])
	}
	return id
}

func TestProxyPGWarpCreateIdempotency(t *testing.T) {
	if warpFixtureSubprocess(t) {
		return
	}
	t.Setenv("ARTEX_PG_DSN", os.Getenv("ARTEX_WARP_CHILD_DSN"))
	s, mux := proxyIntegration(t)
	mux.HandleFunc("DELETE /api/proxies/{id}", s.deleteProxy)
	logPath := filepath.Join(t.TempDir(), "calls.log")
	t.Setenv("ARTEX_WARP_CALL_LOG", logPath)
	t.Setenv("ARTEX_WARP_FIXTURE_MODE", "ready")
	const body = `{"name":"重放测试","kind":"warp","accept_terms":true}`
	var before, after int
	if err := s.m.pg.QueryRow(`SELECT count(*) FROM outbound_proxies`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	code, _, raw := warpCreateHTTP(t, mux, "", body, false)
	if code != 400 {
		t.Errorf("WARP无key仍被创建: %d %s", code, raw)
	}
	if err := s.m.pg.QueryRow(`SELECT count(*) FROM outbound_proxies`).Scan(&after); err != nil || after != before {
		t.Fatalf("缺key请求已建档: before=%d after=%d err=%v", before, after, err)
	}
	key := uuid.NewString()
	code, _, _ = warpCreateHTTP(t, mux, key, body, true)
	if code != 201 {
		t.Fatalf("首次响应状态错误: %d", code)
	}
	code, replay, raw := warpCreateHTTP(t, mux, key, body, false)
	if code != 201 {
		t.Errorf("断响应后同key未复用: %d %s", code, raw)
	}
	id := proxyResponseID(t, replay, "id")
	t.Cleanup(func() { _ = s.m.pg.DeleteProxy(id) })
	var ids []int64
	rows, err := s.m.pg.Query(`SELECT id FROM outbound_proxies WHERE name='重放测试'`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, v)
	}
	rows.Close()
	if len(ids) != 1 || ids[0] != id {
		t.Errorf("相同意图创建了多行: ids=%v replay=%v", ids, id)
	}
	code, _, raw = warpCreateHTTP(t, mux, key, `{"name":"同key异请求","kind":"warp","accept_terms":true}`, false)
	if code != 409 {
		t.Errorf("同key异载荷未拒绝: %d %s", code, raw)
	}
	calls, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(calls), "register\n") != 1 {
		t.Errorf("断响应重放导致重复注册: %s", calls)
	}
	path := fmt.Sprintf("/api/proxies/%d", id)
	if c, _, r := proxyAPI(t, mux, "POST", path+"/warp/stop", ""); c != 200 {
		t.Fatal(r)
	}
	code, stopped, raw := warpCreateHTTP(t, mux, key, body, false)
	if code != 201 || stopped["status"] != "stopped" || stopped["egress_ip"] != nil {
		t.Errorf("成功重放回放旧健康状态: %d %s", code, raw)
	}
	if c, _, r := proxyAPI(t, mux, "DELETE", path, ""); c != 200 {
		t.Fatal(r)
	}
	code, _, raw = warpCreateHTTP(t, mux, key, body, false)
	if code != 410 {
		t.Errorf("删除后旧key被重用: %d %s", code, raw)
	}
}

func TestProxyPGWarpCreateConcurrentManagers(t *testing.T) {
	if warpFixtureSubprocess(t) {
		return
	}
	t.Setenv("ARTEX_PG_DSN", os.Getenv("ARTEX_WARP_CHILD_DSN"))
	s, mux := proxyIntegration(t)
	mux.HandleFunc("DELETE /api/proxies/{id}", s.deleteProxy)
	logPath := filepath.Join(t.TempDir(), "calls.log")
	gate := filepath.Join(t.TempDir(), "block-register")
	if err := os.WriteFile(gate, []byte("wait"), 0600); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(gate)
	t.Setenv("ARTEX_WARP_CALL_LOG", logPath)
	t.Setenv("ARTEX_WARP_FIXTURE_MODE", "register-block")
	t.Setenv("ARTEX_WARP_REGISTER_GATE", gate)
	otherDB, err := db.Open(os.Getenv("ARTEX_PG_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = otherDB.Close() })
	other := &Manager{pg: otherDB}
	rt, err := s.m.proxyRuntime()
	if err != nil {
		t.Fatal(err)
	}
	proxyRuntimes.Store(other, rt)
	t.Cleanup(func() { proxyRuntimes.Delete(other) })
	otherServer := &Server{m: other}
	otherMux := http.NewServeMux()
	otherMux.HandleFunc("POST /api/proxies", otherServer.createProxy)
	key := uuid.NewString()
	const body = `{"name":"并发WARP意图","kind":"warp","accept_terms":true}`
	type response struct {
		code int
		out  map[string]any
		raw  string
	}
	first := make(chan response, 1)
	go func() { c, o, r := warpCreateHTTP(t, mux, key, body, false); first <- response{c, o, r} }()
	var id int64
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		err := otherDB.QueryRow(`SELECT proxy_id FROM warp_proxy_create_requests WHERE request_key=$1`, key).Scan(&id)
		if err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if id == 0 {
		t.Fatal("意图未在注册网络前持久提交")
	}
	t.Cleanup(func() {
		if rt, err := s.m.proxyRuntime(); err == nil {
			_, _ = rt.warp.Stop(t.Context(), warpProfileID(id))
		}
		_ = s.m.pg.DeleteProxy(id)
	})
	start := time.Now()
	code, out, raw := warpCreateHTTP(t, otherMux, key, body, false)
	if time.Since(start) > 500*time.Millisecond {
		t.Errorf("第二Manager等在全局DB网络锁: %s", time.Since(start))
	}
	if code != 202 || out["proxy_id"] != i64s(id) || out["operation_status"] != "pending" {
		t.Errorf("并发重放没有返回同一pending意图: %d %s", code, raw)
	}
	if err := otherDB.SetProxyStatus(id, db.ProxyStatusHealthy, "", time.Now()); !errors.Is(err, db.ErrProxyInUse) {
		t.Errorf("pending允许跨Manager状态写入: %v", err)
	}
	if _, err := s.performWarpAction(t.Context(), id, "stop", false); !errors.Is(err, db.ErrProxyInUse) && !errors.Is(err, db.ErrWarpHealthBlocked) {
		t.Errorf("pending允许直接WARP action: %v", err)
	}
	if actionCode, _, actionRaw := proxyAPI(t, mux, "POST", fmt.Sprintf("/api/proxies/%d/warp/stop", id), ""); actionCode != 409 {
		t.Errorf("pending注册期间允许WARP action: %d %s", actionCode, actionRaw)
	}
	for _, call := range []struct{ method, path, body string }{
		{"POST", fmt.Sprintf("/api/proxies/%d/test", id), ""},
		{"PUT", fmt.Sprintf("/api/proxies/%d", id), `{"name":"并发WARP意图","kind":"warp"}`},
		{"DELETE", fmt.Sprintf("/api/proxies/%d", id), ""},
	} {
		if c, _, body := proxyAPI(t, mux, call.method, call.path, call.body); c != 409 {
			t.Errorf("pending允许%s %s: %d %s", call.method, call.path, c, body)
		}
	}
	if err := os.Remove(gate); err != nil {
		t.Fatal(err)
	}
	result := <-first
	if result.code != 201 || result.out["id"] != i64s(id) {
		t.Errorf("首请求未完成原ID: %d %s", result.code, result.raw)
	}
	code, out, raw = warpCreateHTTP(t, otherMux, key, body, false)
	if code != 201 || out["id"] != i64s(id) {
		t.Errorf("跨Manager完成重放未复用原ID: %d %s", code, raw)
	}
	calls, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(calls), "register\n") != 1 {
		t.Errorf("并发重复注册: %s", calls)
	}
}

func TestProxyPGWarpCreateOrphanPending(t *testing.T) {
	if warpFixtureSubprocess(t) {
		return
	}
	t.Setenv("ARTEX_PG_DSN", os.Getenv("ARTEX_WARP_CHILD_DSN"))
	s, mux := proxyIntegration(t)
	logPath := filepath.Join(t.TempDir(), "calls.log")
	t.Setenv("ARTEX_WARP_CALL_LOG", logPath)
	t.Setenv("ARTEX_WARP_FIXTURE_MODE", "ready")
	key := uuid.NewString()
	const body = `{"name":"遗留未执行意图","kind":"warp","accept_terms":true}`
	var req proxyRequest
	if err := json.Unmarshal([]byte(body), &req); err != nil {
		t.Fatal(err)
	}
	canonical := []byte(`{"name":"遗留未执行意图","kind":"warp","accept_terms":true,"enabled":true}`)
	p, intent, err := s.m.pg.CreateWarpProxyOnce(key, sha256.Sum256(canonical), proxyFromRequest(req))
	if err != nil || !intent.Created {
		t.Fatalf("不能持久登记意图: %v", err)
	}
	if _, err := s.m.pg.CompleteWarpProxyCreate(key, p.ID, uuid.NewString(), nil, "伪造完成"); !errors.Is(err, db.ErrWarpCreateConflict) {
		t.Fatalf("非创建winner完成了意图: %v", err)
	}
	t.Cleanup(func() {
		_, _ = s.m.pg.CompleteWarpProxyCreate(key, p.ID, intent.ExecutionToken, nil, "测试遗留意图结束")
		_ = s.m.pg.DeleteProxy(p.ID)
	})
	code, replay, raw := warpCreateHTTP(t, mux, key, body, false)
	if code != 202 || replay["proxy_id"] != i64s(p.ID) || replay["operation_status"] != "pending" {
		t.Fatalf("遗留意图被重新执行: %d %s", code, raw)
	}
	if _, err := os.Stat(logPath); !os.IsNotExist(err) {
		t.Fatalf("遗留意图触发外部注册: %v", err)
	}
}

func TestProxyPGWarpCreatePayloadWhitelist(t *testing.T) {
	if warpFixtureSubprocess(t) {
		return
	}
	t.Setenv("ARTEX_PG_DSN", os.Getenv("ARTEX_WARP_CHILD_DSN"))
	s, mux := proxyIntegration(t)
	t.Setenv("ARTEX_WARP_CALL_LOG", filepath.Join(t.TempDir(), "calls.log"))
	t.Setenv("ARTEX_WARP_FIXTURE_MODE", "ready")
	const invalid = `{"name":"非法WARP载荷","kind":"warp","accept_terms":true,"url":"socks5://127.0.0.1:1080","password":"private","resin_admin_token":"secret","extra":"ignored"}`
	code, out, raw := warpCreateHTTP(t, mux, uuid.NewString(), invalid, false)
	if rawID, ok := out["id"].(string); ok {
		id, parseErr := strconv.ParseInt(rawID, 10, 64)
		if parseErr != nil {
			t.Fatalf("WARP 错误响应编号无效: %q", rawID)
		}
		if rt, err := s.m.proxyRuntime(); err == nil {
			_, _ = rt.warp.Stop(t.Context(), warpProfileID(id))
		}
		_ = s.m.pg.DeleteProxy(id)
	}
	if code != 400 {
		t.Errorf("额外WARP配置或秘密未拒绝: %d %s", code, raw)
	}
	key := uuid.NewString()
	code, first, raw := warpCreateHTTP(t, mux, key, `{"name":"  规范名称  ","kind":"warp","accept_terms":true}`, false)
	if code != 201 {
		t.Fatalf("规范名称首次创建失败: %d %s", code, raw)
	}
	id := proxyResponseID(t, first, "id")
	t.Cleanup(func() {
		if rt, err := s.m.proxyRuntime(); err == nil {
			_, _ = rt.warp.Stop(t.Context(), warpProfileID(id))
		}
		if err := s.m.pg.DeleteProxy(id); err != nil {
			t.Errorf("测试WARP清理失败: %v", err)
		}
	})
	code, replay, raw := warpCreateHTTP(t, mux, key, `{"name":"规范名称","kind":"warp","accept_terms":true,"enabled":true}`, false)
	if code != 201 || replay["id"] != i64s(id) {
		t.Errorf("同语义请求未复用ID: %d %s", code, raw)
	}
}

func TestProxyPGWarpCreateFailureAndRestart(t *testing.T) {
	if warpFixtureSubprocess(t) {
		return
	}
	t.Setenv("ARTEX_PG_DSN", os.Getenv("ARTEX_WARP_CHILD_DSN"))
	s, mux := proxyIntegration(t)
	logPath := filepath.Join(t.TempDir(), "calls.log")
	t.Setenv("ARTEX_WARP_CALL_LOG", logPath)
	t.Setenv("ARTEX_WARP_FIXTURE_MODE", "register-fail")
	key := uuid.NewString()
	const body = `{"name":"不确定注册失败","kind":"warp","accept_terms":true}`
	code, first, raw := warpCreateHTTP(t, mux, key, body, false)
	if code != 502 {
		t.Fatalf("注册失败状态错误: %d %s", code, raw)
	}
	id := proxyResponseID(t, first, "proxy_id")
	t.Cleanup(func() { _ = s.m.pg.DeleteProxy(id) })
	code, replay, raw := warpCreateHTTP(t, mux, key, body, false)
	if code != 502 || replay["proxy_id"] != i64s(id) {
		t.Errorf("失败重放未保留原ID: %d %s", code, raw)
	}
	if err := s.m.CloseProxyRuntime(); err != nil {
		t.Fatal(err)
	}
	reopened, err := db.Open(os.Getenv("ARTEX_PG_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	restarted := &Manager{pg: reopened, dir: s.m.dir}
	if err := restarted.InitProxyRuntime(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.CloseProxyRuntime() })
	next := &Server{m: restarted}
	nextMux := http.NewServeMux()
	nextMux.HandleFunc("POST /api/proxies", next.createProxy)
	code, replay, raw = warpCreateHTTP(t, nextMux, key, body, false)
	if code != 502 || replay["proxy_id"] != i64s(id) {
		t.Errorf("重启后失败意图丢失: %d %s", code, raw)
	}
	calls, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(calls), "register\n") != 1 {
		t.Errorf("重启后重复注册: %s", calls)
	}
}
