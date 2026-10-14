package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Autumn-27/artex/agent"
	"github.com/Autumn-27/artex/db"
	"github.com/Autumn-27/norma/permission"
	actool "github.com/Autumn-27/norma/tool"
	"github.com/google/uuid"
)

func TestProxyToolsCatalog(t *testing.T) {
	s := &Server{}
	found := map[string]bool{}
	for _, tool := range s.platformTools() {
		found[tool.Name()] = true
		if tool.Name() == "test_proxies" && tool.IsReadOnly(nil) {
			t.Fatal("检测必须标为网络及状态写操作")
		}
	}
	for _, name := range []string{"list_proxies", "add_proxies", "test_proxies"} {
		if !found[name] {
			t.Errorf("平台缺少代理中心工具 %s", name)
		}
	}
}

func TestProxyToolsStrictArguments(t *testing.T) {
	for _, raw := range []string{`null`, `[]`, `{} {}`, `{"limit":null}`, `{"Limit":1}`, `{"limit":1,"limit":2}`, `{"limit":1e1}`, `{"limit":"1"}`, `{"limit":0}`, `{"limit":101}`, `{"after_id":-1}`, `{"extra":"synthetic-secret"}`} {
		if decodeProxyToolInput([]byte(raw), proxyListSchema(), nil) == nil {
			t.Fatalf("歧义参数被接受: %s", raw)
		}
	}
	if decodeProxyToolInput([]byte(`{"limit":1,"after_id":0}`), proxyListSchema(), nil) != nil {
		t.Fatal("合法分页被拒绝")
	}
	for _, req := range []proxyRequest{
		{Name: "x", Kind: db.ProxyKindWarp},
		{Name: "x", Kind: db.ProxyKindHTTP, URL: ptr("http://user:secret@host:0")},
		{Name: "x", Kind: db.ProxyKindHTTP, URL: ptr("http://user:secret@host:70000")},
		{Name: "x", Kind: db.ProxyKindHTTP, URL: ptr("http://host:80?token=secret")},
		{Name: "x", Kind: db.ProxyKindHTTP, URL: ptr("http://host:80?  ")},
		{Name: "x", Kind: db.ProxyKindHTTP, URL: ptr("socks5://host:80")},
		{Name: "x", Kind: db.ProxyKindHTTP, URL: ptr("http://host:80"), Password: ptr("secret")},
		{Name: "x", Kind: db.ProxyKindResin, Protocol: "http", Host: "http://host", Port: 80},
		{Name: "x", Kind: db.ProxyKindResin, Protocol: "http", Host: "host", Port: 80, ResinPlatform: "Default.other"},
		{Name: "x", Kind: db.ProxyKindResin, Protocol: "http", Host: "host", Port: 80, URL: ptr("http://host:80")},
	} {
		if _, err := validatedProxyCreate(req); err == nil {
			t.Fatal("冲突代理参数被接受")
		}
	}
	for _, target := range []string{"http://u:secret@host/", "http://host/?token=secret", "http://host:0/", "file:///secret"} {
		if validProxyProbeTarget(target) {
			t.Fatal("非法检测目标被接受")
		}
	}
}

func ptr(s string) *string { return &s }

func TestProxyToolsIntegration(t *testing.T) {
	s, mux := proxyIntegration(t)
	pg := s.m.pg
	// 此测试只删除自己本轮创建的意图和代理。
	var createdIDs []string
	var keys []string
	t.Cleanup(func() {
		for _, key := range keys {
			_, _ = pg.Exec(`DELETE FROM proxy_create_intents WHERE item_key=$1`, key)
		}
		for _, id := range createdIDs {
			parsed, _ := strconv.ParseInt(id, 10, 64)
			_, _ = pg.Exec(`DELETE FROM outbound_proxies WHERE id=$1`, parsed)
		}
	})
	oldResolve, oldAugment := agent.ToolResolve, agent.ToolAugment
	t.Cleanup(func() { agent.ToolResolve, agent.ToolAugment = oldResolve, oldAugment })
	wireTools(pg, nil)
	s.seedOrchestrationTools()
	wireAgentAugment(pg, t.TempDir(), s.hostTools)
	extra, _, cleanup := agent.ToolAugment(t.Context(), "auto")
	if cleanup != nil {
		defer cleanup()
	}
	resolved := agent.ToolResolve(t.Context(), "auto", extra)
	toolsByName := map[string]actool.CoreTool{}
	for _, tool := range resolved {
		if isProxyCenterTool(tool.Name()) {
			toolsByName[tool.Name()] = tool
		}
	}
	if len(toolsByName) != 3 {
		t.Fatal("Auto 实际装配未获得三个工具")
	}
	for _, who := range []string{"planner", "worker", "mainagent"} {
		for _, tool := range agent.ToolResolve(t.Context(), who, s.proxyCenterTools()) {
			if isProxyCenterTool(tool.Name()) {
				t.Fatal("默认错误绑定其他 Agent")
			}
		}
	}
	call := func(name string, a any) string {
		t.Helper()
		raw, err := json.Marshal(a)
		if err != nil {
			t.Fatal(err)
		}
		result, err := toolsByName[name].Call(t.Context(), raw, nil)
		if err != nil || result.IsError {
			t.Fatalf("工具调用失败: %s %v", result.Flatten(), err)
		}
		return result.Flatten()
	}
	decodeItems := func(text string) []proxyItemResult {
		t.Helper()
		var v struct {
			Items []proxyItemResult `json:"items"`
		}
		if err := json.Unmarshal([]byte(text), &v); err != nil {
			t.Fatal(err)
		}
		return v.Items
	}
	id64 := func(raw string) int64 {
		t.Helper()
		id, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			t.Fatalf("工具返回无效代理编号 %q: %v", raw, err)
		}
		return id
	}
	name := "代理工具-" + uuid.NewString()
	key1, key2 := uuid.NewString(), uuid.NewString()
	keys = append(keys, key1, key2)
	input := map[string]any{"items": []any{
		map[string]any{"item_key": key1, "name": name, "kind": "http", "url": "http://synthetic-user:synthetic-secret@127.0.0.1:18080"},
		map[string]any{"item_key": key2, "name": name, "kind": "http", "url": "http://127.0.0.1:0"},
	}}
	result := decodeItems(call("add_proxies", input))
	if len(result) != 2 || result[0].Proxy == nil || result[0].OperationStatus != "succeeded" || result[1].ErrorCode != "invalid_proxy" {
		t.Fatal("部分成功映射错误")
	}
	id := result[0].Proxy.ID
	createdIDs = append(createdIDs, id)
	// 模拟首次成功响应丢失，然后完整并行重放。
	raw, _ := json.Marshal(input)
	var wg sync.WaitGroup
	failures := make(chan string, 8)
	for n := 0; n < 8; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, e := toolsByName["add_proxies"].Call(t.Context(), raw, nil)
			var out struct {
				Items []proxyItemResult `json:"items"`
			}
			if e != nil || r.IsError || json.Unmarshal([]byte(r.Flatten()), &out) != nil || len(out.Items) != 2 || out.Items[0].Proxy == nil || out.Items[0].Proxy.ID != id || !out.Items[0].Replayed {
				failures <- "重放映射不稳定"
			}
		}()
	}
	wg.Wait()
	close(failures)
	for message := range failures {
		t.Fatal(message)
	}
	var count int
	if err := pg.QueryRow(`SELECT count(*) FROM outbound_proxies WHERE name=$1`, name).Scan(&count); err != nil || count != 1 {
		t.Fatal("重放重复添加")
	}
	input["items"].([]any)[1].(map[string]any)["url"] = "http://127.0.0.1:18081"
	result = decodeItems(call("add_proxies", input))
	if result[1].Proxy == nil || result[1].Proxy.ID == id {
		t.Fatal("失败项不能恢复或按名称去重")
	}
	createdIDs = append(createdIDs, result[1].Proxy.ID)
	// HTTP 批量入口重放同一工具意图。
	body, _ := json.Marshal(input)
	code, _, response := proxyAPI(t, mux, "POST", "/api/proxies/batch", string(body))
	if code != 200 {
		t.Fatalf("HTTP 批量入口失败: %d", code)
	}
	replay := decodeItems(response)
	if replay[0].Proxy.ID != id || !replay[0].Replayed {
		t.Fatal("HTTP 与工具未共享意图")
	}
	input["items"].([]any)[0].(map[string]any)["url"] = "http://changed:changed-secret@127.0.0.1:18080"
	if result = decodeItems(call("add_proxies", input)); result[0].ErrorCode != "item_key_conflict" {
		t.Fatal("相同键不同密码未冲突")
	}
	// 输出、测试错误和日志都不得出现凭据。
	listing := call("list_proxies", map[string]any{"limit": 100})
	for _, secret := range []string{"synthetic-user", "synthetic-secret", "changed-secret"} {
		if strings.Contains(listing, secret) || strings.Contains(response, secret) {
			t.Fatal("代理输出泄露凭据")
		}
	}
	var logs strings.Builder
	oldLog := log.Writer()
	log.SetOutput(&logs)
	defer log.SetOutput(oldLog)
	// 本地合成 HTTP 代理直接响应绝对地址请求，模拟每次动态更换出口；不访问外网。
	var hits atomic.Int32
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !r.URL.IsAbs() || r.Host != "probe.invalid" || r.Header.Get("Proxy-Authorization") == "" {
			http.Error(w, "synthetic-secret", 407)
			return
		}
		hits.Add(1)
		fmt.Fprintln(w, "ip=192.0.2."+strconv.Itoa(int(hits.Load())))
	}))
	defer proxy.Close()
	key3 := uuid.NewString()
	keys = append(keys, key3)
	proxyURL := strings.Replace(proxy.URL, "://", "://synthetic-user:synthetic-secret@", 1)
	result = decodeItems(call("add_proxies", map[string]any{"items": []any{map[string]any{"item_key": key3, "name": "动态出口", "kind": "http", "url": proxyURL}}}))
	pid := result[0].Proxy.ID
	createdIDs = append(createdIDs, pid)
	for n := 0; n < 2; n++ {
		checked := decodeItems(call("test_proxies", map[string]any{"proxy_ids": []int64{id64(pid)}, "target_url": "http://probe.invalid/check"}))
		if checked[0].OperationStatus != "succeeded" || checked[0].Proxy.Status != "healthy" {
			t.Fatal("合成代理检测失败")
		}
	}
	if hits.Load() != 2 {
		t.Fatal("没有执行真实代理请求")
	}
	row, err := pg.GetProxy(id64(pid))
	if err != nil || row.Status != "healthy" || row.LastCheckedAt == nil {
		t.Fatal("检测状态未落库")
	}
	// 坏出口 + 全局/环境代理可用时仍应失败，不得借其他出口测试。
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	dead := listener.Addr().String()
	listener.Close()
	key4 := uuid.NewString()
	keys = append(keys, key4)
	result = decodeItems(call("add_proxies", map[string]any{"items": []any{map[string]any{"item_key": key4, "name": "故障出口", "kind": "http", "url": "http://synthetic-user:synthetic-secret@" + dead}}}))
	badID := result[0].Proxy.ID
	createdIDs = append(createdIDs, badID)
	t.Setenv("HTTP_PROXY", proxy.URL)
	t.Setenv("HTTPS_PROXY", proxy.URL)
	t.Setenv("ALL_PROXY", proxy.URL)
	pid64 := id64(pid)
	if err := pg.SetGlobalProxyID(&pid64); err != nil {
		t.Fatal(err)
	}
	defer pg.SetGlobalProxyID(nil)
	badResult := call("test_proxies", map[string]any{"proxy_ids": []int64{id64(badID)}, "target_url": "http://probe.invalid/check", "timeout_seconds": 1})
	failed := decodeItems(badResult)
	if failed[0].ErrorCode != "probe_failed" || failed[0].Proxy.Status != "failed" || hits.Load() != 2 {
		t.Fatal("坏出口未失败关闭")
	}
	row, err = pg.GetProxy(id64(badID))
	if err != nil || row.LastError != "代理检测失败" {
		t.Fatal("错误未安全落库")
	}
	if strings.Contains(badResult, "secret") || strings.Contains(logs.String(), "secret") {
		t.Fatal("错误或日志泄密")
	}
	// Resin 管理面与数据面均使用本地合成服务，验证认证信息不出现在回显中。
	var adminHits, dataHits atomic.Int32
	admin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		adminHits.Add(1)
		if r.URL.Path != "/healthz" && r.Header.Get("Authorization") != "Bearer resin-admin-secret" {
			http.Error(w, "resin-admin-secret", 401)
			return
		}
		switch r.URL.Path {
		case "/healthz":
			fmt.Fprint(w, `{}`)
		case "/api/v1/system/info":
			fmt.Fprint(w, `{"auth_version":"V1"}`)
		case "/api/v1/platforms":
			fmt.Fprint(w, `{"items":[{"id":"local","name":"Default"}]}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer admin.Close()
	data := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Header.Set("Authorization", r.Header.Get("Proxy-Authorization"))
		user, password, ok := r.BasicAuth()
		if !ok || !strings.HasPrefix(user, "Default.artex_task_probe_") || password != "resin-data-secret" {
			http.Error(w, "resin-data-secret", 407)
			return
		}
		dataHits.Add(1)
		fmt.Fprint(w, "ip=192.0.2.99")
	}))
	defer data.Close()
	host, portString, _ := net.SplitHostPort(strings.TrimPrefix(data.URL, "http://"))
	port, _ := strconv.Atoi(portString)
	resinKey := uuid.NewString()
	keys = append(keys, resinKey)
	resinResult := call("add_proxies", map[string]any{"items": []any{map[string]any{"item_key": resinKey, "name": "Resin 合成入口", "kind": "resin", "protocol": "http", "host": host, "port": port, "password": "resin-data-secret", "resin_admin_url": admin.URL, "resin_admin_token": "resin-admin-secret"}}})
	resinItems := decodeItems(resinResult)
	if resinItems[0].Proxy == nil {
		t.Fatal("结构化 Resin 未添加")
	}
	resinID := resinItems[0].Proxy.ID
	createdIDs = append(createdIDs, resinID)
	resinProbe := call("test_proxies", map[string]any{"proxy_ids": []int64{id64(resinID)}, "target_url": "http://probe.invalid/check"})
	if decodeItems(resinProbe)[0].OperationStatus != "succeeded" || adminHits.Load() != 3 || dataHits.Load() != 1 {
		t.Fatal("Resin 控制面或数据面未真实检测")
	}
	if strings.Contains(resinResult+resinProbe+logs.String(), "resin-admin-secret") || strings.Contains(resinResult+resinProbe+logs.String(), "resin-data-secret") {
		t.Fatal("Resin 令牌泄漏")
	}
	// HTTPS 和 SOCKS5 可登记；登记不尝试连接入口。
	for _, kind := range []string{"https", "socks5"} {
		key := uuid.NewString()
		keys = append(keys, key)
		added := decodeItems(call("add_proxies", map[string]any{"items": []any{map[string]any{"item_key": key, "name": kind, "kind": kind, "url": kind + "://user:synthetic-secret@127.0.0.1:1080"}}}))
		if added[0].Proxy == nil || added[0].Proxy.Status != "unknown" {
			t.Fatal("支持的协议未正确登记")
		}
		createdIDs = append(createdIDs, added[0].Proxy.ID)
	}
	// 限制、未知键、重复键和空值在装饰器前被拒绝。
	for _, raw := range []string{`{"proxy_ids":[1],"concurrency":0}`, `{"proxy_ids":[1],"concurrency":5}`, `{"proxy_ids":[1],"timeout_seconds":16}`, `{"proxy_ids":[1,1]}`, `{"proxy_ids":[1],"target_url":"http://u:secret@host/"}`} {
		r, _ := toolsByName["test_proxies"].Call(t.Context(), []byte(raw), nil)
		if !r.IsError || strings.Contains(r.Flatten(), "secret") {
			t.Fatal("检测非法参数未安全拒绝")
		}
	}
	for _, raw := range []string{`{"limit":1,"limit":2}`, `{"Limit":1}`, `{"limit":null}`} {
		r, _ := toolsByName["list_proxies"].Call(t.Context(), []byte(raw), nil)
		if !r.IsError {
			t.Fatal("实际工具接受歧义参数")
		}
	}
	oversize := map[string]any{"items": make([]any, 51)}
	largeRaw, _ := json.Marshal(oversize)
	if r, _ := toolsByName["add_proxies"].Call(t.Context(), largeRaw, nil); !r.IsError {
		t.Fatal("超限批次被接受")
	}
	if r, _ := toolsByName["add_proxies"].Call(t.Context(), []byte(strings.Repeat(" ", proxyToolInputLimit+1)), nil); !r.IsError {
		t.Fatal("超大请求被接受")
	}
	for _, raw := range []string{`{"items":[],"accept_terms":true}`, `{"items":[{"item_key":"x","name":"x","kind":"http","url":"http://host:80","username":"secret"}]}`, `{"items":null}`} {
		status, _, body := proxyAPI(t, mux, "POST", "/api/proxies/batch", raw)
		if status != 400 || strings.Contains(body, "secret") {
			t.Fatal("HTTP 批量参数未严格校验")
		}
	}
	denied := permission.Evaluate(t.Context(), permission.Request{ToolName: "add_proxies", Input: []byte(`{}`), ToolDecision: toolsByName["add_proxies"].CheckPermissions(t.Context(), nil, permission.Context{}), Ctx: permission.Context{Mode: permission.ModeAuto, Disallowed: []string{"add_proxies"}}})
	if denied.Behavior != permission.Deny {
		t.Fatal("Auto 覆盖了显式权限拒绝")
	}
	// 禁用/解绑在重启播种、旧迁移重跑后保持；已装配工具也立即拒绝调用。
	if _, err := pg.Exec(`UPDATE tools SET enabled=false WHERE key='add_proxies'; UPDATE tools SET agents='[]'::jsonb WHERE key='test_proxies'; DELETE FROM settings WHERE key='auto_default_bindings_v3'`); err != nil {
		t.Fatal(err)
	}
	s.seedOrchestrationTools()
	for _, name := range []string{"add_proxies", "test_proxies"} {
		tool := toolsByName[name]
		if tool.CheckPermissions(t.Context(), nil, permission.Context{}).Behavior != permission.Deny {
			t.Fatal("权限未拒绝")
		}
		r, _ := tool.Call(t.Context(), []byte(`{}`), nil)
		if !r.IsError {
			t.Fatal("旧会话越过禁用或解绑")
		}
	}
	// 恢复测试专属目录配置，避免下一次运行继承本轮禁用。
	if _, err := pg.Exec(`UPDATE tools SET enabled=true,agents='["auto"]'::jsonb WHERE key IN ('list_proxies','add_proxies','test_proxies')`); err != nil {
		t.Fatal(err)
	}
	if _, err := pg.Exec(`DELETE FROM tools WHERE key='list_proxies'`); err != nil {
		t.Fatal(err)
	}
	for _, tool := range agent.ToolResolve(t.Context(), "auto", s.proxyCenterTools()) {
		if tool.Name() == "list_proxies" {
			t.Fatal("缺失授权行仍放行")
		}
	}
	s.seedOrchestrationTools()
}

func TestProxyToolsProbeLimits(t *testing.T) {
	s, _ := proxyIntegration(t)
	var active, peak, hits atomic.Int32
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := active.Add(1)
		defer active.Add(-1)
		hits.Add(1)
		for old := peak.Load(); n > old; old = peak.Load() {
			if peak.CompareAndSwap(old, n) {
				break
			}
		}
		select {
		case <-r.Context().Done():
			return
		case <-time.After(100 * time.Millisecond):
			w.WriteHeader(200)
		}
	}))
	defer proxy.Close()
	var ids []int64
	for i := 0; i < 12; i++ {
		p, err := s.m.pg.CreateProxy(&db.Proxy{Name: "并发测试", Kind: db.ProxyKindHTTP, URL: proxy.URL, Enabled: true})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, p.ID)
	}
	t.Cleanup(func() {
		for _, id := range ids {
			s.m.pg.Exec(`DELETE FROM outbound_proxies WHERE id=$1`, id)
		}
	})
	var wg sync.WaitGroup
	concurrency := 4
	for _, batch := range [][]int64{ids[:6], ids[6:]} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, r := range s.testProxyItems(t.Context(), proxyTestInput{ProxyIDs: batch, Concurrency: &concurrency, TargetURL: "http://probe.invalid/check"}) {
				if r.OperationStatus != "succeeded" {
					t.Error("并发检测失败")
				}
			}
		}()
	}
	wg.Wait()
	if hits.Load() != 12 || peak.Load() > 4 || peak.Load() < 2 {
		t.Fatalf("进程并发上限无效: hits=%d peak=%d", hits.Load(), peak.Load())
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	before := hits.Load()
	if _, code := s.probeProxyCenter(ctx, ids[0], "http://probe.invalid/check"); code == "" || hits.Load() != before {
		t.Fatal("取消请求仍发起检测")
	}
	// 检测正在进行时编辑档案，旧检测结果不能覆盖新配置。
	row, err := s.m.pg.GetProxy(ids[0])
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.m.pg.Exec(`UPDATE outbound_proxies SET name='已编辑' WHERE id=$1`, row.ID); err != nil {
		t.Fatal(err)
	}
	if err = s.m.pg.SaveProxyProbe(t.Context(), row, db.ProxyStatusFailed, "代理检测失败", time.Now()); err == nil {
		t.Fatal("过期检测覆盖了已编辑档案")
	}
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer slow.Close()
	p, err := s.m.pg.CreateProxy(&db.Proxy{Name: "超时入口", Kind: db.ProxyKindHTTP, URL: slow.URL, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	ids = append(ids, p.ID)
	started := time.Now()
	seconds := 1
	result := s.testProxyItems(t.Context(), proxyTestInput{ProxyIDs: []int64{p.ID}, TimeoutSeconds: &seconds, TargetURL: "http://probe.invalid/check"})
	if time.Since(started) > 2500*time.Millisecond || result[0].Proxy.Status != "failed" || result[0].ErrorCode != "probe_failed" {
		t.Fatal("超时未终止网络并保存状态")
	}
}

func TestProxyToolsDatabaseFailureDenies(t *testing.T) {
	s, _ := proxyIntegration(t)
	old := agent.ToolResolve
	t.Cleanup(func() { agent.ToolResolve = old })
	wireTools(s.m.pg, nil)
	s.seedOrchestrationTools()
	resolved := agent.ToolResolve(t.Context(), "auto", s.proxyCenterTools())
	if err := s.m.pg.Close(); err != nil {
		t.Fatal(err)
	}
	for _, tool := range agent.ToolResolve(t.Context(), "auto", s.proxyCenterTools()) {
		if isProxyCenterTool(tool.Name()) {
			t.Fatal("读库失败仍装配代理工具")
		}
	}
	for _, tool := range resolved {
		if isProxyCenterTool(tool.Name()) {
			r, err := tool.Call(t.Context(), []byte(`{}`), nil)
			if err != nil || !r.IsError {
				t.Fatal("旧工具在读库失败时未拒绝")
			}
		}
	}
}
