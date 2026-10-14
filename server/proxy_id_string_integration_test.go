package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/Autumn-27/artex/db"
)

const proxyJSONUnsafeID int64 = 9007199254740993

// 代理中心 ID 必须在 JSON 边界保持字符串；该值故意超过 JavaScript 安全整数上限。
func TestProxyCenterIDsRemainExactAcrossDTOsAndPaths(t *testing.T) {
	s, mux := proxyIntegration(t)
	mux.HandleFunc("GET /api/proxies", s.listProxies)
	s.registerProxyGroupRoutes(mux)
	const big = proxyJSONUnsafeID
	const bigText = "9007199254740993"
	if _, err := s.m.pg.Exec(`INSERT INTO outbound_proxies(id,name,kind,protocol,url,enabled,status) VALUES($1,'大 ID HTTP','http','http','',true,'healthy'),($2,'大 ID WARP','warp','socks5','',true,'stopped')`, big, big+1); err != nil {
		t.Fatal(err)
	}
	if _, err := s.m.pg.Exec(`INSERT INTO warp_proxy_health(proxy_id,state,material) VALUES($1,'stopped','unknown')`, big+1); err != nil {
		t.Fatal(err)
	}
	group, err := s.m.pg.CreateProxyGroup("大 ID 代理组", true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.m.pg.SetProxyGroupMembers(group.ID, group.Revision, []int64{big}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = s.m.pg.Exec(`DELETE FROM proxy_group_members WHERE group_id=$1`, group.ID)
		_, _ = s.m.pg.Exec(`DELETE FROM proxy_groups WHERE id=$1`, group.ID)
		_, _ = s.m.pg.Exec(`DELETE FROM outbound_proxies WHERE id IN ($1,$2)`, big, big+1)
	})

	status, _, body := proxyAPI(t, mux, http.MethodGet, "/api/proxies", "")
	if status != http.StatusOK || !strings.Contains(body, `"id":"`+bigText+`"`) {
		t.Fatalf("代理列表未以字符串保留大 ID: status=%d body=%s", status, body)
	}
	status, _, body = proxyAPI(t, mux, http.MethodGet, "/api/proxies/"+bigText, "")
	if status != http.StatusOK || !strings.Contains(body, `"id":"`+bigText+`"`) || strings.Contains(body, "9007199254740992") {
		t.Fatalf("代理 GET 发生舍入: status=%d body=%s", status, body)
	}

	status, _, body = proxyAPI(t, mux, http.MethodGet, "/api/proxy-groups/"+fmt.Sprint(group.ID), "")
	if status != http.StatusOK || !strings.Contains(body, `"proxy_id":"`+bigText+`"`) || !strings.Contains(body, `"proxy":{"warp_health"`) && !strings.Contains(body, `"proxy":{"`) {
		t.Fatalf("分组成员或嵌入 ProxyDTO 未保留大 ID: status=%d body=%s", status, body)
	}
	var groupBody struct {
		Members []struct {
			ProxyID string    `json:"proxy_id"`
			Proxy   *ProxyDTO `json:"proxy"`
		} `json:"members"`
	}
	if err := json.Unmarshal([]byte(body), &groupBody); err != nil {
		t.Fatal(err)
	}
	if len(groupBody.Members) != 1 || groupBody.Members[0].ProxyID != bigText || groupBody.Members[0].Proxy == nil || groupBody.Members[0].Proxy.ID != bigText {
		t.Fatalf("分组成员与嵌入代理编号不一致: %+v", groupBody.Members)
	}

	status, _, body = proxyAPI(t, mux, http.MethodGet, "/api/proxies/"+fmt.Sprint(big+1)+"/health-policy", "")
	if status != http.StatusOK || !strings.Contains(body, `"proxy_id":"9007199254740994"`) {
		t.Fatalf("WARP 健康策略编号不是字符串: status=%d body=%s", status, body)
	}
	status, _, body = proxyAPI(t, mux, http.MethodPost, "/api/proxies/"+fmt.Sprint(big+1)+"/warp/start", `{}`)
	if status == http.StatusOK || !strings.Contains(body, `"proxy_id":"9007199254740994"`) || strings.Contains(body, "9007199254740992") {
		t.Fatalf("WARP action 错误响应未保留字符串编号: status=%d body=%s", status, body)
	}

	legacy := createRuntimeProxy(t, s, "http://127.0.0.1:1")
	for _, payload := range []string{fmt.Sprintf(`{"proxy_id":%d}`, legacy.ID), fmt.Sprintf(`{"proxy_id":"%d"}`, legacy.ID)} {
		status, out, raw := proxyAPI(t, mux, http.MethodPost, "/api/proxies/global", payload)
		if status != http.StatusOK || out["global_proxy_id"] != i64s(legacy.ID) {
			t.Fatalf("global numeric/string 输入兼容失败: status=%d body=%s", status, raw)
		}
	}
	if _, _, raw := proxyAPI(t, mux, http.MethodPost, "/api/proxies/global", `{"proxy_id":null}`); raw == "" {
		t.Fatal("global 清除响应为空")
	}
}

// TaskDTO 的 proxy_id 保留历史数字形态；主体策略 GET 才是大 ID 的权威字符串回读。
func TestTaskProxyStringIDRoundTripBeyondJavaScriptSafeInteger(t *testing.T) {
	s, mux := proxyIntegration(t)
	s.registerTaskProxyRoutes(mux)
	s.registerProxyGroupRoutes(mux)
	const big = proxyJSONUnsafeID
	const bigText = "9007199254740993"
	created, err := s.m.pg.CreateProxy(&db.Proxy{Name: "任务大 ID", Kind: db.ProxyKindHTTP, URL: "http://127.0.0.1:1", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.m.pg.Exec(`UPDATE outbound_proxies SET id=$2,status='healthy' WHERE id=$1`, created.ID, big); err != nil {
		t.Fatal(err)
	}
	row, err := s.m.pg.CreateTaskWithOptions("任务大 ID 回归", "固定代理精度", db.TaskCreateOptions{ProxyMode: db.TaskProxyModeDirect})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.m.pg.Exec(`UPDATE tasks SET status='done' WHERE id=$1`, row.ID); err != nil {
		t.Fatal(err)
	}
	s.m.tasks = map[string]*Task{fmt.Sprint(row.ID): {ID: fmt.Sprint(row.ID), Status: "done", ProxyMode: db.TaskProxyModeDirect}}
	t.Cleanup(func() {
		_, _ = s.m.pg.Exec(`DELETE FROM tasks WHERE id=$1`, row.ID)
		_, _ = s.m.pg.Exec(`DELETE FROM explorations WHERE id=$1`, row.ExplorationID)
		_, _ = s.m.pg.Exec(`DELETE FROM outbound_proxies WHERE id=$1`, big)
	})
	path := "/api/tasks/" + fmt.Sprint(row.ID) + "/proxy"
	put := func(payload string) string {
		status, _, raw := proxyAPI(t, mux, http.MethodPut, path, payload)
		if status != http.StatusOK {
			t.Fatalf("任务代理保存失败: status=%d body=%s", status, raw)
		}
		return raw
	}
	put(fmt.Sprintf(`{"proxy_mode":"fixed","proxy_id":"%s","expected_revision":0}`, bigText))
	status, _, raw := proxyAPI(t, mux, http.MethodGet, path, "")
	if status != http.StatusOK {
		t.Fatalf("任务主体策略读取失败: status=%d body=%s", status, raw)
	}
	var policy struct {
		ProxyMode     string  `json:"proxy_mode"`
		ProxyID       *string `json:"proxy_id"`
		ProxyRevision int64   `json:"proxy_revision"`
	}
	if err := json.Unmarshal([]byte(raw), &policy); err != nil {
		t.Fatal(err)
	}
	if policy.ProxyMode != "fixed" || policy.ProxyID == nil || *policy.ProxyID != bigText || policy.ProxyRevision != 1 {
		t.Fatalf("主体策略未精确回读大 ID: %+v body=%s", policy, raw)
	}
	put(`{"proxy_mode":"direct","proxy_id":null,"expected_revision":1}`)
	put(fmt.Sprintf(`{"proxy_mode":"fixed","proxy_id":%d,"expected_revision":2}`, big))
	status, _, raw = proxyAPI(t, mux, http.MethodGet, path, "")
	if status != http.StatusOK || !strings.Contains(raw, `"proxy_id":"`+bigText+`"`) || strings.Contains(raw, "9007199254740992") {
		t.Fatalf("历史数字输入后的主体 GET 发生舍入: status=%d body=%s", status, raw)
	}
}
