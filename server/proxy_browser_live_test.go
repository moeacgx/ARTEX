//go:build proxybrowser

package server

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Autumn-27/artex/agent"
	"github.com/Autumn-27/artex/db"
)

// 使用显式测试标签运行真实 Chromium，不给没有浏览器的普通 Go 环境添加依赖。
func TestProxyPGBrowserChromium(t *testing.T) {
	s, _ := proxyIntegration(t)
	module, executable := os.Getenv("ARTEX_TEST_PLAYWRIGHT_MODULE"), os.Getenv("ARTEX_TEST_CHROMIUM")
	if module == "" || executable == "" {
		t.Fatal("真实浏览器测试必须配置本地 Playwright 模块和 Chromium 路径")
	}
	var direct atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { direct.Add(1); fmt.Fprint(w, "DIRECT") }))
	defer target.Close()
	proxy := func(label string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			want := "Basic " + base64.StdEncoding.EncodeToString([]byte("user:secret"))
			if r.Header.Get("Proxy-Authorization") != want {
				w.Header().Set("Proxy-Authenticate", `Basic realm="task"`)
				w.WriteHeader(407)
				return
			}
			if r.Method == "CONNECT" {
				w.WriteHeader(502)
				return
			}
			w.Header().Set("Content-Type", "text/html")
			fmt.Fprint(w, "<html><body>"+label+"</body></html>")
		}))
	}
	pa, pb := proxy("BROWSER_A"), proxy("BROWSER_B")
	defer pa.Close()
	defer pb.Close()
	var paths []string
	for _, upstream := range []*httptest.Server{pa, pb} {
		u, _ := url.Parse(upstream.URL)
		u.User = url.UserPassword("user", "secret")
		p := createRuntimeProxy(t, s, u.String())
		task := createRuntimeTask(t, s, db.TaskProxyModeFixed, &p.ID)
		id, _ := strconv.ParseInt(task.ID, 10, 64)
		ctx := agent.WithRunInfo(t.Context(), agent.RunInfo{TaskID: id})
		config, err := s.m.TaskMCPConfig(ctx, &db.MCPServer{Name: "browser", Transport: "stdio", Command: "local-browser", Args: json.RawMessage(`[]`), Env: json.RawMessage(`{}`)})
		if err != nil {
			t.Fatal(err)
		}
		args := jsonStrSlice(config.Args)
		paths = append(paths, args[len(args)-1])
	}
	script := `const fs=require('fs');const {chromium}=require(process.env.ARTEX_TEST_PLAYWRIGHT_MODULE);const input=JSON.parse(fs.readFileSync(0,'utf8'));(async()=>{const browsers=[];try{const results=await Promise.all(input.paths.map(async(path)=>{const c=JSON.parse(fs.readFileSync(path,'utf8')).browser;const b=await chromium.launch({...c.launchOptions,executablePath:process.env.ARTEX_TEST_CHROMIUM,headless:true,args:[...c.launchOptions.args,'--disable-background-networking','--disable-component-update']});browsers.push(b);const context=await b.newContext(c.contextOptions);const page=await context.newPage();try{await page.goto(input.target,{timeout:5000});return await page.textContent('body')}catch{if(input.allowFailure)return 'FAILED';throw Error('navigation failed')}}));process.stdout.write(JSON.stringify(results))}finally{await Promise.all(browsers.map(b=>b.close()))}})().catch(()=>{process.stderr.write('browser proxy validation failed');process.exitCode=1})`
	for _, failure := range []bool{false, true} {
		if failure {
			pa.Close()
		}
		input, _ := json.Marshal(map[string]any{"paths": paths, "target": target.URL, "allowFailure": failure})
		command := exec.CommandContext(t.Context(), "node", "-e", script)
		command.Stdin = strings.NewReader(string(input))
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("真实Chromium测试失败: %s %v", output, err)
		}
		want := `["BROWSER_A","BROWSER_B"]`
		if failure {
			want = `["FAILED","BROWSER_B"]`
		}
		if string(output) != want || direct.Load() != 0 {
			t.Fatalf("浏览器串线或直连: %s direct=%d", output, direct.Load())
		}
		t.Logf("双 Chromium failure=%t results=%s direct=%d", failure, output, direct.Load())
	}
}

func TestProxyPGGroupBrowserChromiumAtoBtoDirect(t *testing.T) {
	s, _, task := switchFixture(t)
	module, executable := os.Getenv("ARTEX_TEST_PLAYWRIGHT_MODULE"), os.Getenv("ARTEX_TEST_CHROMIUM")
	if module == "" || executable == "" {
		t.Fatal("真实浏览器测试必须配置本地 Playwright 模块和 Chromium 路径")
	}
	var direct atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		direct.Add(1)
		fmt.Fprint(w, "DIRECT")
	}))
	defer target.Close()
	proxy := func(label string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			want := "Basic " + base64.StdEncoding.EncodeToString([]byte("user:secret"))
			if r.Header.Get("Proxy-Authorization") != want {
				w.Header().Set("Proxy-Authenticate", `Basic realm="task"`)
				w.WriteHeader(http.StatusProxyAuthRequired)
				return
			}
			w.Header().Set("Content-Type", "text/html")
			fmt.Fprint(w, "<html><body>"+label+"</body></html>")
		}))
	}
	upstreamA, upstreamB := proxy("BROWSER_A"), proxy("BROWSER_B")
	defer upstreamA.Close()
	defer upstreamB.Close()
	create := func(upstream *httptest.Server) *db.Proxy {
		u, err := url.Parse(upstream.URL)
		if err != nil {
			t.Fatal(err)
		}
		u.User = url.UserPassword("user", "secret")
		return createRuntimeProxy(t, s, u.String())
	}
	proxyA, proxyB := create(upstreamA), create(upstreamB)
	group, err := s.m.pg.CreateProxyGroup("浏览器故障切换组", true)
	if err != nil {
		t.Fatal(err)
	}
	group, err = s.m.pg.SetProxyGroupMembers(group.ID, group.Revision, []int64{proxyA.ID, proxyB.ID})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = s.m.pg.Exec(`DELETE FROM proxy_policy_groups WHERE policy_id IN (SELECT id FROM proxy_subject_policies WHERE task_id=$1)`, switchTaskID(t, task))
		_, _ = s.m.pg.Exec(`DELETE FROM proxy_group_members WHERE group_id=$1`, group.ID)
		_, _ = s.m.pg.Exec(`DELETE FROM proxy_groups WHERE id=$1`, group.ID)
	})
	policy := saveGroupPolicy(t, s, task, group, true, 2)
	subject := policy.Subject
	s.m.closeProxyFailover()
	waitCleanup := func() {
		t.Helper()
		deadline := time.NewTimer(5 * time.Second)
		defer deadline.Stop()
		barrier := s.m.taskProxyBarrier(task.ID)
		for {
			barrier.mu.Lock()
			if !barrier.pending && !barrier.releasing {
				barrier.mu.Unlock()
				return
			}
			changed := barrier.changed
			barrier.mu.Unlock()
			select {
			case <-changed:
			case <-deadline.C:
				t.Fatal("浏览器真实退出后的资源清理未完成")
			}
		}
	}

	browse := func(want string) {
		t.Helper()
		ctx, finish, admitted := s.engine.beginTaskOperationContext(t.Context(), task.ID)
		if !admitted {
			t.Fatal("浏览器正式操作准入失败")
		}
		var finishOnce sync.Once
		finishOperation := func() { finishOnce.Do(finish) }
		defer finishOperation()
		config, err := s.m.TaskMCPConfig(ctx, &db.MCPServer{Name: "browser", Transport: "stdio", Command: "local-browser", Args: json.RawMessage(`[]`), Env: json.RawMessage(`{}`)})
		if err != nil {
			t.Fatal(err)
		}
		args := jsonStrSlice(config.Args)
		path := args[len(args)-1]
		script := `const fs=require('fs');const {chromium}=require(process.env.ARTEX_TEST_PLAYWRIGHT_MODULE);const input=JSON.parse(fs.readFileSync(0,'utf8'));(async()=>{const c=JSON.parse(fs.readFileSync(input.path,'utf8')).browser;const b=await chromium.launch({...c.launchOptions,executablePath:process.env.ARTEX_TEST_CHROMIUM,headless:true,args:[...c.launchOptions.args,'--disable-background-networking','--disable-component-update']});try{const context=await b.newContext(c.contextOptions);const page=await context.newPage();await page.goto(input.target,{timeout:5000});process.stdout.write(await page.textContent('body'))}finally{await b.close()}})().catch(()=>{process.stderr.write('browser proxy validation failed');process.exitCode=1})`
		input, _ := json.Marshal(map[string]any{"path": path, "target": target.URL})
		command := exec.CommandContext(ctx, "node", "-e", script)
		command.Stdin = strings.NewReader(string(input))
		output, err := command.CombinedOutput()
		if err != nil || string(output) != want {
			t.Fatalf("真实 Chromium 出口 want=%s got=%s err=%v", want, output, err)
		}
		finishOperation()
		waitCleanup()
	}

	browse("BROWSER_A")
	if direct.Load() != 0 {
		t.Fatalf("A 阶段错误直连，命中=%d", direct.Load())
	}
	proof, err := s.m.pg.RecordProxyFailureEvidence(subject, policy.State.Revision, proxyA.ID, "connection_failure")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.m.pg.RequestProxyFailover(subject, db.ProxyFailoverInput{ExpectedRevision: policy.State.Revision, Reason: "合成浏览器出口 A 连接失败", Evidence: db.ProxyFailureEvidence{Kind: "connection_failure", Reference: proof}}); err != nil {
		t.Fatal(err)
	}
	if err = s.m.applyProxyFailover(t.Context(), subject); err != nil {
		t.Fatal(err)
	}
	policy, err = s.m.pg.GetProxyPolicy(subject)
	if err != nil || policy.State.ID == nil || *policy.State.ID != proxyB.ID {
		t.Fatalf("故障切换未落到 B: %+v %v", policy.State, err)
	}
	browse("BROWSER_B")
	if direct.Load() != 0 {
		t.Fatalf("B 阶段错误直连，命中=%d", direct.Load())
	}
	policy, err = s.m.setProxyPolicy(t.Context(), subject, policy.State.Revision, db.ProxyPolicyPatch{Mode: db.TaskProxyModeDirect})
	if err != nil || policy.State.Mode != db.TaskProxyModeDirect || policy.State.ID != nil {
		t.Fatalf("人工切换 direct 失败: %+v %v", policy.State, err)
	}
	browse("DIRECT")
	if direct.Load() < 1 {
		t.Fatalf("direct 阶段未命中目标")
	}
}
