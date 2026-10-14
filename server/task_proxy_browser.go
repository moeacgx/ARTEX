package server

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/Autumn-27/artex/agent"
	"github.com/Autumn-27/artex/db"
	"github.com/Autumn-27/artex/egress"
	browserruntime "github.com/Autumn-27/artex/runtime/playwright"
)

// TaskMCPConfig 只克隆本次运行配置，绝不保存到共享 MCP 记录。
// 共享 assembly 在 connectMCP 前调用；失败时该运行不可启动浏览器。
func (m *Manager) TaskMCPConfig(ctx context.Context, original *db.MCPServer) (*db.MCPServer, error) {
	if original == nil || original.Name != browserMCPName {
		return original, nil
	}
	id := agent.RunInfoFrom(ctx).TaskID
	formal, hasFormal := ctx.Value(proxySubjectContextKey{}).(admittedProxySubject)
	if id <= 0 && (!hasFormal || formal.Subject.Kind != "conversation") {
		return original, nil
	}
	if original.Transport != "stdio" {
		return nil, errors.New("任务浏览器要求独立 stdio 运行时")
	}
	var task *Task
	if hasFormal {
		p, err := m.pg.GetProxyPolicy(formal.Subject)
		if err != nil {
			return nil, err
		}
		task = proxySubjectRuntimeTask(p)
		id = formal.Subject.ID
		if formal.Subject.Kind == "conversation" {
			id = -id
		}
	} else {
		row, err := m.pg.GetTask(id)
		if err != nil {
			return nil, err
		}
		task = &Task{ID: strconv.FormatInt(id, 10), ProxyMode: row.ProxyMode, ProxyID: row.ProxyID, ProxySnapshot: row.ProxySnapshot}
	}
	addr, ca, err := m.TaskToolProxy(ctx, task)
	if err != nil {
		return nil, err
	}
	if addr != "" {
		u, err := egress.ProxyURL(addr)
		if err != nil {
			return nil, err
		}
		if u.Scheme == "socks5" || u.Scheme == "https" {
			rt, err := m.proxyRuntime()
			if err != nil {
				return nil, err
			}
			rt.mu.Lock()
			bridge := rt.bridges[task.ID]
			if bridge == nil {
				bridge, err = egress.NewHTTPBridge(addr)
				if err == nil {
					rt.bridges[task.ID] = bridge
				}
			}
			rt.mu.Unlock()
			if err != nil {
				return nil, err
			}
			addr = bridge.ProxyAddr()
		}
	}
	copy, err := taskBrowserConfig(m.dir, id, original, addr, ca)
	if err == nil {
		args := jsonStrSlice(copy.Args)
		path := args[len(args)-1]
		context.AfterFunc(ctx, func() { _ = os.Remove(path) })
	}
	return copy, err
}

func taskBrowserConfig(dir string, id int64, original *db.MCPServer, addr, ca string) (*db.MCPServer, error) {
	copy := *original
	args := stripProxyArgs(jsonStrSlice(original.Args))
	env := jsonStrMap(original.Env)
	for _, key := range []string{"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy", "NO_PROXY", "no_proxy"} {
		env[key] = ""
	}
	delete(env, "NODE_EXTRA_CA_CERTS")
	config := map[string]any{}
	var filtered []string
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--proxy-bypass" {
			i++
			continue
		}
		if strings.HasPrefix(arg, "--proxy-bypass=") {
			continue
		}
		if arg == "--config" || strings.HasPrefix(arg, "--config=") {
			path := strings.TrimPrefix(arg, "--config=")
			if arg == "--config" {
				i++
				if i >= len(args) {
					return nil, errors.New("浏览器配置参数无效")
				}
				path = args[i]
			}
			b, err := os.ReadFile(path)
			if err != nil || json.Unmarshal(b, &config) != nil {
				return nil, errors.New("浏览器配置不可读")
			}
			continue
		}
		filtered = append(filtered, arg)
	}
	object := func(parent map[string]any, key string) map[string]any {
		v, ok := parent[key].(map[string]any)
		if !ok {
			v = map[string]any{}
			parent[key] = v
		}
		return v
	}
	browser := object(config, "browser")
	launch := object(browser, "launchOptions")
	if os.Getenv("ARTEX_PLAYWRIGHT_ROOT") != "" {
		executable, err := browserruntime.Executable(context.Background())
		if err != nil {
			return nil, err
		}
		browser["browserName"] = "chromium"
		launch["executablePath"] = executable
		launch["headless"] = true
		if os.Geteuid() == 0 {
			launch["chromiumSandbox"] = false
		}
		delete(launch, "channel")
	}
	delete(launch, "proxy")
	delete(object(browser, "contextOptions"), "proxy")
	delete(object(browser, "contextOptions"), "ignoreHTTPSErrors")
	var launchArgs []string
	if old, ok := launch["args"].([]any); ok {
		for _, v := range old {
			if arg, ok := v.(string); ok && !strings.HasPrefix(arg, "--proxy-") && arg != "--no-proxy-server" {
				launchArgs = append(launchArgs, arg)
			}
		}
	}
	if addr == "" {
		launchArgs = append(launchArgs, "--no-proxy-server")
	} else {
		launchArgs = append(launchArgs, "--proxy-bypass-list=<-loopback>")
	}
	launch["args"] = launchArgs
	if addr != "" {
		u, err := egress.ProxyURL(addr)
		if err != nil {
			return nil, err
		}
		proxy := map[string]any{"server": u.Scheme + "://" + u.Host, "bypass": "<-loopback>"}
		if u.User != nil {
			proxy["username"] = u.User.Username()
			pw, _ := u.User.Password()
			proxy["password"] = pw
		}
		launch["proxy"] = proxy
		if ca != "" {
			env["NODE_EXTRA_CA_CERTS"] = ca
			// Chromium 不读取 Node 的 CA 环境；仅录制链路允许 MITM 重签证书。
			object(browser, "contextOptions")["ignoreHTTPSErrors"] = true
		}
	}
	configDir := filepath.Join(dir, "egress", "browser")
	if err := os.MkdirAll(configDir, 0700); err != nil {
		return nil, err
	}
	b, err := json.Marshal(config)
	if err != nil {
		return nil, err
	}
	f, err := os.CreateTemp(configDir, "task_"+strconv.FormatInt(id, 10)+"-*.json")
	if err != nil {
		return nil, err
	}
	path := f.Name()
	if _, err = f.Write(b); err != nil {
		f.Close()
		os.Remove(path)
		return nil, err
	}
	if err = f.Close(); err != nil {
		os.Remove(path)
		return nil, err
	}
	filtered = append(filtered, "--isolated", "--config", path)
	copy.Args, _ = json.Marshal(filtered)
	copy.Env, _ = json.Marshal(env)
	return &copy, nil
}
