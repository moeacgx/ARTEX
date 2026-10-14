package server

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/Autumn-27/artex/agent"
	"github.com/Autumn-27/artex/db"
	"github.com/Autumn-27/artex/mcphttp"
	"github.com/Autumn-27/norma/mcp"
	actool "github.com/Autumn-27/norma/tool"
)

// mcpClient is the shared surface of a connected MCP server (stdio, Streamable HTTP,
// or legacy SSE),
// so tools/list and cleanup are handled uniformly regardless of transport.
type mcpClient interface {
	Tools(context.Context) ([]actool.CoreTool, error)
	Close() error
}

// connectMCP dials one MCP server per its transport. Callers must Close the client.
func connectMCP(ctx context.Context, m *db.MCPServer) (mcpClient, error) {
	if formal, ok := ctx.Value(proxySubjectContextKey{}).(admittedProxySubject); ok {
		policy, err := formal.Manager.pg.GetProxyPolicy(formal.Subject)
		if err != nil {
			return nil, err
		}
		if err = formal.Manager.pg.ValidateProxyPolicy(formal.Subject); err != nil {
			return nil, err
		}
		if m.Transport == "stdio" && m.Name != browserMCPName {
			addr, ca, err := formal.Manager.TaskToolProxy(ctx, proxySubjectRuntimeTask(policy))
			if err != nil {
				return nil, err
			}
			copy := *m
			env := jsonStrMap(m.Env)
			for _, entry := range agent.ProxyEnvironment(addr, ca) {
				key, value, _ := strings.Cut(entry, "=")
				env[key] = value
			}
			copy.Env, _ = json.Marshal(env)
			m = &copy
		} else if m.Transport == "http" || m.Transport == "sse" {
			s := &Server{m: formal.Manager}
			transport, err := s.taskHTTPTransport(ctx, httpExec{}, nil)
			if err != nil {
				return nil, err
			}
			if m.Insecure {
				if transport.TLSClientConfig == nil {
					transport.TLSClientConfig = &tls.Config{}
				}
				transport.TLSClientConfig.InsecureSkipVerify = true
			}
			proxy := ""
			if transport.Proxy != nil {
				request, err := http.NewRequest(http.MethodGet, m.URL, nil)
				if err != nil {
					return nil, err
				}
				selected, err := transport.Proxy(request)
				if err != nil {
					return nil, err
				}
				if selected != nil {
					proxy = selected.String()
				}
			}
			ctx = mcphttp.WithHTTPClient(ctx, &http.Client{Transport: proxyObservedTransport{base: transport, ctx: ctx, proxy: proxy}, Timeout: 120 * time.Second})
		}
	}
	switch m.Transport {
	case "stdio":
		if m.Command == "" {
			return nil, fmt.Errorf("stdio 传输缺少命令")
		}
		return mcp.NewStdioClient(ctx, m.Name, m.Command, jsonStrMap(m.Env), jsonStrSlice(m.Args)...)
	case "http":
		if m.URL == "" {
			return nil, fmt.Errorf("http 传输缺少 URL")
		}
		// env map doubles as HTTP headers (e.g. Authorization).
		return mcphttp.New(ctx, m.Name, m.URL, jsonStrMap(m.Env), m.Insecure)
	case "sse":
		if m.URL == "" {
			return nil, fmt.Errorf("sse 传输缺少 URL")
		}
		return mcphttp.NewSSE(ctx, m.Name, m.URL, jsonStrMap(m.Env), m.Insecure)
	default:
		return nil, fmt.Errorf("未知传输方式 %q", m.Transport)
	}
}

// discoverAndCacheMCP connects to one MCP, lists its tools, and persists the tool
// names to mcp_tools_cache so the UI shows them without a live connection.
func (s *Server) discoverAndCacheMCP(ctx context.Context, m *db.MCPServer) error {
	cl, err := connectMCP(ctx, m)
	if err != nil {
		return err
	}
	defer cl.Close()
	ts, err := cl.Tools(ctx)
	if err != nil {
		return err
	}
	tools := make([]db.MCPTool, 0, len(ts))
	for _, t := range ts {
		tools = append(tools, db.MCPTool{Name: t.Name(), Description: t.Description()})
	}
	if err := s.m.pg.SaveMCPTools(m.ID, tools); err != nil {
		return err
	}
	log.Printf("[mcp] %s 发现 %d 个工具并已缓存", m.Name, len(tools))
	return nil
}

// discoverEmptyMCPsOnStartup fills the tool cache for any enabled MCP that has none
// yet (notably the seeded browser MCP on first run). Runs sequentially in one
// goroutine so we never spawn many stdio servers (npx) at once, and never blocks
// startup. Best-effort: a failure leaves the cache empty to retry next start.
func (s *Server) discoverEmptyMCPsOnStartup() {
	servers, err := s.m.pg.ListMCP()
	if err != nil {
		log.Printf("[mcp] 启动自动发现: 读取列表失败: %v", err)
		return
	}
	for _, m := range servers {
		if !m.Enabled || len(m.Tools) > 0 {
			continue
		}
		ctx, cancel := context.WithTimeout(s.ctx, 90*time.Second)
		if err := s.discoverAndCacheMCP(ctx, m); err != nil {
			log.Printf("[mcp] 启动自动发现 %s 失败: %v", m.Name, err)
		}
		cancel()
	}
}
