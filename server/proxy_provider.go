package server

import (
	"context"
	"errors"
	"iter"
	"net"
	"net/url"
	"strconv"
	"strings"

	"github.com/Autumn-27/artex/agent"
	"github.com/Autumn-27/artex/db"
	"github.com/Autumn-27/norma/llm"
)

type proxyPolicyProvider struct {
	inner     llm.Provider
	server    *Server
	subject   db.ProxySubject
	revision  int64
	proxyID   *int64
	proxyURL  string
	closeIdle func()
}

func (s *Server) guardProxyProvider(task *Task, provider llm.Provider, proxy string, proxyID *int64, closeIdle func()) llm.Provider {
	if _, ok := provider.(proxyPolicyProvider); ok {
		return provider
	}
	subject, e := runtimeTaskSubject(task)
	if e != nil {
		return provider
	}
	return proxyPolicyProvider{inner: provider, server: s, subject: subject, revision: task.proxyState().Revision, proxyID: proxyID, proxyURL: proxy, closeIdle: closeIdle}
}

// 最终渠道出口独立计算档案引用，继承模式的工具快照不能冒充渠道实际出口。
func finalLLMProxyID(task *Task, channelURL, mode string, channelID *int64, resolved string) *int64 {
	if task == nil || resolved == "" {
		return nil
	}
	state := task.proxyState()
	if state.Mode == db.TaskProxyModeInherit {
		if mode == "fixed" {
			return channelID
		}
		if mode == "direct" || (mode == "" && channelURL != "") {
			return nil
		}
	}
	return state.ID
}

func runtimeTaskSubject(t *Task) (db.ProxySubject, error) {
	kind, id := "task", t.ID
	if strings.HasPrefix(id, "conv-") {
		kind, id = "conversation", strings.TrimPrefix(id, "conv-")
	}
	n, e := strconv.ParseInt(id, 10, 64)
	if e != nil || n <= 0 {
		return db.ProxySubject{}, db.ErrProxyAuthorization
	}
	return db.ProxySubject{Kind: kind, ID: n}, nil
}
func (p proxyPolicyProvider) admitted() (func(), error) {
	token, e := p.server.m.pg.AcquireProxyIOOperation(p.subject, p.revision, p.proxyID)
	if e != nil {
		return func() {}, e
	}
	return func() { _ = p.server.m.pg.ReleaseProxyIOOperation(p.subject, token) }, nil
}
func (p proxyPolicyProvider) Complete(ctx context.Context, req llm.CompletionRequest) (llm.Message, string, llm.Usage, error) {
	finish, e := p.admitted()
	if e != nil {
		if p.closeIdle != nil {
			p.closeIdle()
		}
		return llm.Message{}, "", llm.Usage{}, e
	}
	defer finish()
	if p.closeIdle != nil {
		defer p.closeIdle()
	}
	message, stop, usage, e := p.inner.Complete(ctx, req)
	p.server.recordInfrastructureFailure(ctx, p.subject, p.revision, p.proxyID, p.proxyURL, e)
	return message, stop, usage, e
}
func (p proxyPolicyProvider) Stream(ctx context.Context, req llm.CompletionRequest) iter.Seq2[llm.StreamEvent, error] {
	return func(yield func(llm.StreamEvent, error) bool) {
		finish, e := p.admitted()
		if e != nil {
			if p.closeIdle != nil {
				p.closeIdle()
			}
			yield(llm.StreamEvent{}, e)
			return
		}
		defer finish()
		if p.closeIdle != nil {
			defer p.closeIdle()
		}
		p.inner.Stream(ctx, req)(func(event llm.StreamEvent, e error) bool {
			p.server.recordInfrastructureFailure(ctx, p.subject, p.revision, p.proxyID, p.proxyURL, e)
			return yield(event, e)
		})
	}
}

func infrastructureProxyFailure(proxyURL string, err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	u, e := url.Parse(proxyURL)
	if e != nil || u.Host == "" {
		return false
	}
	var op *net.OpError
	if !errors.As(err, &op) {
		return false
	}
	if op.Op == "proxyconnect" {
		return true
	}
	if op.Op != "dial" || op.Addr == nil {
		return false
	}
	host, port, e := net.SplitHostPort(op.Addr.String())
	if e != nil {
		return false
	}
	proxyPort := u.Port()
	if proxyPort == "" {
		if u.Scheme == "https" {
			proxyPort = "443"
		} else {
			proxyPort = "80"
		}
	}
	return net.ParseIP(u.Hostname()) != nil && host == u.Hostname() && port == proxyPort
}
func (s *Server) recordInfrastructureFailure(ctx context.Context, subject db.ProxySubject, revision int64, proxyID *int64, proxyURL string, err error) {
	s.m.recordInfrastructureFailure(ctx, subject, revision, proxyID, proxyURL, err)
}

func (m *Manager) recordInfrastructureFailure(ctx context.Context, subject db.ProxySubject, revision int64, proxyID *int64, proxyURL string, err error) {
	if proxyID == nil || !infrastructureProxyFailure(proxyURL, err) {
		return
	}
	proof, e := m.pg.RecordProxyFailureEvidence(subject, revision, *proxyID, "connection_failure")
	if e != nil {
		return
	}
	p, e := m.pg.GetProxyPolicy(subject)
	if e != nil || p.State.Mode != db.TaskProxyModeGroup || !p.AgentFailoverEnabled {
		return
	}
	_, e = m.pg.RequestProxyFailover(subject, db.ProxyFailoverInput{ExpectedRevision: revision, Reason: "正式运行器确认代理连接失败", Evidence: db.ProxyFailureEvidence{Kind: "connection_failure", Reference: proof}})
	if e == nil {
		agent.RequestProxyRunStop(ctx)
		m.wakeProxyFailover()
	}
}
