package server

import (
	"context"
	"iter"
	"path/filepath"
	"sync/atomic"

	"github.com/Autumn-27/artex/agent"
	"github.com/Autumn-27/artex/db"
	"github.com/Autumn-27/norma/llm"
	"github.com/Autumn-27/norma/transcript"
)

// 每次正式模型调用重新读取本对话策略与渠道，不共享可变 ChatAgent 或工具出口。
type conversationPolicyProvider struct {
	s        *Server
	subject  db.ProxySubject
	agentKey string
}

func (p conversationPolicyProvider) resolve(ctx context.Context) (llm.Provider, agent.Config, error) {
	policy, e := p.s.m.pg.GetProxyPolicy(p.subject)
	if e != nil {
		return nil, agent.Config{}, e
	}
	if e = p.s.m.pg.ValidateProxyPolicy(p.subject); e != nil {
		return nil, agent.Config{}, e
	}
	c, e := p.s.m.pg.GetConversation(p.subject.ID)
	if e != nil || c == nil {
		return nil, agent.Config{}, db.ErrProxyUnavailable
	}
	var selection *db.LLMModelSelection
	if a, e := p.s.m.pg.GetAgentByKey(p.agentKey); e != nil {
		return nil, agent.Config{}, e
	} else if a != nil && a.LLMProfileID != nil {
		selection = &db.LLMModelSelection{ProfileID: *a.LLMProfileID, ModelMode: a.LLMModelMode, ModelID: a.LLMModelID}
	}
	if selection == nil && c.LLMProfileID != nil {
		selection = &db.LLMModelSelection{ProfileID: *c.LLMProfileID, ModelMode: c.LLMModelMode, ModelID: c.LLMModelID}
	}
	task := proxySubjectRuntimeTask(policy)
	if selection != nil {
		prov, cfg, ok := p.s.providerForTaskSelection(*selection, task, ctx)
		if !ok {
			return nil, cfg, db.ErrLLMModelUnavailable
		}
		return p.s.poolForBinding(selection.ProfileID, prov, cfg, task), cfg, nil
	}
	prov, cfg, ok := p.s.globalProviderContext(ctx, task)
	if !ok {
		return nil, cfg, db.ErrLLMModelUnavailable
	}
	return prov, cfg, nil
}
func (p conversationPolicyProvider) Complete(ctx context.Context, req llm.CompletionRequest) (llm.Message, string, llm.Usage, error) {
	prov, _, e := p.resolve(ctx)
	if e != nil {
		return llm.Message{}, "", llm.Usage{}, e
	}
	return prov.Complete(ctx, req)
}
func (p conversationPolicyProvider) Stream(ctx context.Context, req llm.CompletionRequest) iter.Seq2[llm.StreamEvent, error] {
	return func(yield func(llm.StreamEvent, error) bool) {
		prov, _, e := p.resolve(ctx)
		if e != nil {
			yield(llm.StreamEvent{}, e)
			return
		}
		prov.Stream(ctx, req)(yield)
	}
}

func (s *Server) conversationChatAgent(ctx context.Context, c *db.Conversation, p db.ProxyPolicy) (*agent.ChatAgent, error) {
	provider := conversationPolicyProvider{s: s, subject: p.Subject, agentKey: c.AgentKey}
	_, cfg, e := provider.resolve(ctx)
	if e != nil {
		return nil, e
	}
	addr, ca, e := s.m.TaskToolProxy(ctx, proxySubjectRuntimeTask(p))
	if e != nil {
		return nil, e
	}
	chat := agent.NewChatAgent(provider, cfg.Model, s.m.dir, transcript.NewStore(filepath.Join(s.m.dir, "transcripts")), cfg.CompactionWindow())
	chat.SetProxy(addr, ca)
	chat.SetStrictProxy(true)
	ws := s.m.WebSearchOpts()
	if p.State.Mode == db.TaskProxyModeGroup || p.State.Mode == db.TaskProxyModeFixed || p.State.Mode == db.TaskProxyModeDirect {
		ws.Proxy = addr
	}
	chat.SetWebSearch(ws)
	chat.SetGuard(s.chatGuard())
	chat.SetNonStreaming(nonStreamingResolver(cfg))
	chat.SetMaxTokens(maxTokensResolver(cfg))
	chat.SetNoaEnabled(s.m.NoaCompactionEnabled)
	return chat, nil
}

type conversationProxyTicket struct {
	token   string
	subject db.ProxySubject
	active  atomic.Bool
}
type conversationProxyTicketKey struct{}

func (s *Server) beginConversationProxy(ctx context.Context, id int64) (context.Context, db.ProxyPolicy, func(), error) {
	p, token, e := s.m.pg.AcquireConversationOperation(id)
	if e != nil {
		return ctx, p, func() {}, e
	}
	ticket := &conversationProxyTicket{token: token, subject: p.Subject}
	ticket.active.Store(true)
	ctx = s.m.withProxySubject(ctx, p)
	ctx = context.WithValue(ctx, conversationProxyTicketKey{}, ticket)
	var cleanupFailed atomic.Bool
	ctx = agent.WithBrowserCleanupFailure(ctx, func() { cleanupFailed.Store(true) })
	finish := func() {
		ticket.active.Store(false)
		if cleanupFailed.Load() {
			return
		}
		_ = s.m.pg.ReleaseConversationOperation(id, token)
		s.m.wakeProxyFailover()
	}
	return ctx, p, finish, nil
}
