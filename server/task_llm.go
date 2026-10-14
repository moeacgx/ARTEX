package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"log"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/Autumn-27/artex/agent"
	"github.com/Autumn-27/artex/db"
	"github.com/Autumn-27/artex/llmpool"
	"github.com/Autumn-27/artex/llmrec"
	"github.com/Autumn-27/norma/llm"
	"github.com/Autumn-27/norma/transcript"
)

type taskAgentBundle struct {
	proxyAddr      string
	proxyCA        string
	proxyRevision  int64
	runtime        *taskLLMRuntime // goal decomposition runtime
	plannerRuntime *taskLLMRuntime
	workerRuntime  *taskLLMRuntime
	mainRuntime    *taskLLMRuntime
	pl             *agent.Planner
	wk             *agent.Worker
	main           *agent.MainAgent
}

type llmAuditProfile struct {
	ID              int64  `json:"id"`
	Name            string `json:"name"`
	Format          string `json:"format"`
	Model           string `json:"model"`
	ModelID         string `json:"model_id,omitempty"`
	UpstreamModelID string `json:"upstream_model_id,omitempty"`
	MemberKey       string `json:"member_key,omitempty"`
	DisplayName     string `json:"display_name,omitempty"`
}

type llmTransitionAudit struct {
	Mode     string           `json:"mode"` // automatic | manual | exhausted
	Reason   string           `json:"reason"`
	Previous *llmAuditProfile `json:"previous,omitempty"`
	Next     *llmAuditProfile `json:"next,omitempty"`
}

type llmActivityMetadata struct {
	LLMTransition llmTransitionAudit `json:"llm_transition"`
}

type taskLLMRuntime struct {
	s        *Server
	taskID   string
	agentKey string
}

type taskLLMError struct {
	taskID         string
	chainExhausted bool
	cause          error
}

func (e *taskLLMError) Error() string {
	if e.chainExhausted {
		return fmt.Sprintf("task %s LLM profile chain exhausted: %v", e.taskID, e.cause)
	}
	return e.cause.Error()
}

func (e *taskLLMError) Unwrap() error { return e.cause }

func isTaskLLMRuntimeError(err error) bool {
	var target *taskLLMError
	return errors.As(err, &target)
}

func isTaskLLMChainExhausted(err error) bool {
	var target *taskLLMError
	return errors.As(err, &target) && target.chainExhausted
}

// isQuotaExhaustedError is intentionally strict. A generic 429, auth error,
// network failure, or 5xx does not rotate providers; the response must explicitly
// identify quota, credits, billing balance, or payment exhaustion.
func isQuotaExhaustedError(err error) bool {
	return err != nil && agent.IsQuotaExhaustedMessage(err.Error())
}

type taskLLMSelection struct {
	task      *Task
	profileID int64
	modelID   int64
	position  int
	revision  int64
	provider  llm.Provider
	// retry 是这次选中的配置解析出来的重试参数(profile 覆盖 → 全局策略 → 内置默认)。
	// 同 provider 安全窗口重试按它走,所以换了 profile 就换一套重试节奏。
	retry agent.RetryConfig
	cfg   agent.Config
}

type taskLLMStreamHooks struct {
	current    func() (taskLLMSelection, error)
	exhaust    func(taskLLMSelection, error) (db.TaskLLMTransition, error)
	transition func(taskLLMSelection, db.TaskLLMTransition, error)
}

// current resolves the provider this role runs on, by precedence:
// Agent 绑定 → 任务 LLM 配置链 → 全局/环境配置。
// 显式绑定解析失败时停止，不静默借用任务链；未绑定时读任务链，空链才继承全局。
// 返回的 profile id 只有走任务链时才非零 —— streamTaskLLM 以此判断额度错误是否
// 应该推进任务的故障转移状态（绑定/全局路径不改任务链状态，沿用既有语义）。
func (r *taskLLMRuntime) current(contexts ...context.Context) (taskLLMSelection, error) {
	ctx := firstContext(contexts)
	taskNum, err := parseTaskID(r.taskID)
	if err != nil {
		return taskLLMSelection{}, err
	}
	pt, err := r.s.m.pg.GetTask(taskNum)
	if err != nil {
		return taskLLMSelection{}, err
	}
	if pt == nil {
		return taskLLMSelection{}, fmt.Errorf("task %s not found", r.taskID)
	}
	r.s.syncTaskLLMState(pt)
	t, _ := r.s.m.Task(r.taskID)
	sel := taskLLMSelection{task: t, revision: pt.LLMChainRevision}
	if prov, cfg, ok := r.s.agentBindingProviderContext(ctx, r.agentKey, t); ok {
		sel.provider, sel.retry, sel.cfg = prov, cfg.Retry, cfg
		return sel, nil
	}
	if r.s.agentModelSelection(r.agentKey) != nil {
		return sel, db.ErrLLMModelUnavailable
	}
	if len(pt.LLMProfileIDs) > 0 {
		if pt.ActiveLLMProfileID == nil {
			return sel, &taskLLMError{taskID: r.taskID, chainExhausted: true, cause: errors.New("all selected profiles are quota exhausted")}
		}
		sel.profileID = *pt.ActiveLLMProfileID
		selection := db.LLMModelSelection{ProfileID: sel.profileID}
		if pt.ActiveLLMPosition != nil && *pt.ActiveLLMPosition < len(pt.LLMMembers) {
			sel.position = *pt.ActiveLLMPosition
			selection = pt.LLMMembers[sel.position]
		}
		model, resolveErr := r.s.m.pg.ResolveLLMModel(context.Background(), selection)
		if resolveErr != nil {
			return sel, resolveErr
		}
		sel.modelID = model.ID
		prov, cfg, ok := r.s.providerForTaskSelection(db.LLMModelSelection{ProfileID: selection.ProfileID, ModelMode: db.LLMModelModeFixed, ModelID: &model.ID}, t, ctx)
		if !ok {
			return sel, fmt.Errorf("LLM profile #%d is missing or invalid", sel.profileID)
		}
		sel.provider, sel.retry, sel.cfg = prov, cfg.Retry, cfg
		return sel, nil
	}
	prov, cfg, ok := r.s.globalProviderContext(ctx, t)
	if !ok {
		return sel, fmt.Errorf("task %s has no available fallback LLM provider", r.taskID)
	}
	sel.provider, sel.retry, sel.cfg = prov, cfg.Retry, cfg
	return sel, nil
}

func (s *Server) applyTaskLLMProxy(ctx context.Context, t *Task, prov llm.Provider, cfg agent.Config, profileIDs ...int64) (llm.Provider, agent.Config, error) {
	if t == nil {
		return prov, cfg, nil
	}
	if pool, ok := prov.(*llmpool.Pool); ok {
		members := make([]*llmpool.Member, 0, len(pool.Members()))
		for _, member := range pool.Members() {
			mcfg, _, ok := s.loadSelectionConfig(db.LLMModelSelection{ProfileID: member.ID, ModelMode: db.LLMModelModeFixed, ModelID: &member.ModelID})
			if !ok {
				return nil, cfg, db.ErrLLMModelUnavailable
			}
			next, _, err := s.applyTaskLLMProxy(ctx, t, member.Prov, mcfg, member.ID)
			if err != nil {
				return nil, cfg, err
			}
			copy := *member
			copy.Prov = next
			members = append(members, &copy)
		}
		return llmpool.New(members, s.llmHealth), cfg, nil
	}
	channelProxy := ""
	channelMode := ""
	var channelProxyID *int64
	if len(profileIDs) > 0 && profileIDs[0] > 0 {
		p, err := s.m.pg.ProfileByID(profileIDs[0])
		if err != nil {
			return nil, cfg, err
		}
		if p != nil {
			channelProxy, channelMode, channelProxyID = p.Proxy, p.ProxyMode, p.ProxyID
		}
	} else {
		channelProxy = cfg.Proxy
	}
	override, err := s.m.ResolveLLMProxySelection(ctx, t, channelProxy, channelMode, channelProxyID)
	if err != nil {
		return nil, cfg, err
	}
	if cfg.Proxy == override {
		return s.guardProxyProvider(t, prov, cfg.Proxy, finalLLMProxyID(t, channelProxy, channelMode, channelProxyID, override), agent.ProviderCloseIdle(prov)), cfg, nil
	}
	cfg.Proxy = override
	next, err := cfg.NewProvider()
	if err != nil {
		return nil, cfg, fmt.Errorf("build task LLM proxy: %w", err)
	}
	closeIdle := agent.ProviderCloseIdle(next)
	if s.m != nil && s.m.pg != nil && len(profileIDs) > 0 && profileIDs[0] > 0 {
		if p, _ := s.m.pg.ProfileByID(profileIDs[0]); p != nil {
			next = llmrec.Wrap(next, s.m.pg, cfg.Model, p.Name, cfg.ThinkingType, cfg.ReasoningEffort, s.m.LLMRecordEnabled)
			next = bindSideProvider(next, cfg, p.ID, p.Name)
		}
	}
	return s.guardProxyProvider(t, next, cfg.Proxy, finalLLMProxyID(t, channelProxy, channelMode, channelProxyID, override), closeIdle), cfg, nil
}

// activeCfg resolves the task's currently-active LLM config, mirroring current()'s
// source precedence (agent binding → active chain profile → global). Read-only and
// best-effort: ok=false when nothing resolves, leaving the per-setting fallback to
// the caller. If failover switches profiles, the change takes effect on the next
// agent run (a fresh Session is built per run in captureRun).
func (r *taskLLMRuntime) activeCfg() (agent.Config, bool) {
	taskNum, err := parseTaskID(r.taskID)
	if err != nil {
		return agent.Config{}, false
	}
	pt, err := r.s.m.pg.GetTask(taskNum)
	if err != nil || pt == nil {
		return agent.Config{}, false
	}
	if selection := r.s.agentModelSelection(r.agentKey); selection != nil {
		cfg, _, ok := r.s.loadSelectionConfig(*selection)
		return cfg, ok
	}
	if len(pt.LLMProfileIDs) > 0 && pt.ActiveLLMProfileID != nil {
		selection := db.LLMModelSelection{ProfileID: *pt.ActiveLLMProfileID}
		if pt.ActiveLLMPosition != nil && *pt.ActiveLLMPosition < len(pt.LLMMembers) {
			selection = pt.LLMMembers[*pt.ActiveLLMPosition]
		}
		if cfg, _, ok := r.s.loadSelectionConfig(selection); ok {
			return cfg, true
		}
	}
	if p, _ := r.s.m.pg.ActiveProfile(); p != nil {
		selection := db.LLMModelSelection{ProfileID: p.ID}
		if r.s.m.LLMPoolEnabled() {
			selection = r.s.poolModelSelection(p.ID)
		}
		cfg, _, ok := r.s.loadSelectionConfig(selection)
		return cfg, ok
	}
	r.s.cfgMu.Lock()
	cfg, on := r.s.llmCfg, r.s.llmOn
	r.s.cfgMu.Unlock()
	if on {
		return cfg, true
	}
	return agent.Config{}, false
}

// nonStreaming reports whether the task's currently-active LLM source is set to
// non-streaming. Unresolvable → streaming (false), the safe default.
func (r *taskLLMRuntime) nonStreaming() bool {
	cfg, ok := r.activeCfg()
	return ok && !cfg.Stream
}

// maxTokens returns the currently-active source's per-reply output cap.
// Unresolvable → 0, i.e. send no cap, matching the pre-setting behaviour.
func (r *taskLLMRuntime) maxTokens() int {
	cfg, _ := r.activeCfg() // 未解析出配置时是零值 0
	return cfg.MaxTokens
}

func parseTaskID(id string) (int64, error) {
	n, err := strconv.ParseInt(id, 10, 64)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("invalid task id %q", id)
	}
	return n, nil
}

// streamHooks builds the failover/exhaustion callbacks shared by Stream and
// Complete: how to read the current profile selection, how to mark it quota
// exhausted, and how to emit a failover transition.
func (r *taskLLMRuntime) streamHooks(contexts ...context.Context) taskLLMStreamHooks {
	return taskLLMStreamHooks{
		current: func() (taskLLMSelection, error) { return r.current(contexts...) },
		exhaust: func(selection taskLLMSelection, cause error) (db.TaskLLMTransition, error) {
			taskNum, _ := parseTaskID(r.taskID)
			transition, err := r.s.m.pg.MarkTaskLLMModelQuotaExhausted(taskNum, selection.position, selection.modelID, selection.revision, cause.Error())
			if err != nil {
				return transition, err
			}
			if pt, getErr := r.s.m.pg.GetTask(taskNum); getErr == nil && pt != nil {
				r.s.syncTaskLLMState(pt)
			}
			return transition, nil
		},
		transition: func(selection taskLLMSelection, transition db.TaskLLMTransition, cause error) {
			r.s.emitTaskLLMTransition(selection.task, transition, cause)
		},
	}
}

func (r *taskLLMRuntime) Stream(ctx context.Context, req llm.CompletionRequest) iter.Seq2[llm.StreamEvent, error] {
	return func(yield func(llm.StreamEvent, error) bool) {
		callCtx, finish, ok := r.admitCall(ctx)
		if !ok {
			yield(llm.StreamEvent{}, db.ErrProxyInUse)
			return
		}
		defer finish()
		callCtx = llmrec.WithTaskID(callCtx, r.taskID)
		streamTaskLLM(callCtx, r.taskID, req, r.streamHooks(callCtx))(yield)
	}
}

// Complete is the non-streaming counterpart of Stream. A non-streaming call is
// atomic — it never delivers partial output — so every failure is safe to retry
// on the same provider or fail over to the next profile without risking
// duplicated model output or tool execution (the "committed" bookkeeping the
// streaming path needs is unnecessary here).
func (r *taskLLMRuntime) Complete(ctx context.Context, req llm.CompletionRequest) (llm.Message, string, llm.Usage, error) {
	ctx, finish, ok := r.admitCall(ctx)
	if !ok {
		return llm.Message{}, "", llm.Usage{}, db.ErrProxyInUse
	}
	defer finish()
	ctx = llmrec.WithTaskID(ctx, r.taskID)
	return completeTaskLLM(ctx, r.taskID, req, r.streamHooks(ctx))
}

func (r *taskLLMRuntime) admitCall(ctx context.Context) (context.Context, func(), bool) {
	if r.s.m.TaskProxyOperationAdmitted(ctx, r.taskID) {
		return ctx, func() {}, true
	}
	return r.s.engine.beginTaskOperationContext(ctx, r.taskID)
}

func completeTaskLLM(ctx context.Context, taskID string, req llm.CompletionRequest, hooks taskLLMStreamHooks) (llm.Message, string, llm.Usage, error) {
	for {
		selection, err := hooks.current()
		if err != nil {
			return llm.Message{}, "", llm.Usage{}, err
		}
		var (
			msg     llm.Message
			sr      string
			usage   llm.Usage
			callErr error
		)
		// 同 provider 安全窗口重试:非流式调用要么整体成功、要么整体失败,没有
		// 中途已交付输出的问题,所以任何瞬时失败都可原样重试。
		retries, backoffOf := sameProviderRetryPolicy(selection.retry)
		for attempt := 0; ; attempt++ {
			msg, sr, usage, callErr = selection.provider.Complete(ctx, req)
			if callErr != nil && ctx.Err() == nil &&
				attempt < retries && isRetryableStreamError(callErr) {
				backoff := backoffOf(attempt)
				log.Printf("[task-llm] task %s 非流式调用失败,%v 后同 provider 重试 (%d/%d): %v",
					taskID, backoff, attempt+1, retries, callErr)
				if sleepCtx(ctx, backoff) {
					break // 退避期间 ctx 取消 → 停止重试
				}
				continue
			}
			break
		}
		if callErr == nil {
			return msg, sr, usage, nil
		}
		// profileID=0: 显式链已被清空;非额度错误:透传。二者都不改任务 failover 状态。
		if selection.profileID == 0 || !isQuotaExhaustedError(callErr) {
			return llm.Message{}, "", llm.Usage{}, callErr
		}
		transition, markErr := hooks.exhaust(selection, callErr)
		if markErr != nil {
			return llm.Message{}, "", llm.Usage{}, fmt.Errorf("mark profile quota exhausted after %v: %w", callErr, markErr)
		}
		if transition.Advanced && !transition.Stale && hooks.transition != nil {
			hooks.transition(selection, transition, callErr)
		}
		if !transition.Stale && transition.NextProfileID == nil {
			return llm.Message{}, "", llm.Usage{}, &taskLLMError{taskID: taskID, chainExhausted: transition.ChainExhausted, cause: callErr}
		}
		// 没有任何输出交付给调用方,换下一个 profile 重放同一逻辑请求是安全的。
	}
}

func streamTaskLLM(ctx context.Context, taskID string, req llm.CompletionRequest, hooks taskLLMStreamHooks) iter.Seq2[llm.StreamEvent, error] {
	return func(yield func(llm.StreamEvent, error) bool) {
		for {
			selection, err := hooks.current()
			if err != nil {
				yield(llm.StreamEvent{}, err)
				return
			}
			committed := false
			var pending []llm.StreamEvent
			var streamErr error
			retries, backoffOf := sameProviderRetryPolicy(selection.retry)
			// 同 provider 安全窗口重试:committed 之前(还没向调用方交付任何输出)
			// 的瞬时失败可以原样重放,不会重复模型输出或工具执行。committed 之后、
			// ctx 取消、或确定性/额度错误则跳出,交给下方原有的透传/故障转移逻辑。
			for attempt := 0; ; attempt++ {
				committed = false
				pending = nil
				streamErr = nil
				for event, err := range selection.provider.Stream(ctx, req) {
					if err != nil {
						streamErr = err
						break
					}
					if !committed && !streamEventCommitsOutput(event) {
						pending = append(pending, event)
						continue
					}
					if !committed {
						for _, buffered := range pending {
							if !yield(buffered, nil) {
								return
							}
						}
						pending = nil
						committed = true
					}
					if !yield(event, nil) {
						return
					}
				}
				if streamErr != nil && !committed && ctx.Err() == nil &&
					attempt < retries && isRetryableStreamError(streamErr) {
					backoff := backoffOf(attempt)
					log.Printf("[task-llm] task %s 提交前流失败,%v 后同 provider 重试 (%d/%d): %v",
						taskID, backoff, attempt+1, retries, streamErr)
					if sleepCtx(ctx, backoff) {
						break // 退避期间 ctx 取消 → 停止重试
					}
					continue
				}
				break
			}
			if streamErr == nil {
				for _, buffered := range pending {
					if !yield(buffered, nil) {
						return
					}
				}
				return
			}
			// profileID=0 means the explicit chain was cleared while this stable task
			// bundle was still in use. Agent/global fallback errors follow the legacy
			// behavior and never mutate task failover state.
			if selection.profileID == 0 || !isQuotaExhaustedError(streamErr) {
				for _, buffered := range pending {
					if !yield(buffered, nil) {
						return
					}
				}
				yield(llm.StreamEvent{}, streamErr)
				return
			}
			transition, markErr := hooks.exhaust(selection, streamErr)
			if markErr != nil {
				cause := fmt.Errorf("mark profile quota exhausted after %v: %w", streamErr, markErr)
				if committed {
					// Output may already have driven tool execution. Report the persistence
					// failure, but classify it as router-handled so the worker does not
					// replay the entire intent and duplicate those side effects.
					yield(llm.StreamEvent{}, &taskLLMError{taskID: taskID, cause: cause})
				} else {
					yield(llm.StreamEvent{}, cause)
				}
				return
			}
			if transition.Advanced && !transition.Stale && hooks.transition != nil {
				hooks.transition(selection, transition, streamErr)
			}
			if committed || (!transition.Stale && transition.NextProfileID == nil) {
				yield(llm.StreamEvent{}, &taskLLMError{taskID: taskID, chainExhausted: transition.ChainExhausted, cause: streamErr})
				return
			}
			// No event reached the caller, so replaying the same logical request on
			// the next profile cannot duplicate model output or tool execution.
		}
	}
}

func streamEventCommitsOutput(event llm.StreamEvent) bool {
	switch event.Type {
	case llm.SETextDelta, llm.SEThinkingDelta, llm.SEToolInputJSON:
		return event.Text != ""
	case llm.SEToolUseStart, llm.SEMessageDelta, llm.SEMessageStop:
		return true
	default:
		return false
	}
}

// 提交前安全窗口内、对同一 provider 的【默认】重试次数。SDK 的 doStream 只重试建连
// 阶段(拿到 200 之前);流一旦开始,中途断流 / overloaded / 流内 429 等瞬时故障会直接
// 冒泡成 model_error,零重试。只要一个 token 都还没交给调用方(!committed),重放
// 完全相同的请求就不会重复模型输出或工具副作用,因此这里补一层同 provider 退避重试,
// 把这类抖动挡在意图整体重跑之前。可被 LLM 配置的重试覆盖/全局重试策略改写。
const sameProviderStreamRetries = 2

// sameProviderRetryBackoff 是第 attempt 次重试前的【默认】退避(0.5s、1s…,上限 4s),
// 与 SDK 的指数梯度同风格但封顶更小,避免拖住 worker 的收尾/取消响应。
// 以变量形式暴露,便于测试将退避置零。
var sameProviderRetryBackoff = func(attempt int) time.Duration {
	return min(500*time.Millisecond*(1<<attempt), 4*time.Second)
}

// sameProviderRetryPolicy 解析这次调用用哪套同 provider 重试参数:配置里设了次数就
// 用配置的(负数 = 关掉这层重试),设了间隔就把指数退避换成固定间隔,两者都没设时
// 与改可配之前逐字节一致。
func sameProviderRetryPolicy(r agent.RetryConfig) (retries int, backoff func(int) time.Duration) {
	retries, backoff = sameProviderStreamRetries, sameProviderRetryBackoff
	if r.StreamAttempts != 0 {
		retries = max(r.StreamAttempts, 0)
	}
	if r.StreamInterval > 0 {
		d := r.StreamInterval
		backoff = func(int) time.Duration { return d }
	}
	return retries, backoff
}

// isRetryableStreamError 判断「提交前的流失败」是否值得在同一 provider 上重放。
// 瞬时的传输中断 / 供应商过载 / 限流会自行恢复,可安全重试;而以下三类不重试:
//   - 额度耗尽:交给 profile 故障转移处理,别在这里白烧重试
//   - 上下文过长:相同请求重放也没用,交给 harness 的 reactive 压缩兜底
//   - 4xx 确定性拒绝(400/401/403/404/422):到哪个 provider 都一样会失败
func isRetryableStreamError(err error) bool {
	if err == nil {
		return false
	}
	if isQuotaExhaustedError(err) {
		return false
	}
	s := strings.ToLower(err.Error())
	if strings.Contains(s, "too long") || strings.Contains(s, "context length") ||
		strings.Contains(s, "context_length") || strings.Contains(s, "maximum context") ||
		strings.Contains(s, "status 413") {
		return false
	}
	for _, code := range []string{"status 400", "status 401", "status 403", "status 404", "status 422"} {
		if strings.Contains(s, code) {
			return false
		}
	}
	// 其余(传输 reset/EOF/timeout、408/429/5xx、流内 error 事件如 anthropic
	// overloaded_error 等)一律视为瞬时,允许重试。
	return true
}

// CompactionWindow mirrors current()'s precedence so the context window always
// matches the provider the role will actually stream on.
func (r *taskLLMRuntime) CompactionWindow() int {
	if selection := r.s.agentModelSelection(r.agentKey); selection != nil {
		if cfg, _, ok := r.s.loadSelectionConfig(*selection); ok {
			return cfg.CompactionWindow()
		}
		return (agent.Config{}).CompactionWindow()
	}
	taskNum, err := parseTaskID(r.taskID)
	if err != nil {
		return (agent.Config{}).CompactionWindow()
	}
	chain, err := r.s.m.pg.TaskLLMProfiles(taskNum)
	if err != nil {
		return (agent.Config{}).CompactionWindow()
	}
	if len(chain) == 0 {
		if cfg, ok := r.activeCfg(); ok {
			return cfg.CompactionWindow()
		}
		return (agent.Config{}).CompactionWindow()
	}
	minimum := 0
	for _, entry := range chain {
		if cfg, _, ok := r.s.loadSelectionConfig(db.LLMModelSelection{ProfileID: entry.ProfileID, ModelMode: entry.ModelMode, ModelID: entry.ModelID}); ok {
			window := cfg.CompactionWindow()
			if minimum == 0 || window < minimum {
				minimum = window
			}
		}
	}
	if minimum == 0 {
		return (agent.Config{}).CompactionWindow()
	}
	return minimum
}

// agentBindingProvider resolves the profile a role is explicitly bound to
// (agents.llm_profile_id) — the highest-precedence level for task agents. ok=false
// when the role has no binding or the bound profile no longer builds, so callers
// fall through to the task chain. A bound profile stays exclusive unless
// llm_pool_bind_fallback is on, which is what poolForBinding encodes.
func (s *Server) agentBindingProvider(agentKey string, tasks ...*Task) (llm.Provider, agent.Config, bool) {
	return s.agentBindingProviderContext(context.Background(), agentKey, firstTask(tasks))
}

func (s *Server) agentBindingProviderContext(ctx context.Context, agentKey string, task *Task) (llm.Provider, agent.Config, bool) {
	selection := s.agentModelSelection(agentKey)
	if selection == nil {
		return nil, agent.Config{}, false
	}
	prov, cfg, ok := s.providerForTaskSelection(*selection, task, ctx)
	if !ok {
		return nil, agent.Config{}, false
	}
	if s.m.LLMPoolEnabled() && s.m.LLMPoolBindFallback() {
		if pool := s.poolChainContext(ctx, selection.ProfileID, prov, cfg, false, task); pool != nil {
			prov = pool
		}
	}
	return prov, cfg, true
}

func (s *Server) agentModelSelection(key string) *db.LLMModelSelection {
	if s.m != nil && s.m.pg != nil {
		if a, _ := s.m.pg.GetAgentByKey(key); a != nil && a.LLMProfileID != nil {
			return &db.LLMModelSelection{ProfileID: *a.LLMProfileID, ModelMode: a.LLMModelMode, ModelID: a.LLMModelID}
		}
	}
	return nil
}

// globalProvider returns the process-wide provider (persisted active profile or
// environment config) — the last resort once a role has neither a binding nor a
// task chain.
func (s *Server) globalProvider(tasks ...*Task) (llm.Provider, agent.Config, bool) {
	return s.globalProviderContext(context.Background(), firstTask(tasks))
}

func (s *Server) globalProviderContext(ctx context.Context, task *Task) (llm.Provider, agent.Config, bool) {
	if s.m != nil && s.m.pg != nil {
		if p, _ := s.m.pg.ActiveProfile(); p != nil {
			selection := db.LLMModelSelection{ProfileID: p.ID}
			if s.m.LLMPoolEnabled() {
				selection = s.poolModelSelection(p.ID)
			}
			prov, cfg, ok := s.providerForTaskSelection(selection, task, ctx)
			if !ok {
				return nil, cfg, false
			}
			if pool := s.poolChainContext(ctx, p.ID, prov, cfg, true, task); pool != nil {
				prov = pool
			}
			return prov, cfg, true
		}
	}
	s.cfgMu.Lock()
	if !s.llmOn || s.llmProv == nil {
		s.cfgMu.Unlock()
		return nil, agent.Config{}, false
	}
	prov, cfg := s.llmProv, s.llmCfg
	s.cfgMu.Unlock()
	if task != nil {
		var err error
		prov, cfg, err = s.applyTaskLLMProxy(ctx, task, prov, cfg)
		if err != nil {
			return nil, cfg, false
		}
	}
	return prov, cfg, true
}

func firstTask(tasks []*Task) *Task {
	if len(tasks) > 0 {
		return tasks[0]
	}
	return nil
}

// taskRuntimeAvailable reports whether every listed role can resolve a provider
// under the runtime precedence in current(): the role's own binding first, then
// the task chain, then global. An exhausted chain is a hard stop for unbound
// roles rather than a silent fall through to global — same as current().
func (s *Server) taskRuntimeAvailable(t *Task, agentKeys ...string) bool {
	if t == nil || len(agentKeys) == 0 {
		return false
	}
	state := t.llmStateSnapshot()
	unboundReady := false
	if len(state.ProfileIDs) > 0 {
		if state.ActiveID != nil {
			selection := db.LLMModelSelection{ProfileID: *state.ActiveID}
			if state.ActivePosition != nil && *state.ActivePosition < len(state.Members) {
				selection = state.Members[*state.ActivePosition]
			}
			_, _, unboundReady = s.loadSelectionConfig(selection)
		}
	} else {
		var p *db.LLMProfile
		if s.m.pg != nil {
			p, _ = s.m.pg.ActiveProfile()
		}
		if p != nil {
			selection := db.LLMModelSelection{ProfileID: p.ID}
			if s.m.LLMPoolEnabled() {
				selection = s.poolModelSelection(p.ID)
			}
			_, _, unboundReady = s.loadSelectionConfig(selection)
		} else {
			s.cfgMu.Lock()
			unboundReady = s.llmOn && s.llmProv != nil
			s.cfgMu.Unlock()
		}
	}
	for _, key := range agentKeys {
		if selection := s.agentModelSelection(key); selection != nil {
			if _, _, ok := s.loadSelectionConfig(*selection); !ok {
				return false
			}
			continue
		}
		if !unboundReady {
			return false
		}
	}
	return true
}

func (s *Server) invalidateTaskAgents() {
	s.taskAgentMu.Lock()
	s.taskAgents = map[string]*taskAgentBundle{}
	s.taskAgentMu.Unlock()
}

func (s *Server) agentsForTask(t *Task, contexts ...context.Context) (*taskAgentBundle, error) {
	ctx := firstContext(contexts)
	proxyAddr, proxyCA, err := s.m.TaskToolProxy(ctx, t)
	if err != nil {
		return nil, fmt.Errorf("任务工具代理解析失败: %w", err)
	}
	s.taskAgentMu.Lock()
	defer s.taskAgentMu.Unlock()
	if s.taskAgents == nil {
		s.taskAgents = map[string]*taskAgentBundle{}
	}
	proxyRevision := t.proxyState().Revision
	if bundle := s.taskAgents[t.ID]; bundle != nil && bundle.proxyAddr == proxyAddr && bundle.proxyCA == proxyCA && bundle.proxyRevision == proxyRevision {
		return bundle, nil
	}
	goalRuntime := &taskLLMRuntime{s: s, taskID: t.ID, agentKey: "goals"}
	plannerRuntime := &taskLLMRuntime{s: s, taskID: t.ID, agentKey: "planner"}
	workerRuntime := &taskLLMRuntime{s: s, taskID: t.ID, agentKey: "worker"}
	mainRuntime := &taskLLMRuntime{s: s, taskID: t.ID, agentKey: "mainagent"}
	tx := transcript.NewStore(filepath.Join(s.m.dir, "transcripts"))
	window := workerRuntime.CompactionWindow()
	wk := agent.NewWorker(workerRuntime, "task-router", s.m.dir, tx, window, s.agentMaxTurns("worker"))
	wk.SetFindingRecorder(s.evidenceStore())
	wk.SetCompactionWindowResolver(workerRuntime.CompactionWindow)
	wk.SetNonStreaming(workerRuntime.nonStreaming) // 按任务当前激活 profile 的流式开关(每轮读)
	wk.SetMaxTokens(workerRuntime.maxTokens)       // 同上,输出上限也跟随当前激活 profile
	wk.SetNoaEnabled(s.m.NoaCompactionEnabled)     // 实验功能:noa 上下文压缩(平台级开关,每 run 读)
	wk.SetRunTimeout(time.Duration(s.agentRunSeconds("worker")) * time.Second)
	wk.SetProxy(proxyAddr, proxyCA)
	wk.SetStrictProxy(true)
	workerSearch := s.webSearchFor("worker")
	if t.proxyState().Mode == db.TaskProxyModeGroup {
		workerSearch.Proxy = proxyAddr
	}
	wk.SetWebSearch(workerSearch)
	wk.SetConstraintInject(s.constraintInjectWorker) // 操作约束注入 worker(可配置,默认开)
	pl := agent.NewPlanner(plannerRuntime, "task-router", s.m.dir, tx, plannerRuntime.CompactionWindow(), s.agentMaxTurns("planner"))
	pl.SetFindingRecorder(s.evidenceStore())
	pl.SetCompactionWindowResolver(plannerRuntime.CompactionWindow)
	pl.SetNonStreaming(plannerRuntime.nonStreaming)
	pl.SetMaxTokens(plannerRuntime.maxTokens)
	pl.SetNoaEnabled(s.m.NoaCompactionEnabled) // 实验功能:noa 上下文压缩(平台级开关,每 run 读)
	pl.SetKillWork(s.engine.KillWork)
	pl.SetSteerWork(s.engine.SteerWork)
	pl.SetProxy(proxyAddr, proxyCA)
	pl.SetStrictProxy(true)
	plannerSearch := s.webSearchFor("planner")
	if t.proxyState().Mode == db.TaskProxyModeGroup {
		plannerSearch.Proxy = proxyAddr
	}
	pl.SetWebSearch(plannerSearch)
	pl.SetConstraintInject(s.constraintInjectPlanner) // 操作约束注入 planner(可配置,默认开)
	// cold-digest §7: 冷节点后台压缩。引擎经权威解析器实际驱动的就是这套 per-task planner
	// (agentsForTask),Compactor 必须接在这里。走任务路由的 planner provider(§4:与 agent
	// 同模型,随任务 LLM 链解析),压缩用 Complete 一次性生成 body。
	pl.SetCompactor(agent.NewCompactor(plannerRuntime, "task-router"))
	main := agent.NewMainAgent(mainRuntime, "task-router", s.m.dir, tx, mainRuntime.CompactionWindow(), s.agentMaxTurns("mainagent"))
	main.SetFindingRecorder(s.evidenceStore())
	main.SetCompactionWindowResolver(mainRuntime.CompactionWindow)
	main.SetNonStreaming(mainRuntime.nonStreaming)
	main.SetMaxTokens(mainRuntime.maxTokens)
	main.SetNoaEnabled(s.m.NoaCompactionEnabled) // 实验功能:noa 上下文压缩(平台级开关,每 run 读)
	main.SetProxy(proxyAddr, proxyCA)
	main.SetStrictProxy(true)
	mainSearch := s.webSearchFor("mainagent")
	if t.proxyState().Mode == db.TaskProxyModeGroup {
		mainSearch.Proxy = proxyAddr
	}
	main.SetWebSearch(mainSearch)
	main.SetSteerWork(s.engine.SteerWork) // steer_work：人对运行中 work 实时纠偏
	bundle := &taskAgentBundle{
		proxyAddr: proxyAddr, proxyCA: proxyCA, proxyRevision: proxyRevision,
		runtime: goalRuntime, plannerRuntime: plannerRuntime, workerRuntime: workerRuntime,
		mainRuntime: mainRuntime, pl: pl, wk: wk, main: main,
	}
	s.taskAgents[t.ID] = bundle
	return bundle, nil
}

func firstContext(contexts []context.Context) context.Context {
	if len(contexts) > 0 && contexts[0] != nil {
		return contexts[0]
	}
	return context.Background()
}

func (s *Server) syncTaskLLMState(pt *db.Task) {
	if pt == nil {
		return
	}
	id := fmt.Sprintf("%d", pt.ID)
	s.m.mu.Lock()
	if task := s.m.tasks[id]; task != nil {
		task.setLLMState(pt.LLMProfileID, pt.ActiveLLMProfileID, pt.LLMProfileIDs, pt.LLMChainRevision, pt.LLMFailoverState, pt.LLMFailoverReason, pt)
	}
	s.m.mu.Unlock()
}

func (s *Server) emitTaskLLMTransition(t *Task, transition db.TaskLLMTransition, cause error) {
	if t == nil {
		return
	}
	previous := s.llmAuditModel(transition.PreviousProfileID, transition.PreviousModelID)
	var next *llmAuditProfile
	if transition.NextProfileID != nil {
		state := t.llmStateSnapshot()
		if actual := t.activeLLMMember(state); actual != nil {
			next = s.llmAuditModel(*transition.NextProfileID, func() int64 { id, _ := strconv.ParseInt(actual.ModelID, 10, 64); return id }())
		} else {
			next = s.llmAuditProfile(*transition.NextProfileID)
		}
	}
	mode := "automatic"
	kind := "llm_switch"
	summary := fmt.Sprintf("%s 额度不足", llmAuditProfileLabel(previous))
	if transition.NextProfileID != nil {
		summary += fmt.Sprintf("，后续调用切换到 %s", llmAuditProfileLabel(next))
	} else {
		mode = "exhausted"
		kind = "llm_failover"
		summary += "，配置链已耗尽"
	}
	metadata, _ := json.Marshal(llmActivityMetadata{LLMTransition: llmTransitionAudit{
		Mode: mode, Reason: cause.Error(), Previous: previous, Next: next,
	}})
	s.engine.emitActivity(t, db.Activity{Worker: "system", Kind: kind, IsError: transition.ChainExhausted, Summary: summary, Detail: cause.Error(), Metadata: metadata})
	log.Printf("[llm-failover] task %s: %s", t.ID, summary)
}

func (s *Server) llmAuditProfile(id int64) *llmAuditProfile {
	if id <= 0 || s.m == nil || s.m.pg == nil {
		return nil
	}
	model, err := s.m.pg.ResolveLLMModel(context.Background(), db.LLMModelSelection{ProfileID: id})
	if err == nil {
		return s.llmAuditModel(id, model.ID)
	}
	return s.llmAuditModel(id, 0)
}

func (s *Server) llmAuditModel(id, modelID int64) *llmAuditProfile {
	if id <= 0 || s.m == nil || s.m.pg == nil {
		return nil
	}
	p, err := s.m.pg.ProfileByID(id)
	if err != nil || p == nil {
		return &llmAuditProfile{ID: id, Name: fmt.Sprintf("配置 #%d", id)}
	}
	out := &llmAuditProfile{ID: p.ID, Name: p.Name, Format: p.Format, Model: p.Model}
	if modelID > 0 {
		if set, err := s.m.pg.ListLLMModels(id); err == nil {
			for _, model := range set.Models {
				if model.ID == modelID {
					out.Model = model.UpstreamModelID
					out.ModelID = i64s(model.ID)
					out.UpstreamModelID = model.UpstreamModelID
					out.DisplayName = model.DisplayName
					out.MemberKey = model.MemberKey()
					break
				}
			}
		}
	}
	return out
}

func llmAuditProfileLabel(profile *llmAuditProfile) string {
	if profile == nil {
		return "默认配置"
	}
	name := profile.Name
	if name == "" {
		name = fmt.Sprintf("配置 #%d", profile.ID)
	}
	detail := []string{}
	if profile.Format != "" {
		detail = append(detail, profile.Format)
	}
	if profile.Model != "" {
		detail = append(detail, profile.Model)
	}
	if len(detail) == 0 {
		return name
	}
	return fmt.Sprintf("%s（%s）", name, strings.Join(detail, " / "))
}

func sameOptionalID(a, b *int64) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func sameResolvedMember(a, b *LLMResolvedMemberDTO) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.MemberKey == b.MemberKey && a.ModelMode == b.ModelMode
}

func auditResolvedMember(actual *LLMResolvedMemberDTO, base *llmAuditProfile) *llmAuditProfile {
	if actual == nil {
		return base
	}
	if base == nil {
		base = &llmAuditProfile{}
	}
	base.ModelID = actual.ModelID
	base.Model = actual.UpstreamModelID
	base.UpstreamModelID = actual.UpstreamModelID
	base.DisplayName = actual.DisplayName
	base.MemberKey = actual.MemberKey
	return base
}

func (s *Server) emitManualTaskLLMSwitch(t *Task, previousID, nextID *int64, actual ...*LLMResolvedMemberDTO) db.Activity {
	previous := (*llmAuditProfile)(nil)
	next := (*llmAuditProfile)(nil)
	if previousID != nil {
		previous = s.llmAuditProfile(*previousID)
	}
	if nextID != nil {
		next = s.llmAuditProfile(*nextID)
	}
	if len(actual) == 2 {
		previous = auditResolvedMember(actual[0], previous)
		next = auditResolvedMember(actual[1], next)
	}
	summary := fmt.Sprintf("用户手动将任务 LLM 从 %s 切换到 %s", llmAuditProfileLabel(previous), llmAuditProfileLabel(next))
	metadata, _ := json.Marshal(llmActivityMetadata{LLMTransition: llmTransitionAudit{
		Mode: "manual", Reason: "用户手动切换任务 LLM", Previous: previous, Next: next,
	}})
	return s.engine.emitActivity(t, db.Activity{Worker: "system", Kind: "llm_switch", Summary: summary, Detail: summary, Metadata: metadata})
}
