package server

import (
	"context"
	"fmt"
	"net/http"

	"github.com/Autumn-27/artex/db"
)

type taskLLMResolution struct {
	ModelMode       string  `json:"model_mode"`
	ModelID         *string `json:"model_id"`
	UpstreamModelID string  `json:"upstream_model_id"`
	DisplayName     string  `json:"display_name"`
	MemberKey       string  `json:"member_key"`
	ProfileID       *int64  `json:"profile_id,omitempty"`
	Name            string  `json:"name"`
	Format          string  `json:"format"`
	Model           string  `json:"model"`
	Source          string  `json:"source"`
	Available       bool    `json:"available"`
	Reason          string  `json:"reason,omitempty"`
}

func (s *Server) resolutionFromProfile(p *db.LLMProfile, source string) taskLLMResolution {
	return s.resolutionFromSelection(p, db.LLMModelSelection{ProfileID: func() int64 {
		if p == nil {
			return 0
		}
		return p.ID
	}()}, source)
}

func (s *Server) resolutionFromSelection(p *db.LLMProfile, selection db.LLMModelSelection, source string, tasks ...*Task) taskLLMResolution {
	if p == nil {
		return taskLLMResolution{Source: source, Reason: "LLM 配置不存在"}
	}
	id := p.ID
	result := taskLLMResolution{
		ProfileID: &id,
		Name:      p.Name,
		Format:    p.Format,
		Model:     p.Model,
		Source:    source,
	}
	if p.APIKey == "" {
		result.Reason = "LLM 配置未设置 API Key"
		return result
	}
	model, err := s.m.pg.ResolveLLMModel(context.Background(), selection)
	if err != nil {
		result.Reason = "模型成员不存在或已禁用"
		return result
	}
	result.ModelMode = selection.Normalized().ModelMode
	result.ModelID = optionalIDString(&model.ID)
	result.Model = model.UpstreamModelID
	result.UpstreamModelID = model.UpstreamModelID
	result.DisplayName = model.DisplayName
	result.MemberKey = model.MemberKey()
	cfg, _, ok := s.loadSelectionConfig(selection)
	if !ok {
		result.Reason = "LLM 配置格式或参数无效"
		return result
	}
	if task := firstTask(tasks); task != nil {
		proxyState := task.proxyState()
		if proxyState.Mode == db.TaskProxyModeDirect {
			cfg.Proxy = ""
		} else if proxyState.Mode == db.TaskProxyModeFixed || proxyState.Mode == db.TaskProxyModeGroup || cfg.Proxy == "" {
			taskCopy := &Task{ID: task.ID}
			taskCopy.setProxyState(proxyState)
			snapshot, err := s.m.runtimeSnapshot(taskCopy)
			if err != nil {
				result.Reason = "任务代理不可用"
				return result
			}
			cfg.Proxy = snapshotProxyURL(snapshot, task.ID)
		}
	}
	if _, err := cfg.NewProvider(); err != nil {
		result.Reason = "LLM 配置格式或代理参数无效"
		return result
	}
	result.Available = true
	return result
}

// resolveTaskRoleLLM mirrors taskLLMRuntime.current without creating a provider:
// role Agent binding -> explicit task chain -> active/global environment config.
// Database failures are returned rather than represented as an unavailable model.
func (s *Server) resolveTaskRoleLLM(t *Task, agentKey string) (taskLLMResolution, error) {
	if s == nil || s.m == nil || s.m.pg == nil {
		return taskLLMResolution{}, fmt.Errorf("database unavailable")
	}
	a, err := s.m.pg.GetAgentByKey(agentKey)
	if err != nil {
		return taskLLMResolution{}, fmt.Errorf("load agent %q: %w", agentKey, err)
	}
	if a != nil && a.LLMProfileID != nil {
		p, err := s.m.pg.ProfileByID(*a.LLMProfileID)
		if err != nil {
			return taskLLMResolution{}, fmt.Errorf("load agent LLM profile: %w", err)
		}
		return s.resolutionFromSelection(p, db.LLMModelSelection{ProfileID: *a.LLMProfileID, ModelMode: a.LLMModelMode, ModelID: a.LLMModelID}, "agent_binding", t), nil
	}
	state := t.llmStateSnapshot()
	if len(state.ProfileIDs) > 0 {
		if state.ActiveID == nil {
			return taskLLMResolution{Source: "task_chain", Reason: "任务 LLM 配置链额度已耗尽"}, nil
		}
		p, err := s.m.pg.ProfileByID(*state.ActiveID)
		if err != nil {
			return taskLLMResolution{}, fmt.Errorf("load task LLM profile: %w", err)
		}
		selection := db.LLMModelSelection{ProfileID: *state.ActiveID}
		if state.ActivePosition != nil && *state.ActivePosition < len(state.Members) {
			selection = state.Members[*state.ActivePosition]
		}
		return s.resolutionFromSelection(p, selection, "task_chain", t), nil
	}
	p, err := s.m.pg.ActiveProfile()
	if err != nil {
		return taskLLMResolution{}, fmt.Errorf("load global LLM profile: %w", err)
	}
	if p != nil {
		selection := db.LLMModelSelection{ProfileID: p.ID}
		if s.m.LLMPoolEnabled() {
			selection = s.poolModelSelection(p.ID)
		}
		return s.resolutionFromSelection(p, selection, "global_profile", t), nil
	}
	s.cfgMu.Lock()
	on, name, cfg := s.llmOn, s.llmProf, s.llmCfg
	s.cfgMu.Unlock()
	if on {
		if name == "" {
			name = "全局配置"
		}
		return taskLLMResolution{
			Name: name, Format: cfg.Provider(), Model: cfg.Model,
			Source: "environment", Available: true,
		}, nil
	}
	return taskLLMResolution{Source: "global", Reason: "没有可用的 LLM 配置"}, nil
}

func (s *Server) taskLLMResolutionHandler(w http.ResponseWriter, r *http.Request) {
	t, ok := s.m.Task(r.PathValue("id"))
	if !ok {
		writeErr(w, 404, "task not found")
		return
	}
	roles := map[string]taskLLMResolution{}
	for responseKey, agentKey := range map[string]string{
		"mainagent": "mainagent",
		"planner":   "planner",
		"worker":    "worker",
	} {
		resolved, err := s.resolveTaskRoleLLM(t, agentKey)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
		roles[responseKey] = resolved
	}
	writeJSON(w, http.StatusOK, roles)
}
