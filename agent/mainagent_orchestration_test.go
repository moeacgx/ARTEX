package agent

import (
	"context"
	"encoding/json"
	"iter"
	"strings"
	"testing"

	"github.com/Autumn-27/artex/db"
	"github.com/Autumn-27/norma/llm"
	actool "github.com/Autumn-27/norma/tool"
)

type orchestrationProvider struct {
	complete func(llm.CompletionRequest) (llm.Message, string, llm.Usage, error)
}

func (p *orchestrationProvider) Complete(_ context.Context, r llm.CompletionRequest) (llm.Message, string, llm.Usage, error) {
	return p.complete(r)
}
func (p *orchestrationProvider) Stream(context.Context, llm.CompletionRequest) iter.Seq2[llm.StreamEvent, error] {
	panic("仅使用合成非流式模型")
}

func TestMainAgentDelegatesFollowupActualRequest(t *testing.T) {
	d := testDB(t)
	defer d.Close()
	exp, err := d.CreateExploration("追加工作", "原目标")
	if err != nil {
		t.Fatal(err)
	}
	defer d.Exec(`DELETE FROM explorations WHERE id=$1`, exp)
	ts := d.Exploration(exp)
	old, err := ts.AddNode(db.KindGoal, map[string]any{"text": "旧目标"}, 0, "met", "human", nil)
	if err != nil {
		t.Fatal(err)
	}
	previous := ToolAugment
	ToolAugment = func(context.Context, string) ([]actool.CoreTool, DeferredInfo, func()) {
		return []actool.CoreTool{writeTool("mcp__fixture__execute", "执行", obj(nil), nil)}, DeferredInfo{Deferred: []string{"mcp__fixture__execute"}}, func() {}
	}
	defer func() { ToolAugment = previous }()
	calls, resumed := 0, 0
	p := &orchestrationProvider{complete: func(req llm.CompletionRequest) (llm.Message, string, llm.Usage, error) {
		calls++
		raw, _ := json.Marshal(req.Tools)
		for _, name := range []string{"Bash", "Write", "Edit", "Task", "TaskOutput", "WebFetch", "WebSearch", "Skill", "mcp__fixture__execute"} {
			if strings.Contains(string(raw), `"name":"`+name+`"`) {
				t.Errorf("主 Agent 仍暴露执行工具 %s", name)
			}
		}
		if calls == 1 {
			return llm.Message{Role: llm.RoleAssistant, Content: []llm.ContentBlock{{Type: llm.BlockToolUse, ID: "append", Name: "add_intent", Input: json.RawMessage(`{"summary":"追加核验本地结果"}`)}}}, "tool_use", llm.Usage{}, nil
		}
		return llm.Message{Role: llm.RoleAssistant, Content: []llm.ContentBlock{llm.TextBlock("已下发追加工作")}}, "end_turn", llm.Usage{}, nil
	}}
	m := NewMainAgent(p, "fixture", t.TempDir(), nil, 10000, 3)
	m.SetNonStreaming(func() bool { return true })
	_, err = m.Chat(t.Context(), 1, 0, nil, ts, "原目标", "请追加核验", nil, nil, func() { resumed++ }, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if resumed != 1 {
		t.Fatalf("未恢复任务 %d", resumed)
	}
	goals, err := ts.ListByKind(db.KindGoal, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(goals) != 2 {
		t.Fatalf("追加后目标数量 %d，期望旧目标+新目标", len(goals))
	}
	n, _ := ts.GetNode(old)
	if n.State != "met" {
		t.Fatal("旧目标被重置")
	}
	open, _ := ts.HasOpenGoal()
	if !open {
		t.Fatal("Planner 没有新的未完成目标")
	}
	intents, _ := ts.ListByKind(db.KindIntent, 100)
	if len(intents) != 1 || intents[0].Origin != "human" {
		t.Fatal("人工意图未登记正确来源")
	}
}

func TestMainAgentEmptyPermissionsDoNotRestoreExecutionTools(t *testing.T) {
	d := testDB(t)
	defer d.Close()
	exp, err := d.CreateExploration("空工具权限", "")
	if err != nil {
		t.Fatal(err)
	}
	defer d.Exec(`DELETE FROM explorations WHERE id=$1`, exp)
	previous := ToolResolve
	ToolResolve = func(context.Context, string, []actool.CoreTool) []actool.CoreTool { return nil }
	defer func() { ToolResolve = previous }()
	p := &orchestrationProvider{complete: func(req llm.CompletionRequest) (llm.Message, string, llm.Usage, error) {
		for _, tool := range req.Tools {
			if tool.Name != "TodoWrite" {
				t.Errorf("空授权回退工具: %s", tool.Name)
			}
		}
		return llm.Message{Role: llm.RoleAssistant, Content: []llm.ContentBlock{llm.TextBlock("无可用编排工具")}}, "end_turn", llm.Usage{}, nil
	}}
	m := NewMainAgent(p, "fixture", t.TempDir(), nil, 10000, 1)
	m.SetNonStreaming(func() bool { return true })
	for _, ctx := range []context.Context{t.Context(), WithResultQuestion(t.Context())} {
		if _, err := m.Chat(ctx, 1, 0, nil, d.Exploration(exp), "", "请读取现状", nil, nil, nil, nil, nil); err != nil {
			t.Fatal(err)
		}
	}
}
