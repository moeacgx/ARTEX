package agent

import (
	"context"
	"encoding/json"
	"iter"
	"strings"
	"testing"

	"github.com/Autumn-27/norma/llm"
)

type questionProvider struct {
	t    *testing.T
	seen bool
}

func (p *questionProvider) Stream(context.Context, llm.CompletionRequest) iter.Seq2[llm.StreamEvent, error] {
	panic("只使用非流式合成模型")
}
func (p *questionProvider) Complete(_ context.Context, req llm.CompletionRequest) (llm.Message, string, llm.Usage, error) {
	p.seen = true
	data, _ := json.Marshal(req.Tools)
	for _, forbidden := range []string{"Bash", "Write", "Edit", "add_intent", "add_hint", "set_goals", "WebFetch", "WebSearch", "add_proxies", "Skill", "mcp__"} {
		if strings.Contains(string(data), `"name":"`+forbidden+`"`) || (forbidden == "mcp__" && strings.Contains(string(data), "mcp__")) {
			p.t.Fatalf("追问开放了执行工具 %s", forbidden)
		}
	}
	return llm.Message{Role: llm.RoleAssistant, Content: []llm.ContentBlock{llm.TextBlock("这是已有结果的解释")}}, "end_turn", llm.Usage{}, nil
}
func TestResultQuestionMainAgentRestrictsActualRequest(t *testing.T) {
	p := &questionProvider{t: t}
	d := testDB(t)
	defer d.Close()
	id, err := d.CreateExploration("追问只读", "")
	if err != nil {
		t.Fatal(err)
	}
	defer d.Exec(`DELETE FROM explorations WHERE id=$1`, id)
	m := NewMainAgent(p, "fixture", t.TempDir(), nil, 10000, 2)
	m.SetNonStreaming(func() bool { return true })
	resume := 0
	answer, err := m.Chat(WithResultQuestion(t.Context()), 1, 0, nil, d.Exploration(id), "已完成任务", "解释已有结果", nil, nil, func() { resume++ }, nil, nil)
	if err != nil || !p.seen || resume != 0 || answer != "这是已有结果的解释" {
		t.Fatalf("追问结果 %q / %v / resume=%d", answer, err, resume)
	}
}
