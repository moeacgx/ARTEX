package agent

import (
	"context"
	actool "github.com/Autumn-27/norma/tool"
)

type resultQuestionKey struct{}

func WithResultQuestion(ctx context.Context) context.Context {
	return context.WithValue(ctx, resultQuestionKey{}, true)
}

// 结果追问只开放已有记录读取，不让新增目标或联网工具恢复执行。
func resultQuestionTools(tools []actool.CoreTool) []actool.CoreTool {
	allowed := map[string]bool{"Read": true, "LS": true, "Glob": true, "Grep": true,
		"graph_overview": true, "list_findings": true, "list_facts": true, "node_detail": true,
		"expand_digest": true, "get_worker_output": true, "get_worker_trace": true,
		"search_all_worker_traces": true, "list_assets": true, "list_untested_assets": true}
	out := make([]actool.CoreTool, 0, len(tools))
	for _, tool := range tools {
		if allowed[tool.Name()] {
			out = append(out, tool)
		}
	}
	return out
}
