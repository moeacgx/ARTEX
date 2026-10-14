package agent

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"

	"github.com/Autumn-27/artex/db"
	"github.com/Autumn-27/norma/agentcore"
	"github.com/Autumn-27/norma/harness"
	"github.com/Autumn-27/norma/llm"
	"github.com/Autumn-27/norma/permission"
	"github.com/Autumn-27/norma/tool"
)

// 穿过真实 SDK 调用与活动记录；切换意图返回后不开始第二模型轮或后续副作用工具。
func TestProxyRunStopAfterToolResult(t *testing.T) {
	ctx, _ := WithProxyRunStop(t.Context())
	var turns, calls atomic.Int32
	provider := captureUsageProvider{stream: func(_ context.Context, yield func(llm.StreamEvent, error) bool) {
		turns.Add(1)
		for _, event := range []llm.StreamEvent{{Type: llm.SEToolUseStart, ToolID: "switch", ToolName: "request_proxy_failover"}, {Type: llm.SEToolInputJSON, Text: `{}`}, {Type: llm.SEMessageDelta, StopReason: "tool_use"}} {
			if !yield(event, nil) {
				return
			}
		}
	}}
	request := tool.Build(tool.Spec{Name: "request_proxy_failover", Schema: map[string]any{"type": "object"}, Run: func(ctx context.Context, _ json.RawMessage, _ *tool.ToolContext) (tool.Result, error) {
		calls.Add(1)
		RequestProxyRunStop(ctx)
		return tool.Text(`{"accepted":true,"end_turn":true}`), nil
	}})
	var recorded bool
	_, reason, err := captureRun(ctx, agentcore.Options{Provider: provider, Tools: []tool.CoreTool{request}, PermissionMode: permission.ModeBypass, WorkingDir: t.TempDir(), MaxTurns: 3}, "合成切换", func(a db.Activity) {
		if a.Kind == "tool_result" && a.ToolUseID == "switch" {
			recorded = true
		}
	})
	if err != nil || reason != harness.ReasonHookStopped || turns.Load() != 1 || calls.Load() != 1 || !recorded {
		t.Fatalf("未在真实工具结果处退出: reason=%s turns=%d calls=%d recorded=%v err=%v", reason, turns.Load(), calls.Load(), recorded, err)
	}
}
