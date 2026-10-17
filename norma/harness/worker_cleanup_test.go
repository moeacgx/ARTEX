package harness

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/Autumn-27/norma/llm"
	"github.com/Autumn-27/norma/permission"
	"github.com/Autumn-27/norma/tool"
)

// 运行预算为零仅代表不限时，不能跳过暂停时对已启动工具的真实退出等待。
func TestUnlimitedCancellationRetainsPendingToolExit(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	exited := make(chan struct{})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	stuck := tool.Build(tool.Spec{Name: "unlimited_tool", Schema: map[string]any{"type": "object"},
		Run: func(ctx context.Context, _ json.RawMessage, _ *tool.ToolContext) (tool.Result, error) {
			close(started)
			<-ctx.Done()
			<-release
			close(exited)
			return tool.Text("已退出"), nil
		},
	})
	model := &scriptedModel{turns: [][]llm.StreamEvent{toolTurn("once", "unlimited_tool", `{}`)}}
	terminal := make(chan *Terminal, 1)
	go func() {
		for ev := range Query(ctx, QueryInput{Tools: tool.NewRegistry(stuck), PermissionMode: permission.ModeBypass, HardCleanupTimeout: 30 * time.Millisecond}, QueryDeps{CallModel: model.call}) {
			if ev.Terminal != nil {
				terminal <- ev.Terminal
			}
		}
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("工具未启动")
	}
	cancel()
	var term *Terminal
	select {
	case term = <-terminal:
	case <-time.After(time.Second):
		t.Fatal("取消后没有有界返回清理结果")
	}
	if !errors.Is(term.Err, ErrHardDeadlineCleanup) || errors.Is(term.Err, ErrHardDeadlineExceeded) {
		t.Fatalf("终态=%+v，期望待退出清理状态且不冒充硬截止", term)
	}
	waitCtx, stop := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer stop()
	if err := WaitForToolExit(waitCtx, term.Err); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("退出等待提前成功: %v", err)
	}
	select {
	case <-exited:
		t.Fatal("工具在显式释放前退出")
	default:
	}
	close(release)
	finalCtx, stopFinal := context.WithTimeout(t.Context(), time.Second)
	defer stopFinal()
	if err := WaitForToolExit(finalCtx, term.Err); err != nil {
		t.Fatalf("真实退出后仍未解除等待: %v", err)
	}
	select {
	case <-exited:
	default:
		t.Fatal("退出等待成功但工具仍未退出")
	}
}

func TestUnlimitedNormalToolTurnStillCompletes(t *testing.T) {
	run := tool.Build(tool.Spec{Name: "unlimited_tool", Schema: map[string]any{"type": "object"},
		Run: func(ctx context.Context, _ json.RawMessage, _ *tool.ToolContext) (tool.Result, error) {
			if ctx.Err() != nil {
				t.Error("正常工具运行提前被关闭")
			}
			return tool.Text("已完成"), nil
		},
	})
	model := &scriptedModel{turns: [][]llm.StreamEvent{toolTurn("once", "unlimited_tool", `{}`), textTurn("完成")}}
	term := drain(t, QueryInput{Tools: tool.NewRegistry(run), PermissionMode: permission.ModeBypass}, QueryDeps{CallModel: model.call})
	if term.Reason != ReasonCompleted || term.Err != nil || term.Text != "完成" {
		t.Fatalf("正常不限执行受影响: %+v", term)
	}
}

// 消费方收到切换工具结果后可提前停止，但并发工具的待退出信息不能被丢弃。
func TestConsumerStopWaitsForConcurrentToolExit(t *testing.T) {
	for _, nonStreaming := range []bool{false, true} {
		name := "streaming"
		if nonStreaming {
			name = "non_streaming"
		}
		t.Run(name, func(t *testing.T) {
			started, canceled, exited := make(chan struct{}), make(chan struct{}), make(chan struct{})
			release := make(chan struct{})
			defer func() {
				select {
				case <-release:
				default:
					close(release)
				}
			}()
			safe := func(json.RawMessage) bool { return true }
			stuck := tool.Build(tool.Spec{Name: "concurrent_tool", Schema: map[string]any{"type": "object"}, Concurrent: safe,
				Run: func(ctx context.Context, _ json.RawMessage, _ *tool.ToolContext) (tool.Result, error) {
					close(started)
					<-ctx.Done()
					close(canceled)
					<-release
					close(exited)
					return tool.Text("并发工具已真正退出"), nil
				},
			})
			trigger := tool.Build(tool.Spec{Name: "stop_consumer", Schema: map[string]any{"type": "object"}, Concurrent: safe,
				Run: func(ctx context.Context, _ json.RawMessage, _ *tool.ToolContext) (tool.Result, error) {
					select {
					case <-started:
						return tool.Text("请求停止消费"), nil
					case <-ctx.Done():
						return tool.Result{}, ctx.Err()
					}
				},
			})
			events := []llm.StreamEvent{
				{Type: llm.SEToolUseStart, ToolID: "pending", ToolName: "concurrent_tool"},
				{Type: llm.SEToolInputJSON, Text: "{}"},
				{Type: llm.SEToolUseStart, ToolID: "stop", ToolName: "stop_consumer"},
				{Type: llm.SEToolInputJSON, Text: "{}"},
				{Type: llm.SEMessageDelta, StopReason: "tool_use"},
			}
			model := &scriptedModel{turns: [][]llm.StreamEvent{events}}
			deps := QueryDeps{CallModel: model.call, CallModelSync: func(context.Context, llm.CompletionRequest) (llm.Message, string, llm.Usage, error) {
				return llm.Message{Role: llm.RoleAssistant, Content: []llm.ContentBlock{
					{Type: llm.BlockToolUse, ID: "pending", Name: "concurrent_tool", Input: json.RawMessage(`{}`)},
					{Type: llm.BlockToolUse, ID: "stop", Name: "stop_consumer", Input: json.RawMessage(`{}`)},
				}}, "tool_use", llm.Usage{}, nil
			}}
			consumed, returned := make(chan struct{}), make(chan struct{})
			go func() {
				defer close(returned)
				for ev := range Query(t.Context(), QueryInput{
					Tools: tool.NewRegistry(stuck, trigger), PermissionMode: permission.ModeBypass,
					NonStreaming: nonStreaming, HardMaxDuration: time.Second, HardCleanupTimeout: 30 * time.Millisecond,
				}, deps) {
					if ev.Kind == KindToolResult && ev.ToolResult.ToolUseID == "stop" {
						close(consumed)
						break
					}
				}
			}()
			select {
			case <-consumed:
			case <-time.After(time.Second):
				t.Fatal("没有收到触发停止消费的工具结果")
			}
			select {
			case <-canceled:
			case <-time.After(time.Second):
				t.Fatal("消费停止后并发工具未收到取消")
			}
			select {
			case <-returned:
				t.Fatal("消费提前停止丢失待退出工具，迭代在真实退出前返回")
			case <-time.After(90 * time.Millisecond):
			}
			close(release)
			select {
			case <-returned:
			case <-time.After(time.Second):
				t.Fatal("并发工具真实退出后迭代未返回")
			}
			select {
			case <-exited:
			default:
				t.Fatal("迭代返回时并发工具尚未真实退出")
			}
		})
	}
}
