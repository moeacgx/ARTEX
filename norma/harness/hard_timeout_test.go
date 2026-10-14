package harness

import (
	"context"
	"encoding/json"
	"errors"
	"iter"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Autumn-27/norma/llm"
	"github.com/Autumn-27/norma/permission"
	"github.com/Autumn-27/norma/tool"
)

type blockingPreCompactor struct {
	entered chan struct{}
}

func (c *blockingPreCompactor) Pre(ctx context.Context, msgs []llm.Message, _ int) []llm.Message {
	close(c.entered)
	<-ctx.Done()
	return msgs
}

func (*blockingPreCompactor) Reactive(context.Context, []llm.Message) ([]llm.Message, bool) {
	return nil, false
}

func (*blockingPreCompactor) IsOverflow(error) bool { return false }

func blockingModel(calls *atomic.Int32) func(context.Context, llm.CompletionRequest) iter.Seq2[llm.StreamEvent, error] {
	return func(ctx context.Context, _ llm.CompletionRequest) iter.Seq2[llm.StreamEvent, error] {
		calls.Add(1)
		return func(yield func(llm.StreamEvent, error) bool) {
			<-ctx.Done()
			yield(llm.StreamEvent{}, context.Cause(ctx))
		}
	}
}

func TestHardMaxDurationCoversCompactorPre(t *testing.T) {
	compactor := &blockingPreCompactor{entered: make(chan struct{})}
	var calls atomic.Int32
	term := drain(t, QueryInput{
		Messages:        []llm.Message{llm.UserText("go")},
		Tools:           tool.NewRegistry(),
		Compactor:       compactor,
		HardMaxDuration: 30 * time.Millisecond,
	}, QueryDeps{CallModel: blockingModel(&calls)})
	if term.Reason != ReasonTimeout || !errors.Is(term.Err, ErrHardDeadlineExceeded) {
		t.Fatalf("terminal=%+v, want hard timeout", term)
	}
	if calls.Load() != 0 {
		t.Fatalf("model calls=%d, want 0", calls.Load())
	}
}

func TestHardMaxDurationCoversStreamingModel(t *testing.T) {
	var calls atomic.Int32
	term := drain(t, QueryInput{
		Messages:        []llm.Message{llm.UserText("go")},
		Tools:           tool.NewRegistry(),
		HardMaxDuration: 30 * time.Millisecond,
	}, QueryDeps{CallModel: blockingModel(&calls)})
	if term.Reason != ReasonTimeout || !errors.Is(term.Err, ErrHardDeadlineExceeded) {
		t.Fatalf("terminal=%+v, want hard timeout", term)
	}
	if calls.Load() != 1 {
		t.Fatalf("model calls=%d, want 1", calls.Load())
	}
}

func TestHardTimeoutDoesNotReplayToolSideEffect(t *testing.T) {
	var calls atomic.Int32
	var effects atomic.Int32
	sideEffect := tool.Build(tool.Spec{
		Name:   "side_effect",
		Schema: map[string]any{"type": "object"},
		Run: func(context.Context, json.RawMessage, *tool.ToolContext) (tool.Result, error) {
			effects.Add(1)
			return tool.Text("done"), nil
		},
	})
	model := func(ctx context.Context, _ llm.CompletionRequest) iter.Seq2[llm.StreamEvent, error] {
		call := calls.Add(1)
		if call == 1 {
			return func(yield func(llm.StreamEvent, error) bool) {
				for _, event := range toolTurn("once", "side_effect", `{}`) {
					if !yield(event, nil) {
						return
					}
				}
			}
		}
		return func(yield func(llm.StreamEvent, error) bool) {
			<-ctx.Done()
			yield(llm.StreamEvent{}, context.Cause(ctx))
		}
	}
	term := drain(t, QueryInput{
		Messages:        []llm.Message{llm.UserText("go")},
		Tools:           tool.NewRegistry(sideEffect),
		PermissionMode:  permission.ModeBypass,
		HardMaxDuration: 40 * time.Millisecond,
	}, QueryDeps{CallModel: model})
	if term.Reason != ReasonTimeout {
		t.Fatalf("reason=%s, want timeout", term.Reason)
	}
	if effects.Load() != 1 {
		t.Fatalf("side effects=%d, want exactly one", effects.Load())
	}
	if calls.Load() != 2 {
		t.Fatalf("model calls=%d, want initial tool turn plus one blocked follow-up", calls.Load())
	}
}

func TestHardTimeoutWaitsForStartedToolExit(t *testing.T) {
	toolExited := make(chan struct{})
	delayedExit := tool.Build(tool.Spec{
		Name:   "delayed_exit",
		Schema: map[string]any{"type": "object"},
		Run: func(ctx context.Context, _ json.RawMessage, _ *tool.ToolContext) (tool.Result, error) {
			<-ctx.Done()
			time.Sleep(45 * time.Millisecond)
			close(toolExited)
			return tool.Text("stopped"), nil
		},
	})
	model := &scriptedModel{turns: [][]llm.StreamEvent{toolTurn("slow", "delayed_exit", `{}`)}}
	started := time.Now()
	term := drain(t, QueryInput{
		Tools:              tool.NewRegistry(delayedExit),
		PermissionMode:     permission.ModeBypass,
		HardMaxDuration:    20 * time.Millisecond,
		HardCleanupTimeout: 200 * time.Millisecond,
	}, QueryDeps{CallModel: model.call})
	if term.Reason != ReasonTimeout || term.Err == nil {
		t.Fatalf("terminal=%+v, want timeout", term)
	}
	select {
	case <-toolExited:
	default:
		t.Fatal("query returned before the started tool actually exited")
	}
	if elapsed := time.Since(started); elapsed < 55*time.Millisecond {
		t.Fatalf("query returned too early after %s", elapsed)
	}
}

func TestHardTimeoutReportsBoundedCleanupFailure(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	stuck := tool.Build(tool.Spec{
		Name:   "stuck_cleanup",
		Schema: map[string]any{"type": "object"},
		Run: func(ctx context.Context, _ json.RawMessage, _ *tool.ToolContext) (tool.Result, error) {
			<-ctx.Done()
			<-release
			return tool.Text("released"), nil
		},
	})
	model := &scriptedModel{turns: [][]llm.StreamEvent{toolTurn("stuck", "stuck_cleanup", `{}`)}}
	term := drain(t, QueryInput{
		Tools:              tool.NewRegistry(stuck),
		PermissionMode:     permission.ModeBypass,
		HardMaxDuration:    20 * time.Millisecond,
		HardCleanupTimeout: 25 * time.Millisecond,
	}, QueryDeps{CallModel: model.call})
	if term.Reason != ReasonTimeout || !errors.Is(term.Err, ErrHardDeadlineCleanup) {
		t.Fatalf("terminal=%+v, want bounded cleanup failure", term)
	}
}

func TestHardTimeoutStopsLateNonStreamingTools(t *testing.T) {
	var effects atomic.Int32
	late := tool.Build(tool.Spec{
		Name:   "late",
		Schema: map[string]any{"type": "object"},
		Run: func(context.Context, json.RawMessage, *tool.ToolContext) (tool.Result, error) {
			effects.Add(1)
			return tool.Text("ran"), nil
		},
	})
	syncModel := func(context.Context, llm.CompletionRequest) (llm.Message, string, llm.Usage, error) {
		time.Sleep(40 * time.Millisecond)
		return llm.Message{Role: llm.RoleAssistant, Content: []llm.ContentBlock{{Type: llm.BlockToolUse, ID: "late", Name: "late", Input: json.RawMessage(`{}`)}}}, "tool_use", llm.Usage{}, nil
	}
	term := drain(t, QueryInput{
		Tools:           tool.NewRegistry(late),
		PermissionMode:  permission.ModeBypass,
		NonStreaming:    true,
		HardMaxDuration: 20 * time.Millisecond,
	}, QueryDeps{CallModelSync: syncModel})
	if term.Reason != ReasonTimeout {
		t.Fatalf("reason=%s, want timeout", term.Reason)
	}
	if effects.Load() != 0 {
		t.Fatalf("late tool ran %d times after deadline", effects.Load())
	}
}

func TestHardStreamErrorDoesNotDispatchIncompleteTool(t *testing.T) {
	var effects atomic.Int32
	late := tool.Build(tool.Spec{Name: "late", Schema: map[string]any{"type": "object"},
		Run: func(context.Context, json.RawMessage, *tool.ToolContext) (tool.Result, error) {
			effects.Add(1)
			return tool.Text("ran"), nil
		},
	})
	model := func(context.Context, llm.CompletionRequest) iter.Seq2[llm.StreamEvent, error] {
		return func(yield func(llm.StreamEvent, error) bool) {
			if !yield(llm.StreamEvent{Type: llm.SEToolUseStart, ToolID: "late", ToolName: "late"}, nil) {
				return
			}
			if !yield(llm.StreamEvent{Type: llm.SEToolInputJSON, Text: `{"partial":`}, nil) {
				return
			}
			yield(llm.StreamEvent{}, context.DeadlineExceeded)
		}
	}
	term := drain(t, QueryInput{Tools: tool.NewRegistry(late), PermissionMode: permission.ModeBypass, HardMaxDuration: time.Second}, QueryDeps{CallModel: model})
	if !errors.Is(term.Err, context.DeadlineExceeded) || effects.Load() != 0 {
		t.Fatalf("terminal=%+v effects=%d", term, effects.Load())
	}
}
