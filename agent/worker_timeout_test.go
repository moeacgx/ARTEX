package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Autumn-27/artex/db"
	"github.com/Autumn-27/norma/harness"
	"github.com/Autumn-27/norma/llm"
	"github.com/Autumn-27/norma/tool"
)

func workerTimeoutFixture(t *testing.T, provider llm.Provider, extra ...tool.CoreTool) (*Worker, *db.ExplorationStore, *db.Node) {
	t.Helper()
	d := testDB(t)
	t.Cleanup(func() { _ = d.Close() })
	expID, err := d.CreateExploration("Worker 运行截止隔离测试", "仅访问本地测试服务")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = d.Exec(`DELETE FROM explorations WHERE id=$1`, expID) })
	ts := d.Exploration(expID)
	payload := map[string]any{"summary": "仅访问本地测试服务"}
	intentID, err := ts.AddNode(db.KindIntent, payload, 0, "running", "worker-test", nil)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(payload)
	w := NewWorker(provider, "test-model", t.TempDir(), nil, 0, 0, extra...)
	return w, ts, &db.Node{ID: intentID, Payload: raw}
}

// 心跳或空白持续到达时，网络空闲计时不会触发，仍须由整轮预算结束等待。
func TestWorkerHardDeadlineStopsHeartbeatAndNonStreamingWithoutReplay(t *testing.T) {
	for _, nonStreaming := range []bool{false, true} {
		t.Run(fmt.Sprintf("non_streaming_%t", nonStreaming), func(t *testing.T) {
			t.Setenv("ARTEX_WORKER_SETTLEMENT_TIMEOUT", "150ms")
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				_, _ = io.Copy(io.Discard, r.Body)
				if nonStreaming {
					w.Header().Set("content-type", "application/json")
					_, _ = io.WriteString(w, `{"choices":[`)
				} else {
					w.Header().Set("content-type", "text/event-stream")
				}
				w.(http.Flusher).Flush()
				ticker := time.NewTicker(10 * time.Millisecond)
				defer ticker.Stop()
				for {
					select {
					case <-r.Context().Done():
						return
					case <-ticker.C:
						chunk := ": heartbeat\n\n"
						if nonStreaming {
							chunk = " "
						}
						_, _ = io.WriteString(w, chunk)
						w.(http.Flusher).Flush()
					}
				}
			}))
			defer server.Close()
			provider, err := timeoutTestConfig(server.URL, time.Second, 100*time.Millisecond).NewProvider()
			if err != nil {
				t.Fatal(err)
			}
			worker, ts, intent := workerTimeoutFixture(t, provider)
			worker.SetRunTimeout(250 * time.Millisecond)
			worker.SetNonStreaming(func() bool { return nonStreaming })
			guard, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			var results []db.Activity
			reason, _, err := worker.Execute(guard, "worker-test", ts.ID(), nil, ts, intent, nil, func(a db.Activity) {
				if a.Kind == "result" {
					results = append(results, a)
				}
			}, nil, nil)
			if guard.Err() != nil {
				t.Fatalf("运行未被自身预算中断，已耗尽外部测试截止: reason=%s err=%v", reason, err)
			}
			if reason != harness.ReasonTimeout || !errors.Is(err, harness.ErrHardDeadlineExceeded) {
				t.Fatalf("reason=%s err=%v，期望明确整轮超时", reason, err)
			}
			if got := requests.Load(); got != 1 {
				t.Fatalf("模型请求=%d，超时后不得重放", got)
			}
			if len(results) != 1 {
				t.Fatalf("超时结果数=%d，期望立即可见的唯一终态", len(results))
			}
		})
	}
}

func TestWorkerSoftBudgetStillWritesFactsDuringSettlement(t *testing.T) {
	t.Setenv("ARTEX_WORKER_SETTLEMENT_TIMEOUT", "500ms")
	var turn int
	var intentID int64
	provider := captureUsageProvider{stream: func(_ context.Context, yield func(llm.StreamEvent, error) bool) {
		turn++
		if turn == 3 {
			if yield(llm.StreamEvent{Type: llm.SETextDelta, Text: "已写回测试事实"}, nil) {
				yield(llm.StreamEvent{Type: llm.SEMessageDelta, StopReason: "end_turn"}, nil)
			}
			return
		}
		name, input := "WaitForBudget", "{}"
		if turn == 2 {
			name, input = "record_fact", fmt.Sprintf(`{"intent_id":%d,"summary":"测试已观察到预算到达","detail":"在正常收尾中保存","evidence":"本地测试工具返回","confidence":"observed"}`, intentID)
		}
		for _, ev := range []llm.StreamEvent{
			{Type: llm.SEToolUseStart, ToolID: fmt.Sprintf("call-%d", turn), ToolName: name},
			{Type: llm.SEToolInputJSON, Text: input},
			{Type: llm.SEMessageDelta, StopReason: "tool_use"},
		} {
			if !yield(ev, nil) {
				return
			}
		}
	}}
	wait := tool.Build(tool.Spec{Name: "WaitForBudget", Schema: map[string]any{"type": "object"}, Run: func(ctx context.Context, _ json.RawMessage, _ *tool.ToolContext) (tool.Result, error) {
		<-ctx.Done()
		return tool.Text("已观察到预算到达"), nil
	}})
	worker, ts, intent := workerTimeoutFixture(t, provider, wait)
	intentID = intent.ID
	worker.SetRunTimeout(300 * time.Millisecond)
	reason, writes, err := worker.Execute(t.Context(), "worker-test", ts.ID(), nil, ts, intent, nil, nil, nil, nil)
	if err != nil || reason != harness.ReasonTimeout {
		t.Fatalf("正常收尾 reason=%s err=%v", reason, err)
	}
	facts, err := ts.ListByKind(db.KindFact, 10)
	if err != nil || len(facts) != 1 || writes.Facts != 1 {
		t.Fatalf("收尾事实未保存: facts=%d writes=%+v err=%v", len(facts), writes, err)
	}
}

func TestWorkerHardDeadlineWaitsForActualToolExitAfterVisibleResult(t *testing.T) {
	t.Setenv("ARTEX_WORKER_SETTLEMENT_TIMEOUT", "60ms")
	started, exited := make(chan struct{}), make(chan struct{})
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	var modelCalls atomic.Int32
	provider := captureUsageProvider{stream: func(_ context.Context, yield func(llm.StreamEvent, error) bool) {
		modelCalls.Add(1)
		for _, ev := range []llm.StreamEvent{
			{Type: llm.SEToolUseStart, ToolID: "one", ToolName: "SlowCleanup"},
			{Type: llm.SEToolInputJSON, Text: "{}"},
			{Type: llm.SEMessageDelta, StopReason: "tool_use"},
		} {
			if !yield(ev, nil) {
				return
			}
		}
	}}
	probe := tool.Build(tool.Spec{Name: "SlowCleanup", Schema: map[string]any{"type": "object"}, Run: func(ctx context.Context, _ json.RawMessage, _ *tool.ToolContext) (tool.Result, error) {
		close(started)
		<-ctx.Done()
		<-release
		close(exited)
		return tool.Text("已真正退出"), nil
	}})
	worker, ts, intent := workerTimeoutFixture(t, provider, probe)
	worker.SetRunTimeout(time.Second)
	resultVisible := make(chan struct{}, 1)
	type outcome struct {
		reason harness.TerminalReason
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		reason, _, err := worker.Execute(t.Context(), "worker-test", ts.ID(), nil, ts, intent, nil, func(a db.Activity) {
			if a.Kind == "result" {
				resultVisible <- struct{}{}
			}
		}, nil, nil)
		done <- outcome{reason, err}
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("工具没有开始")
	}
	// SDK 有界清理窗口为 10 秒，超限结果必须先可见，之后宿主继续等真退出。
	select {
	case <-resultVisible:
	case <-time.After(12 * time.Second):
		t.Fatal("工具未退出时超时结果不可见")
	}
	select {
	case out := <-done:
		t.Fatalf("工具未退出但 Worker 提前释放: %+v", out)
	default:
	}
	close(release)
	select {
	case out := <-done:
		if out.reason != harness.ReasonTimeout || !errors.Is(out.err, harness.ErrHardDeadlineExceeded) {
			t.Fatalf("硬截止终态丢失: %+v", out)
		}
	case <-time.After(time.Second):
		t.Fatal("工具退出后 Worker 仍未返回")
	}
	select {
	case <-exited:
	default:
		t.Fatal("Worker 未确认工具真实退出")
	}
	if got := modelCalls.Load(); got != 1 {
		t.Fatalf("模型调用=%d，超时或清理期间发生了重复调用", got)
	}
}

func TestWorkerUnlimitedCancellationWaitsForToolExit(t *testing.T) {
	t.Setenv("ARTEX_WORKER_SETTLEMENT_TIMEOUT", "5ms")
	started, release := make(chan struct{}), make(chan struct{})
	exited, canceled := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	provider := captureUsageProvider{stream: func(_ context.Context, yield func(llm.StreamEvent, error) bool) {
		for _, ev := range []llm.StreamEvent{
			{Type: llm.SEToolUseStart, ToolID: "one", ToolName: "PauseCleanup"},
			{Type: llm.SEToolInputJSON, Text: "{}"},
			{Type: llm.SEMessageDelta, StopReason: "tool_use"},
		} {
			if !yield(ev, nil) {
				return
			}
		}
	}}
	probe := tool.Build(tool.Spec{Name: "PauseCleanup", Schema: map[string]any{"type": "object"}, Run: func(ctx context.Context, _ json.RawMessage, _ *tool.ToolContext) (tool.Result, error) {
		close(started)
		<-ctx.Done()
		close(canceled)
		<-release
		close(exited)
		return tool.Text("已退出"), nil
	}})
	worker, ts, intent := workerTimeoutFixture(t, provider, probe)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	type outcome struct {
		reason harness.TerminalReason
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		reason, _, err := worker.Execute(ctx, "worker-test", ts.ID(), nil, ts, intent, nil, nil, nil, nil)
		done <- outcome{reason, err}
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("工具没有开始")
	}
	select {
	case <-canceled:
		t.Fatal("明确不限的运行被收尾宽限意外取消")
	case <-time.After(30 * time.Millisecond):
	}
	cancel()
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("工具未收到暂停取消")
	}
	select {
	case out := <-done:
		t.Fatalf("暂停后未等真实退出即返回: %+v", out)
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	select {
	case out := <-done:
		if out.reason == harness.ReasonTimeout || !errors.Is(out.err, context.Canceled) || errors.Is(out.err, harness.ErrHardDeadlineExceeded) {
			t.Fatalf("取消原因丢失或被误分类: %+v", out)
		}
	case <-time.After(time.Second):
		t.Fatal("工具退出后取消未完成")
	}
	select {
	case <-exited:
	default:
		t.Fatal("真实退出尚未发生")
	}
}

func TestWorkerHardDeadlineIncludesToolAugmentation(t *testing.T) {
	t.Setenv("ARTEX_WORKER_SETTLEMENT_TIMEOUT", "30ms")
	var modelCalls atomic.Int32
	provider := captureUsageProvider{stream: func(_ context.Context, yield func(llm.StreamEvent, error) bool) {
		modelCalls.Add(1)
		yield(llm.StreamEvent{Type: llm.SEMessageDelta, StopReason: "end_turn"}, nil)
	}}
	worker, ts, intent := workerTimeoutFixture(t, provider)
	worker.SetRunTimeout(30 * time.Millisecond)
	previous := ToolAugment
	t.Cleanup(func() { ToolAugment = previous })
	var cleanup atomic.Bool
	ToolAugment = func(ctx context.Context, _ string) ([]tool.CoreTool, DeferredInfo, func()) {
		<-ctx.Done()
		return nil, DeferredInfo{}, func() { cleanup.Store(true) }
	}
	guard, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	var results []db.Activity
	reason, _, err := worker.Execute(guard, "worker-test", ts.ID(), nil, ts, intent, nil, func(a db.Activity) {
		if a.Kind == "result" {
			results = append(results, a)
		}
	}, nil, nil)
	if guard.Err() != nil || reason != harness.ReasonTimeout || !errors.Is(err, harness.ErrHardDeadlineExceeded) {
		t.Fatalf("工具装配未受整轮截止约束: reason=%s err=%v", reason, err)
	}
	if modelCalls.Load() != 0 || !cleanup.Load() {
		t.Fatalf("过期装配后仍启动模型或未清理: calls=%d cleanup=%t", modelCalls.Load(), cleanup.Load())
	}
	if len(results) != 1 {
		t.Fatalf("工具装配超时结果数=%d，期望可见唯一终态", len(results))
	}
	result := results[0]
	if result.NodeID == nil || *result.NodeID != intent.ID || result.Worker != "worker-test" || !result.IsError || !strings.Contains(result.Detail, "硬截止") {
		t.Fatalf("工具装配超时未明确标明执行者、意图及原因: %+v", result)
	}
}
