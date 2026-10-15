package server

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Autumn-27/artex/agent"
	"github.com/Autumn-27/artex/guard"
	"github.com/Autumn-27/norma/llm"
)

// 真实隔离 PG 回归：首次 Worker 准入被持久代理恢复屏障拒绝，屏障解除后
// 同一个 Worker 槽位必须继续轮询、领取 open 意图并写入终态。
func TestProxyPGWorkerAdmissionFailureRetriesAndSettlesIntent(t *testing.T) {
	s, _, task := switchFixture(t)
	id := switchTaskID(t, task)
	row, err := s.m.pg.GetTask(id)
	if err != nil {
		t.Fatal(err)
	}
	task.Store = s.m.pg.Exploration(row.ExplorationID)
	task.ExpID = row.ExplorationID
	task.Guard = guard.New()
	if _, err := s.m.pg.Exec(`UPDATE tasks SET status='running',paused=false,queued=false WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
	task.Status, task.Paused, task.Queued = "running", false, false
	intentID, err := task.Store.AddIntent(map[string]any{"summary": "准入恢复后执行"}, 1, nil, "planner")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.m.pg.Exec(`INSERT INTO task_operation_activity(task_id,active_count,operation_owners,switch_token) VALUES($1,0,'{}'::jsonb,'recovery-block') ON CONFLICT(task_id) DO UPDATE SET active_count=0,operation_owners='{}'::jsonb,switch_token='recovery-block'`, id); err != nil {
		t.Fatal(err)
	}
	var workerCalls atomic.Int32
	worker := agent.NewWorker(retestProvider{complete: func(context.Context, llm.CompletionRequest) (llm.Message, string, llm.Usage, error) {
		workerCalls.Add(1)
		return followupText("Worker 已完成准入恢复回归")
	}}, "fixture", s.m.dir, nil, 10000, 2)
	worker.SetNonStreaming(func() bool { return true })
	s.engine.SetAuthoritativeAgentResolver(func(*Task) (*agent.Planner, *agent.Worker) {
		return new(agent.Planner), worker
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	s.engine.Run(ctx, task)
	t.Cleanup(func() { s.engine.StopTask(task.ID) })
	time.Sleep(250 * time.Millisecond)
	node, err := task.Store.GetNode(intentID)
	if err != nil || node == nil || node.State != "open" {
		t.Fatalf("屏障期间意图不应被领取: node=%+v err=%v", node, err)
	}
	if _, err := s.m.pg.Exec(`UPDATE task_operation_activity SET switch_token='' WHERE task_id=$1`, id); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		node, err = task.Store.GetNode(intentID)
		if err == nil && node != nil && node.State == "done" {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if node == nil || node.State != "done" {
		t.Fatalf("准入恢复后 Worker 未领取并写入终态: node=%+v err=%v calls=%d", node, err, workerCalls.Load())
	}
	if workerCalls.Load() == 0 {
		t.Fatal("Worker 没有实际调用模型")
	}
	var events int
	if err := s.m.pg.QueryRow(`SELECT count(*) FROM activity WHERE node_id=$1 AND worker LIKE 'work#%'`, intentID).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if events == 0 {
		t.Fatal("Worker 未写入实际 activity")
	}
}
