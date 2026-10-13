package server

import (
	"github.com/Autumn-27/artex/db"
	"testing"
	"time"
)

func TestTaskFollowupCompletionRejectsPendingWork(t *testing.T) {
	m, err := NewManager(t.TempDir(), "")
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	task, err := m.CreateTask("追加完成屏障", "旧目标", nil, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer m.DeleteTask(task.ID, DeleteTaskOptions{})
	if err = m.SetTaskStatus(task.ID, "running"); err != nil {
		t.Fatal(err)
	}
	id, err := task.Store.AddIntent(map[string]any{"summary": "新的验收范围"}, 1, nil, "human")
	if err != nil {
		t.Fatal(err)
	}
	if won, err := m.completeTaskIfIdle(task.ID); err != nil || won {
		t.Fatalf("未完成目标被旧规划关闭: %v %v", won, err)
	}
	goals, _ := task.Store.ListByKind(db.KindGoal, 100)
	for _, g := range goals {
		if err = task.Store.SetNodeState(g.ID, "met"); err != nil {
			t.Fatal(err)
		}
	}
	if won, err := m.completeTaskIfIdle(task.ID); err != nil || won {
		t.Fatalf("未退出 Worker 被提前关闭: %v %v", won, err)
	}
	if err = task.Store.SetNodeState(id, "done"); err != nil {
		t.Fatal(err)
	}
	if won, err := m.completeTaskIfIdle(task.ID); err != nil || !won {
		t.Fatalf("验收且排空后未完成: %v %v", won, err)
	}
}

func TestTaskFollowupCompletedRunGetsFreshClock(t *testing.T) {
	m, err := NewManager(t.TempDir(), "")
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	task, err := m.CreateTask("追加预算", "旧目标", nil, 60, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer m.DeleteTask(task.ID, DeleteTaskOptions{})
	tid, _ := parseTaskID(task.ID)
	_, err = m.pg.Exec(`UPDATE tasks SET first_run_at=now()-interval '1 day',deadline_at=now()-interval '1 hour' WHERE id=$1`, tid)
	if err != nil {
		t.Fatal(err)
	}
	if err = m.SetTaskStatus(task.ID, "done"); err != nil {
		t.Fatal(err)
	}
	if err = m.ApplyTaskAdmission(task.ID, "done", "running", false, "resume", false); err != nil {
		t.Fatal(err)
	}
	deadline, err := m.StampTaskFirstRun(task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if deadline <= time.Now().Unix() {
		t.Fatal("追加工作继承了已经过期的旧执行预算")
	}
}
