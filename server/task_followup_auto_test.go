package server

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/Autumn-27/artex/db"
)

func TestTaskFollowupAutoQueuesBeforePlanner(t *testing.T) {
	m, err := NewManager(t.TempDir(), "")
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	defer restoreConcurrencySetting(t, m)()
	if err := m.SetConcurrency(true, 1); err != nil {
		t.Fatal(err)
	}
	holder, err := m.CreateTask("占用并发槽", "旧工作", nil, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer m.DeleteTask(holder.ID, DeleteTaskOptions{})
	s := newAdmissionTestServer(m, nil)
	s.engine.started.Store(holder.ID, true)
	for _, tool := range s.orchestrationTools() {
		if tool.Name() != "add_task_hint" && tool.Name() != "add_task_goal" {
			continue
		}
		t.Run(tool.Name(), func(t *testing.T) {
			task, err := m.CreateTask("已完成后排队", "保留原目标", nil, 0, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer m.DeleteTask(task.ID, DeleteTaskOptions{})
			if err := m.SetTaskStatus(task.ID, "done"); err != nil {
				t.Fatal(err)
			}
			res, err := tool.Call(t.Context(), json.RawMessage(fmt.Sprintf(`{"task_id":%q,"text":"新增核验"}`, task.ID)), nil)
			if err != nil || res.IsError {
				t.Fatalf("追加排队失败: %s %v", res.Flatten(), err)
			}
			tid, _ := parseTaskID(task.ID)
			row, err := m.pg.GetTask(tid)
			if err != nil || row.Status != "running" || !row.Queued || row.CompletedAt != nil || !s.engine.IsPaused(task.ID) {
				t.Fatalf("未先取消完成并排队: %+v %v", row, err)
			}
			if won, err := m.completeTaskIfIdle(task.ID); err != nil || won {
				t.Fatalf("旧完成门关闭追加工作: %v %v", won, err)
			}
			goals, err := task.Store.ListByKind(db.KindGoal, 100)
			if err != nil || len(goals) != 1 || goals[0].State != "open" {
				t.Fatalf("排队丢失目标: %v %v", goals, err)
			}
		})
	}
}

func TestTaskFollowupAutoAdmissionFailureRetainsIdentity(t *testing.T) {
	m, err := NewManager(t.TempDir(), "")
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	s := newAdmissionTestServer(m, nil)
	for _, tool := range s.orchestrationTools() {
		if tool.Name() != "add_task_hint" && tool.Name() != "add_task_goal" {
			continue
		}
		t.Run(tool.Name(), func(t *testing.T) {
			task, err := m.CreateTask("准入冲突", "旧成果", nil, 0, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer m.DeleteTask(task.ID, DeleteTaskOptions{})
			if err := m.SetTaskStatus(task.ID, "done"); err != nil {
				t.Fatal(err)
			}
			tid, _ := parseTaskID(task.ID)
			// 模拟另一完成写入发生在缓存快照与调度提交之间。
			if err := m.pg.SetStatus(tid, "timeout"); err != nil {
				t.Fatal(err)
			}
			res, err := tool.Call(t.Context(), json.RawMessage(fmt.Sprintf(`{"task_id":%q,"text":"新增核验"}`, task.ID)), nil)
			if err != nil || !res.IsError || !strings.Contains(res.Flatten(), "ids=[") || !strings.Contains(res.Flatten(), "不要重复登记") {
				t.Fatalf("准入失败假报成功或丢 ID: %s %v", res.Flatten(), err)
			}
			goals, err := task.Store.ListByKind(db.KindGoal, 100)
			if err != nil || len(goals) != 1 || goals[0].State != "open" {
				t.Fatalf("失败丢失追加目标: %v %v", goals, err)
			}
			row, err := m.pg.GetTask(tid)
			if err != nil || row.Status != "timeout" {
				t.Fatalf("覆盖并发状态: %+v %v", row, err)
			}
		})
	}
}

func TestTaskFollowupToolSeedPreservesPermission(t *testing.T) {
	m, err := NewManager(t.TempDir(), "")
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	s := newAdmissionTestServer(m, nil)
	s.seedOrchestrationTools()
	var enabled bool
	var agents, description string
	if err := m.pg.QueryRow(`SELECT enabled,agents::text,description FROM tools WHERE key='add_task_goal'`).Scan(&enabled, &agents, &description); err != nil {
		t.Fatal(err)
	}
	if !enabled || agents != `["auto"]` {
		t.Fatalf("首次未仅默认绑定 Auto: %v %s", enabled, agents)
	}
	defer m.pg.Exec(`UPDATE tools SET enabled=$1,agents=$2::jsonb,description=$3 WHERE key='add_task_goal'`, enabled, agents, description)
	if _, err := m.pg.Exec(`UPDATE tools SET enabled=false,agents='[]'::jsonb WHERE key='add_task_goal'; DELETE FROM settings WHERE key='tool_schema_refresh_task_followup_v1'`); err != nil {
		t.Fatal(err)
	}
	s.seedOrchestrationTools()
	if err := m.pg.QueryRow(`SELECT enabled,agents::text FROM tools WHERE key='add_task_goal'`).Scan(&enabled, &agents); err != nil {
		t.Fatal(err)
	}
	if enabled || agents != `[]` {
		t.Fatalf("升级恢复了用户禁用或解绑: %v %s", enabled, agents)
	}
}
