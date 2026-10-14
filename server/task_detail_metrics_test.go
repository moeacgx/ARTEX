package server

import (
	"net/http"
	"testing"
	"time"

	"github.com/Autumn-27/artex/db"
)

func TestProxyPGTaskDetailMetricsMatchPersistedAndLiveActivity(t *testing.T) {
	s, _ := proxyIntegration(t)
	task := createRuntimeTask(t, s, db.TaskProxyModeDirect, nil)
	row, err := s.m.pg.GetTask(switchTaskID(t, task))
	if err != nil {
		t.Fatal(err)
	}
	task.ExpID = row.ExplorationID
	s.m.tasks = map[string]*Task{task.ID: task}
	s.engine = NewEngine(s.m)
	s.m.pg.SetMaxOpenConns(1)
	stamp := time.Now().Add(-time.Minute).Truncate(time.Second)
	if _, err = s.m.pg.Exec(`INSERT INTO exploration_nodes(exploration_id,kind,payload,state)
SELECT $1,'goal','{}',CASE WHEN n<=9 THEN 'met' ELSE 'open' END FROM generate_series(1,11) n`, task.ExpID); err != nil {
		t.Fatal(err)
	}
	if _, err = s.m.pg.Exec(`INSERT INTO exploration_nodes(exploration_id,kind,payload,state)
VALUES($1,'intent','{}','running'),($1,'intent','{}','done')`, task.ExpID); err != nil {
		t.Fatal(err)
	}
	if _, err = s.m.pg.Exec(`INSERT INTO activity(exploration_id,kind,input_tokens,output_tokens,created_at)
VALUES($1,'result',17,5,$2)`, task.ExpID, stamp); err != nil {
		t.Fatal(err)
	}
	// Planner 活动引用不能混入运行中意图计数，单连接业务池仍须可读详情。
	_, finish, ok := s.engine.beginTaskOperationContext(t.Context(), task.ID)
	if !ok {
		t.Fatal("合成操作准入失败")
	}
	defer finish()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/tasks/{id}", s.getTask)
	mux.HandleFunc("GET /api/tasks", s.listTasks)
	code, detail, body := proxyAPI(t, mux, "GET", "/api/tasks/"+task.ID, "")
	if code != 200 || detail["goals_total"] != float64(11) || detail["goals_met"] != float64(9) || detail["in_flight"] != float64(1) || detail["last_activity_unix"] != float64(stamp.Unix()) {
		t.Fatalf("详情未读取权威统计: %d %s", code, body)
	}
	tokens := detail["tokens"].(map[string]any)
	if tokens["input_tokens"] != float64(17) || tokens["output_tokens"] != float64(5) {
		t.Fatalf("详情未读取令牌统计: %s", body)
	}
	code, list, body := proxyAPI(t, mux, "GET", "/api/tasks", "")
	if code != 200 {
		t.Fatalf("列表: %d %s", code, body)
	}
	listed := list["tasks"].([]any)[0].(map[string]any)
	for _, field := range []string{"goals_total", "goals_met", "in_flight", "last_activity_unix"} {
		if detail[field] != listed[field] {
			t.Errorf("详情与列表 %s 不一致: %v %v", field, detail[field], listed[field])
		}
	}
	live := stamp.Unix() + 10
	s.engine.lastAct.Store(task.ID, live)
	_, detail, body = proxyAPI(t, mux, "GET", "/api/tasks/"+task.ID, "")
	if detail["last_activity_unix"] != float64(live) {
		t.Fatalf("详情未合并较新的心跳: %s", body)
	}
	if _, err = s.m.pg.Exec(`UPDATE exploration_nodes SET state='done' WHERE exploration_id=$1 AND kind='intent'`, task.ExpID); err != nil {
		t.Fatal(err)
	}
	_, detail, body = proxyAPI(t, mux, "GET", "/api/tasks/"+task.ID, "")
	if detail["in_flight"] != float64(0) || s.engine.inflightCount(task.ID) != 1 {
		t.Fatalf("Planner活动不能被显示为运行中意图: %s", body)
	}
}

func TestProxyPGTaskMetricsReadFailureDoesNotReturnZero(t *testing.T) {
	s, _ := proxyIntegration(t)
	task := createRuntimeTask(t, s, db.TaskProxyModeDirect, nil)
	s.m.tasks = map[string]*Task{task.ID: task}
	s.engine = NewEngine(s.m)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/tasks/{id}", s.getTask)
	mux.HandleFunc("GET /api/tasks", s.listTasks)
	// 只在本轮隔离数据库注入统计读取故障，结束恢复原表；不清任何历史数据。
	if _, err := s.m.pg.Exec(`ALTER TABLE activity RENAME TO release_metrics_fault_activity`); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := s.m.pg.Exec(`ALTER TABLE release_metrics_fault_activity RENAME TO activity`); err != nil {
			t.Errorf("恢复合成故障表: %v", err)
		}
	}()
	for _, path := range []string{"/api/tasks/" + task.ID, "/api/tasks"} {
		code, _, body := proxyAPI(t, mux, "GET", path, "")
		if code != 503 {
			t.Errorf("统计失败却返回零值成功 %s: %d %s", path, code, body)
		}
	}
}
