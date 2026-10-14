package server

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Autumn-27/artex/db"
)

func controlRecoveryFixture(t *testing.T) (*Server, *http.ServeMux, *Task, *db.Proxy) {
	t.Helper()
	s, _ := proxyIntegration(t)
	p := createRuntimeProxy(t, s, "http://127.0.0.1:19019")
	task := createRuntimeTask(t, s, db.TaskProxyModeFixed, &p.ID)
	task.Status = "created"
	s.m.tasks = map[string]*Task{task.ID: task}
	s.engine = NewEngine(s.m)
	s.ctx = context.Background()
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/tasks/{id}/control", s.control)
	mux.HandleFunc("DELETE /api/tasks/{id}", s.pgDeleteTask)
	s.registerTaskProxyRoutes(mux)
	return s, mux, task, p
}

func orphanTaskActivity(t *testing.T, s *Server, task *Task, p *db.Proxy) {
	t.Helper()
	if _, err := s.m.pg.Exec(`INSERT INTO task_operation_activity(task_id,active_count,operation_owners) VALUES($1,1,'{"2147483647":1}'::jsonb)`, task.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.m.pg.Exec(`INSERT INTO task_proxy_activity(task_id,proxy_id,active_count) VALUES($1,$2,1)`, task.ID, p.ID); err != nil {
		t.Fatal(err)
	}
}

func waitControlRelease(t *testing.T, s *Server, id string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		b := s.m.taskProxyBarrier(id)
		b.mu.Lock()
		busy := b.pending || b.releasing
		b.mu.Unlock()
		if !busy {
			// 等待暂停后异步的并发槽协调退出，再清理测试数据库。
			s.concMu.Lock()
			s.concMu.Unlock()
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("出口回收未结束")
}

func TestProxyPGTaskPauseWithOrphanActivity(t *testing.T) {
	s, mux, task, p := controlRecoveryFixture(t)
	orphanTaskActivity(t, s, task, p)
	code, _, body := proxyAPI(t, mux, "POST", "/api/tasks/"+task.ID+"/control", `{"action":"pause"}`)
	if code != http.StatusOK {
		t.Fatalf("异常占用不应阻止暂停: %d %s", code, body)
	}
	waitControlRelease(t, s, task.ID)
	id, _ := strconv.ParseInt(task.ID, 10, 64)
	row, err := s.m.pg.GetTask(id)
	if err != nil || !row.Paused || !s.engine.IsPaused(task.ID) {
		t.Fatalf("暂停没有持久化: row=%+v err=%v", row, err)
	}
	var operations, references int
	if err := s.m.pg.QueryRow(`SELECT a.active_count,p.active_count FROM task_operation_activity a JOIN task_proxy_activity p USING(task_id) WHERE a.task_id=$1`, id).Scan(&operations, &references); err != nil || operations != 1 || references != 1 {
		t.Fatalf("暂停不得擅自清理异常引用: %d/%d err=%v", operations, references, err)
	}
	code, _, body = proxyAPI(t, mux, "POST", "/api/tasks/"+task.ID+"/proxy/recovery", `{"action":"clear_stale_activity","expected_revision":0}`)
	if code != http.StatusOK {
		t.Fatalf("确认旧会话退出后恢复失败: %d %s", code, body)
	}
	code, _, body = proxyAPI(t, mux, "DELETE", "/api/tasks/"+task.ID, `{}`)
	if code != http.StatusOK {
		t.Fatalf("恢复后仍不能删除: %d %s", code, body)
	}
	if row, err := s.m.pg.GetTask(id); err != nil || row != nil {
		t.Fatalf("删除结果不一致: row=%+v err=%v", row, err)
	}
}

func TestProxyPGTaskPauseWithUnavailableProxy(t *testing.T) {
	s, mux, task, p := controlRecoveryFixture(t)
	if _, err := s.m.pg.Exec(`UPDATE outbound_proxies SET enabled=false WHERE id=$1`, p.ID); err != nil {
		t.Fatal(err)
	}
	code, _, body := proxyAPI(t, mux, "POST", "/api/tasks/"+task.ID+"/control", `{"action":"pause"}`)
	if code != http.StatusOK {
		t.Fatalf("代理不可用不应阻止暂停: %d %s", code, body)
	}
	waitControlRelease(t, s, task.ID)
	if s.engine.inflightCount(task.ID) != 0 {
		t.Fatal("暂停错误地增加了执行引用")
	}
}

func TestProxyPGTaskPauseConflictPreservesExecution(t *testing.T) {
	s, mux, task, _ := controlRecoveryFixture(t)
	ctx := s.engine.execContextFor(context.Background(), task.ID)
	// 模拟另一实例已改变持久状态，本实例仍握有旧任务快照。
	if _, err := s.m.pg.Exec(`UPDATE tasks SET status='done' WHERE id=$1`, task.ID); err != nil {
		t.Fatal(err)
	}
	code, _, body := proxyAPI(t, mux, "POST", "/api/tasks/"+task.ID+"/control", `{"action":"pause"}`)
	if code != http.StatusConflict {
		t.Fatalf("陈旧暂停请求应被拒绝: %d %s", code, body)
	}
	waitControlRelease(t, s, task.ID)
	if ctx.Err() != nil || s.engine.IsPaused(task.ID) {
		t.Fatal("持久化失败的暂停不应取消旧执行或回收出口")
	}
}

func TestProxyPGTaskDeleteReportsActivityConflict(t *testing.T) {
	s, mux, task, p := controlRecoveryFixture(t)
	orphanTaskActivity(t, s, task, p)
	code, out, body := proxyAPI(t, mux, "DELETE", "/api/tasks/"+task.ID, `{}`)
	if code != http.StatusConflict || out["code"] != "task_activity_conflict" || !strings.Contains(body, "活动占用") {
		t.Fatalf("删除占用应返回明确的冲突及恢复提示: %d %s", code, body)
	}
	id, _ := strconv.ParseInt(task.ID, 10, 64)
	if row, err := s.m.pg.GetTask(id); err != nil || row == nil {
		t.Fatalf("拒绝删除后任务应保留: row=%+v err=%v", row, err)
	}
	if s.engine.IsDeleting(task.ID) {
		t.Fatal("失败删除遗留了删除屏障")
	}
}
