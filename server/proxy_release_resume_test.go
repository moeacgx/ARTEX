package server

import (
	"context"
	"errors"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Autumn-27/artex/agent"
	"github.com/Autumn-27/artex/db"
)

func releaseResumeFixture(t *testing.T) (*Server, *Task) {
	t.Helper()
	s, _ := proxyIntegration(t)
	p, err := s.m.pg.CreateProxy(&db.Proxy{Name: "回收故障合成代理", Kind: db.ProxyKindSOCKS5, URL: "socks5://127.0.0.1:19090", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = s.m.pg.Exec(`DELETE FROM outbound_proxies WHERE id=$1`, p.ID) })
	// 普通 SOCKS5 出口没有外部租约；本组断言只验证回收占用与跨 Manager 准入。
	if err := s.m.pg.SetGlobalProxyID(&p.ID); err != nil {
		t.Fatal(err)
	}
	task := createRuntimeTask(t, s, db.TaskProxyModeInherit, nil)
	t.Cleanup(func() { _ = s.m.pg.SetGlobalProxyID(nil) })
	if _, err := s.m.pg.Exec(`UPDATE tasks SET status='running',paused=true WHERE id=$1`, task.ID); err != nil {
		t.Fatal(err)
	}
	task.Status, task.Paused = "running", true
	s.m.tasks = map[string]*Task{task.ID: task}
	s.engine = NewEngine(s.m)
	return s, task
}

func waitReleaseOccupancy(t *testing.T, s *Server, task *Task) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var found bool
		if err := s.m.pg.QueryRow(`SELECT EXISTS(SELECT 1 FROM task_proxy_activity WHERE task_id=$1 AND active_count=1 AND exclusive_release)`, task.ID).Scan(&found); err != nil {
			t.Fatal(err)
		}
		if found {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("未进入持久回收占用")
}

func TestProxyPGReleaseResumeReportsCleanupFailure(t *testing.T) {
	s, task := releaseResumeFixture(t)
	s.m.pg.SetMaxOpenConns(1)
	rt, _ := s.m.proxyRuntime()
	lock := rt.taskLock(task.ID)
	lock.Lock()
	var unlock sync.Once
	defer unlock.Do(lock.Unlock)
	finished := make(chan error, 1)
	go func() { finished <- s.m.ReleaseTaskProxyWhenIdle(t.Context(), task) }()
	waitReleaseOccupancy(t, s, task)
	key := int64(0x5357500000000000) ^ switchTaskID(t, task)
	var killed bool
	// 断开本测试任务的独占锁连接，模拟回收登记后数据库连接丢失。
	err := s.m.pg.QueryRow(`SELECT pg_terminate_backend(pid) FROM pg_locks WHERE locktype='advisory' AND database=(SELECT oid FROM pg_database WHERE datname=current_database()) AND classid=(($1::bigint>>32)&4294967295)::oid AND objid=($1::bigint&4294967295)::oid AND objsubid=1 AND mode='ExclusiveLock' AND granted`, key).Scan(&killed)
	if err != nil || !killed {
		t.Fatalf("断开合成回收连接: %v %v", killed, err)
	}
	unlock.Do(lock.Unlock)
	select {
	case err := <-finished:
		if !errors.Is(err, db.ErrProxySwitchRecovery) {
			t.Errorf("回收解除持久占用失败未明确要求恢复: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("回收未退出")
	}
	waitReleaseOccupancy(t, s, task)
	_, err = s.admitPausedTask(task)
	if err == nil || strings.Contains(err.Error(), "being deleted") || !errors.Is(err, db.ErrProxyInUse) {
		t.Errorf("代理回收占用被误报为删除/丢失原因: %v", err)
	}
	controlMux := http.NewServeMux()
	controlMux.HandleFunc("POST /api/tasks/{id}/control", s.control)
	code, _, body := proxyAPI(t, controlMux, "POST", "/api/tasks/"+task.ID+"/control", `{"action":"resume"}`)
	if code != http.StatusConflict || strings.Contains(body, "being deleted") || !strings.Contains(body, "回收占用") {
		t.Errorf("继续HTTP未返回真实准入冲突: %d %s", code, body)
	}
	// 不自动清理。恢复必须显式调用正式接口，且保留选择与 revision。
	mux := http.NewServeMux()
	s.registerTaskProxyRoutes(mux)
	code, out, body := proxyAPI(t, mux, "POST", "/api/tasks/"+task.ID+"/proxy/recovery", `{"expected_revision":0,"action":"clear_stale_activity"}`)
	if code != 200 || out["proxy_revision"] != float64(0) || out["proxy_mode"] != "inherit" {
		t.Fatalf("正式恢复: %d %s", code, body)
	}
	if _, err := s.m.pg.AcquireTaskOperation(switchTaskID(t, task)); err != nil {
		t.Fatalf("恢复后准入: %v", err)
	}
	if err := s.m.pg.ReleaseTaskOperation(switchTaskID(t, task)); err != nil {
		t.Fatal(err)
	}
}

func TestProxyPGReleaseResumeAfterRestartNeedsExplicitRecovery(t *testing.T) {
	s, task := releaseResumeFixture(t)
	dsn := os.Getenv("ARTEX_PG_DSN")
	old, err := db.Open(dsn)
	if err != nil {
		t.Fatal(err)
	}
	executor, err := old.AcquireTaskProxySwitchExecutor(t.Context(), switchTaskID(t, task))
	if err != nil {
		old.Close()
		t.Fatal(err)
	}
	if err = executor.AcquireIdleTaskProxyActivity(); err != nil {
		executor.Close()
		old.Close()
		t.Fatal(err)
	}
	// 模拟进程在登记后停止：会话锁消失，持久引用不能被启动代码清零。
	executor.Close()
	if err = old.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := db.Open(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	restarted.SetMaxOpenConns(1)
	other := &Manager{pg: restarted, dir: s.m.dir, tasks: map[string]*Task{task.ID: task}}
	if err = other.InitProxyRuntime(t.Context()); err != nil {
		t.Fatal(err)
	}
	defer other.CloseProxyRuntime()
	if _, err = restarted.AcquireTaskOperation(switchTaskID(t, task)); !errors.Is(err, db.ErrProxyInUse) {
		t.Fatalf("重启自动清理了回收残留: %v", err)
	}
	if _, err = other.recoverTaskProxy(t.Context(), task.ID, 0, "clear_stale_activity"); err != nil {
		t.Fatal(err)
	}
	if _, err = restarted.AcquireTaskOperation(switchTaskID(t, task)); err != nil {
		t.Fatal(err)
	}
	if err = restarted.ReleaseTaskOperation(switchTaskID(t, task)); err != nil {
		t.Fatal(err)
	}
}

func TestProxyPGReleaseResumeCancellationCannotStealLiveRecovery(t *testing.T) {
	s, _ := proxyIntegration(t)
	entered, unblock := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(unblock) })
	_, task := lifecycleResin(t, s, func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method != "DELETE" {
			return false
		}
		close(entered)
		<-unblock
		w.WriteHeader(204)
		return true
	})
	if _, _, err := s.m.TaskToolProxy(t.Context(), task); err != nil {
		t.Fatal(err)
	}
	pg2, err := db.Open(os.Getenv("ARTEX_PG_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	defer pg2.Close()
	other := &Manager{pg: pg2, dir: s.m.dir}
	if err = other.InitProxyRuntime(t.Context()); err != nil {
		t.Fatal(err)
	}
	defer other.CloseProxyRuntime()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := s.m.ReleaseTaskProxyWhenIdle(ctx, task); err == nil {
		t.Fatal("取消准入不应留下回收工作")
	}
	done := make(chan error, 1)
	go func() { done <- s.m.ReleaseTaskProxyWhenIdle(t.Context(), task) }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("未进入回收")
	}
	if _, err := other.recoverTaskProxy(t.Context(), task.ID, 0, "clear_stale_activity"); !errors.Is(err, db.ErrProxyInUse) {
		t.Errorf("正式恢复抢走正在执行的回收: %v", err)
	}
	if _, err := pg2.AcquireTaskOperation(switchTaskID(t, task)); !errors.Is(err, db.ErrProxyInUse) {
		t.Errorf("另一Manager未被回收隔离: %v", err)
	}
	once.Do(func() { close(unblock) })
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	ctx, cancel = context.WithCancel(t.Context())
	s.m.tasks = map[string]*Task{task.ID: task}
	e := NewEngine(s.m)
	_, finish, ok := e.beginTaskOperationContext(ctx, task.ID)
	if !ok {
		t.Fatal("回收后普通准入失败")
	}
	defer finish()
	cancel()
	if _, err := other.recoverTaskProxy(t.Context(), task.ID, 0, "clear_stale_activity"); !errors.Is(err, db.ErrProxyInUse) {
		t.Errorf("取消但未退出的操作被人工恢复清除: %v", err)
	}
	finish()
}

func TestProxyPGReleaseResumeCanceledAdmissionDoesNotWaitForCleanup(t *testing.T) {
	s, task := releaseResumeFixture(t)
	b := s.m.taskProxyBarrier(task.ID)
	b.mu.Lock()
	b.releasing = true
	b.mu.Unlock()
	var unblock sync.Once
	clear := func() { b.mu.Lock(); b.releasing = false; b.signal(); b.mu.Unlock() }
	defer unblock.Do(clear)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan bool, 1)
	go func() {
		_, finish, admitted := s.engine.beginTaskOperationContext(ctx, task.ID)
		finish()
		done <- admitted
	}()
	// 等待准入进入回收屏障；取消必须立即退出，不能依赖远端回收完成。
	time.Sleep(30 * time.Millisecond)
	cancel()
	select {
	case admitted := <-done:
		if admitted {
			t.Error("取消后仍正式准入")
		}
	case <-time.After(300 * time.Millisecond):
		t.Error("取消的准入仍等待回收屏障")
		unblock.Do(clear)
		<-done
	}
}

func TestProxyPGReleaseResumeAutoReleaseDrainsBeforeContinue(t *testing.T) {
	s, task := releaseResumeFixture(t)
	s.m.pg.SetMaxOpenConns(1)
	row, err := s.m.pg.GetTask(switchTaskID(t, task))
	if err != nil {
		t.Fatal(err)
	}
	task.Store = s.m.pg.Exploration(row.ExplorationID)
	task.FirstRunAt = 1
	task.Paused = false
	if _, err = s.m.pg.Exec(`UPDATE tasks SET paused=false WHERE id=$1`, task.ID); err != nil {
		t.Fatal(err)
	}
	s.ctx = t.Context()
	// 已启动的循环只测试控制/准入，不创建真实外部模型调用。
	s.engine.started.Store(task.ID, true)
	exec := s.engine.execContextFor(t.Context(), task.ID)
	op, finish, admitted := s.engine.beginTaskOperationContext(exec, task.ID)
	if !admitted {
		t.Fatal("暂停前操作准入失败")
	}
	defer finish()
	if _, err = s.applyTaskControlWithCause(task, "pause", agent.AbortPausedByUser); err != nil {
		t.Fatal(err)
	}
	if op.Err() == nil {
		t.Fatal("暂停没有取消工具上下文")
	}
	var active int
	var exclusive bool
	if err = s.m.pg.QueryRow(`SELECT active_count,exclusive_release FROM task_proxy_activity WHERE task_id=$1`, task.ID).Scan(&active, &exclusive); err != nil || active != 1 || exclusive {
		t.Fatalf("取消但尚未退出不能提前回收: %d/%v %v", active, exclusive, err)
	}
	if _, err = s.m.recoverTaskProxy(t.Context(), task.ID, 0, "clear_stale_activity"); !errors.Is(err, db.ErrProxyInUse) {
		t.Fatalf("人工恢复不得抢走未退出操作: %v", err)
	}
	rt, _ := s.m.proxyRuntime()
	lock := rt.taskLock(task.ID)
	lock.Lock()
	var unlock sync.Once
	defer unlock.Do(lock.Unlock)
	finish()
	waitReleaseOccupancy(t, s, task)
	// 自动回收正在锁外清理；用户继续须等其结束，而不是误报删除。
	done := make(chan error, 1)
	go func() { _, err := s.applyTaskControl(task, "resume"); done <- err }()
	unlock.Do(lock.Unlock)
	select {
	case err = <-done:
		if err != nil {
			t.Fatalf("自动回收完成后继续失败: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("单连接自动回收/继续阻塞")
	}
	waitControlRelease(t, s, task.ID)
	row, err = s.m.pg.GetTask(switchTaskID(t, task))
	if err != nil || row.Paused || s.engine.IsPaused(task.ID) || row.ProxyRevision != 0 {
		t.Fatalf("继续后状态错误: %+v %v", row, err)
	}
	var references int
	if err = s.m.pg.QueryRow(`SELECT count(*) FROM task_proxy_activity WHERE task_id=$1`, task.ID).Scan(&references); err != nil || references != 0 {
		t.Fatalf("正常回收残留独占引用: %d %v", references, err)
	}
}
