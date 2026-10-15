package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/Autumn-27/artex/agent"
	"github.com/Autumn-27/artex/db"
)

func TestProxyPGReadyForDoesNotStealTaskOperationTokens(t *testing.T) {
	s, _ := proxyIntegration(t)
	task := createRuntimeTask(t, s, db.TaskProxyModeDirect, nil)
	s.m.tasks = map[string]*Task{task.ID: task}
	s.engine = NewEngine(s.m)
	s.engine.SetAuthoritativeAgentResolver(func(*Task) (*agent.Planner, *agent.Worker) {
		return &agent.Planner{}, &agent.Worker{}
	})
	id := switchTaskID(t, task)
	first, err := s.m.pg.AcquireTaskOperation(id)
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.m.pg.AcquireTaskOperation(id)
	if err != nil {
		t.Fatal(err)
	}
	third, err := s.m.pg.AcquireTaskOperation(id)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		if !s.engine.ReadyFor(task) {
			t.Fatal("任务应保持可运行")
		}
	}
	var tokens []string
	rows, err := s.m.pg.Query(`SELECT token FROM proxy_subject_activity a JOIN proxy_subject_policies p ON p.id=a.policy_id WHERE p.task_id=$1 AND a.kind='task' ORDER BY a.created_at,a.token`, id)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var token string
		if err := rows.Scan(&token); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		tokens = append(tokens, token)
	}
	rows.Close()
	if len(tokens) != 3 || tokens[0] != first.OperationToken || tokens[1] != second.OperationToken || tokens[2] != third.OperationToken {
		t.Fatalf("readiness 轮询改变了活动 token: got=%v want=%v,%v,%v", tokens, first.OperationToken, second.OperationToken, third.OperationToken)
	}
	for _, token := range []string{first.OperationToken, second.OperationToken, third.OperationToken} {
		if err := s.m.pg.ReleaseTaskOperationToken(id, token); err != nil {
			t.Fatalf("释放原 token 失败: %v", err)
		}
	}
	var count int
	if err := s.m.pg.QueryRow(`SELECT COALESCE(active_count,0) FROM task_operation_activity WHERE task_id=$1`, id).Scan(&count); err == nil && count != 0 {
		t.Fatalf("精确释放后 active_count=%d", count)
	}
}

type proxyActivityStore interface {
	AcquireTaskProxyActivity(int64) error
	ReleaseTaskProxyActivity(int64) error
}

func TestProxyPGTerminalActivityProtectsMutations(t *testing.T) {
	s, _ := proxyIntegration(t)
	activity, ok := any(s.m.pg).(proxyActivityStore)
	if !ok {
		t.Fatal("缺少原子代理活动引用入口")
	}
	for _, mode := range []db.TaskProxyMode{db.TaskProxyModeFixed, db.TaskProxyModeInherit} {
		t.Run(string(mode), func(t *testing.T) {
			p := createRuntimeProxy(t, s, "http://127.0.0.1:19019")
			if err := s.m.pg.SetGlobalProxyID(&p.ID); err != nil {
				t.Fatal(err)
			}
			task := createRuntimeTask(t, s, mode, &p.ID)
			if err := s.m.pg.SetGlobalProxyID(nil); err != nil {
				t.Fatal(err)
			}
			id, _ := strconv.ParseInt(task.ID, 10, 64)
			if _, err := s.m.pg.Exec(`UPDATE tasks SET status='done' WHERE id=$1`, id); err != nil {
				t.Fatal(err)
			}
			if err := activity.AcquireTaskProxyActivity(id); err != nil {
				t.Fatal(err)
			}
			if err := activity.AcquireTaskProxyActivity(id); err != nil {
				t.Fatal(err)
			}
			if _, err := s.m.pg.UpdateProxy(p); !errors.Is(err, db.ErrProxyInUse) {
				t.Errorf("终态操作可修改出口: %v", err)
			}
			if err := s.m.pg.DeleteProxy(p.ID); !errors.Is(err, db.ErrProxyInUse) {
				t.Errorf("终态操作可删除出口: %v", err)
			}
			called := false
			if _, err := s.m.pg.WithIdleProxy(p.ID, func(*db.Proxy) error { called = true; return nil }); !errors.Is(err, db.ErrProxyInUse) || called {
				t.Errorf("终态操作可启动WARP生命周期: %v", err)
			}
			if err := activity.ReleaseTaskProxyActivity(id); err != nil {
				t.Fatal(err)
			}
			if err := s.m.pg.DeleteProxy(p.ID); !errors.Is(err, db.ErrProxyInUse) {
				t.Errorf("一个操作退出清除了另一个操作的引用: %v", err)
			}
			if err := activity.ReleaseTaskProxyActivity(id); err != nil {
				t.Fatal(err)
			}
			if _, err := s.m.pg.UpdateProxy(p); err != nil {
				t.Fatalf("最后操作退出未解除引用: %v", err)
			}
		})
	}
}

func lifecycleResin(t *testing.T, s *Server, block func(http.ResponseWriter, *http.Request) bool) (*db.Proxy, *Task) {
	t.Helper()
	admin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if block != nil && block(w, r) {
			return
		}
		switch {
		case r.URL.Path == "/healthz":
			w.WriteHeader(200)
		case r.URL.Path == "/api/v1/system/info":
			fmt.Fprint(w, `{"auth_version":"V1"}`)
		case r.URL.Path == "/api/v1/platforms":
			fmt.Fprint(w, `{"items":[{"id":"p","name":"Default"}]}`)
		case r.Method == http.MethodDelete:
			w.WriteHeader(204)
		default:
			w.WriteHeader(404)
		}
	}))
	t.Cleanup(admin.Close)
	p, err := s.m.pg.CreateProxy(&db.Proxy{Name: "生命周期测试", Kind: db.ProxyKindResin, Protocol: "http", Host: "127.0.0.1", Port: 19020, Password: "synthetic", ResinAdminURL: admin.URL, ResinAdminToken: "synthetic", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = s.m.pg.Exec(`DELETE FROM outbound_proxies WHERE id=$1`, p.ID) })
	return p, createRuntimeTask(t, s, db.TaskProxyModeFixed, &p.ID)
}

func TestProxyPGResinNetworkDoesNotBlockOtherTasks(t *testing.T) {
	for _, phase := range []string{"prepare", "release"} {
		t.Run(phase, func(t *testing.T) {
			s, _ := proxyIntegration(t)
			entered, unblock := make(chan struct{}), make(chan struct{})
			var enteredOnce, unblockOnce sync.Once
			_, task := lifecycleResin(t, s, func(w http.ResponseWriter, r *http.Request) bool {
				if (phase == "prepare" && r.URL.Path == "/healthz") || (phase == "release" && r.Method == http.MethodDelete) {
					enteredOnce.Do(func() { close(entered) })
					<-unblock
					w.WriteHeader(204)
					return true
				}
				return false
			})
			t.Cleanup(func() { unblockOnce.Do(func() { close(unblock) }) })
			otherProxy := createRuntimeProxy(t, s, "http://127.0.0.1:19021")
			other := createRuntimeTask(t, s, db.TaskProxyModeFixed, &otherProxy.ID)
			if phase == "release" {
				if _, _, err := s.m.TaskToolProxy(t.Context(), task); err != nil {
					t.Fatal(err)
				}
			}
			done := make(chan error, 1)
			go func() {
				if phase == "release" {
					done <- s.m.ReleaseTaskProxy(t.Context(), task)
					return
				}
				_, _, err := s.m.TaskToolProxy(t.Context(), task)
				done <- err
			}()
			select {
			case <-entered:
			case <-time.After(2 * time.Second):
				t.Fatal("控制面没有进入阻塞点")
			}
			otherDone := make(chan error, 1)
			go func() { _, _, err := s.m.TaskToolProxy(t.Context(), other); otherDone <- err }()
			blocked := false
			select {
			case err := <-otherDone:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(300 * time.Millisecond):
				blocked = true
			}
			unblockOnce.Do(func() { close(unblock) })
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			if blocked {
				<-otherDone
				t.Fatal("Resin控制面请求持全局锁，阻塞另一任务出口")
			}
		})
	}
}

func TestProxyPGManualReleaseRejectsTerminalInflight(t *testing.T) {
	s, mux := proxyIntegration(t)
	s.m.tasks = map[string]*Task{}
	s.engine = NewEngine(s.m)
	p, task := lifecycleResin(t, s, nil)
	s.m.tasks[task.ID] = task
	if _, _, err := s.m.TaskToolProxy(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	if !s.engine.beginTaskOperation(task.ID) {
		t.Fatal("操作未准入")
	}
	defer s.engine.decInflight(task.ID)
	if _, err := s.m.pg.Exec(`UPDATE tasks SET status='done' WHERE id=$1`, task.ID); err != nil {
		t.Fatal(err)
	}
	code, _, body := proxyAPI(t, mux, "POST", fmt.Sprintf("/api/proxies/%d/resin/release", p.ID), fmt.Sprintf(`{"task_id":%q}`, task.ID))
	if code != 409 {
		t.Fatalf("终态但仍在途时人工回收未拒绝: %d %s", code, body)
	}
}

func TestProxyPGReleasedLeaseRequiresLiveAdmission(t *testing.T) {
	s, _ := proxyIntegration(t)
	s.m.tasks = map[string]*Task{}
	s.engine = NewEngine(s.m)
	p, task := lifecycleResin(t, s, nil)
	s.m.tasks[task.ID] = task
	if _, _, err := s.m.TaskToolProxy(t.Context(), task); err != nil {
		t.Fatal(err)
	}
	if err := s.m.ReleaseTaskProxy(t.Context(), task); err != nil {
		t.Fatal(err)
	}
	if _, err := s.m.pg.Exec(`UPDATE tasks SET status='done',paused=true WHERE id=$1`, task.ID); err != nil {
		t.Fatal(err)
	}
	id, _ := strconv.ParseInt(task.ID, 10, 64)
	for _, fake := range []context.Context{context.Background(), agent.WithRunInfo(context.Background(), agent.RunInfo{TaskID: id})} {
		if _, _, err := s.m.TaskToolProxy(fake, task); err == nil {
			t.Fatal("Background或仅任务ID伪造了租约重获证明")
		}
	}
	parent, cancel := context.WithCancel(t.Context())
	defer cancel()
	ctx, finish, ok := s.engine.beginTaskOperationContext(parent, task.ID)
	if !ok {
		t.Fatal("终态操作未正式准入")
	}
	defer finish()
	if s.m.TaskProxyOperationAdmitted(ctx, "999999999") {
		t.Fatal("其他任务接受了此ticket")
	}
	type derivedKey struct{}
	derived := context.WithValue(ctx, derivedKey{}, true)
	if _, _, err := s.m.TaskToolProxy(derived, task); err != nil {
		t.Fatalf("正式准入的终态MainAgent不能重获租约: %v", err)
	}
	if err := s.m.ReleaseTaskProxy(t.Context(), task); err != nil {
		t.Fatal(err)
	}
	ctx2, finish2, ok := s.engine.beginTaskOperationContext(t.Context(), task.ID)
	if !ok {
		t.Fatal("第二操作未准入")
	}
	defer finish2()
	cancel()
	if s.m.TaskProxyOperationAdmitted(ctx, task.ID) {
		t.Fatal("父context取消后证明仍有效")
	}
	if _, _, err := s.m.TaskToolProxy(ctx, task); err == nil {
		t.Fatal("已取消操作仍能重新申请租约")
	}
	if err := s.m.pg.DeleteProxy(p.ID); !errors.Is(err, db.ErrProxyInUse) {
		t.Fatalf("取消但未真实退出提前解除了DB引用: %v", err)
	}
	finish()
	finish()
	if s.m.TaskProxyOperationAdmitted(ctx, task.ID) {
		t.Fatal("退出后证明仍有效")
	}
	if err := s.m.pg.DeleteProxy(p.ID); !errors.Is(err, db.ErrProxyInUse) {
		t.Fatalf("重复结束减掉了第二操作引用: %v", err)
	}
	if !s.m.TaskProxyOperationAdmitted(ctx2, task.ID) {
		t.Fatal("第一操作退出影响第二ticket")
	}
	finish2()
	if _, err := s.m.pg.UpdateProxy(p); err != nil {
		t.Fatalf("最后操作结束后引用未释放: %v", err)
	}
}

func TestProxyPGUncertainLeaseRejectsEvenAdmitted(t *testing.T) {
	s, _ := proxyIntegration(t)
	s.engine = NewEngine(s.m)
	_, task := lifecycleResin(t, s, nil)
	if _, _, err := s.m.TaskToolProxy(t.Context(), task); err != nil {
		t.Fatal(err)
	}
	rt, err := s.m.proxyRuntime()
	if err != nil {
		t.Fatal(err)
	}
	path, err := leasePath(rt, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	ctx, finish, ok := s.engine.beginTaskOperationContext(t.Context(), task.ID)
	if !ok {
		t.Fatal("操作未准入")
	}
	defer finish()
	for _, state := range []string{"releasing", "uncertain"} {
		if err := writeLease(path, resinLeaseRecord{State: state, Snapshot: task.ProxySnapshot}); err != nil {
			t.Fatal(err)
		}
		if _, _, err := s.m.TaskToolProxy(ctx, task); err == nil {
			t.Fatalf("%s被正式ticket自动重试", state)
		}
	}
}

func TestProxyPGActivityAcquisitionSharesLifecycleLock(t *testing.T) {
	s, _ := proxyIntegration(t)
	p := createRuntimeProxy(t, s, "http://127.0.0.1:19022")
	task := createRuntimeTask(t, s, db.TaskProxyModeFixed, &p.ID)
	id, _ := strconv.ParseInt(task.ID, 10, 64)
	if _, err := s.m.pg.Exec(`UPDATE tasks SET status='done' WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
	entered, unblock := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(unblock) })
	mutated := make(chan error, 1)
	go func() {
		_, err := s.m.pg.WithIdleProxy(p.ID, func(proxy *db.Proxy) error {
			close(entered)
			<-unblock
			proxy.Status = db.ProxyStatusStopped
			return nil
		})
		mutated <- err
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("生命周期操作未启动")
	}
	acquired := make(chan error, 1)
	go func() { acquired <- s.m.pg.AcquireTaskProxyActivity(id) }()
	select {
	case err := <-acquired:
		t.Fatalf("活动引用绕过生命周期事务锁: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	once.Do(func() { close(unblock) })
	if err := <-mutated; err != nil {
		t.Fatal(err)
	}
	if err := <-acquired; !errors.Is(err, db.ErrProxyUnavailable) {
		t.Fatalf("停用已提交后仍取得活动引用: %v", err)
	}
}

func TestProxyPGManualReleaseRejectsPersistedActivity(t *testing.T) {
	s, mux := proxyIntegration(t)
	p, task := lifecycleResin(t, s, nil)
	if _, _, err := s.m.TaskToolProxy(t.Context(), task); err != nil {
		t.Fatal(err)
	}
	if _, err := s.m.pg.Exec(`UPDATE tasks SET status='done' WHERE id=$1`, task.ID); err != nil {
		t.Fatal(err)
	}
	id, _ := strconv.ParseInt(task.ID, 10, 64)
	// 不登记本 Manager 的内存操作，模拟另一个 Manager 或异常退出遗留的真实占用。
	if err := s.m.pg.AcquireTaskProxyActivity(id); err != nil {
		t.Fatal(err)
	}
	defer s.m.pg.ReleaseTaskProxyActivity(id)
	code, _, body := proxyAPI(t, mux, "POST", fmt.Sprintf("/api/proxies/%d/resin/release", p.ID), fmt.Sprintf(`{"task_id":%q}`, task.ID))
	if code != 409 {
		t.Fatalf("人工回收忽略持久活动引用: %d %s", code, body)
	}
	var count int
	if err := s.m.pg.QueryRow(`SELECT active_count FROM task_proxy_activity WHERE task_id=$1`, id).Scan(&count); err != nil || count != 1 {
		t.Fatalf("残留占用被改变: count=%d err=%v", count, err)
	}
}

func TestProxyPGReleaseExcludesOtherManagerAdmission(t *testing.T) {
	s, _ := proxyIntegration(t)
	entered, unblock := make(chan struct{}), make(chan struct{})
	var enteredOnce, unblockOnce sync.Once
	_, task := lifecycleResin(t, s, func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method != http.MethodDelete {
			return false
		}
		enteredOnce.Do(func() { close(entered) })
		<-unblock
		w.WriteHeader(204)
		return true
	})
	t.Cleanup(func() { unblockOnce.Do(func() { close(unblock) }) })
	if _, _, err := s.m.TaskToolProxy(t.Context(), task); err != nil {
		t.Fatal(err)
	}
	if _, err := s.m.pg.Exec(`UPDATE tasks SET status='done' WHERE id=$1`, task.ID); err != nil {
		t.Fatal(err)
	}
	otherDB, err := db.Open(os.Getenv("ARTEX_PG_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = otherDB.Close() })
	// 第二个 Manager 使用独立连接池和内存 gate，只共享 PostgreSQL 活动表。
	other := &Manager{pg: otherDB}
	engine := NewEngine(other)
	released := make(chan error, 1)
	go func() { released <- s.m.ReleaseTaskProxyWhenIdle(t.Context(), task) }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("回收未进入真实控制面DELETE")
	}
	type admission struct {
		ctx    context.Context
		finish func()
		ok     bool
	}
	admitted := make(chan admission, 1)
	go func() {
		ctx, finish, ok := engine.beginTaskOperationContext(t.Context(), task.ID)
		admitted <- admission{ctx, finish, ok}
	}()
	var result admission
	select {
	case result = <-admitted:
	case <-time.After(500 * time.Millisecond):
		unblockOnce.Do(func() { close(unblock) })
		result = <-admitted
		t.Error("回收网络期间仍持DB事务锁，普通准入被阻塞")
	}
	if result.ok {
		t.Error("另一Manager在独占回收期间取得普通操作准入")
	}
	id, _ := strconv.ParseInt(task.ID, 10, 64)
	if err := otherDB.AcquireTaskProxyActivity(id); !errors.Is(err, db.ErrProxyInUse) {
		t.Errorf("独占回收未向另一连接池返回ErrProxyInUse: %v", err)
	}
	var count int
	var exclusive bool
	if err := s.m.pg.QueryRow(`SELECT active_count,exclusive_release FROM task_proxy_activity WHERE task_id=$1`, task.ID).Scan(&count, &exclusive); err != nil || count != 1 || !exclusive {
		t.Errorf("回收独占状态改变: count=%d exclusive=%t err=%v", count, exclusive, err)
	}
	result.finish()
	unblockOnce.Do(func() { close(unblock) })
	if err := <-released; err != nil {
		t.Fatal(err)
	}
	ctx, finish, ok := engine.beginTaskOperationContext(t.Context(), task.ID)
	if !ok {
		t.Fatal("回收完成后普通准入未恢复")
	}
	defer finish()
	if !other.TaskProxyOperationAdmitted(ctx, task.ID) {
		t.Fatal("恢复准入未取得有效ticket")
	}
}

func TestProxyPGNormalActivityConcurrentCounts(t *testing.T) {
	s, _ := proxyIntegration(t)
	p := createRuntimeProxy(t, s, "http://127.0.0.1:19023")
	task := createRuntimeTask(t, s, db.TaskProxyModeFixed, &p.ID)
	id, _ := strconv.ParseInt(task.ID, 10, 64)
	const operations = 12
	for _, acquire := range []bool{true, false} {
		start := make(chan struct{})
		results := make(chan error, operations)
		for i := 0; i < operations; i++ {
			go func() {
				<-start
				if acquire {
					results <- s.m.pg.AcquireTaskProxyActivity(id)
				} else {
					results <- s.m.pg.ReleaseTaskProxyActivity(id)
				}
			}()
		}
		close(start)
		for i := 0; i < operations; i++ {
			if err := <-results; err != nil {
				t.Fatal(err)
			}
		}
		var count int
		if err := s.m.pg.QueryRow(`SELECT COALESCE(sum(active_count),0) FROM task_proxy_activity WHERE task_id=$1`, id).Scan(&count); err != nil {
			t.Fatal(err)
		}
		want := 0
		if acquire {
			want = operations
		}
		if count != want {
			t.Fatalf("普通并发计数错误: acquire=%t got=%d want=%d", acquire, count, want)
		}
	}
}

func TestProxyPGActivitySchemaUpgradePreservesOccupancy(t *testing.T) {
	s, _ := proxyIntegration(t)
	p := createRuntimeProxy(t, s, "http://127.0.0.1:19024")
	task := createRuntimeTask(t, s, db.TaskProxyModeFixed, &p.ID)
	id, _ := strconv.ParseInt(task.ID, 10, 64)
	for i := 0; i < 2; i++ {
		if err := s.m.pg.AcquireTaskProxyActivity(id); err != nil {
			t.Fatal(err)
		}
	}
	// 专用隔离库中模拟已有旧表，保留普通占用行后运行幂等迁移。
	t.Cleanup(func() {
		if err := s.m.pg.EnsureProxyActivitySchema(); err != nil {
			t.Errorf("恢复代理活动表失败: %v", err)
		}
	})
	if _, err := s.m.pg.Exec(`ALTER TABLE task_proxy_activity DROP COLUMN exclusive_release`); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := s.m.pg.EnsureProxyActivitySchema(); err != nil {
			t.Fatal(err)
		}
	}
	var count int
	var exclusive bool
	if err := s.m.pg.QueryRow(`SELECT active_count,exclusive_release FROM task_proxy_activity WHERE task_id=$1`, id).Scan(&count, &exclusive); err != nil || count != 2 || exclusive {
		t.Fatalf("迁移改变旧普通引用: count=%d exclusive=%t err=%v", count, exclusive, err)
	}
	if err := s.m.pg.AcquireIdleTaskProxyActivity(id); !errors.Is(err, db.ErrProxyInUse) {
		t.Fatalf("迁移后回收忽略旧占用: %v", err)
	}
	if err := s.m.pg.AcquireTaskProxyActivity(id); err != nil {
		t.Fatalf("迁移后不能增加普通引用: %v", err)
	}
	for i := 0; i < 3; i++ {
		if err := s.m.pg.ReleaseTaskProxyActivity(id); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.m.pg.AcquireIdleTaskProxyActivity(id); err != nil {
		t.Fatal(err)
	}
	if err := s.m.pg.EnsureProxyActivitySchema(); err != nil {
		t.Fatal(err)
	}
	for _, acquire := range []func(int64) error{s.m.pg.AcquireTaskProxyActivity, s.m.pg.AcquireIdleTaskProxyActivity} {
		if err := acquire(id); !errors.Is(err, db.ErrProxyInUse) {
			t.Fatalf("重复初始化清除了独占回收保护: %v", err)
		}
	}
	if err := s.m.pg.ReleaseTaskProxyActivity(id); err != nil {
		t.Fatal(err)
	}
	if err := s.m.pg.AcquireTaskProxyActivity(id); err != nil {
		t.Fatalf("独占结束后普通引用未恢复: %v", err)
	}
	if err := s.m.pg.ReleaseTaskProxyActivity(id); err != nil {
		t.Fatal(err)
	}
}
