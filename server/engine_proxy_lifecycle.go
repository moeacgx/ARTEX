package server

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/Autumn-27/artex/agent"
	"github.com/Autumn-27/artex/db"
)

// 操作登记覆盖模型、工具和 MCP 清理。回收与新操作串行，远端回收不持生命周期锁。
type taskProxyBarrier struct {
	mu        sync.Mutex
	changed   chan struct{}
	pending   bool
	releasing bool
	force     bool
}

func (e *Engine) proxyBarrier(id string) *taskProxyBarrier {
	if e.m != nil {
		return e.m.taskProxyBarrier(id)
	}
	v, _ := e.proxyBarriers.LoadOrStore(id, &taskProxyBarrier{changed: make(chan struct{})})
	return v.(*taskProxyBarrier)
}

func (m *Manager) taskProxyBarrier(id string) *taskProxyBarrier {
	v, _ := m.proxyBarriers.LoadOrStore(id, &taskProxyBarrier{changed: make(chan struct{})})
	return v.(*taskProxyBarrier)
}

type taskProxyTicketKey struct{}
type taskProxyTicket struct {
	manager *Manager
	taskID  string
	ctx     context.Context
	active  atomic.Bool
}

func (e *Engine) beginTaskOperationContext(ctx context.Context, id string) (context.Context, func(), bool) {
	if ctx == nil || e.beginTaskOperationError(ctx, id) != nil {
		return ctx, func() {}, false
	}
	ticket := &taskProxyTicket{manager: e.m, taskID: id, ctx: ctx}
	ticket.active.Store(true)
	if e.m != nil {
		e.m.proxyTickets.Store(ticket, struct{}{})
	}
	stop := context.AfterFunc(ctx, func() { ticket.active.Store(false) })
	var cleanupFailed atomic.Bool
	ctx = agent.WithBrowserCleanupFailure(ctx, func() { cleanupFailed.Store(true) })
	var once sync.Once
	finish := func() {
		once.Do(func() {
			stop()
			ticket.active.Store(false)
			if e.m != nil {
				e.m.proxyTickets.Delete(ticket)
			}
			if cleanupFailed.Load() {
				// 保留持久引用与会话屏障，避免未退出的浏览器跨越出口切换。
				log.Printf("[proxy] 任务 %s 浏览器未确认退出，保留操作占用", id)
				return
			}
			e.decInflight(id)
		})
	}
	return context.WithValue(ctx, taskProxyTicketKey{}, ticket), finish, true
}

// 只有仍存活的准入凭证可重新申请已回收租约；任务 ID 和计数不能构成证明。
func (m *Manager) TaskProxyOperationAdmitted(ctx context.Context, id string) bool {
	if ctx == nil || ctx.Err() != nil {
		return false
	}
	ticket, ok := ctx.Value(taskProxyTicketKey{}).(*taskProxyTicket)
	if !ok || ticket.manager != m || ticket.taskID != id || !ticket.active.Load() || ticket.ctx.Err() != nil {
		return false
	}
	_, registered := m.proxyTickets.Load(ticket)
	return registered
}

func (e *Engine) taskExecutionContext(ctx context.Context, id string) context.Context {
	if e.m != nil && e.m.TaskProxyOperationAdmitted(ctx, id) {
		return ctx
	}
	return e.execContextFor(ctx, id)
}

func (m *Manager) ReleaseTaskProxyWhenIdle(ctx context.Context, t *Task) error {
	if t == nil {
		return nil
	}
	b := m.taskProxyBarrier(t.ID)
	b.mu.Lock()
	if b.releasing || (m.proxyEngine != nil && m.proxyEngine.inflightCount(t.ID) > 0) {
		b.mu.Unlock()
		return db.ErrProxyInUse
	}
	b.releasing = true
	b.mu.Unlock()
	defer func() { b.mu.Lock(); b.releasing = false; b.signal(); b.mu.Unlock() }()
	return m.releaseTaskProxyOccupied(ctx, t)
}

func (m *Manager) releaseTaskProxyOccupied(ctx context.Context, t *Task) (resultErr error) {
	id, err := strconv.ParseInt(t.ID, 10, 64)
	if err != nil {
		return err
	}
	executor, err := m.pg.AcquireTaskProxySwitchExecutor(ctx, id)
	if err != nil {
		return err
	}
	defer executor.Close()
	if m.pg != nil {
		if err := executor.AcquireIdleTaskProxyActivity(); err != nil {
			return err
		}
		defer func() {
			if err := executor.ReleaseTaskProxyActivity(); err != nil {
				// 锁连接已丢失时不能换连接盲目清引用；保留占用交正式恢复。
				resultErr = errors.Join(resultErr, db.ErrProxySwitchRecovery, fmt.Errorf("任务 %s 回收占用解除失败: %w", t.ID, err))
			}
		}()
	}
	return m.ReleaseTaskProxy(ctx, t)
}

func (b *taskProxyBarrier) signal() { close(b.changed); b.changed = make(chan struct{}) }

func (e *Engine) requestProxyRelease(id string, force bool) {
	if e.m == nil {
		return
	}
	b := e.proxyBarrier(id)
	b.mu.Lock()
	b.force = b.force || force
	if b.pending {
		b.mu.Unlock()
		return
	}
	b.pending = true
	b.mu.Unlock()
	go func() {
		b.mu.Lock()
		for e.inflightCount(id) > 0 || b.releasing {
			changed := b.changed
			b.mu.Unlock()
			<-changed
			b.mu.Lock()
		}
		task, ok := e.m.Task(id)
		if !ok || (!b.force && !e.IsPaused(id) && !isTerminalStatus(task.lifecycleSnapshot().Status)) {
			b.pending = false
			b.force = false
			b.signal()
			b.mu.Unlock()
			return
		}
		b.releasing = true
		b.mu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), proxyReleaseTimeout)
		if err := e.m.releaseTaskProxyOccupied(ctx, task); err != nil {
			log.Printf("[proxy] 任务 %s 出口回收失败: %v", id, err)
		}
		cancel()
		b.mu.Lock()
		b.releasing = false
		b.pending = false
		b.force = false
		b.signal()
		b.mu.Unlock()
	}()
}
