package server

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Autumn-27/artex/agent"
)

// 关停等待独立于调用方的超时。调用方超时不会释放模型、工具或代理占用，
// 后续调用可以继续等待同一次真实退出。
type engineShutdownState struct {
	done chan struct{}
	err  error
}

// Shutdown 在根运行上下文取消、HTTP 停止接收请求之后调用。
// 等待所有已准入执行与出口回收完成，不改变任务的暂停、排队或终态。
func (s *Server) Shutdown(ctx context.Context) error {
	if s == nil || s.engine == nil {
		return nil
	}
	e := s.engine
	e.deleteMu.Lock()
	state := e.shutdown
	if state == nil {
		state = &engineShutdownState{done: make(chan struct{})}
		e.shutdown, e.shuttingDown = state, true
		e.runtimeMu.Lock()
		runtimes := make([]*taskRuntime, 0, len(e.runtimes))
		taskIDs := make(map[string]struct{}, len(e.runtimes))
		for id, runtime := range e.runtimes {
			taskIDs[id] = struct{}{}
			runtimes = append(runtimes, runtime)
			runtime.cancel()
		}
		e.runtimeMu.Unlock()
		e.execMu.Lock()
		for _, cancel := range e.execCancel {
			cancel(agent.AbortShutdown)
		}
		e.execMu.Unlock()
		e.deleteMu.Unlock()
		s.cancelShutdownConversations()
		go s.finishShutdown(state, runtimes, taskIDs)
	} else {
		e.deleteMu.Unlock()
	}
	select {
	case <-state.done:
		return state.err
	case <-ctx.Done():
		return fmt.Errorf("服务关停仍在等待模型、工具或出口真实退出: %w", ctx.Err())
	}
}

func (s *Server) cancelShutdownConversations() {
	s.chatMu.Lock()
	for _, cancel := range s.chatCancel {
		cancel(agent.AbortShutdown)
	}
	s.chatMu.Unlock()
	if s.side != nil {
		s.side.mu.Lock()
		for _, run := range s.side.runs {
			run.cancel()
		}
		s.side.mu.Unlock()
	}
}

func (s *Server) finishShutdown(state *engineShutdownState, runtimes []*taskRuntime, taskIDs map[string]struct{}) {
	defer close(state.done)
	e := s.engine
	for _, runtime := range runtimes {
		runtime.wg.Wait()
	}
	// Run 的回收请求在任务 WaitGroup 之外；必须等请求完成登记，不能只
	// 观察瞬间为空的回收屏障，随后就让旧 goroutine 再插入持久回收引用。
	e.proxyReleaseWG.Wait()
	s.archiveWG.Wait()
	for s.hasShutdownActivity() {
		time.Sleep(20 * time.Millisecond)
	}
	if s.side != nil && s.side.done != nil {
		<-s.side.done
		// 最后的模型快照可能晚于后台写入循环退出。
		s.flushSideSnapshots()
	}
	e.inflight.Range(func(key, _ any) bool {
		taskIDs[key.(string)] = struct{}{}
		return true
	})
	for id := range taskIDs {
		// Run 已登记自己的关停回收。没有任务循环的 MainAgent 等独立
		// 操作也会创建出口，需在它们真实退出后补齐同一条正式回收路径。
		if !e.Started(id) && e.hasShutdownProxy(id) {
			e.requestProxyRelease(id, true)
		}
	}
	for {
		pending, releaseErr := e.shutdownProxyReleases()
		if !pending {
			state.err = releaseErr
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (e *Engine) hasShutdownProxy(taskID string) bool {
	if e.m == nil || e.m.pg == nil {
		return false
	}
	task, ok := e.m.Task(taskID)
	return ok && task.proxyState().Snapshot.ID != 0
}

func (s *Server) hasShutdownActivity() bool {
	e := s.engine
	active := false
	for _, values := range []*sync.Map{&e.inflight, &e.llmCalls} {
		values.Range(func(_, value any) bool {
			active = atomic.LoadInt64(value.(*int64)) > 0
			return !active
		})
		if active {
			return true
		}
	}
	e.plannerRuns.Range(func(_, value any) bool {
		active = value.(*atomic.Int64).Load() > 0
		return !active
	})
	if active {
		return true
	}
	if e.m != nil {
		e.m.proxyTickets.Range(func(_, _ any) bool { active = true; return false })
		if active {
			return true
		}
	}
	s.chatMu.Lock()
	for _, busy := range s.chatBusy {
		active = active || busy
	}
	s.chatMu.Unlock()
	if active {
		return true
	}
	s.queueMu.Lock()
	for _, count := range s.triggerActive {
		active = active || count > 0
	}
	s.queueMu.Unlock()
	if active {
		return true
	}
	if s.side != nil {
		s.side.mu.Lock()
		active = len(s.side.runs) > 0
		s.side.mu.Unlock()
	}
	return active
}

func (e *Engine) shutdownProxyReleases() (bool, error) {
	pending := false
	var failures []error
	collect := func(id, value any) bool {
		barrier := value.(*taskProxyBarrier)
		barrier.mu.Lock()
		pending = pending || barrier.pending || barrier.releasing
		if barrier.releaseErr != nil {
			failures = append(failures, fmt.Errorf("任务 %v 出口回收未成功: %w", id, barrier.releaseErr))
		}
		barrier.mu.Unlock()
		return true
	}
	e.proxyBarriers.Range(collect)
	if e.m != nil {
		e.m.proxyBarriers.Range(collect)
	}
	return pending, errors.Join(failures...)
}
