package tool

import (
	"context"
	"os"
	"path/filepath"
	"time"
)

// CleanupAndWait 供要求真退出的宿主使用；调用前必须停止工具派发。
// 取消与 kill 只是请求，done 关闭才证明进程已被 Wait 回收。
func (m *Manager) CleanupAndWait(ctx context.Context) error {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	var waits []<-chan struct{}
	for _, task := range m.tasks {
		waits = append(waits, task.done)
	}
	m.mu.Unlock()
	m.sessMu.Lock()
	var sessions []*ptySession
	for _, session := range m.sessions {
		waits = append(waits, session.done)
		sessions = append(sessions, session)
	}
	m.sessMu.Unlock()
	m.KillAll()
	for _, session := range sessions {
		session.kill()
	}
	if m.rootCancel != nil {
		m.rootCancel()
	}
	for _, done := range waits {
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	// 只有确认退出才删句柄，清理失败后的复查仍必须等待原进程。
	m.sessMu.Lock()
	for _, session := range sessions {
		if m.sessions[session.id] == session {
			delete(m.sessions, session.id)
		}
	}
	m.sessMu.Unlock()
	target := filepath.Dir(m.dir)
	for {
		if err := os.RemoveAll(target); err == nil {
			return nil
		} else if ctx.Done() == nil {
			return err
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
