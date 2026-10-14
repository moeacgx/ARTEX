package server

import (
	"context"
	"errors"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/Autumn-27/artex/agent"
	"github.com/Autumn-27/artex/db"
	actool "github.com/Autumn-27/norma/tool"
)

type proxySubjectContextKey struct{}
type admittedProxySubject struct {
	Manager  *Manager
	Subject  db.ProxySubject
	Revision int64
	Active   bool
}

func (m *Manager) withProxySubject(ctx context.Context, p db.ProxyPolicy) context.Context {
	ctx, _ = agent.WithProxyRunStop(ctx)
	ctx = context.WithValue(ctx, proxySubjectContextKey{}, admittedProxySubject{Manager: m, Subject: p.Subject, Revision: p.State.Revision, Active: true})
	ctx = actool.WithOperationNetworkFailure(ctx, func(callCtx context.Context, proxy string, err error) {
		m.recordInfrastructureFailure(callCtx, p.Subject, p.State.Revision, p.State.ID, proxy, err)
	})
	return actool.WithOperationAdmission(ctx, func(callCtx context.Context, name string) error {
		if agent.ProxyRunStopRequested(callCtx) {
			return db.ErrProxyInUse
		}
		if name == "list_allowed_proxies" || name == "request_proxy_failover" {
			return nil
		}
		return m.pg.ValidateProxyPolicy(p.Subject)
	})
}
func (s *Server) proxySubjectFromContext(ctx context.Context) (db.ProxySubject, error) {
	a, ok := ctx.Value(proxySubjectContextKey{}).(admittedProxySubject)
	if !ok || a.Manager != s.m || !a.Active || ctx.Err() != nil {
		return db.ProxySubject{}, db.ErrProxyAuthorization
	}
	if a.Subject.Kind == "task" && !s.m.TaskProxyOperationAdmitted(ctx, i64s(a.Subject.ID)) {
		return db.ProxySubject{}, db.ErrProxyAuthorization
	}
	if a.Subject.Kind == "conversation" {
		ticket, ok := ctx.Value(conversationProxyTicketKey{}).(*conversationProxyTicket)
		if !ok || ticket.subject != a.Subject || !ticket.active.Load() {
			return db.ProxySubject{}, db.ErrProxyAuthorization
		}
	}
	return a.Subject, nil
}
func proxySubjectRuntimeTask(p db.ProxyPolicy) *Task {
	prefix := ""
	if p.Subject.Kind == "conversation" {
		prefix = "conv-"
	}
	t := &Task{ID: prefix + i64s(p.Subject.ID)}
	t.setProxyState(p.State)
	return t
}

func (m *Manager) setProxyPolicy(ctx context.Context, subject db.ProxySubject, expected int64, patch db.ProxyPolicyPatch) (db.ProxyPolicy, error) {
	if subject.Kind == "task" && patch.Mode != db.TaskProxyModeGroup && patch.AllowedGroupIDs == nil && patch.AgentFailoverEnabled == nil && patch.FailoverCooldownSeconds == nil && patch.FailoverMaxSwitches == nil {
		old, err := m.pg.GetProxyPolicy(subject)
		if err != nil {
			return old, err
		}
		if old.State.Mode != db.TaskProxyModeGroup {
			if _, err = m.switchTaskProxy(ctx, i64s(subject.ID), expected, patch.Mode, patch.ProxyID); err != nil {
				return old, err
			}
			return m.pg.GetProxyPolicy(subject)
		}
	}
	x, e := m.pg.AcquireProxyPolicyExecutor(ctx, subject)
	if e != nil {
		return db.ProxyPolicy{}, e
	}
	defer x.Close()
	old, token, e := x.Begin(expected, patch)
	if e != nil {
		return old, e
	}
	if e = m.ReleaseTaskProxy(ctx, proxySubjectRuntimeTask(old)); e != nil {
		return old, db.ErrProxySwitchRecovery
	}
	p, e := x.Complete(expected, token)
	if e != nil {
		return old, errors.Join(db.ErrProxySwitchRecovery, e)
	}
	if subject.Kind == "task" {
		if t, ok := m.Task(i64s(subject.ID)); ok {
			if e = m.refreshTaskProxyState(t, p.State); e != nil {
				return p, e
			}
		}
	}
	return p, nil
}

type proxyFailoverScheduler struct {
	cancel context.CancelFunc
	wake   chan struct{}
	wg     sync.WaitGroup
}

var proxyFailoverSchedulers sync.Map

func (m *Manager) startProxyFailover(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	scheduler := &proxyFailoverScheduler{cancel: cancel, wake: make(chan struct{}, 1)}
	actual, loaded := proxyFailoverSchedulers.LoadOrStore(m, scheduler)
	if loaded {
		cancel()
		_ = actual
		return
	}
	scheduler.wg.Add(1)
	go func() {
		defer scheduler.wg.Done()
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			case <-scheduler.wake:
			}
			subjects, e := m.pg.PendingProxySwitchSubjects()
			if e != nil {
				log.Printf("[proxy] 读取切换队列失败")
				continue
			}
			for _, subject := range subjects {
				if ctx.Err() != nil {
					return
				}
				if e = m.applyProxyFailover(ctx, subject); e != nil && !errors.Is(e, db.ErrProxyInUse) {
					log.Printf("[proxy] %s %d 切换未完成: %v", subject.Kind, subject.ID, e)
				}
			}
		}
	}()
}
func (m *Manager) wakeProxyFailover() {
	if v, ok := proxyFailoverSchedulers.Load(m); ok {
		select {
		case v.(*proxyFailoverScheduler).wake <- struct{}{}:
		default:
		}
	}
}
func (m *Manager) closeProxyFailover() {
	if v, ok := proxyFailoverSchedulers.LoadAndDelete(m); ok {
		s := v.(*proxyFailoverScheduler)
		s.cancel()
		s.wg.Wait()
	}
}
func (m *Manager) applyProxyFailover(ctx context.Context, subject db.ProxySubject) error {
	if e := m.pg.MarkProxyFailoverDraining(subject); e != nil {
		return e
	}
	x, e := m.pg.AcquireProxyPolicyExecutor(ctx, subject)
	if e != nil {
		return e
	}
	defer x.Close()
	old, request, e := x.ClaimFailover()
	if e != nil {
		return e
	}
	// 独占会话锁保证所有 Manager 的真实操作已经退出，网络清理在生命周期事务外。
	if e = m.ReleaseTaskProxy(ctx, proxySubjectRuntimeTask(old)); e != nil {
		_ = x.RequireFailoverRecovery(request, "proxy_release_uncertain")
		return db.ErrProxySwitchRecovery
	}
	p, e := x.CompleteFailover(request)
	if e != nil {
		_ = x.RequireFailoverRecovery(request, "proxy_commit_failed")
		return errors.Join(db.ErrProxySwitchRecovery, e)
	}
	if subject.Kind == "task" {
		if t, ok := m.Task(strconv.FormatInt(subject.ID, 10)); ok {
			return m.refreshTaskProxyState(t, p.State)
		}
	}
	return nil
}
