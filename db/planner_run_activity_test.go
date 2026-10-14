package db

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"
)

func plannerRunTestDBs(t *testing.T) (*DB, *DB, *Task) {
	t.Helper()
	dsn := testDSN(t)
	first, err := Open(dsn)
	if err != nil {
		t.Skipf("postgres unavailable (%v)", err)
	}
	second, err := Open(dsn)
	if err != nil {
		first.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.Close(); _ = first.Close() })
	task, err := first.CreateTask("planner run lease", "cross-manager resume", nil, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = first.DeleteTaskCascade(task.ID, false, false) })
	return first, second, task
}

func TestPlannerRunLeaseBlocksCrossManagerResumeMutation(t *testing.T) {
	first, second, task := plannerRunTestDBs(t)
	if err := first.SetPaused(task.ID, true); err != nil {
		t.Fatal(err)
	}
	lease, err := first.AcquirePlannerRun(context.Background(), task.ID, true)
	if err != nil {
		t.Fatal(err)
	}
	called := false
	err = second.WithPlannerIdleTaskMutation(task.ID, func(tx *sql.Tx) error {
		called = true
		_, err := tx.Exec(`UPDATE tasks SET paused=false WHERE id=$1`, task.ID)
		return err
	})
	if !errors.Is(err, ErrPlannerRunBusy) || called {
		t.Fatalf("live lease did not block resume mutation: called=%v err=%v", called, err)
	}
	if got, err := second.GetTask(task.ID); err != nil || got == nil || !got.Paused {
		t.Fatalf("blocked mutation changed paused: task=%+v err=%v", got, err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	err = second.WithPlannerIdleTaskMutation(task.ID, func(tx *sql.Tx) error {
		_, err := tx.Exec(`UPDATE tasks SET paused=false WHERE id=$1`, task.ID)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := second.GetTask(task.ID); err != nil || got == nil || got.Paused {
		t.Fatalf("idle mutation did not resume task: task=%+v err=%v", got, err)
	}
}

func TestPlannerRunAdmissionSerializesWithPauseMutation(t *testing.T) {
	first, second, task := plannerRunTestDBs(t)
	entered, release := make(chan struct{}), make(chan struct{})
	paused := make(chan error, 1)
	go func() {
		paused <- first.withProxyLifecycle(func(tx *sql.Tx) error {
			if _, err := tx.Exec(`UPDATE tasks SET paused=true WHERE id=$1`, task.ID); err != nil {
				return err
			}
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered
	acquired := make(chan error, 1)
	go func() {
		lease, err := second.AcquirePlannerRun(context.Background(), task.ID, false)
		if lease != nil {
			_ = lease.Close()
		}
		acquired <- err
	}()
	select {
	case err := <-acquired:
		t.Fatalf("planner admission bypassed lifecycle mutation: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	if err := <-paused; err != nil {
		t.Fatal(err)
	}
	if err := <-acquired; !errors.Is(err, ErrPlannerRunPaused) {
		t.Fatalf("planner started after pause committed: %v", err)
	}
}
