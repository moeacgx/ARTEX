package tool

import (
	"context"
	"errors"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestCleanupAndWaitRequiresActualProcessExit(t *testing.T) {
	done := make(chan struct{})
	m := &Manager{tasks: map[string]*Task{"late": {Status: TaskKilled, done: done}}, dir: filepath.Join(t.TempDir(), "session", "tasks")}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := m.CleanupAndWait(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err=%v", err)
	}
	close(done)
	if err := m.CleanupAndWait(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestCleanupAndWaitReapsBackgroundCommand(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows taskkill is covered by the isolated integration path; avoid host timing flakes")
	}
	m, err := NewManager("cleanup-wait-" + t.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer m.Cleanup()
	command := "sleep 30"
	task, err := m.Spawn(SpawnSpec{Command: command})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err = m.CleanupAndWait(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-task.done:
	default:
		t.Fatal("cleanup returned before cmd.Wait")
	}
}
