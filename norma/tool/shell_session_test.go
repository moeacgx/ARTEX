package tool

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

func syntheticPTYManager(t *testing.T) (*Manager, *ptySession) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "session", "tasks")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &ptySession{id: "sh_test", state: "running", lastOut: time.Now(), done: make(chan struct{}), notifyCh: make(chan struct{}, 1)}
	m := &Manager{tasks: map[string]*Task{}, dir: dir, rootCtx: ctx, rootCancel: cancel, sessions: map[string]*ptySession{s.id: s}}
	return m, s
}

func assertStrictCleanupTracksRetiredPTY(t *testing.T, retire func(*Manager, *ptySession)) {
	t.Helper()
	m, session := syntheticPTYManager(t)
	retire(m, session)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := m.CleanupAndWait(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("strict cleanup treated kill request as exit: %v", err)
	}
	close(session.done)
	if err := m.CleanupAndWait(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestStrictCleanupTracksShellCloseUntilPTYExit(t *testing.T) {
	assertStrictCleanupTracksRetiredPTY(t, func(m *Manager, session *ptySession) {
		input, _ := json.Marshal(map[string]string{"session_id": session.id})
		result, err := NewShellClose().Call(context.Background(), input, &ToolContext{Tasks: m})
		if err != nil || result.IsError {
			t.Fatalf("shell_close failed: result=%s err=%v", result.Flatten(), err)
		}
	})
}

func TestStrictCleanupTracksReapedPTYUntilExit(t *testing.T) {
	assertStrictCleanupTracksRetiredPTY(t, func(m *Manager, session *ptySession) {
		session.lastOut = time.Now().Add(-time.Duration(sessDefIdleMs+1000) * time.Millisecond)
		m.reapSessions()
	})
}

func TestStrictCleanupTracksCloseAllPTYUntilExit(t *testing.T) {
	assertStrictCleanupTracksRetiredPTY(t, func(m *Manager, _ *ptySession) { m.closeAllSessions() })
}

// TestShellToolsJSONSessionID drives the tools via their JSON input (snake_case
// session_id) to guard against missing json tags — without tags session_id never
// binds and every read/send/close reports "会话不存在".
func TestShellToolsJSONSessionID(t *testing.T) {
	if !ptySupported() {
		t.Skip("no PTY on this platform")
	}
	m, err := NewManager("test-tools")
	if err != nil {
		t.Fatal(err)
	}
	defer m.Cleanup()
	s, err := m.openSession("cat", "", nil, 24, 80, "hands-free", nil)
	if err != nil {
		t.Fatal(err)
	}
	tc := &ToolContext{Tasks: m}
	ctx := context.Background()
	res, _ := NewShellRead().Call(ctx, []byte(`{"session_id":"`+s.id+`","view":"stream"}`), tc)
	if res.IsError {
		t.Fatalf("shell_read by session_id failed: %s", res.Flatten())
	}
	res, _ = NewShellClose().Call(ctx, []byte(`{"session_id":"`+s.id+`"}`), tc)
	if res.IsError {
		t.Fatalf("shell_close by session_id failed: %s", res.Flatten())
	}
}

// TestShellMonitor: a session in monitor mode fires a notification when a watched
// pattern shows up in the output — without ending the session.
func TestShellMonitor(t *testing.T) {
	if !ptySupported() {
		t.Skip("no PTY on this platform")
	}
	m, err := NewManager("test-mon")
	if err != nil {
		t.Fatal(err)
	}
	defer m.Cleanup()
	s, err := m.openSession("printf 'START\\n'; sleep 0.3; printf 'TRIGGERWORD\\n'; sleep 5", "", nil, 24, 80, "monitor", nil)
	if err != nil {
		t.Fatal(err)
	}
	go m.monitorWaiter(s, []*regexp.Regexp{regexp.MustCompile("TRIGGERWORD")})
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, n := range m.DrainNotifications() {
			if strings.Contains(n.Summary, "TRIGGERWORD") {
				return // success
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("expected monitor notification for TRIGGERWORD")
}

// TestShellSessionEcho drives a real PTY: `cat` echoes stdin, so sending a line
// should show up in the incremental read — exercising open/send/read end to end.
func TestShellSessionEcho(t *testing.T) {
	if !ptySupported() {
		t.Skip("no PTY on this platform")
	}
	m, err := NewManager("test-shell")
	if err != nil {
		t.Fatal(err)
	}
	defer m.Cleanup()

	s, err := m.openSession("cat", "", nil, 24, 80, "hands-free", nil)
	if err != nil {
		t.Fatalf("openSession: %v", err)
	}
	if _, err := s.ptmx.Write([]byte("hello-pty\r")); err != nil {
		t.Fatalf("write: %v", err)
	}
	deadline := time.Now().Add(3 * time.Second)
	var out string
	for time.Now().Before(deadline) {
		out, _, _ = s.readSince(0, 0, 8192, true)
		if strings.Contains(out, "hello-pty") {
			return // success
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("expected echoed input in output, got %q", out)
}

func TestKeyToBytes(t *testing.T) {
	cases := map[string]string{
		"ctrl+c": "\x03", "ctrl+d": "\x04", "enter": "\r", "tab": "\t",
		"esc": "\x1b", "up": "\x1b[A", "backspace": "\x7f",
	}
	for k, want := range cases {
		got, ok := keyToBytes(k)
		if !ok || got != want {
			t.Errorf("keyToBytes(%q)=%q,%v want %q", k, got, ok, want)
		}
	}
	if _, ok := keyToBytes("bogus"); ok {
		t.Errorf("expected bogus key to be rejected")
	}
}

func TestHexToBytes(t *testing.T) {
	got := hexToBytes([]string{"0x1b", "5b", "0x41"})
	if string(got) != "\x1b[A" {
		t.Errorf("hexToBytes=%q want ESC[A", got)
	}
}

func TestReadSinceIncremental(t *testing.T) {
	s := &ptySession{notifyCh: make(chan struct{}, 1)}
	s.append([]byte("line1\nline2\n"))
	out, cur, _ := s.readSince(0, 0, 8192, false)
	if out != "line1\nline2\n" {
		t.Fatalf("got %q", out)
	}
	s.append([]byte("line3\n"))
	out2, _, _ := s.readSince(cur, 0, 8192, false)
	if out2 != "line3\n" {
		t.Fatalf("incremental got %q", out2)
	}
}
