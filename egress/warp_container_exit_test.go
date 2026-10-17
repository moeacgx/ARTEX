package egress

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

type containerProcessExitConfirmer interface {
	ConfirmContainerProcessExit(context.Context, string, string, func(context.Context, string, int) error) error
}

func confirmContainerExitForTest(t *testing.T, m *WarpManager, digest string, verifier func(context.Context, string, int) error) error {
	t.Helper()
	confirmer, ok := any(m).(containerProcessExitConfirmer)
	if !ok {
		t.Fatal("缺少依据容器真实退出凭证确认旧进程的正式入口")
	}
	return confirmer.ConfirmContainerProcessExit(t.Context(), "a", digest, verifier)
}

func containerExitFixture(t *testing.T, record warpProcessRecord) (*WarpManager, string, []byte, string) {
	t.Helper()
	m, err := NewWarpManager(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })
	healthProfile(t, m, "a")
	path := filepath.Join(m.dir, "a", "process.json")
	if err := privateWrite(path, record); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(data)
	return m, path, data, hex.EncodeToString(digest[:])
}

func assertContainerRecordUnchanged(t *testing.T, path string, want []byte) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil || string(data) != string(want) {
		t.Fatalf("拒绝确认后进程记录发生改变: %s, %v", data, err)
	}
}

func TestWarpContainerExitPreservesOriginalOwnerAfterVerification(t *testing.T) {
	m, path, _, digest := containerExitFixture(t, warpProcessRecord{Host: "old-container", PID: 206})
	called := false
	err := confirmContainerExitForTest(t, m, digest, func(ctx context.Context, host string, pid int) error {
		called = true
		if host != "old-container" || pid != 206 || ctx.Err() != nil {
			t.Fatalf("宿主核验接收到错误的原进程身份: %s %d %v", host, pid, ctx.Err())
		}
		return nil
	})
	if err != nil || !called {
		t.Fatalf("有效退出证明未接受: called=%v err=%v", called, err)
	}
	data, err := os.ReadFile(path)
	var record warpProcessRecord
	if err != nil || json.Unmarshal(data, &record) != nil || record.Host != "old-container" || record.PID != 206 || !record.Exited {
		t.Fatalf("确认未保留原 Host/PID 或没有记录真实退出: %s %v", data, err)
	}
	if err := m.ConfirmStopped(t.Context(), "a"); err != nil {
		t.Fatalf("已确认跨容器退出仍阻止正式恢复: %v", err)
	}
	p, err := m.Get("a")
	if err != nil || !p.Desired {
		t.Fatalf("确认过程改变了运行意图: %+v %v", p, err)
	}
}

func TestWarpContainerExitRejectsWrongHashBeforeVerifier(t *testing.T) {
	m, path, original, _ := containerExitFixture(t, warpProcessRecord{Host: "old-container", PID: 206})
	wrong := sha256.Sum256([]byte("另一条进程记录"))
	err := confirmContainerExitForTest(t, m, hex.EncodeToString(wrong[:]), func(context.Context, string, int) error {
		t.Fatal("摘要不匹配仍然进入宿主确认")
		return nil
	})
	if !errors.Is(err, ErrWarpExitUnknown) {
		t.Fatalf("错误摘要未保留退出未知屏障: %v", err)
	}
	assertContainerRecordUnchanged(t, path, original)
}

func TestWarpContainerExitRejectsVerifierFailure(t *testing.T) {
	m, path, original, digest := containerExitFixture(t, warpProcessRecord{Host: "old-container", PID: 206})
	failure := errors.New("旧容器仍在运行")
	err := confirmContainerExitForTest(t, m, digest, func(context.Context, string, int) error { return failure })
	if !errors.Is(err, ErrWarpExitUnknown) || !errors.Is(err, failure) {
		t.Fatalf("拒绝退出证明未保留原因和屏障: %v", err)
	}
	assertContainerRecordUnchanged(t, path, original)
}

func TestWarpContainerExitRejectsMissingVerifier(t *testing.T) {
	m, path, original, digest := containerExitFixture(t, warpProcessRecord{Host: "old-container", PID: 206})
	if err := confirmContainerExitForTest(t, m, digest, nil); !errors.Is(err, ErrWarpExitUnknown) {
		t.Fatalf("缺少宿主验证器仍确认退出: %v", err)
	}
	assertContainerRecordUnchanged(t, path, original)
}

func TestWarpContainerExitRejectsCancellationDuringVerification(t *testing.T) {
	m, path, original, digest := containerExitFixture(t, warpProcessRecord{Host: "old-container", PID: 206})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	err := m.ConfirmContainerProcessExit(ctx, "a", digest, func(context.Context, string, int) error {
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("已取消的退出核验写回成功: %v", err)
	}
	assertContainerRecordUnchanged(t, path, original)
}

func TestWarpContainerExitRejectsChangedRecordAfterVerification(t *testing.T) {
	m, path, _, digest := containerExitFixture(t, warpProcessRecord{Host: "old-container", PID: 206})
	var newer []byte
	err := confirmContainerExitForTest(t, m, digest, func(context.Context, string, int) error {
		if err := privateWrite(path, warpProcessRecord{Host: "new-container", PID: 999}); err != nil {
			t.Fatal(err)
		}
		var err error
		newer, err = os.ReadFile(path)
		return err
	})
	if !errors.Is(err, ErrWarpExitUnknown) {
		t.Fatalf("宿主核验期间变化的记录被覆盖: %v", err)
	}
	assertContainerRecordUnchanged(t, path, newer)
}

func TestWarpContainerExitRejectsLocalManagerProcess(t *testing.T) {
	m, path, original, digest := containerExitFixture(t, warpProcessRecord{Host: "old-container", PID: 206})
	done := make(chan struct{})
	m.processes["a"] = &warpProcess{done: done}
	defer close(done)
	err := confirmContainerExitForTest(t, m, digest, func(context.Context, string, int) error {
		t.Fatal("本 Manager 进程未退出仍调用宿主核验")
		return nil
	})
	if !errors.Is(err, ErrWarpExitUnknown) {
		t.Fatalf("本 Manager 活进程未阻止退出确认: %v", err)
	}
	assertContainerRecordUnchanged(t, path, original)
}

func TestWarpContainerExitRejectsLocalLivePID(t *testing.T) {
	host, err := os.Hostname()
	if err != nil {
		t.Fatal(err)
	}
	m, path, original, digest := containerExitFixture(t, warpProcessRecord{Host: host, PID: os.Getpid()})
	err = confirmContainerExitForTest(t, m, digest, func(context.Context, string, int) error { return nil })
	if !errors.Is(err, ErrWarpExitUnknown) {
		t.Fatalf("仍存活的本机 PID 被记为退出: %v", err)
	}
	assertContainerRecordUnchanged(t, path, original)
}

func TestWarpContainerExitRecordTrustsOnlyConfirmedCrossHostExit(t *testing.T) {
	for _, tc := range []struct {
		name   string
		record warpProcessRecord
		allow  bool
	}{
		{"未确认跨容器", warpProcessRecord{Host: "old-container", PID: 206}, false},
		{"已确认跨容器", warpProcessRecord{Host: "old-container", PID: 206, Exited: true}, true},
		{"未曾启动成功", warpProcessRecord{Host: "old-container", PID: 0, Exited: true}, true},
		{"缺少原宿主", warpProcessRecord{PID: 206, Exited: true}, false},
		{"非法原宿主", warpProcessRecord{Host: "old/container", PID: 206, Exited: true}, false},
		{"非法原进程", warpProcessRecord{Host: "old-container", PID: -1, Exited: true}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, path, original, _ := containerExitFixture(t, tc.record)
			err := m.ConfirmStopped(t.Context(), "a")
			if tc.allow && err != nil || !tc.allow && !errors.Is(err, ErrWarpExitUnknown) {
				t.Fatalf("跨容器退出凭证判定不符: allow=%v err=%v", tc.allow, err)
			}
			assertContainerRecordUnchanged(t, path, original)
		})
	}
}

func TestWarpContainerExitRejectsMalformedRecord(t *testing.T) {
	m, path, _, _ := containerExitFixture(t, warpProcessRecord{Host: "old-container", PID: 206})
	original := []byte(`{"host":"old-container","pid":206,"exited":true`)
	if err := os.WriteFile(path, original, 0600); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(original)
	if err := confirmContainerExitForTest(t, m, hex.EncodeToString(digest[:]), func(context.Context, string, int) error {
		t.Fatal("损坏进程记录仍进入宿主验证")
		return nil
	}); !errors.Is(err, ErrWarpExitUnknown) {
		t.Fatalf("损坏进程记录没有阻止确认: %v", err)
	}
	if err := m.ConfirmStopped(t.Context(), "a"); !errors.Is(err, ErrWarpExitUnknown) {
		t.Fatalf("损坏进程记录没有阻止正式恢复: %v", err)
	}
	assertContainerRecordUnchanged(t, path, original)
}
