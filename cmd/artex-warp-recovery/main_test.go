package main

import (
	"io"
	"strings"
	"testing"
)

func recoveryTestArgs() []string {
	return []string{"--proxy-secrets", "existing.key", "--warp-dir", "existing-warp", "--proxy-id", "12", "--expected-revision", "3", "--expected-claim", "1e15b603-39f7-4149-8b55-deca814bf5f4", "--expected-process-sha", strings.Repeat("a", 64), "--exit-evidence", "exit.json", "--expected-evidence-sha", strings.Repeat("b", 64), "--old-container-full-id", strings.Repeat("c", 64), "--current-container-full-id", strings.Repeat("d", 64)}
}

func TestRecoveryRequiresAllEvidenceArguments(t *testing.T) {
	args := recoveryTestArgs()
	if _, err := parseRecoveryOptions(args); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < len(args); i += 2 {
		t.Run(args[i], func(t *testing.T) {
			missing := append(append([]string{}, args[:i]...), args[i+2:]...)
			if _, err := parseRecoveryOptions(missing); err == nil {
				t.Fatalf("缺少 %s 仍可执行", args[i])
			}
		})
	}
	if _, err := parseRecoveryOptions(append(args, "--force")); err == nil {
		t.Fatal("维护命令接受了强制解除屏障")
	}
}

func TestRecoveryRequiresEnvironmentDSN(t *testing.T) {
	t.Setenv("ARTEX_PG_DSN", "")
	if err := runRecovery(t.Context(), recoveryTestArgs(), io.Discard); err == nil || !strings.Contains(err.Error(), "ARTEX_PG_DSN") {
		t.Fatalf("未配置环境 DSN 应在访问文件前拒绝: %v", err)
	}
}
