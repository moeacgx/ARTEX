package maintenance

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDockerContainerExitVerifierChecksDaemonAndPinnedEvidence(t *testing.T) {
	for _, test := range []struct {
		name                                                       string
		oldStatus                                                  int
		oldRunning, badDigest, livePID, badMount, badHost, wrongID bool
		wantOK                                                     bool
	}{
		{name: "已移除且退出证据匹配", oldStatus: 404, wantOK: true},
		{name: "旧容器仍运行", oldStatus: 200, oldRunning: true},
		{name: "Docker查询失败", oldStatus: 500},
		{name: "证据被改写", oldStatus: 404, badDigest: true},
		{name: "旧宿主进程仍存在", oldStatus: 404, livePID: true},
		{name: "档案挂载不匹配", oldStatus: 404, badMount: true},
		{name: "原进程身份不匹配", oldStatus: 404, badHost: true},
		{name: "Docker返回其他容器", oldStatus: 404, wrongID: true},
		{name: "旧容器正常停止", oldStatus: 200, wantOK: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			oldID, currentID := strings.Repeat("1", 64), strings.Repeat("2", 64)
			dir := t.TempDir()
			warpDir := filepath.Join(dir, "data", "egress", "warp")
			if err := os.MkdirAll(warpDir, 0700); err != nil {
				t.Fatal(err)
			}
			oldPID := 2147483647
			if test.livePID {
				oldPID = os.Getpid()
			}
			evidence := map[string]any{"id": oldID, "status": "exited", "pid": 0, "exit_code": 0, "finished": time.Now().Add(-time.Minute).UTC().Format(time.RFC3339Nano), "old_pids": []int{oldPID}, "remaining_old_pids": []int{}}
			data, _ := json.Marshal(evidence)
			file := filepath.Join(dir, "exit.json")
			if err := os.WriteFile(file, data, 0600); err != nil {
				t.Fatal(err)
			}
			hash := sha256.Sum256(data)
			digest := hex.EncodeToString(hash[:])
			if test.badDigest {
				digest = strings.Repeat("0", 64)
			}
			var oldChecks, currentChecks int
			daemon := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.URL.Path == "/containers/"+oldID+"/json" {
					oldChecks++
					w.WriteHeader(test.oldStatus)
					if test.oldStatus == 200 {
						_ = json.NewEncoder(w).Encode(map[string]any{"Id": oldID, "State": map[string]any{"Running": test.oldRunning, "Pid": map[bool]int{true: 31, false: 0}[test.oldRunning], "Status": map[bool]string{true: "running", false: "exited"}[test.oldRunning], "ExitCode": 0}})
					}
					return
				}
				if r.URL.Path == "/containers/"+currentID+"/json" {
					currentChecks++
					mount := filepath.Join(dir, "data")
					if test.badMount {
						mount = filepath.Join(dir, "other")
					}
					id := currentID
					if test.wrongID {
						id = oldID
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"Id": id, "State": map[string]any{"Running": true, "Pid": os.Getpid(), "Status": "running"}, "Config": map[string]any{"Labels": map[string]string{"com.docker.compose.service": "artex"}}, "Mounts": []map[string]string{{"Type": "bind", "Source": mount, "Destination": "/app/data"}}})
					return
				}
				http.NotFound(w, r)
			}))
			defer daemon.Close()
			client := daemon.Client()
			client.Transport = rewriteDockerHost{base: client.Transport, url: daemon.URL}
			verify, err := dockerContainerExitVerifier(DockerExitOptions{OldContainerID: oldID, CurrentContainerID: currentID, EvidencePath: file, EvidenceSHA256: digest, WarpDir: warpDir}, client)
			if err == nil {
				host := oldID[:12]
				if test.badHost {
					host = "another-host"
				}
				err = verify(context.Background(), host, 206)
			}
			if (err == nil) != test.wantOK {
				t.Fatalf("验证结果不符：通过=%v，错误=%v", test.wantOK, err)
			}
			if test.wantOK && (oldChecks == 0 || currentChecks == 0) {
				t.Fatal("未向 Docker 核实旧容器和当前挂载")
			}
		})
	}
}

type rewriteDockerHost struct {
	base http.RoundTripper
	url  string
}

func (r rewriteDockerHost) RoundTrip(req *http.Request) (*http.Response, error) {
	copy := req.Clone(req.Context())
	copy.URL.Scheme = "http"
	copy.URL.Host = strings.TrimPrefix(r.url, "http://")
	return r.base.RoundTrip(copy)
}
