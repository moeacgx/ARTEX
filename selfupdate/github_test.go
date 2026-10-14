package selfupdate

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
)

type releaseTransport func(*http.Request) (*http.Response, error)

func (f releaseTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// 检查真实查询入口与发布包约定，避免 fork 用户再次收到原作者的更新。
func TestFetchLatestUsesForkRelease(t *testing.T) {
	client := &http.Client{Transport: releaseTransport(func(r *http.Request) (*http.Response, error) {
		if r.URL.String() != "https://api.github.com/repos/moeacgx/ARTEX-P/releases/latest" {
			t.Fatalf("更新查询指向错误仓库: %s", r.URL)
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{
			"tag_name":"v2.2.1", "html_url":"https://github.com/moeacgx/ARTEX-P/releases/tag/v2.2.1",
			"assets":[
				{"name":"artex-2.2.1-linux-amd64.zip"},
				{"name":"artex-2.2.1-linux-arm64.zip"},
				{"name":"artex-2.2.1-darwin-amd64.zip"},
				{"name":"artex-2.2.1-darwin-arm64.zip"},
				{"name":"artex-2.2.1-windows-amd64.zip"},
				{"name":"SHA256SUMS"}
			]}`))}, nil
	})}
	rel, err := FetchLatest(context.Background(), client)
	if err != nil {
		t.Fatal(err)
	}
	for _, target := range [][2]string{{"linux", "amd64"}, {"linux", "arm64"}, {"darwin", "amd64"}, {"darwin", "arm64"}, {"windows", "amd64"}} {
		if _, ok := rel.FindAsset(AssetName(rel.TagName, target[0], target[1])); !ok {
			t.Fatalf("缺少 %s/%s 更新包", target[0], target[1])
		}
	}
	if _, ok := rel.FindAsset(sumsAsset); !ok {
		t.Fatal("缺少更新校验清单")
	}
}
