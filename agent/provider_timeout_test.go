package agent

import (
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Autumn-27/norma/harness"
	"github.com/Autumn-27/norma/llm"
	"github.com/Autumn-27/norma/tool"
)

func timeoutTestConfig(baseURL string, headerTimeout, bodyIdleTimeout time.Duration) Config {
	return Config{
		Format:                  llm.FormatOpenAI,
		BaseURL:                 baseURL,
		APIKey:                  "test-key",
		Model:                   "test-model",
		ResponseHeaderTimeout:   headerTimeout,
		ResponseBodyIdleTimeout: bodyIdleTimeout,
		Retry: RetryConfig{
			ConnectAttempts: 3,
			EmptyAttempts:   3,
		},
	}
}

func streamError(provider llm.Provider, ctx context.Context) error {
	var result error
	for _, err := range provider.Stream(ctx, llm.CompletionRequest{Messages: []llm.Message{llm.UserText("ping")}}) {
		if err != nil {
			result = err
		}
	}
	return result
}

func TestProviderResponseHeaderTimeoutDoesNotReplayRequest(t *testing.T) {
	var requests atomic.Int32
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer server.Close()
	defer close(release)

	provider, err := timeoutTestConfig(server.URL, 30*time.Millisecond, time.Second).NewProvider()
	if err != nil {
		t.Fatal(err)
	}
	err = streamError(provider, context.Background())
	if err == nil || !errors.Is(err, ErrLLMResponseHeaderTimeout) {
		t.Fatalf("err=%v, want response-header timeout", err)
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("requests=%d, want exactly one", got)
	}
}

func TestProviderSSEBodyIdleTimeoutClosesBodyWithoutReplay(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("content-type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer server.Close()

	provider, err := timeoutTestConfig(server.URL, time.Second, 30*time.Millisecond).NewProvider()
	if err != nil {
		t.Fatal(err)
	}
	err = streamError(provider, context.Background())
	if err == nil || !errors.Is(err, ErrLLMResponseBodyIdleTimeout) {
		t.Fatalf("err=%v, want response-body idle timeout", err)
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("requests=%d, want exactly one", got)
	}
}

func TestProviderSSEHeartbeatsResetBodyIdleTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("content-type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher := w.(http.Flusher)
		flusher.Flush()
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-r.Context().Done():
				return
			case <-ticker.C:
				_, _ = io.WriteString(w, ": heartbeat\n\n")
				flusher.Flush()
			}
		}
	}))
	defer server.Close()

	provider, err := timeoutTestConfig(server.URL, time.Second, 35*time.Millisecond).NewProvider()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Millisecond)
	defer cancel()
	err = streamError(provider, ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err=%v, want caller deadline; heartbeats must prevent idle timeout", err)
	}
	if errors.Is(err, ErrLLMResponseBodyIdleTimeout) {
		t.Fatalf("heartbeats failed to reset body idle timer: %v", err)
	}
}

func TestProviderNonStreamingBodyIdleTimeout(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("content-type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"choices":[`)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer server.Close()

	provider, err := timeoutTestConfig(server.URL, time.Second, 30*time.Millisecond).NewProvider()
	if err != nil {
		t.Fatal(err)
	}
	_, _, _, err = provider.Complete(context.Background(), llm.CompletionRequest{Messages: []llm.Message{llm.UserText("ping")}})
	if err == nil || !errors.Is(err, ErrLLMResponseBodyIdleTimeout) {
		t.Fatalf("err=%v, want non-streaming body idle timeout", err)
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("requests=%d, want exactly one", got)
	}
}

func TestProviderTimeoutDefaults(t *testing.T) {
	t.Setenv("ARTEX_LLM_RESPONSE_HEADER_TIMEOUT", "0")
	t.Setenv("ARTEX_LLM_RESPONSE_BODY_IDLE_TIMEOUT", "invalid")
	if defaultLLMResponseHeaderTimeout != 60*time.Second {
		t.Fatalf("header default=%s", defaultLLMResponseHeaderTimeout)
	}
	if defaultLLMResponseBodyIdleTimeout != 120*time.Second {
		t.Fatalf("body idle default=%s", defaultLLMResponseBodyIdleTimeout)
	}
	if got := fmt.Sprint(ErrLLMResponseHeaderTimeout); got == "" {
		t.Fatal("timeout error must be diagnosable")
	}
	client, err := quotaAwareHTTPClient("", "")
	if err != nil {
		t.Fatal(err)
	}
	if client.Timeout != 0 {
		t.Fatalf("global client timeout=%s", client.Timeout)
	}
	bounded := client.Transport.(quotaAwareTransport).base.(responseTimeoutTransport)
	if bounded.headerTimeout != 60*time.Second || bounded.bodyIdle != 120*time.Second {
		t.Fatalf("defaults=%+v", bounded)
	}
	t.Setenv("ARTEX_LLM_RESPONSE_HEADER_TIMEOUT", "2s")
	if got := configuredTimeout(0, "ARTEX_LLM_RESPONSE_HEADER_TIMEOUT", time.Minute); got != 2*time.Second {
		t.Fatalf("configured header=%s", got)
	}
}

func TestProviderCompressedBodyIdleTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Set("Content-Type", "application/json")
		compressed := gzip.NewWriter(w)
		_, _ = compressed.Write([]byte(`{"choices":[`))
		_ = compressed.Flush()
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer server.Close()
	provider, err := timeoutTestConfig(server.URL, time.Second, 40*time.Millisecond).NewProvider()
	if err != nil {
		t.Fatal(err)
	}
	_, _, _, err = provider.Complete(context.Background(), llm.CompletionRequest{Messages: []llm.Message{llm.UserText("go")}})
	if !errors.Is(err, ErrLLMResponseBodyIdleTimeout) {
		t.Fatalf("compressed body: %v", err)
	}
}

func TestProviderCompressedHeaderProgressResetsIdle(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Encoding", "gzip")
		// 带扩展字段的合法 gzip 头持续到达，但解码器一直等头读完。
		_, _ = w.Write([]byte{0x1f, 0x8b, 8, 4, 0, 0, 0, 0, 0, 255, 100, 0})
		w.(http.Flusher).Flush()
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-r.Context().Done():
				return
			case <-ticker.C:
				_, _ = w.Write([]byte{0})
				w.(http.Flusher).Flush()
			}
		}
	}))
	defer server.Close()
	provider, err := timeoutTestConfig(server.URL, time.Second, 60*time.Millisecond).NewProvider()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Millisecond)
	defer cancel()
	_, _, _, err = provider.Complete(ctx, llm.CompletionRequest{Messages: []llm.Message{llm.UserText("go")}})
	if !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrLLMResponseBodyIdleTimeout) {
		t.Fatalf("progress misclassified as idle: %v", err)
	}
}

func TestProviderHeartbeatsStillReachHardRoundDeadline(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-r.Context().Done():
				return
			case <-ticker.C:
				_, _ = io.WriteString(w, ": heartbeat\n\n")
				w.(http.Flusher).Flush()
			}
		}
	}))
	defer server.Close()
	provider, err := timeoutTestConfig(server.URL, time.Second, 100*time.Millisecond).NewProvider()
	if err != nil {
		t.Fatal(err)
	}
	var terminal *harness.Terminal
	for event, _ := range harness.Query(context.Background(), harness.QueryInput{
		Tools: tool.NewRegistry(), HardMaxDuration: 150 * time.Millisecond,
		Messages: []llm.Message{llm.UserText("go")},
	}, harness.QueryDeps{CallModel: provider.Stream}) {
		if event.Terminal != nil {
			terminal = event.Terminal
		}
	}
	if terminal == nil || terminal.Reason != harness.ReasonTimeout || !errors.Is(terminal.Err, harness.ErrHardDeadlineExceeded) || requests.Load() != 1 {
		t.Fatalf("terminal=%+v requests=%d", terminal, requests.Load())
	}
}
