package agent

import (
	"compress/gzip"
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Autumn-27/norma/llm"
)

const (
	defaultLLMResponseHeaderTimeout   = 60 * time.Second
	defaultLLMResponseBodyIdleTimeout = 120 * time.Second
)

var (
	ErrLLMResponseHeaderTimeout   = errors.New("LLM 响应头等待超时")
	ErrLLMResponseBodyIdleTimeout = errors.New("LLM 响应正文读取空闲超时")
)

// 只接受有限正时限，配置错误或零值回到默认保护，不能无意关闭上限。
func configuredTimeout(value time.Duration, key string, fallback time.Duration) time.Duration {
	if value > 0 {
		return value
	}
	raw := strings.TrimSpace(os.Getenv(key))
	if seconds, err := strconv.ParseInt(raw, 10, 32); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	if duration, err := time.ParseDuration(raw); err == nil && duration > 0 {
		return duration
	}
	return fallback
}

type responseTimeoutTransport struct {
	base          http.RoundTripper
	headerTimeout time.Duration
	bodyIdle      time.Duration
}

func (t responseTimeoutTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx, cancel := context.WithCancelCause(req.Context())
	outgoing := req.Clone(ctx)
	decompress := outgoing.Header.Get("Accept-Encoding") == "" && outgoing.Header.Get("Range") == "" && outgoing.Method != http.MethodHead
	if decompress {
		outgoing.Header.Set("Accept-Encoding", "gzip")
	}
	var headerMu sync.Mutex
	waiting := true
	headerTimer := time.AfterFunc(t.headerTimeout, func() {
		headerMu.Lock()
		defer headerMu.Unlock()
		if waiting {
			cancel(ErrLLMResponseHeaderTimeout)
		}
	})
	resp, err := t.base.RoundTrip(outgoing)
	headerMu.Lock()
	waiting = false
	headerTimer.Stop()
	headerMu.Unlock()
	if err != nil || ctx.Err() != nil {
		cause := context.Cause(ctx)
		cancel(context.Canceled)
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		if req.Context().Err() != nil {
			return nil, context.Cause(req.Context())
		}
		if errors.Is(cause, ErrLLMResponseHeaderTimeout) || llm.IsRequestTimeout(err) {
			return nil, errors.Join(ErrLLMResponseHeaderTimeout, context.DeadlineExceeded)
		}
		return nil, err
	}
	body := &idleResponseBody{body: resp.Body, ctx: ctx, cancel: cancel, idle: t.bodyIdle}
	context.AfterFunc(ctx, func() { _ = body.Close() })
	resp.Body = body
	// 在原始网络正文层重置空闲计时，不能把解压器等待更多字节当作网络静默。
	if decompress && strings.EqualFold(resp.Header.Get("Content-Encoding"), "gzip") {
		decoder, decodeErr := gzip.NewReader(body)
		if decodeErr != nil {
			_ = body.Close()
			return nil, decodeErr
		}
		resp.Body = &decodedResponseBody{Reader: decoder, body: body}
		resp.Header.Del("Content-Encoding")
		resp.Header.Del("Content-Length")
		resp.ContentLength = -1
		resp.Uncompressed = true
	}
	return resp, nil
}

type decodedResponseBody struct {
	*gzip.Reader
	body io.ReadCloser
}

func (b *decodedResponseBody) Close() error {
	return b.body.Close()
}

// 每次底层 Read 期间计时；任何字节（包括 SSE 注释心跳）都会结束本次等待。
// 定时器取消请求并关闭正文，压缩解码器内部阻塞同样会被打断。
type idleResponseBody struct {
	body      io.ReadCloser
	ctx       context.Context
	cancel    context.CancelCauseFunc
	idle      time.Duration
	closeOnce sync.Once
	closeErr  error
}

func (b *idleResponseBody) Read(p []byte) (int, error) {
	var readMu sync.Mutex
	waiting := true
	timer := time.AfterFunc(b.idle, func() {
		readMu.Lock()
		defer readMu.Unlock()
		if !waiting {
			return
		}
		b.cancel(ErrLLMResponseBodyIdleTimeout)
		_ = b.Close()
	})
	n, err := b.body.Read(p)
	readMu.Lock()
	waiting = false
	timer.Stop()
	readMu.Unlock()
	if cause := context.Cause(b.ctx); cause != nil {
		if errors.Is(cause, ErrLLMResponseBodyIdleTimeout) {
			return n, errors.Join(cause, context.DeadlineExceeded)
		}
		return n, cause
	}
	return n, err
}

func (b *idleResponseBody) Close() error {
	b.closeOnce.Do(func() {
		b.cancel(context.Canceled)
		b.closeErr = b.body.Close()
	})
	return b.closeErr
}
