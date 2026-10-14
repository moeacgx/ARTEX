package server

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/Autumn-27/artex/db"
)

func TestProxyPGWarpTermsBeforeCreation(t *testing.T) {
	s, mux := proxyIntegration(t)
	var before, after int
	if err := s.m.pg.QueryRow(`SELECT count(*) FROM outbound_proxies`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	for _, body := range []string{`{"name":"条款未确认","kind":"warp"}`, `{"name":"条款未确认","kind":"warp","accept_terms":false}`} {
		code, out, raw := proxyAPI(t, mux, "POST", "/api/proxies", body)
		if rawID, ok := out["id"].(string); ok {
			id, err := strconv.ParseInt(rawID, 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _, _ = s.m.pg.Exec(`DELETE FROM outbound_proxies WHERE id=$1`, id) })
		}
		if code != 400 {
			t.Errorf("未确认条款仍建档: %d %s", code, raw)
		}
	}
	if err := s.m.pg.QueryRow(`SELECT count(*) FROM outbound_proxies`).Scan(&after); err != nil || after != before {
		t.Fatalf("条款校验晚于建档: before=%d after=%d err=%v", before, after, err)
	}
}

func TestProxyPGWarpStaleReadyDoesNotExposeIP(t *testing.T) {
	s, mux := proxyIntegration(t)
	mux.HandleFunc("GET /api/proxies", s.listProxies)
	p, err := s.m.pg.CreateProxy(&db.Proxy{Name: "无进程旧ready", Kind: db.ProxyKindWarp, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = s.m.pg.Exec(`DELETE FROM outbound_proxies WHERE id=$1`, p.ID) })
	dir := filepath.Join(os.Getenv("ARTEX_WARP_DIR"), warpProfileID(p.ID))
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	meta := fmt.Sprintf(`{"id":%q,"phase":"ready","desired":true,"port":1080,"egress_ip":"8.8.8.8","registered":true,"generated":true}`, warpProfileID(p.ID))
	if err := os.WriteFile(filepath.Join(dir, "meta.json"), []byte(meta), 0600); err != nil {
		t.Fatal(err)
	}
	code, out, raw := proxyAPI(t, mux, "GET", fmt.Sprintf("/api/proxies/%d", p.ID), "")
	if code != 200 || out["warp_phase"] != "failed" || out["status"] == "healthy" || out["egress_ip"] != nil {
		t.Fatalf("无进程的旧ready虚报在线或单项GET缺失: %d %s", code, raw)
	}
}
