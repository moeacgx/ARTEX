package db

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"strings"
)

var ErrLLMProfileProxyInvalid = errors.New("渠道代理选择无效")

// MigrateLLMProfileProxies 保留历史URL，不把中心凭据复制到渠道。
// Open 持有 schema 锁；这里使用独立模型锁，避免跨连接重复取 schema 锁。
func (d *DB) MigrateLLMProfileProxies(ctx context.Context) error {
	tx, err := d.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(4706915047962005592)`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `ALTER TABLE llm_profiles
ADD COLUMN IF NOT EXISTS proxy_mode text NOT NULL DEFAULT '',
ADD COLUMN IF NOT EXISTS proxy_id bigint REFERENCES outbound_proxies(id) ON DELETE RESTRICT;
DO $$ BEGIN
IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='llm_profiles'::regclass AND conname='llm_profiles_proxy_selection_check') THEN
ALTER TABLE llm_profiles ADD CONSTRAINT llm_profiles_proxy_selection_check CHECK (
  (proxy_mode IN ('','inherit','direct') AND proxy_id IS NULL)
  OR (proxy_mode='fixed' AND proxy_id IS NOT NULL AND proxy_id>0)
);
END IF;
END $$;`); err != nil {
		return err
	}
	return tx.Commit()
}

func mergeLLMProfileProxy(tx *sql.Tx, p, previous *LLMProfile) error {
	if p.ProxyMode != "" {
		switch p.ProxyMode {
		case "inherit", "direct":
			if p.ProxyID != nil {
				return ErrLLMProfileProxyInvalid
			}
		case "fixed":
			if p.ProxyID == nil || *p.ProxyID <= 0 {
				return ErrLLMProfileProxyInvalid
			}
			proxy, err := getProxyTx(tx, *p.ProxyID)
			if err != nil {
				return err
			}
			if err := usableProxy(proxy); err != nil {
				return err
			}
			if err := CheckWarpHealthAdmissionTx(context.Background(), tx, proxy.ID); err != nil {
				return err
			}
			// 渠道还没有独立Resin租约生命周期，不能挤占任务工具租约。
			if proxy.Kind == ProxyKindResin {
				return ErrProxyUnavailable
			}
		default:
			return ErrLLMProfileProxyInvalid
		}
		p.Proxy = ""
		return nil
	}
	if p.ProxyID != nil {
		return ErrLLMProfileProxyInvalid
	}
	if !p.ProxySpecified && p.Proxy == "" && p.ID != 0 {
		p.Proxy, p.ProxyMode, p.ProxyID = previous.Proxy, previous.ProxyMode, previous.ProxyID
		return nil
	}
	p.Proxy = strings.TrimSpace(p.Proxy)
	if p.Proxy == "" {
		p.ProxyMode = "inherit"
		return nil
	}
	u, err := url.Parse(p.Proxy)
	if err != nil || u.Hostname() == "" || (u.Scheme != "http" && u.Scheme != "https" && u.Scheme != "socks5" && u.Scheme != "socks5h") {
		return ErrLLMProfileProxyInvalid
	}
	// 脱敏回显不能变成真实凭据；旧客户端原样回传时保留服务器值。
	if u.User != nil {
		password, _ := u.User.Password()
		if maskedProxyValue(password) || maskedProxyValue(u.User.Username()) {
			old, err := url.Parse(previous.Proxy)
			if p.ID == 0 || err != nil || old.Host != u.Host || old.Scheme != u.Scheme || old.Path != u.Path || old.RawQuery != u.RawQuery {
				return ErrLLMProfileProxyInvalid
			}
			p.Proxy, p.ProxyMode, p.ProxyID = previous.Proxy, previous.ProxyMode, previous.ProxyID
		}
	}
	return nil
}
