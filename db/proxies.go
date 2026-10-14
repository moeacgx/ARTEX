package db

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type ProxyKind string

const (
	ProxyKindHTTP   ProxyKind = "http"
	ProxyKindHTTPS  ProxyKind = "https"
	ProxyKindSOCKS5 ProxyKind = "socks5"
	ProxyKindWarp   ProxyKind = "warp"
	ProxyKindResin  ProxyKind = "resin"
)

const (
	ProxyStatusUnknown  = "unknown"
	ProxyStatusHealthy  = "healthy"
	ProxyStatusFailed   = "failed"
	ProxyStatusStarting = "starting"
	ProxyStatusStopped  = "stopped"
)

type TaskProxyMode string

const (
	TaskProxyModeInherit TaskProxyMode = "inherit"
	TaskProxyModeDirect  TaskProxyMode = "direct"
	TaskProxyModeFixed   TaskProxyMode = "fixed"
	TaskProxyModeGroup   TaskProxyMode = "group"
)

type Proxy struct {
	ID              int64      `json:"id"`
	Name            string     `json:"name"`
	Kind            ProxyKind  `json:"kind"`
	Protocol        string     `json:"protocol,omitempty"`
	URL             string     `json:"url"`
	Host            string     `json:"host"`
	Port            int        `json:"port"`
	Username        string     `json:"username,omitempty"`
	Password        string     `json:"-"`
	ResinPlatform   string     `json:"resin_platform,omitempty"`
	ResinAdminURL   string     `json:"resin_admin_url,omitempty"`
	ResinAdminToken string     `json:"-"`
	Enabled         bool       `json:"enabled"`
	Status          string     `json:"status"`
	LastError       string     `json:"last_error,omitempty"`
	LastCheckedAt   *time.Time `json:"last_checked_at,omitempty"`
	CreatedAt       time.Time  `json:"created_at"`
	UpdatedAt       time.Time  `json:"updated_at"`
}

type ProxySnapshot struct {
	ID         int64     `json:"id,omitempty"`
	Name       string    `json:"name,omitempty"`
	Kind       ProxyKind `json:"kind,omitempty"`
	URL        string    `json:"url,omitempty"`
	Host       string    `json:"host,omitempty"`
	Port       int       `json:"port,omitempty"`
	Platform   string    `json:"platform,omitempty"`
	Protocol   string    `json:"protocol,omitempty"`
	Username   string    `json:"username,omitempty"`
	Password   string    `json:"password,omitempty"`
	AdminToken string    `json:"admin_token,omitempty"`
}

var proxySecretsPath string
var proxySecretsMu sync.Mutex

// ConfigureProxySecrets selects the independent key file used for encrypted
// proxy credentials. It intentionally does not alter DB, preserving struct
// literals used by callers and tests.
func ConfigureProxySecrets(path string) error {
	proxySecretsMu.Lock()
	defer proxySecretsMu.Unlock()
	if strings.TrimSpace(path) == "" {
		return errors.New("proxy secrets path is empty")
	}
	proxySecretsPath = path
	return nil
}

func proxyCipher() (cipher.AEAD, error) {
	proxySecretsMu.Lock()
	defer proxySecretsMu.Unlock()
	path := proxySecretsPath
	if path == "" {
		return nil, errors.New("proxy secrets key path is not configured")
	}
	key, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		key = make([]byte, 32)
		if _, err = io.ReadFull(rand.Reader, key); err != nil {
			return nil, err
		}
		if err = os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			return nil, err
		}
		file, createErr := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if os.IsExist(createErr) {
			key, err = os.ReadFile(path)
		} else if createErr != nil {
			err = createErr
		} else {
			_, err = file.Write(key)
			closeErr := file.Close()
			if err == nil {
				err = closeErr
			}
		}
		if err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	if len(key) != 32 {
		return nil, errors.New("proxy secrets key must be 32 bytes")
	}
	b, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(b)
}

// InitializeProxySecrets 在启动边界验证密钥可用性；失败时禁止启动。
func InitializeProxySecrets() error {
	_, err := proxyCipher()
	return err
}

func encryptProxySecret(raw string) (string, error) {
	if raw == "" {
		return "", nil
	}
	a, err := proxyCipher()
	if err != nil {
		return "", err
	}
	nonce := make([]byte, a.NonceSize())
	if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}
	return fmt.Sprintf("v1:%x", a.Seal(nonce, nonce, []byte(raw), nil)), nil
}

func decryptProxySecret(raw string) (string, error) {
	if raw == "" {
		return "", nil
	}
	if !strings.HasPrefix(raw, "v1:") {
		return "", errors.New("proxy secret is not encrypted")
	}
	b, err := fmtHexDecode(raw[3:])
	if err != nil {
		return "", err
	}
	a, err := proxyCipher()
	if err != nil {
		return "", err
	}
	if len(b) < a.NonceSize() {
		return "", errors.New("proxy secret ciphertext is truncated")
	}
	plain, err := a.Open(nil, b[:a.NonceSize()], b[a.NonceSize():], nil)
	if err != nil {
		return "", err
	}
	return string(plain), nil
}

func fmtHexDecode(s string) ([]byte, error) {
	const hexchars = "0123456789abcdef"
	if len(s)%2 != 0 {
		return nil, errors.New("invalid proxy secret encoding")
	}
	out := make([]byte, len(s)/2)
	for i := range out {
		hi, lo := strings.IndexByte(hexchars, s[i*2]), strings.IndexByte(hexchars, s[i*2+1])
		if hi < 0 || lo < 0 {
			return nil, errors.New("invalid proxy secret encoding")
		}
		out[i] = byte(hi<<4 | lo)
	}
	return out, nil
}

func (p *Proxy) SnapshotEncrypted() (ProxySnapshot, error) {
	if p == nil {
		return ProxySnapshot{}, nil
	}
	u, err := encryptProxySecret(p.URL)
	if err != nil {
		return ProxySnapshot{}, err
	}
	pw, err := encryptProxySecret(p.Password)
	if err != nil {
		return ProxySnapshot{}, err
	}
	at, err := encryptProxySecret(p.ResinAdminToken)
	if err != nil {
		return ProxySnapshot{}, err
	}
	return ProxySnapshot{ID: p.ID, Name: p.Name, Kind: p.Kind, URL: u, Host: p.Host, Port: p.Port, Platform: p.ResinPlatform, Protocol: p.Protocol, Username: p.Username, Password: pw, AdminToken: at}, nil
}

var (
	ErrProxyInvalidKind = errors.New("invalid proxy kind")
	ErrProxyUnavailable = errors.New("proxy unavailable")
	ErrProxyInvalidMode = errors.New("invalid task proxy mode")
	ErrProxyIDRequired  = errors.New("fixed task proxy requires proxy id")
	ErrProxyInUse       = errors.New("proxy is used by a running task")
)

func ValidateProxyKind(kind ProxyKind) error {
	switch kind {
	case ProxyKindHTTP, ProxyKindHTTPS, ProxyKindSOCKS5, ProxyKindWarp, ProxyKindResin:
		return nil
	default:
		return fmt.Errorf("%w: %q", ErrProxyInvalidKind, kind)
	}
}

func NormalizeTaskProxyMode(mode TaskProxyMode) (TaskProxyMode, error) {
	if mode == "" {
		return TaskProxyModeInherit, nil
	}
	switch mode {
	case TaskProxyModeInherit, TaskProxyModeDirect, TaskProxyModeFixed, TaskProxyModeGroup:
		return mode, nil
	default:
		return "", fmt.Errorf("%w: %q", ErrProxyInvalidMode, mode)
	}
}

func ValidateProxy(p *Proxy) error {
	if p == nil {
		return ErrProxyUnavailable
	}
	if strings.TrimSpace(p.Name) == "" {
		return errors.New("proxy name is required")
	}
	if err := ValidateProxyKind(p.Kind); err != nil {
		return err
	}
	if maskedProxyValue(p.Password) || maskedProxyValue(p.ResinAdminToken) || maskedProxyValue(p.Username) {
		return errors.New("脱敏值不能作为凭据保存")
	}
	if p.Kind == ProxyKindResin {
		if p.Protocol == "" {
			p.Protocol = "socks5"
		}
		if p.Protocol != "http" && p.Protocol != "socks5" {
			return errors.New("resin protocol must be http or socks5")
		}
		if strings.TrimSpace(p.Host) == "" || p.Port < 1 || p.Port > 65535 {
			return errors.New("resin data-plane host and port are required")
		}
		if p.ResinPlatform == "" {
			p.ResinPlatform = "Default"
		}
		if p.ResinAdminURL != "" {
			u, err := url.Parse(p.ResinAdminURL)
			if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
				return errors.New("invalid resin admin url")
			}
		}
		p.URL, p.Username = "", "" // 粘性账号只能由可信运行时生成。
		return nil
	}
	if p.Kind == ProxyKindWarp {
		if p.Protocol == "" {
			p.Protocol = "socks5"
		}
		if p.Protocol != "http" && p.Protocol != "socks5" {
			return errors.New("warp protocol must be http or socks5")
		}
		if strings.TrimSpace(p.URL) == "" {
			return nil
		}
	} else {
		if p.Protocol != "" && p.Protocol != string(p.Kind) {
			return errors.New("proxy protocol conflicts with kind")
		}
		p.Protocol = string(p.Kind)
	}
	if strings.TrimSpace(p.URL) == "" {
		return errors.New("proxy url is required")
	}
	u, err := url.Parse(p.URL)
	if err != nil || (p.Kind != ProxyKindWarp && u.Scheme != string(p.Kind)) || u.Host == "" || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") || (u.Scheme != "http" && u.Scheme != "https" && u.Scheme != "socks5") {
		return errors.New("invalid proxy url")
	}
	if u.User != nil && (maskedProxyValue(u.User.Username()) || func() bool { v, _ := u.User.Password(); return maskedProxyValue(v) }()) {
		return errors.New("脱敏代理地址不能作为凭据保存")
	}
	if maskedProxyValue(p.Password) || maskedProxyValue(p.ResinAdminToken) {
		return errors.New("脱敏值不能作为凭据保存")
	}
	if p.Username == "" && u.User != nil {
		p.Username = u.User.Username()
	}
	if p.Password == "" && u.User != nil {
		p.Password, _ = u.User.Password()
	}
	if p.Password != "" {
		u.User = url.UserPassword(p.Username, p.Password)
	} else if p.Username != "" {
		u.User = url.User(p.Username)
	}
	p.URL = u.String()
	p.Host = u.Hostname()
	p.Port, _ = strconv.Atoi(u.Port())
	return nil
}

func maskedProxyValue(v string) bool {
	return strings.Contains(v, "***") || strings.Contains(v, "•••")
}

func (d *DB) ListProxies() ([]*Proxy, error) {
	rows, err := d.Query(`SELECT id,name,kind,url,host,port,username,password,resin_platform,resin_admin_url,resin_admin_token,enabled,status,last_error,last_checked_at,created_at,updated_at,protocol FROM outbound_proxies ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Proxy
	for rows.Next() {
		p, err := scanProxy(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func (d *DB) GetProxy(id int64) (*Proxy, error) {
	return scanProxy(d.QueryRow(`SELECT id,name,kind,url,host,port,username,password,resin_platform,resin_admin_url,resin_admin_token,enabled,status,last_error,last_checked_at,created_at,updated_at,protocol FROM outbound_proxies WHERE id=$1`, id))
}

func (d *DB) CreateProxy(p *Proxy) (*Proxy, error) {
	if err := ValidateProxy(p); err != nil {
		return nil, err
	}
	var out *Proxy
	err := d.withProxyLifecycle(func(tx *sql.Tx) error {
		var err error
		out, err = insertProxyTx(tx, p)
		return err
	})
	return out, err
}

func insertProxyTx(tx *sql.Tx, p *Proxy) (*Proxy, error) {
	encURL, e := encryptProxySecret(p.URL)
	if e != nil {
		return nil, e
	}
	encPassword, e := encryptProxySecret(p.Password)
	if e != nil {
		return nil, e
	}
	encToken, e := encryptProxySecret(p.ResinAdminToken)
	if e != nil {
		return nil, e
	}
	out, err := scanProxy(tx.QueryRow(`INSERT INTO outbound_proxies(name,kind,url,host,port,username,password,resin_platform,resin_admin_url,resin_admin_token,enabled,status,protocol)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
RETURNING id,name,kind,url,host,port,username,password,resin_platform,resin_admin_url,resin_admin_token,enabled,status,last_error,last_checked_at,created_at,updated_at,protocol`, p.Name, p.Kind, encURL, p.Host, p.Port, p.Username, encPassword, p.ResinPlatform, p.ResinAdminURL, encToken, p.Enabled, initialProxyStatus(p), p.Protocol))
	if err != nil || out.Kind != ProxyKindWarp {
		return out, err
	}
	_, err = tx.Exec(`INSERT INTO warp_proxy_health(proxy_id,state) VALUES($1,'unknown') ON CONFLICT DO NOTHING`, out.ID)
	return out, err
}

func (d *DB) UpdateProxy(p *Proxy) (*Proxy, error) {
	if p == nil {
		return nil, ErrProxyUnavailable
	}
	opts := ProxyUpdateOptions{Enabled: &p.Enabled}
	if p.URL != "" {
		opts.URL = &p.URL
	}
	if p.Password != "" {
		opts.Password = &p.Password
	}
	if p.ResinAdminToken != "" {
		opts.AdminToken = &p.ResinAdminToken
	}
	return d.UpdateProxyWithOptions(p, opts)
}

// ProxyUpdateOptions 区分未提供秘密与明确清除，合并发生在生命周期事务内。
type ProxyUpdateOptions struct {
	URL, Password, AdminToken      *string
	ClearPassword, ClearAdminToken bool
	Enabled                        *bool
}

func mergeProxySecrets(copy *Proxy, existing *Proxy, opts ProxyUpdateOptions) error {
	if copy == nil || existing == nil {
		return ErrProxyUnavailable
	}
	if opts.URL != nil {
		copy.URL = strings.TrimSpace(*opts.URL)
	} else {
		copy.URL = existing.URL
	}
	if copy.Name == "" {
		copy.Name = existing.Name
	}
	if copy.Kind == "" {
		copy.Kind = existing.Kind
	}
	if copy.Username == "" {
		copy.Username = existing.Username
	}
	if copy.Kind != ProxyKindResin && copy.URL != "" {
		u, err := url.Parse(copy.URL)
		if err != nil {
			return errors.New("invalid proxy url")
		}
		if u.User != nil {
			pw, _ := u.User.Password()
			if maskedProxyValue(u.User.Username()) || maskedProxyValue(pw) {
				return errors.New("脱敏代理地址不能作为凭据保存")
			}
			copy.Username = u.User.Username()
			if opts.URL != nil && opts.Password == nil && !opts.ClearPassword {
				if _, present := u.User.Password(); present {
					copy.Password = pw
					opts.Password = &copy.Password
				}
			}
		}
	}
	if opts.ClearPassword {
		copy.Password = ""
	} else if opts.Password != nil {
		copy.Password = *opts.Password
	} else {
		copy.Password = existing.Password
	}
	if opts.ClearAdminToken {
		copy.ResinAdminToken = ""
	} else if opts.AdminToken != nil {
		copy.ResinAdminToken = *opts.AdminToken
	} else {
		copy.ResinAdminToken = existing.ResinAdminToken
	}
	if opts.Enabled != nil {
		copy.Enabled = *opts.Enabled
	} else {
		copy.Enabled = existing.Enabled
	}
	// 先重建 userinfo，避免 ValidateProxy 又从旧 URL 恢复刚清除的密码。
	if copy.Kind != ProxyKindResin && copy.URL != "" {
		u, err := url.Parse(copy.URL)
		if err != nil {
			return errors.New("invalid proxy url")
		}
		u.User = nil
		if copy.Password != "" {
			u.User = url.UserPassword(copy.Username, copy.Password)
		} else if copy.Username != "" {
			u.User = url.User(copy.Username)
		}
		copy.URL = u.String()
	}
	return nil
}

func (d *DB) UpdateProxyWithOptions(p *Proxy, opts ProxyUpdateOptions) (*Proxy, error) {
	if p == nil {
		return nil, ErrProxyUnavailable
	}
	var out *Proxy
	err := d.withProxyLifecycle(func(tx *sql.Tx) error {
		existing, err := getProxyTx(tx, p.ID)
		if err != nil {
			return err
		}
		if existing.Kind == ProxyKindWarp {
			if err = CheckWarpHealthMutationTx(context.Background(), tx, p.ID); err != nil {
				return err
			}
			if existing.Status != ProxyStatusStopped {
				return ErrProxyInUse
			}
		}
		inUse, err := proxyInUseTx(tx, p.ID)
		if err != nil {
			return err
		}
		if inUse {
			return ErrProxyInUse
		}
		pending, err := warpCreatePendingTx(tx, p.ID)
		if err != nil {
			return err
		}
		if pending {
			return ErrProxyInUse
		}
		globalID, err := globalProxyIDTx(tx)
		if err != nil {
			return err
		}
		if globalID != nil && *globalID == p.ID {
			return ErrProxyInUse
		}
		copy := *p
		if err := mergeProxySecrets(&copy, existing, opts); err != nil {
			return err
		}
		if copy.Protocol == "" {
			copy.Protocol = existing.Protocol
		}
		if err := ValidateProxy(&copy); err != nil {
			return err
		}
		if (existing.Kind == ProxyKindWarp) != (copy.Kind == ProxyKindWarp) {
			return ErrProxyInvalidKind
		}
		encURL, err := encryptProxySecret(copy.URL)
		if err != nil {
			return err
		}
		encPass, err := encryptProxySecret(copy.Password)
		if err != nil {
			return err
		}
		encToken, err := encryptProxySecret(copy.ResinAdminToken)
		if err != nil {
			return err
		}
		out, err = scanProxy(tx.QueryRow(`UPDATE outbound_proxies SET name=$2,kind=$3,url=$4,host=$5,port=$6,username=$7,password=$8,resin_platform=$9,resin_admin_url=$10,resin_admin_token=$11,enabled=$12,status=$13,last_error='',last_checked_at=NULL,protocol=$14 WHERE id=$1
RETURNING id,name,kind,url,host,port,username,password,resin_platform,resin_admin_url,resin_admin_token,enabled,status,last_error,last_checked_at,created_at,updated_at,protocol`, copy.ID, copy.Name, copy.Kind, encURL, copy.Host, copy.Port, copy.Username, encPass, copy.ResinPlatform, copy.ResinAdminURL, encToken, copy.Enabled, initialProxyStatus(&copy), copy.Protocol))
		if err != nil || out.Kind != ProxyKindWarp {
			return err
		}
		state, err := healthTx(context.Background(), tx, out.ID)
		if err != nil {
			return err
		}
		return SyncWarpHealthIntentTx(context.Background(), tx, out.ID, WarpHealthIntent{Desired: state.Desired, Material: state.Material, ProfileRevision: state.ProfileRevision})
	})
	return out, err
}

func (d *DB) DeleteProxy(id int64) error {
	return d.DeleteProxyWithCleanup(id, nil)
}

// DeleteProxyWithCleanup 与任务创建及渠道绑定共享锁，在不可回滚的文件清理前检查引用。
func (d *DB) DeleteProxyWithCleanup(id int64, cleanup func(*Proxy) error) error {
	return d.withProxyLifecycle(func(tx *sql.Tx) error {
		proxy, err := getProxyTx(tx, id)
		if err != nil {
			return err
		}
		if proxy.Kind == ProxyKindWarp {
			if err = CheckWarpHealthMutationTx(context.Background(), tx, id); err != nil {
				return err
			}
			state, err := healthTx(context.Background(), tx, id)
			if err != nil {
				return err
			}
			if state.Claim.Token != "" {
				return ErrWarpHealthBlocked
			}
		}
		if err := checkProxyDeleteReferencesTx(tx, id); err != nil {
			return err
		}
		if cleanup != nil {
			p, err := getProxyTx(tx, id)
			if err != nil {
				return err
			}
			if err = cleanup(p); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(`UPDATE warp_proxy_create_requests SET state='deleted',proxy_id=NULL,updated_at=now() WHERE proxy_id=$1`, id); err != nil {
			return err
		}
		result, err := tx.Exec(`DELETE FROM outbound_proxies WHERE id=$1`, id)
		if err != nil {
			return err
		}
		n, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			return ErrProxyUnavailable
		}
		return nil
	})
}

// checkProxyDeleteReferencesTx 在生命周期事务里先挡住所有静态和活动引用。
// WARP 删除会在网络清理前、提交前各检查一次，避免删掉仍被引用的运行材料。
func checkProxyDeleteReferencesTx(tx *sql.Tx, id int64) error {
	globalID, err := globalProxyIDTx(tx)
	if err != nil {
		return err
	}
	if globalID != nil && *globalID == id {
		return ErrProxyInUse
	}
	inUse, err := proxyInUseTx(tx, id)
	if err != nil {
		return err
	}
	if inUse {
		return ErrProxyInUse
	}
	var referenced bool
	if err = tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM llm_profiles WHERE proxy_id=$1) OR EXISTS(SELECT 1 FROM proxy_group_members WHERE proxy_id=$1)`, id).Scan(&referenced); err != nil {
		return err
	}
	if referenced {
		return ErrProxyInUse
	}
	pending, err := warpCreatePendingTx(tx, id)
	if err != nil {
		return err
	}
	if pending {
		return ErrProxyInUse
	}
	return nil
}

func (d *DB) SetProxyStatus(id int64, status, lastError string, checkedAt time.Time) error {
	return d.withProxyLifecycle(func(tx *sql.Tx) error {
		pending, err := warpCreatePendingTx(tx, id)
		if err != nil {
			return err
		}
		if pending {
			return ErrProxyInUse
		}
		_, err = tx.Exec(`UPDATE outbound_proxies SET status=$2,last_error=$3,last_checked_at=$4 WHERE id=$1`, id, status, lastError, checkedAt)
		return err
	})
}

func (d *DB) SetGlobalProxyID(id *int64) error {
	value := ""
	if id != nil {
		value = fmt.Sprintf("%d", *id)
	}
	return d.withProxyLifecycle(func(tx *sql.Tx) error {
		if id != nil {
			p, err := getProxyTx(tx, *id)
			if err != nil {
				return err
			}
			if err := usableProxy(p); err != nil {
				return err
			}
			if err := CheckWarpHealthAdmissionTx(context.Background(), tx, p.ID); err != nil {
				return err
			}
		}
		_, err := tx.Exec(`INSERT INTO settings(key,value) VALUES ('global_proxy_id',$1) ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value,updated_at=now()`, value)
		return err
	})
}

func (d *DB) GlobalProxyID() (*int64, error) {
	var v string
	err := d.QueryRow(`SELECT value FROM settings WHERE key='global_proxy_id'`).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(v) == "" {
		return nil, nil
	}
	id, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
	if err != nil || id <= 0 {
		return nil, fmt.Errorf("invalid global proxy id %q", v)
	}
	return &id, nil
}

func scanProxy(sc interface{ Scan(...any) error }) (*Proxy, error) {
	var p Proxy
	err := sc.Scan(&p.ID, &p.Name, &p.Kind, &p.URL, &p.Host, &p.Port, &p.Username, &p.Password, &p.ResinPlatform, &p.ResinAdminURL, &p.ResinAdminToken, &p.Enabled, &p.Status, &p.LastError, &p.LastCheckedAt, &p.CreatedAt, &p.UpdatedAt, &p.Protocol)
	if err != nil {
		return nil, err
	}
	for field, value := range map[string]*string{"url": &p.URL, "password": &p.Password, "admin_token": &p.ResinAdminToken} {
		if *value == "" {
			continue
		}
		plain, derr := decryptProxySecret(*value)
		if derr != nil {
			return nil, fmt.Errorf("decode proxy %s: %w", field, derr)
		}
		*value = plain
	}
	return &p, nil
}

func initialProxyStatus(p *Proxy) string {
	if p.Kind == ProxyKindWarp {
		return ProxyStatusStopped
	}
	return ProxyStatusUnknown
}

func (p *Proxy) Snapshot() ProxySnapshot {
	if p == nil {
		return ProxySnapshot{}
	}
	protocol := p.Protocol
	if protocol == "" {
		protocol = string(p.Kind)
	}
	return ProxySnapshot{ID: p.ID, Name: p.Name, Kind: p.Kind, URL: p.URL, Host: p.Host, Port: p.Port, Platform: p.ResinPlatform, Protocol: protocol, Username: p.Username, Password: p.Password, AdminToken: p.ResinAdminToken}
}

func (p ProxySnapshot) MarshalJSON() ([]byte, error) {
	type wire struct {
		ID         int64     `json:"id,omitempty"`
		Name       string    `json:"name,omitempty"`
		Kind       ProxyKind `json:"kind,omitempty"`
		URL        string    `json:"url,omitempty"`
		Host       string    `json:"host,omitempty"`
		Port       int       `json:"port,omitempty"`
		Platform   string    `json:"platform,omitempty"`
		Protocol   string    `json:"protocol,omitempty"`
		Username   string    `json:"username,omitempty"`
		Password   string    `json:"password,omitempty"`
		AdminToken string    `json:"admin_token,omitempty"`
	}
	// Values already carrying v1: are ciphertext (the DB task path); plaintext
	// values are encrypted here so generic json.Marshal cannot leak credentials.
	enc := func(v string) (string, error) {
		if strings.HasPrefix(v, "v1:") {
			return v, nil
		}
		return encryptProxySecret(v)
	}
	u, err := enc(p.URL)
	if err != nil {
		return nil, err
	}
	pw, err := enc(p.Password)
	if err != nil {
		return nil, err
	}
	at, err := enc(p.AdminToken)
	if err != nil {
		return nil, err
	}
	return json.Marshal(wire{p.ID, p.Name, p.Kind, u, p.Host, p.Port, p.Platform, p.Protocol, p.Username, pw, at})
}

func (p *ProxySnapshot) UnmarshalJSON(data []byte) error {
	type wire struct {
		ID         int64     `json:"id"`
		Name       string    `json:"name"`
		Kind       ProxyKind `json:"kind"`
		URL        string    `json:"url"`
		Host       string    `json:"host"`
		Port       int       `json:"port"`
		Platform   string    `json:"platform"`
		Protocol   string    `json:"protocol"`
		Username   string    `json:"username"`
		Password   string    `json:"password"`
		AdminToken string    `json:"admin_token"`
	}
	var w wire
	if err := json.Unmarshal(data, &w); err != nil {
		return err
	}
	for _, value := range []string{w.URL, w.Password, w.AdminToken} {
		if value != "" && !strings.HasPrefix(value, "v1:") {
			return errors.New("proxy snapshot contains unencrypted secret")
		}
		if value != "" {
			if _, err := fmtHexDecode(value[3:]); err != nil {
				return err
			}
		}
	}
	*p = ProxySnapshot{ID: w.ID, Name: w.Name, Kind: w.Kind, URL: w.URL, Host: w.Host, Port: w.Port, Platform: w.Platform, Protocol: w.Protocol, Username: w.Username, Password: w.Password, AdminToken: w.AdminToken}
	return nil
}

// ResolveProxySnapshot is the only DB boundary that exposes a task snapshot's
// credentials to trusted runtime code. Database rows and generic JSON stay encrypted.
func (d *DB) ResolveProxySnapshot(s ProxySnapshot) (ProxySnapshot, error) {
	var err error
	if s.URL, err = decryptProxySecret(s.URL); err != nil {
		return ProxySnapshot{}, err
	}
	if s.Password, err = decryptProxySecret(s.Password); err != nil {
		return ProxySnapshot{}, err
	}
	if s.AdminToken, err = decryptProxySecret(s.AdminToken); err != nil {
		return ProxySnapshot{}, err
	}
	return s, nil
}

func (p *Proxy) String() string {
	if p == nil {
		return ""
	}
	return fmt.Sprintf("#%d %s", p.ID, p.Name)
}
