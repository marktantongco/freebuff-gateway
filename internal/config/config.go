package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

// Config holds all gateway configuration
type Config struct {
	ListenAddr      string          `json:"listen_addr"`
	AdminPassword   string          `json:"admin_password"`
	AdminSessionTTL time.Duration   `json:"admin_session_ttl"`
	DBPath          string          `json:"db_path"`
	Session         SessionConfig   `json:"session"`
	Transport       TransportConfig `json:"transport"`
	ProxyPool       ProxyPoolConfig `json:"proxy_pool"`
	Stealth         StealthConfig   `json:"stealth"`
	RateLimit       RateLimitConfig `json:"rate_limit"`
	Logging         LoggingConfig   `json:"logging"`
	HealthCheck     HealthCheckCfg  `json:"health_check"`
	Models          ModelsConfig    `json:"models"`
}

// SessionConfig holds session settings
type SessionConfig struct {
	WaitOnFull    bool              `json:"wait_on_full"`
	CreateLimits  CreateLimitConfig `json:"create_limits"`
}

// CreateLimitConfig holds session creation limits
type CreateLimitConfig struct {
	MaxParallelGlobal   int `json:"max_parallel_global"`
	MaxParallelPerKey   int `json:"max_parallel_per_key"`
	MaxParallelPerModel int `json:"max_parallel_per_model"`
	MaxParallelPerGroup int `json:"max_parallel_per_group"`
}

// TransportConfig holds HTTP transport settings
type TransportConfig struct {
	Timeout      time.Duration `json:"timeout"`
	RequestReuse bool          `json:"request_reuse"`
	BodyPreview  int           `json:"body_preview_bytes"`
}

// ProxyPoolConfig holds proxy pool settings
type ProxyPoolConfig struct {
	Enabled     bool                    `json:"enabled"`
	PrimaryURL  string                  `json:"primary_url"`
	HealthCheck ProxyPoolHealthConfig   `json:"health_check"`
}

// ProxyPoolHealthConfig holds proxy health check settings
type ProxyPoolHealthConfig struct {
	Enabled     bool          `json:"enabled"`
	URL         string        `json:"url"`
	Interval    time.Duration `json:"interval"`
	Timeout     time.Duration `json:"timeout"`
	Concurrency int           `json:"concurrency"`
}

// StealthConfig holds stealth transport settings
type StealthConfig struct {
	Enabled         bool   `json:"enabled"`
	TLSFingerprint  string `json:"tls_fingerprint"`
	HeaderRandomize bool   `json:"header_randomization"`
	ProxyRotation   bool   `json:"proxy_rotation"`
}

// RateLimitConfig holds rate limiting settings
type RateLimitConfig struct {
	Enabled           bool `json:"enabled"`
	RequestsPerMinute int  `json:"requests_per_minute"`
}

// LoggingConfig holds logging settings
type LoggingConfig struct {
	Level        string `json:"level"`
	File         string `json:"file"`
	RedactTokens bool   `json:"redact_tokens"`
}

// HealthCheckCfg holds health check settings
type HealthCheckCfg struct {
	Enabled  bool          `json:"enabled"`
	Interval time.Duration `json:"interval"`
}

// ModelsConfig holds model registry settings
type ModelsConfig struct {
	Aliases   map[string]string `json:"aliases"`
	Allowlist []string          `json:"allowlist"`
}

// DefaultConfig returns the default configuration
func DefaultConfig() *Config {
	return &Config{
		ListenAddr:      ":30080",
		AdminPassword:   "admin",
		AdminSessionTTL: 12 * time.Hour,
		DBPath:          "./data/gateway.db",
		Session: SessionConfig{
			WaitOnFull: false,
			CreateLimits: CreateLimitConfig{
				MaxParallelGlobal:   128,
				MaxParallelPerKey:   32,
				MaxParallelPerModel: 32,
				MaxParallelPerGroup: 96,
			},
		},
		Transport: TransportConfig{
			Timeout:      60 * time.Second,
			RequestReuse: false,
			BodyPreview:  8192,
		},
		ProxyPool: ProxyPoolConfig{
			Enabled:    false,
			PrimaryURL: "",
			HealthCheck: ProxyPoolHealthConfig{
				Enabled:     true,
				URL:         "http://ip-api.com/json/?fields=status,message,country,regionName,city,query",
				Interval:    1 * time.Minute,
				Timeout:     10 * time.Second,
				Concurrency: 5,
			},
		},
		Stealth: StealthConfig{
			Enabled:         false,
			TLSFingerprint:  "chrome",
			HeaderRandomize: true,
			ProxyRotation:   true,
		},
		RateLimit: RateLimitConfig{
			Enabled:           true,
			RequestsPerMinute: 60,
		},
		Logging: LoggingConfig{
			Level:        "info",
			File:         "",
			RedactTokens: true,
		},
		HealthCheck: HealthCheckCfg{
			Enabled:  true,
			Interval: 30 * time.Second,
		},
		Models: ModelsConfig{
			Aliases: map[string]string{
				"deepseek-v4-pro":   "deepseek/deepseek-v4-pro",
				"deepseek-v4-flash": "deepseek/deepseek-v4-flash",
				"mimo-v2.5":         "mimo/mimo-v2.5",
				"kimi-k2.6":         "moonshotai/kimi-k2.6",
				"minimax-m2.7":      "minimax/minimax-m2.7",
				"gemini-pro":        "google/gemini-3.1-pro-preview",
			},
			Allowlist: []string{"*"},
		},
	}
}

// Load loads configuration from file and environment variables
func Load(configPath string) (*Config, error) {
	cfg := DefaultConfig()

	if configPath != "" {
		if err := loadFromFile(cfg, configPath); err != nil {
			return nil, fmt.Errorf("load config file: %w", err)
		}
	} else {
		for _, path := range []string{
			"data/config.json",
			"config.json",
			"/etc/freebuff-gateway/config.json",
			filepath.Join(os.Getenv("HOME"), ".config/freebuff-gateway/config.json"),
		} {
			if _, err := os.Stat(path); err == nil {
				if err := loadFromFile(cfg, path); err == nil {
					break
				}
			}
		}
	}

	applyEnvOverrides(cfg)

	if err := Validate(cfg); err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}

	return cfg, nil
}

func loadFromFile(cfg *Config, path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}
	if err := json.Unmarshal(data, cfg); err != nil {
		return fmt.Errorf("parse %s: %w", path, err)
	}
	return nil
}

func applyEnvOverrides(cfg *Config) {
	cfg.ListenAddr = envStr("LISTEN_ADDR", cfg.ListenAddr)
	cfg.AdminPassword = envStr("ADMIN_PASSWORD", cfg.AdminPassword)
	cfg.DBPath = envStr("DB_PATH", cfg.DBPath)

	cfg.Session.WaitOnFull = envBool("SESSION_WAIT_ON_FULL", cfg.Session.WaitOnFull)
	cfg.Session.CreateLimits.MaxParallelGlobal = envInt("SESSION_CREATE_MAX_PARALLEL_GLOBAL", cfg.Session.CreateLimits.MaxParallelGlobal)
	cfg.Session.CreateLimits.MaxParallelPerKey = envInt("SESSION_CREATE_MAX_PARALLEL_PER_KEY", cfg.Session.CreateLimits.MaxParallelPerKey)
	cfg.Session.CreateLimits.MaxParallelPerModel = envInt("SESSION_CREATE_MAX_PARALLEL_PER_MODEL", cfg.Session.CreateLimits.MaxParallelPerModel)
	cfg.Session.CreateLimits.MaxParallelPerGroup = envInt("SESSION_CREATE_MAX_PARALLEL_PER_GROUP", cfg.Session.CreateLimits.MaxParallelPerGroup)

	cfg.Transport.Timeout = envDuration("TRANSPORT_TIMEOUT", cfg.Transport.Timeout)
	cfg.Transport.RequestReuse = envBool("TRANSPORT_REQUEST_REUSE", cfg.Transport.RequestReuse)
	cfg.Transport.BodyPreview = envInt("TRANSPORT_BODY_PREVIEW_BYTES", cfg.Transport.BodyPreview)

	cfg.ProxyPool.Enabled = envBool("PROXY_POOL_ENABLED", cfg.ProxyPool.Enabled)
	cfg.ProxyPool.PrimaryURL = envStr("PROXY_PRIMARY_URL", cfg.ProxyPool.PrimaryURL)
	cfg.ProxyPool.HealthCheck.Enabled = envBool("PROXY_HEALTHCHECK_ENABLED", cfg.ProxyPool.HealthCheck.Enabled)
	cfg.ProxyPool.HealthCheck.URL = envStr("PROXY_HEALTHCHECK_URL", cfg.ProxyPool.HealthCheck.URL)
	cfg.ProxyPool.HealthCheck.Interval = envDuration("PROXY_HEALTHCHECK_INTERVAL", cfg.ProxyPool.HealthCheck.Interval)
	cfg.ProxyPool.HealthCheck.Timeout = envDuration("PROXY_HEALTHCHECK_TIMEOUT", cfg.ProxyPool.HealthCheck.Timeout)
	cfg.ProxyPool.HealthCheck.Concurrency = envInt("PROXY_HEALTHCHECK_CONCURRENCY", cfg.ProxyPool.HealthCheck.Concurrency)

	cfg.Stealth.Enabled = envBool("STEALTH_ENABLED", cfg.Stealth.Enabled)
	cfg.Stealth.TLSFingerprint = envStr("STEALTH_TLS_FINGERPRINT", cfg.Stealth.TLSFingerprint)
	cfg.Stealth.HeaderRandomize = envBool("STEALTH_HEADER_RANDOMIZATION", cfg.Stealth.HeaderRandomize)
	cfg.Stealth.ProxyRotation = envBool("STEALTH_PROXY_ROTATION", cfg.Stealth.ProxyRotation)

	cfg.RateLimit.Enabled = envBool("RATE_LIMIT_ENABLED", cfg.RateLimit.Enabled)
	cfg.RateLimit.RequestsPerMinute = envInt("RATE_LIMIT_RPM", cfg.RateLimit.RequestsPerMinute)

	cfg.Logging.Level = envStr("LOG_LEVEL", cfg.Logging.Level)
	cfg.Logging.File = envStr("LOG_FILE", cfg.Logging.File)
	cfg.Logging.RedactTokens = envBool("LOG_REDACT_TOKENS", cfg.Logging.RedactTokens)

	cfg.HealthCheck.Enabled = envBool("HEALTH_CHECK_ENABLED", cfg.HealthCheck.Enabled)
	cfg.HealthCheck.Interval = envDuration("HEALTH_CHECK_INTERVAL", cfg.HealthCheck.Interval)

	if envBool("FREEBUFF_TRANSPORT_REUSE", false) {
		cfg.Transport.RequestReuse = true
	}
	if envStr("FREEBUFF_TRANSPORT_REUSE_SCOPE", "") == "request" {
		cfg.Transport.RequestReuse = true
	}
}

// Validate validates the configuration
func Validate(cfg *Config) error {
	if cfg.ListenAddr == "" {
		return fmt.Errorf("listen_addr is required")
	}
	if cfg.DBPath == "" {
		return fmt.Errorf("db_path is required")
	}
	if cfg.AdminPassword == "" {
		return fmt.Errorf("admin_password is required")
	}
	if cfg.Session.CreateLimits.MaxParallelGlobal <= 0 {
		return fmt.Errorf("session.create_limits.max_parallel_global must be > 0")
	}
	if cfg.Transport.Timeout <= 0 {
		return fmt.Errorf("transport.timeout must be > 0")
	}
	if cfg.Transport.BodyPreview <= 0 {
		return fmt.Errorf("transport.body_preview_bytes must be > 0")
	}
	if cfg.ProxyPool.HealthCheck.Concurrency <= 0 {
		return fmt.Errorf("proxy_pool.health_check.concurrency must be > 0")
	}
	if cfg.RateLimit.RequestsPerMinute <= 0 {
		return fmt.Errorf("rate_limit.requests_per_minute must be > 0")
	}
	validLevels := map[string]bool{"debug": true, "info": true, "warn": true, "error": true}
	if !validLevels[cfg.Logging.Level] {
		return fmt.Errorf("logging.level must be one of: debug, info, warn, error")
	}
	if len(cfg.Models.Allowlist) == 0 {
		return fmt.Errorf("models.allowlist must not be empty")
	}
	return nil
}

// Save saves the configuration to a JSON file
func Save(cfg *Config, path string) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("create directory: %w", err)
	}
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}
	return os.WriteFile(path, data, 0600)
}

// String returns a redacted string representation of the config
func (cfg *Config) String() string {
	copy := *cfg
	if copy.AdminPassword != "" {
		copy.AdminPassword = "****"
	}
	data, _ := json.MarshalIndent(copy, "", "  ")
	return string(data)
}


// Watcher watches a config file for changes
type Watcher struct {
	cfg      *Config
	path     string
	interval time.Duration
	onChange func(*Config)
	stopCh   chan struct{}
	mu       sync.RWMutex
}

// NewWatcher creates a new config file watcher
func NewWatcher(cfg *Config, path string, interval time.Duration, onChange func(*Config)) *Watcher {
	return &Watcher{cfg: cfg, path: path, interval: interval, onChange: onChange, stopCh: make(chan struct{})}
}

// Start starts watching
func (w *Watcher) Start() {}

// Stop stops watching
func (w *Watcher) Stop() { close(w.stopCh) }

// Get returns the current config
func (w *Watcher) Get() *Config { w.mu.RLock(); defer w.mu.RUnlock(); return w.cfg }


// Env helper functions
func envStr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envBool(key string, fallback bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	return v == "true" || v == "1" || v == "yes"
}

func envInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return fallback
	}
	return n
}

func envDuration(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return fallback
	}
	return d
}

// customDuration wraps time.Duration for JSON string parsing
type customDuration time.Duration

func (d *customDuration) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		parsed, err := time.ParseDuration(s)
		if err != nil {
			return err
		}
		*d = customDuration(parsed)
		return nil
	}
	var n int64
	if err := json.Unmarshal(data, &n); err != nil {
		return err
	}
	*d = customDuration(n)
	return nil
}

func (d customDuration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

// Account represents a provider account
type Account struct {
	ID       string `json:"id"`
	Email    string `json:"email,omitempty"`
	UserId   string `json:"userId,omitempty"`
	Nickname string `json:"nickname,omitempty"`

	AccessToken  string `json:"accessToken"`
	RefreshToken string `json:"refreshToken"`
	ClientID     string `json:"clientId,omitempty"`
	ClientSecret string `json:"clientSecret,omitempty"`
	AuthMethod   string `json:"authMethod"`
	Provider     string `json:"provider,omitempty"`
	Region       string `json:"region"`
	StartUrl     string `json:"startUrl,omitempty"`
	ExpiresAt    int64  `json:"expiresAt,omitempty"`
	MachineId    string `json:"machineId,omitempty"`
	ProfileArn   string `json:"profileArn,omitempty"`
	ProxyURL     string `json:"proxyURL,omitempty"`

	Weight int `json:"weight,omitempty"`

	AllowOverage      bool    `json:"allowOverage,omitempty"`
	OverageWeight     int     `json:"overageWeight,omitempty"`
	OverageStatus     string  `json:"overageStatus,omitempty"`
	OverageCapability string  `json:"overageCapability,omitempty"`
	OverageCap        float64 `json:"overageCap,omitempty"`
	OverageRate       float64 `json:"overageRate,omitempty"`
	CurrentOverages   float64 `json:"currentOverages,omitempty"`
	OverageCheckedAt  int64   `json:"overageCheckedAt,omitempty"`

	Enabled   bool   `json:"enabled"`
	BanStatus string `json:"banStatus,omitempty"`
	BanReason string `json:"banReason,omitempty"`
	BanTime   int64  `json:"banTime,omitempty"`

	SubscriptionType  string `json:"subscriptionType,omitempty"`
	SubscriptionTitle string `json:"subscriptionTitle,omitempty"`
	DaysRemaining     int    `json:"daysRemaining,omitempty"`

	UsageCurrent  float64 `json:"usageCurrent,omitempty"`
	UsageLimit    float64 `json:"usageLimit,omitempty"`
	UsagePercent  float64 `json:"usagePercent,omitempty"`
	NextResetDate string  `json:"nextResetDate,omitempty"`
	LastRefresh   int64   `json:"lastRefresh,omitempty"`

	TrialUsageCurrent float64 `json:"trialUsageCurrent,omitempty"`
	TrialUsageLimit   float64 `json:"trialUsageLimit,omitempty"`
	TrialUsagePercent float64 `json:"trialUsagePercent,omitempty"`
	TrialStatus       string  `json:"trialStatus,omitempty"`
	TrialExpiresAt    int64   `json:"trialExpiresAt,omitempty"`

	RequestCount int     `json:"requestCount,omitempty"`
	ErrorCount   int     `json:"errorCount,omitempty"`
	LastUsed     int64   `json:"lastUsed,omitempty"`
	TotalTokens  int     `json:"totalTokens,omitempty"`
	TotalCredits float64 `json:"totalCredits,omitempty"`
}

// GetEnabledAccounts returns all enabled accounts
func GetEnabledAccounts() []Account {
	return nil
}

// GetProxyURL returns the proxy URL for an account
func GetProxyURL() string {
	return ""
}

// GetAllowOverUsage returns whether overage is allowed
func GetAllowOverUsage() bool {
	return false
}

// UpdateAccountStats updates account statistics
func UpdateAccountStats(id string, requestCount, errorCount, totalTokens int, totalCredits float64, lastUsed int64) error {
	return nil
}
