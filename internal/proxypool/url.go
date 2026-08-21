package proxypool

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
)

var supportedSchemes = map[string]struct{}{
	"http":    {},
	"https":   {},
	"socks5":  {},
	"socks5h": {},
}

type NormalizedProxy struct {
	URL      string
	Scheme   string
	Host     string
	Port     int
	Username string
}

func NormalizeProxyURL(raw string) (NormalizedProxy, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return NormalizedProxy{}, fmt.Errorf("proxy url required")
	}
	parsed, err := url.Parse(trimmed)
	if err != nil {
		return NormalizedProxy{}, fmt.Errorf("proxy url invalid: %w", err)
	}
	scheme := strings.ToLower(strings.TrimSpace(parsed.Scheme))
	if _, ok := supportedSchemes[scheme]; !ok {
		return NormalizedProxy{}, fmt.Errorf("proxy scheme must be http, https, socks5 or socks5h")
	}
	host := strings.ToLower(strings.TrimSpace(parsed.Hostname()))
	if host == "" {
		return NormalizedProxy{}, fmt.Errorf("proxy host required")
	}
	if parsed.Path != "" && parsed.Path != "/" {
		return NormalizedProxy{}, fmt.Errorf("proxy url must not include a path")
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return NormalizedProxy{}, fmt.Errorf("proxy url must not include query or fragment")
	}
	port := 0
	portText := parsed.Port()
	if portText != "" {
		n, err := strconv.Atoi(portText)
		if err != nil || n < 1 || n > 65535 {
			return NormalizedProxy{}, fmt.Errorf("proxy port invalid")
		}
		port = n
	}
	hostPort := host
	if portText != "" {
		hostPort = net.JoinHostPort(host, portText)
	} else if strings.Contains(host, ":") && !strings.HasPrefix(host, "[") {
		hostPort = "[" + host + "]"
	}
	normalized := &url.URL{
		Scheme: scheme,
		User:   parsed.User,
		Host:   hostPort,
	}
	return NormalizedProxy{
		URL:      normalized.String(),
		Scheme:   scheme,
		Host:     host,
		Port:     port,
		Username: parsed.User.Username(),
	}, nil
}

func RedactProxyURL(raw string) string {
	normalized, err := NormalizeProxyURL(raw)
	if err != nil {
		return ""
	}
	parsed, err := url.Parse(normalized.URL)
	if err != nil {
		return ""
	}
	if parsed.User != nil {
		username := parsed.User.Username()
		if _, hasPassword := parsed.User.Password(); hasPassword {
			parsed.User = url.UserPassword(username, "***")
		} else {
			parsed.User = url.User(username)
		}
	}
	return strings.ReplaceAll(parsed.String(), "%2A%2A%2A", "***")
}

func DefaultName(raw string) string {
	normalized, err := NormalizeProxyURL(raw)
	if err != nil {
		return "proxy"
	}
	if normalized.Port > 0 {
		return fmt.Sprintf("%s:%d", normalized.Host, normalized.Port)
	}
	return normalized.Host
}

func ProxyKey(normalizedURL string) string {
	normalized, err := NormalizeProxyURL(normalizedURL)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256([]byte(normalized.URL))
	return hex.EncodeToString(sum[:])
}
