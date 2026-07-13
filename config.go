package main

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	managedByLabel = "app.kubernetes.io/managed-by"
	managedByValue = "netbox-dns-sync"
)

type Config struct {
	NetBoxURL          string
	NetBoxToken        string
	NetBoxFilters      string
	NetBoxPageSize     int
	Namespace          string
	ExternalDNSOwnerID string
	Allowlist          []string
	SyncInterval       time.Duration
	DryRun             bool
	MaxChanges         int
	EnabledStatuses    map[string]struct{}
	TTLField           string
	MetricsAddress     string
	KubeconfigPath     string
	HTTPTimeout        time.Duration
	RetryCount         int
	DeleteOnPartial    bool
}

func loadConfig() (Config, error) {
	cfg := Config{
		NetBoxURL:          strings.TrimRight(os.Getenv("NETBOX_URL"), "/"),
		NetBoxToken:        os.Getenv("NETBOX_TOKEN"),
		NetBoxFilters:      strings.TrimPrefix(strings.TrimSpace(os.Getenv("NETBOX_FILTERS")), "?"),
		NetBoxPageSize:     envInt("NETBOX_PAGE_SIZE", 100),
		Namespace:          envString("TARGET_NAMESPACE", "external-records"),
		ExternalDNSOwnerID: envString("EXTERNAL_DNS_OWNER_ID", "netbox-dns-sync"),
		Allowlist:          normalizeList(os.Getenv("DOMAIN_ALLOWLIST")),
		DryRun:             envBool("DRY_RUN", false),
		MaxChanges:         envInt("MAX_CHANGES_PER_RUN", 0),
		EnabledStatuses: listToSet(defaultList(
			normalizeList(os.Getenv("NETBOX_ENABLED_STATUSES")),
			[]string{"active"},
		)),
		TTLField:       envString("NETBOX_TTL_FIELD", "external_dns_ttl"),
		MetricsAddress: strings.TrimSpace(os.Getenv("METRICS_ADDRESS")),
		KubeconfigPath: strings.TrimSpace(os.Getenv("KUBECONFIG")),
		HTTPTimeout:    envDuration("HTTP_TIMEOUT", 30*time.Second),
		RetryCount:     envInt("RETRY_COUNT", 3),
	}

	cfg.DeleteOnPartial = envBool("DELETE_ON_PARTIAL_FAILURE", false)

	if raw := strings.TrimSpace(os.Getenv("SYNC_INTERVAL")); raw != "" {
		interval, err := time.ParseDuration(raw)
		if err != nil {
			return Config{}, fmt.Errorf("parse SYNC_INTERVAL: %w", err)
		}
		cfg.SyncInterval = interval
	}

	if cfg.NetBoxURL == "" {
		return Config{}, errors.New("NETBOX_URL is required")
	}
	if cfg.NetBoxToken == "" {
		return Config{}, errors.New("NETBOX_TOKEN is required")
	}
	if len(cfg.Allowlist) == 0 {
		return Config{}, errors.New("DOMAIN_ALLOWLIST must contain at least one suffix")
	}
	if cfg.ExternalDNSOwnerID == "" {
		return Config{}, errors.New("EXTERNAL_DNS_OWNER_ID must not be empty")
	}
	if cfg.NetBoxPageSize <= 0 {
		return Config{}, errors.New("NETBOX_PAGE_SIZE must be greater than zero")
	}
	if cfg.HTTPTimeout <= 0 {
		return Config{}, errors.New("HTTP_TIMEOUT must be greater than zero")
	}
	if cfg.RetryCount <= 0 {
		return Config{}, errors.New("RETRY_COUNT must be greater than zero")
	}

	return cfg, nil
}

func envString(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func envBool(key string, fallback bool) bool {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func envInt(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func envDuration(key string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func normalizeList(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.FieldsFunc(raw, func(r rune) bool {
		return r == ',' || r == '\n'
	})
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.ToLower(strings.TrimSpace(strings.TrimSuffix(part, ".")))
		if trimmed == "" {
			continue
		}
		values = append(values, trimmed)
	}
	return values
}

func defaultList(values, fallback []string) []string {
	if len(values) == 0 {
		return fallback
	}
	return values
}

func listToSet(values []string) map[string]struct{} {
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		result[value] = struct{}{}
	}
	return result
}
