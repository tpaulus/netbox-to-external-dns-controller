package main

import (
	"errors"
	"fmt"
	"strings"
	"time"

	env "github.com/caarlos0/env/v11"
)

const (
	managedByLabel = "app.kubernetes.io/managed-by"
	managedByValue = "netbox-dns-sync"
)

type Config struct {
	NetBoxURL       string              `env:"NETBOX_URL,required,notEmpty"`
	NetBoxToken     string              `env:"NETBOX_TOKEN,required,notEmpty"`
	NetBoxFilters   string              `env:"NETBOX_FILTERS"`
	NetBoxPageSize  int                 `env:"NETBOX_PAGE_SIZE" envDefault:"100"`
	Namespace       string              `env:"TARGET_NAMESPACE" envDefault:"external-records"`
	DomainAllowlist string              `env:"DOMAIN_ALLOWLIST,required,notEmpty"`
	Allowlist       []string            `env:"-"`
	SyncInterval    time.Duration       `env:"SYNC_INTERVAL"`
	DryRun          bool                `env:"DRY_RUN"`
	MaxChanges      int                 `env:"MAX_CHANGES_PER_RUN"`
	NetBoxStatuses  string              `env:"NETBOX_ENABLED_STATUSES" envDefault:"active"`
	EnabledStatuses map[string]struct{} `env:"-"`
	TTLField        string              `env:"NETBOX_TTL_FIELD" envDefault:"external_dns_ttl"`
	MetricsAddress  string              `env:"METRICS_ADDRESS"`
	KubeconfigPath  string              `env:"KUBECONFIG"`
	HTTPTimeout     time.Duration       `env:"HTTP_TIMEOUT" envDefault:"30s"`
	RetryCount      int                 `env:"RETRY_COUNT" envDefault:"3"`
	DeleteOnPartial bool                `env:"DELETE_ON_PARTIAL_FAILURE"`
}

func loadConfig() (Config, error) {
	var cfg Config
	if err := env.Parse(&cfg); err != nil {
		return Config{}, fmt.Errorf("parse environment: %w", err)
	}

	cfg.NetBoxURL = strings.TrimRight(strings.TrimSpace(cfg.NetBoxURL), "/")
	cfg.NetBoxToken = strings.TrimSpace(cfg.NetBoxToken)
	cfg.NetBoxFilters = strings.TrimPrefix(strings.TrimSpace(cfg.NetBoxFilters), "?")
	cfg.Namespace = strings.TrimSpace(cfg.Namespace)
	cfg.Allowlist = normalizeList(cfg.DomainAllowlist)
	cfg.NetBoxStatuses = strings.TrimSpace(cfg.NetBoxStatuses)
	cfg.EnabledStatuses = listToSet(normalizeList(cfg.NetBoxStatuses))
	cfg.TTLField = strings.TrimSpace(cfg.TTLField)
	cfg.MetricsAddress = strings.TrimSpace(cfg.MetricsAddress)
	cfg.KubeconfigPath = strings.TrimSpace(cfg.KubeconfigPath)

	if cfg.NetBoxURL == "" {
		return Config{}, errors.New("NETBOX_URL is required")
	}
	if cfg.NetBoxToken == "" {
		return Config{}, errors.New("NETBOX_TOKEN is required")
	}
	if len(cfg.Allowlist) == 0 {
		return Config{}, errors.New("DOMAIN_ALLOWLIST must contain at least one suffix")
	}
	if len(cfg.EnabledStatuses) == 0 {
		return Config{}, errors.New("NETBOX_ENABLED_STATUSES must contain at least one status")
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

func listToSet(values []string) map[string]struct{} {
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		result[value] = struct{}{}
	}
	return result
}
