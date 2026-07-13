package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"
)

type netBoxClient struct {
	baseURL    *url.URL
	httpClient *http.Client
	token      string
	pageSize   int
	filters    string
	retries    int
}

type netBoxListResponse struct {
	Count   int               `json:"count"`
	Next    string            `json:"next"`
	Results []netBoxIPAddress `json:"results"`
}

type netBoxIPAddress struct {
	ID           int            `json:"id"`
	Address      string         `json:"address"`
	DNSName      string         `json:"dns_name"`
	Description  string         `json:"description"`
	Status       netBoxStatus   `json:"status"`
	Tags         []netBoxTag    `json:"tags"`
	CustomFields map[string]any `json:"custom_fields"`
}

type netBoxTag struct {
	Name string `json:"name"`
	Slug string `json:"slug"`
}

type netBoxStatus struct {
	Value string
}

func (s *netBoxStatus) UnmarshalJSON(data []byte) error {
	var asString string
	if err := json.Unmarshal(data, &asString); err == nil {
		s.Value = strings.ToLower(strings.TrimSpace(asString))
		return nil
	}

	var asObject struct {
		Value string `json:"value"`
		Label string `json:"label"`
	}
	if err := json.Unmarshal(data, &asObject); err != nil {
		return err
	}
	switch {
	case strings.TrimSpace(asObject.Value) != "":
		s.Value = strings.ToLower(strings.TrimSpace(asObject.Value))
	case strings.TrimSpace(asObject.Label) != "":
		s.Value = strings.ToLower(strings.TrimSpace(asObject.Label))
	}
	return nil
}

type sourceRecord struct {
	ObjectID int
	DNSName  string
	Target   string
	Type     string
	TTL      *int64
	Tags     []string
	Group    string
}

type fetchResult struct {
	Records       []sourceRecord
	RecordErrors  []error
	SourceHealthy bool
}

func newNetBoxClient(cfg Config) (*netBoxClient, error) {
	parsed, err := url.Parse(cfg.NetBoxURL)
	if err != nil {
		return nil, fmt.Errorf("parse NETBOX_URL: %w", err)
	}
	return &netBoxClient{
		baseURL: parsed,
		httpClient: &http.Client{
			Timeout: cfg.HTTPTimeout,
		},
		token:    cfg.NetBoxToken,
		pageSize: cfg.NetBoxPageSize,
		filters:  cfg.NetBoxFilters,
		retries:  cfg.RetryCount,
	}, nil
}

func (c *netBoxClient) fetchRecords(ctx context.Context, cfg Config, logger *slog.Logger) (fetchResult, error) {
	start := time.Now()
	defer func() {
		getMetrics().sourceFetchDuration.Observe(time.Since(start).Seconds())
	}()

	nextURL, err := c.ipAddressURL()
	if err != nil {
		return fetchResult{}, err
	}

	result := fetchResult{SourceHealthy: true}
	for nextURL != nil {
		var page netBoxListResponse
		if err := retry(ctx, c.retries, func() error {
			return c.getPage(ctx, nextURL.String(), &page)
		}); err != nil {
			return fetchResult{Records: result.Records, RecordErrors: result.RecordErrors, SourceHealthy: false}, err
		}

		for _, item := range page.Results {
			record, recordErr, keep := transformIPAddress(item, cfg)
			if recordErr != nil {
				result.SourceHealthy = false
				result.RecordErrors = append(result.RecordErrors, recordErr)
				logger.Warn("skipping malformed NetBox record", "record_id", item.ID, "error", recordErr)
				continue
			}
			if !keep {
				continue
			}
			result.Records = append(result.Records, record)
		}

		if strings.TrimSpace(page.Next) == "" {
			nextURL = nil
			continue
		}
		parsedNext, err := url.Parse(page.Next)
		if err != nil {
			return fetchResult{Records: result.Records, RecordErrors: result.RecordErrors, SourceHealthy: false}, fmt.Errorf("parse next page URL: %w", err)
		}
		nextURL = parsedNext
	}

	return result, nil
}

func (c *netBoxClient) ipAddressURL() (*url.URL, error) {
	endpoint := *c.baseURL
	endpoint.Path = path.Join(endpoint.Path, "/api/ipam/ip-addresses/")
	query, err := url.ParseQuery(c.filters)
	if err != nil {
		return nil, fmt.Errorf("parse NETBOX_FILTERS: %w", err)
	}
	query.Set("limit", strconv.Itoa(c.pageSize))
	endpoint.RawQuery = query.Encode()
	return &endpoint, nil
}

func (c *netBoxClient) getPage(ctx context.Context, rawURL string, target any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Token "+c.token)
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode >= http.StatusBadRequest {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("netbox request failed: status=%d body=%q", resp.StatusCode, string(body))
	}

	if err := json.NewDecoder(resp.Body).Decode(target); err != nil {
		return fmt.Errorf("decode netbox response: %w", err)
	}
	return nil
}

func transformIPAddress(item netBoxIPAddress, cfg Config) (sourceRecord, error, bool) {
	name := normalizeDNSName(item.DNSName)
	if name == "" {
		return sourceRecord{}, nil, false
	}
	if _, ok := cfg.EnabledStatuses[item.Status.Value]; !ok {
		return sourceRecord{}, nil, false
	}

	ip, err := parseIPAddress(item.Address)
	if err != nil {
		return sourceRecord{}, fmt.Errorf("record %d invalid address %q: %w", item.ID, item.Address, err), false
	}

	group := matchAllowlist(name, cfg.Allowlist)
	if group == "" {
		return sourceRecord{}, fmt.Errorf("record %d name %q is outside DOMAIN_ALLOWLIST", item.ID, name), false
	}

	ttl, err := parseTTL(item.CustomFields, cfg.TTLField)
	if err != nil {
		return sourceRecord{}, fmt.Errorf("record %d invalid ttl: %w", item.ID, err), false
	}

	recordType := "AAAA"
	if ip.To4() != nil {
		recordType = "A"
	}

	return sourceRecord{
		ObjectID: item.ID,
		DNSName:  name,
		Target:   ip.String(),
		Type:     recordType,
		TTL:      ttl,
		Tags:     extractTagNames(item.Tags),
		Group:    group,
	}, nil, true
}

func parseIPAddress(raw string) (net.IP, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, errors.New("empty address")
	}
	ip, _, err := net.ParseCIDR(raw)
	if err == nil {
		return ip, nil
	}
	parsed := net.ParseIP(strings.TrimSpace(raw))
	if parsed == nil {
		return nil, fmt.Errorf("parse IP: %w", err)
	}
	return parsed, nil
}

func parseTTL(customFields map[string]any, field string) (*int64, error) {
	if field == "" || len(customFields) == 0 {
		return nil, nil
	}
	value, ok := customFields[field]
	if !ok || value == nil {
		return nil, nil
	}

	var ttl int64
	switch typed := value.(type) {
	case float64:
		ttl = int64(typed)
	case int:
		ttl = int64(typed)
	case int64:
		ttl = typed
	case json.Number:
		parsed, err := typed.Int64()
		if err != nil {
			return nil, err
		}
		ttl = parsed
	case string:
		parsed, err := strconv.ParseInt(strings.TrimSpace(typed), 10, 64)
		if err != nil {
			return nil, err
		}
		ttl = parsed
	default:
		return nil, fmt.Errorf("unsupported ttl type %T", value)
	}

	if ttl <= 0 {
		return nil, fmt.Errorf("ttl must be greater than zero, got %d", ttl)
	}
	return &ttl, nil
}

func extractTagNames(tags []netBoxTag) []string {
	if len(tags) == 0 {
		return nil
	}
	values := make([]string, 0, len(tags))
	for _, tag := range tags {
		switch {
		case strings.TrimSpace(tag.Slug) != "":
			values = append(values, strings.ToLower(strings.TrimSpace(tag.Slug)))
		case strings.TrimSpace(tag.Name) != "":
			values = append(values, strings.ToLower(strings.TrimSpace(tag.Name)))
		}
	}
	return values
}

func normalizeDNSName(name string) string {
	return strings.ToLower(strings.TrimSpace(strings.TrimSuffix(name, ".")))
}

func matchAllowlist(name string, allowlist []string) string {
	for _, suffix := range sortBySpecificity(allowlist) {
		if name == suffix || strings.HasSuffix(name, "."+suffix) {
			return suffix
		}
	}
	return ""
}
