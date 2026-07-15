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

	netbox "github.com/netbox-community/go-netbox/v4"
)

type netBoxClient struct {
	client   *netbox.APIClient
	pageSize int
	filters  url.Values
	retries  int
}

type sourceRecord struct {
	ObjectID   int
	DeviceID   int
	DeviceName string
	DNSName    string
	Target     string
	Type       string
	TTL        *int64
	Group      string
}

type fetchResult struct {
	Records       []sourceRecord
	RecordErrors  []error
	SourceHealthy bool
}

type assignedObjectDetails struct {
	Device *assignedDevice `json:"device,omitempty"`
}

type assignedDevice struct {
	ID      int32  `json:"id"`
	Name    string `json:"name"`
	Display string `json:"display"`
}

func newNetBoxClient(cfg Config) (*netBoxClient, error) {
	filters, err := url.ParseQuery(cfg.NetBoxFilters)
	if err != nil {
		return nil, fmt.Errorf("parse NETBOX_FILTERS: %w", err)
	}

	client := netbox.NewAPIClientFor(cfg.NetBoxURL, cfg.NetBoxToken)
	client.GetConfig().HTTPClient = &http.Client{Timeout: cfg.HTTPTimeout}

	return &netBoxClient{
		client:   client,
		pageSize: cfg.NetBoxPageSize,
		filters:  filters,
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
		var page netbox.PaginatedIPAddressList
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
				logger.Warn("skipping malformed NetBox record", "record_id", item.GetId(), "error", recordErr)
				continue
			}
			if !keep {
				continue
			}
			result.Records = append(result.Records, record)
		}

		next := strings.TrimSpace(page.GetNext())
		if next == "" {
			nextURL = nil
			continue
		}
		parsedNext, err := url.Parse(next)
		if err != nil {
			return fetchResult{Records: result.Records, RecordErrors: result.RecordErrors, SourceHealthy: false}, fmt.Errorf("parse next page URL: %w", err)
		}
		nextURL = parsedNext
	}

	return result, nil
}

func (c *netBoxClient) ipAddressURL() (*url.URL, error) {
	baseURL, err := c.client.GetConfig().ServerURL(0, nil)
	if err != nil {
		return nil, fmt.Errorf("resolve NetBox base URL: %w", err)
	}
	endpoint, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("parse NetBox base URL: %w", err)
	}
	endpoint.Path = path.Join(endpoint.Path, "/api/ipam/ip-addresses/")

	query := url.Values{}
	for key, values := range c.filters {
		query[key] = append([]string(nil), values...)
	}
	query.Set("limit", strconv.Itoa(c.pageSize))
	endpoint.RawQuery = query.Encode()
	return endpoint, nil
}

func (c *netBoxClient) getPage(ctx context.Context, rawURL string, target any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return err
	}
	for key, value := range c.client.GetConfig().DefaultHeader {
		req.Header.Set(key, value)
	}
	if req.Header.Get("Accept") == "" {
		req.Header.Set("Accept", "application/json")
	}

	resp, err := c.client.GetConfig().HTTPClient.Do(req)
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

func transformIPAddress(item netbox.IPAddress, cfg Config) (sourceRecord, error, bool) {
	name := normalizeDNSName(stringValue(item.DnsName))
	if name == "" {
		return sourceRecord{}, nil, false
	}
	if _, ok := cfg.EnabledStatuses[normalizeStatus(item.Status)]; !ok {
		return sourceRecord{}, nil, false
	}

	deviceID, deviceName, err := deviceIdentity(item)
	if err != nil {
		return sourceRecord{}, fmt.Errorf("record %d missing device identity: %w", item.GetId(), err), false
	}

	ip, err := parseIPAddress(item.Address)
	if err != nil {
		return sourceRecord{}, fmt.Errorf("record %d invalid address %q: %w", item.GetId(), item.Address, err), false
	}

	group := matchAllowlist(name, cfg.Allowlist)
	if group == "" {
		return sourceRecord{}, fmt.Errorf("record %d name %q is outside DOMAIN_ALLOWLIST", item.GetId(), name), false
	}

	ttl, err := parseTTL(item.CustomFields, cfg.TTLField)
	if err != nil {
		return sourceRecord{}, fmt.Errorf("record %d invalid ttl: %w", item.GetId(), err), false
	}

	recordType := "AAAA"
	if ip.To4() != nil {
		recordType = "A"
	}

	return sourceRecord{
		ObjectID:   int(item.GetId()),
		DeviceID:   deviceID,
		DeviceName: deviceName,
		DNSName:    name,
		Target:     ip.String(),
		Type:       recordType,
		TTL:        ttl,
		Group:      group,
	}, nil, true
}

func deviceIdentity(item netbox.IPAddress) (int, string, error) {
	if !item.HasAssignedObject() {
		return 0, "", errors.New("assigned_object is empty")
	}

	data, err := json.Marshal(item.GetAssignedObject())
	if err != nil {
		return 0, "", fmt.Errorf("marshal assigned_object: %w", err)
	}

	var details assignedObjectDetails
	if err := json.Unmarshal(data, &details); err != nil {
		return 0, "", fmt.Errorf("decode assigned_object: %w", err)
	}
	if details.Device == nil {
		return 0, "", errors.New("assigned_object does not reference a device")
	}

	name := strings.TrimSpace(details.Device.Name)
	if name == "" {
		name = strings.TrimSpace(details.Device.Display)
	}
	if name == "" {
		return 0, "", errors.New("assigned device name is empty")
	}
	if details.Device.ID == 0 {
		return 0, "", errors.New("assigned device id is empty")
	}
	return int(details.Device.ID), name, nil
}

func normalizeStatus(status *netbox.IPAddressStatus) string {
	if status == nil {
		return ""
	}
	if value, ok := status.GetValueOk(); ok && value != nil {
		return strings.ToLower(strings.TrimSpace(string(*value)))
	}
	if label, ok := status.GetLabelOk(); ok && label != nil {
		return strings.ToLower(strings.TrimSpace(string(*label)))
	}
	return ""
}

func stringValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
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
	case int32:
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
