package main

import (
	"testing"

	netbox "github.com/netbox-community/go-netbox/v4"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestBuildDesiredResourcesDeterministic(t *testing.T) {
	ttl := int64(300)
	resources := buildDesiredResources("external-records", []sourceRecord{
		{ObjectID: 2, DeviceID: 10, DeviceName: "device-a", DNSName: "host.example.com", Target: "2001:db8::1", Type: "AAAA", Group: "example.com"},
		{ObjectID: 1, DeviceID: 10, DeviceName: "device-a", DNSName: "host.example.com", Target: "192.0.2.10", Type: "A", TTL: &ttl, Group: "example.com", Tags: []string{"prod", "edge"}},
		{ObjectID: 3, DeviceID: 10, DeviceName: "device-a", DNSName: "host.example.com", Target: "192.0.2.11", Type: "A", TTL: &ttl, Group: "example.com", Tags: []string{"edge"}},
		{ObjectID: 4, DeviceID: 10, DeviceName: "device-a", DNSName: "host-ipmi.example.com", Target: "192.0.2.20", Type: "A", Group: "example.com"},
		{ObjectID: 5, DeviceID: 20, DeviceName: "device-b", DNSName: "other.example.org", Target: "192.0.2.30", Type: "A", Group: "example.org"},
	})

	if len(resources) != 2 {
		t.Fatalf("expected 2 resources, got %d", len(resources))
	}
	if resources[0].Name != resourceNameForDevice("device-a", 10) {
		t.Fatalf("unexpected first resource name: %s", resources[0].Name)
	}
	if got := resources[0].Annotations["netbox.object.ids"]; got != "1,2,3,4" {
		t.Fatalf("unexpected provenance annotation: %s", got)
	}
	if got := resources[0].Annotations["netbox.device.id"]; got != "10" {
		t.Fatalf("unexpected device annotation: %s", got)
	}
	if got := resources[0].Annotations["netbox.domain.groups"]; got != "example.com" {
		t.Fatalf("unexpected domain groups annotation: %s", got)
	}
	if len(resources[0].Endpoints) != 3 {
		t.Fatalf("expected 3 endpoints for first resource, got %d", len(resources[0].Endpoints))
	}
	var endpoint endpointSpec
	for _, candidate := range resources[0].Endpoints {
		if candidate.DNSName == "host.example.com" && candidate.RecordType == "A" {
			endpoint = candidate
			break
		}
	}
	if endpoint.DNSName == "" {
		t.Fatalf("expected merged host A endpoint, got %+v", resources[0].Endpoints)
	}
	if len(endpoint.Labels) != 0 {
		t.Fatalf("expected no endpoint labels, got %+v", endpoint.Labels)
	}
	if len(endpoint.Targets) != 2 || endpoint.Targets[0] != "192.0.2.10" || endpoint.Targets[1] != "192.0.2.11" {
		t.Fatalf("targets were not deterministically merged: %+v", endpoint.Targets)
	}
}

func TestResourceNameForDeviceNormalizesIdentity(t *testing.T) {
	name := resourceNameForDevice("API.Example.com.", 42)
	if name != resourceNameForDevice("api.example.com", 42) {
		t.Fatalf("expected canonical name to ignore case and trailing dot, got %s", name)
	}
	if name == resourceNameForDevice("api.example.com", 43) {
		t.Fatalf("expected device id to affect resource naming")
	}
}

func TestPlanChangesSkipsDeletesWhenSourceUnhealthy(t *testing.T) {
	desired := []desiredResource{{
		Name:      "netbox-device-a",
		Namespace: "external-records",
		Labels: map[string]string{
			managedByLabel: managedByValue,
		},
		Annotations: map[string]string{"netbox.object.ids": "1"},
		Endpoints:   []endpointSpec{{DNSName: "a.example.com", RecordType: "A", Targets: []string{"192.0.2.1"}}},
	}}
	existing := map[string]*unstructured.Unstructured{
		"netbox-device-a": toUnstructured(desired[0]),
		"netbox-old-device": func() *unstructured.Unstructured {
			obj := toUnstructured(desired[0])
			obj.SetName("netbox-old-device")
			return obj
		}(),
	}

	plans, summary, err := planChanges(desired, existing, false, 0)
	if err != nil {
		t.Fatalf("planChanges returned error: %v", err)
	}
	if len(plans) != 0 {
		t.Fatalf("expected no changes, got %d", len(plans))
	}
	if summary.SkippedDelete != 1 {
		t.Fatalf("expected one skipped delete, got %d", summary.SkippedDelete)
	}
	if summary.Unchanged != 1 {
		t.Fatalf("expected one unchanged resource, got %d", summary.Unchanged)
	}
}

func TestPlanChangesMaxChangesGuardrail(t *testing.T) {
	desired := []desiredResource{
		{Name: "one", Namespace: "external-records"},
		{Name: "two", Namespace: "external-records"},
	}
	plans, _, err := planChanges(desired, map[string]*unstructured.Unstructured{}, true, 1)
	if err == nil {
		t.Fatal("expected max changes error")
	}
	if plans != nil {
		t.Fatalf("expected nil plans when guardrail trips, got %v", plans)
	}
}

func TestTransformIPAddressValidatesAllowlistAndTTL(t *testing.T) {
	cfg := Config{
		Allowlist:       []string{"example.com"},
		EnabledStatuses: map[string]struct{}{"active": {}},
		TTLField:        "ttl",
	}
	status := netbox.NewIPAddressStatus()
	value := netbox.IPAddressStatusValue("active")
	status.SetValue(value)
	dnsName := "host.example.com"
	ttl := "600"
	record, err, keep := transformIPAddress(netbox.IPAddress{
		Id:      42,
		Address: "192.0.2.5/24",
		DnsName: &dnsName,
		Status:  status,
		AssignedObject: map[string]any{
			"device": map[string]any{"id": 10, "name": "device-a"},
		},
		CustomFields: map[string]any{
			"ttl": ttl,
		},
	}, cfg)
	if err != nil || !keep {
		t.Fatalf("expected record to be kept, err=%v keep=%v", err, keep)
	}
	if record.DeviceID != 10 || record.DeviceName != "device-a" {
		t.Fatalf("unexpected device identity: %+v", record)
	}
	if record.Group != "example.com" || record.Type != "A" {
		t.Fatalf("unexpected transformed record: %+v", record)
	}
	if record.TTL == nil || *record.TTL != 600 {
		t.Fatalf("expected TTL 600, got %+v", record.TTL)
	}

	otherDNSName := "host.other.com"
	_, err, keep = transformIPAddress(netbox.IPAddress{
		Id:      43,
		Address: "192.0.2.6/24",
		DnsName: &otherDNSName,
		Status:  status,
		AssignedObject: map[string]any{
			"device": map[string]any{"id": 10, "name": "device-a"},
		},
	}, cfg)
	if err == nil || keep {
		t.Fatalf("expected out-of-scope record to produce an error and be skipped")
	}
}

func TestEquivalentResourceIgnoresUnrelatedMetadata(t *testing.T) {
	desired := desiredResource{
		Name:      "netbox-device-a",
		Namespace: "external-records",
		Labels: map[string]string{
			managedByLabel:           managedByValue,
			"netbox-dns-sync/device": "device-a",
		},
		Annotations: map[string]string{
			"netbox.device.id":   "10",
			"netbox.device.name": "device-a",
			"netbox.object.ids":  "1,2",
		},
		Endpoints: []endpointSpec{{DNSName: "a.example.com", RecordType: "A", Targets: []string{"192.0.2.1"}}},
	}
	existing := toUnstructured(desired)
	existing.SetCreationTimestamp(metav1.Now())
	existing.SetLabels(map[string]string{
		managedByLabel:           managedByValue,
		"netbox-dns-sync/device": "device-a",
		"extra":                  "ignored",
	})
	existing.SetAnnotations(map[string]string{
		"netbox.device.id":   "10",
		"netbox.device.name": "device-a",
		"netbox.object.ids":  "1,2",
		"kubectl.kubernetes.io/last-applied-configuration": "ignored",
	})
	if !equivalentResource(desired, existing) {
		t.Fatal("expected resources to be equivalent")
	}
}
