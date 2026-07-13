package main

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestBuildDesiredResourcesDeterministic(t *testing.T) {
	ttl := int64(300)
	resources := buildDesiredResources("external-records", "netbox-dns-sync", []sourceRecord{
		{ObjectID: 2, DNSName: "b.example.com", Target: "2001:db8::1", Type: "AAAA", Group: "example.com"},
		{ObjectID: 1, DNSName: "a.example.com", Target: "192.0.2.10", Type: "A", TTL: &ttl, Group: "example.com", Tags: []string{"prod", "edge"}},
		{ObjectID: 3, DNSName: "a.example.com", Target: "192.0.2.11", Type: "A", TTL: &ttl, Group: "example.com", Tags: []string{"edge"}},
		{ObjectID: 4, DNSName: "x.lab.example.org", Target: "192.0.2.20", Type: "A", Group: "example.org"},
	})

	if len(resources) != 3 {
		t.Fatalf("expected 3 resources, got %d", len(resources))
	}
	if resources[0].Name != resourceNameForRecord("a.example.com", "A") {
		t.Fatalf("unexpected first resource name: %s", resources[0].Name)
	}
	if got := resources[0].Annotations["netbox.object.ids"]; got != "1,3" {
		t.Fatalf("unexpected provenance annotation: %s", got)
	}
	if got := resources[0].Annotations["netbox.domain.group"]; got != "example.com" {
		t.Fatalf("unexpected domain group annotation: %s", got)
	}
	endpoint := resources[0].Endpoint
	if endpoint.DNSName != "a.example.com" || endpoint.RecordType != "A" {
		t.Fatalf("unexpected first endpoint: %+v", endpoint)
	}
	if endpoint.Labels["owner"] != "netbox-dns-sync" {
		t.Fatalf("unexpected endpoint owner label: %+v", endpoint.Labels)
	}
	if len(endpoint.Targets) != 2 || endpoint.Targets[0] != "192.0.2.10" || endpoint.Targets[1] != "192.0.2.11" {
		t.Fatalf("targets were not deterministically merged: %+v", endpoint.Targets)
	}
}

func TestResourceNameForRecordNormalizesIdentity(t *testing.T) {
	name := resourceNameForRecord("API.Example.com.", "a")
	if name != resourceNameForRecord("api.example.com", "A") {
		t.Fatalf("expected canonical name to ignore case and trailing dot, got %s", name)
	}
	if name == resourceNameForRecord("api.example.com", "AAAA") {
		t.Fatalf("expected record type to affect resource naming")
	}
}

func TestPlanChangesSkipsDeletesWhenSourceUnhealthy(t *testing.T) {
	desired := []desiredResource{{
		Name:      "netbox-example-com",
		Namespace: "external-records",
		Labels: map[string]string{
			managedByLabel: managedByValue,
		},
		Annotations: map[string]string{"netbox.object.ids": "1"},
		Endpoint:    endpointSpec{DNSName: "a.example.com", RecordType: "A", Targets: []string{"192.0.2.1"}},
	}}
	existing := map[string]*unstructured.Unstructured{
		"netbox-example-com": toUnstructured(desired[0]),
		"netbox-old-example-com": func() *unstructured.Unstructured {
			obj := toUnstructured(desired[0])
			obj.SetName("netbox-old-example-com")
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
	ttl := "600"
	record, err, keep := transformIPAddress(netBoxIPAddress{
		ID:      42,
		Address: "192.0.2.5/24",
		DNSName: "host.example.com",
		Status:  netBoxStatus{Value: "active"},
		CustomFields: map[string]any{
			"ttl": ttl,
		},
	}, cfg)
	if err != nil || !keep {
		t.Fatalf("expected record to be kept, err=%v keep=%v", err, keep)
	}
	if record.Group != "example.com" || record.Type != "A" {
		t.Fatalf("unexpected transformed record: %+v", record)
	}
	if record.TTL == nil || *record.TTL != 600 {
		t.Fatalf("expected TTL 600, got %+v", record.TTL)
	}

	_, err, keep = transformIPAddress(netBoxIPAddress{
		ID:      43,
		Address: "192.0.2.6/24",
		DNSName: "host.other.com",
		Status:  netBoxStatus{Value: "active"},
	}, cfg)
	if err == nil || keep {
		t.Fatalf("expected out-of-scope record to produce an error and be skipped")
	}
}

func TestEquivalentResourceIgnoresUnrelatedMetadata(t *testing.T) {
	desired := desiredResource{
		Name:      "netbox-example-com",
		Namespace: "external-records",
		Labels: map[string]string{
			managedByLabel:          managedByValue,
			"netbox-dns-sync/group": "example-com",
		},
		Annotations: map[string]string{
			"netbox.object.ids":   "1,2",
			"netbox.domain.group": "example.com",
		},
		Endpoint: endpointSpec{DNSName: "a.example.com", RecordType: "A", Targets: []string{"192.0.2.1"}},
	}
	existing := toUnstructured(desired)
	existing.SetCreationTimestamp(metav1.Now())
	existing.SetLabels(map[string]string{
		managedByLabel:          managedByValue,
		"netbox-dns-sync/group": "example-com",
		"extra":                 "ignored",
	})
	existing.SetAnnotations(map[string]string{
		"netbox.object.ids":   "1,2",
		"netbox.domain.group": "example.com",
		"kubectl.kubernetes.io/last-applied-configuration": "ignored",
	})
	if !equivalentResource(desired, existing) {
		t.Fatal("expected resources to be equivalent")
	}
}
