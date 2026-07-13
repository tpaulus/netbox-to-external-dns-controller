package main

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/record"
)

var dnsEndpointGVR = schema.GroupVersionResource{
	Group:    "externaldns.k8s.io",
	Version:  "v1alpha1",
	Resource: "dnsendpoints",
}

type reconciler struct {
	dynamicClient dynamic.Interface
	coreClient    kubernetes.Interface
	recorder      record.EventRecorder
	config        Config
	logger        *slog.Logger
}

type desiredResource struct {
	Name        string
	Namespace   string
	Labels      map[string]string
	Annotations map[string]string
	Endpoints   []endpointSpec
}

type endpointSpec struct {
	DNSName          string             `json:"dnsName"`
	RecordTTL        *int64             `json:"recordTTL,omitempty"`
	RecordType       string             `json:"recordType"`
	Targets          []string           `json:"targets"`
	ProviderSpecific []providerSpecific `json:"providerSpecific,omitempty"`
}

type providerSpecific struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type changePlan struct {
	Operation string
	Resource  desiredResource
	Existing  *unstructured.Unstructured
}

type reconcileSummary struct {
	Created       int
	Updated       int
	Deleted       int
	Unchanged     int
	SkippedDelete int
	RecordErrors  int
}

func buildKubeConfig(path string) (*rest.Config, error) {
	if strings.TrimSpace(path) != "" {
		return clientcmd.BuildConfigFromFlags("", path)
	}
	return rest.InClusterConfig()
}

func newReconciler(cfg Config, logger *slog.Logger) (*reconciler, error) {
	restConfig, err := buildKubeConfig(cfg.KubeconfigPath)
	if err != nil {
		return nil, err
	}
	coreClient, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, err
	}
	dynamicClient, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		return nil, err
	}

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return nil, err
	}
	broadcaster := record.NewBroadcaster()
	broadcaster.StartRecordingToSink(&kubernetesEventSink{interface_: coreClient})
	recorder := broadcaster.NewRecorder(scheme, corev1.EventSource{Component: managedByValue})

	return &reconciler{
		dynamicClient: dynamicClient,
		coreClient:    coreClient,
		recorder:      recorder,
		config:        cfg,
		logger:        logger,
	}, nil
}

type kubernetesEventSink struct {
	interface_ kubernetes.Interface
}

func (s *kubernetesEventSink) Create(event *corev1.Event) (*corev1.Event, error) {
	return s.interface_.CoreV1().Events(event.Namespace).Create(context.Background(), event, metav1.CreateOptions{})
}

func (s *kubernetesEventSink) Update(event *corev1.Event) (*corev1.Event, error) {
	return s.interface_.CoreV1().Events(event.Namespace).Update(context.Background(), event, metav1.UpdateOptions{})
}

func (s *kubernetesEventSink) Patch(event *corev1.Event, data []byte) (*corev1.Event, error) {
	return s.interface_.CoreV1().Events(event.Namespace).Patch(context.Background(), event.Name, types.StrategicMergePatchType, data, metav1.PatchOptions{})
}

func (r *reconciler) reconcile(ctx context.Context, records []sourceRecord, sourceHealthy bool, recordErrors []error) (reconcileSummary, error) {
	desired := buildDesiredResources(r.config.Namespace, records)
	existing, err := r.listManaged(ctx)
	if err != nil {
		return reconcileSummary{RecordErrors: len(recordErrors)}, err
	}

	plans, summary, err := planChanges(desired, existing, sourceHealthy || r.config.DeleteOnPartial, r.config.MaxChanges)
	if err != nil {
		return reconcileSummary{RecordErrors: len(recordErrors)}, err
	}
	summary.RecordErrors = len(recordErrors)

	for _, change := range plans {
		if r.config.DryRun {
			r.logger.Info("dry-run planned change", "operation", change.Operation, "resource", change.Resource.Name, "namespace", change.Resource.Namespace)
			continue
		}
		if err := r.applyChange(ctx, change); err != nil {
			r.emitEvent(corev1.EventTypeWarning, "SyncFailed", fmt.Sprintf("%s %s failed: %v", change.Operation, change.Resource.Name, err))
			return summary, err
		}
	}

	r.emitEvent(corev1.EventTypeNormal, "SyncCompleted", fmt.Sprintf("created=%d updated=%d deleted=%d unchanged=%d skippedDelete=%d recordErrors=%d", summary.Created, summary.Updated, summary.Deleted, summary.Unchanged, summary.SkippedDelete, summary.RecordErrors))
	return summary, nil
}

func (r *reconciler) emitEvent(eventType, reason, message string) {
	r.recorder.Event(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: r.config.Namespace, Namespace: r.config.Namespace}}, eventType, reason, message)
}

func buildDesiredResources(namespace string, records []sourceRecord) []desiredResource {
	grouped := make(map[string]map[string]*endpointAccumulator)
	resourceIDs := make(map[string]map[int]struct{})
	for _, record := range records {
		if _, ok := grouped[record.Group]; !ok {
			grouped[record.Group] = map[string]*endpointAccumulator{}
			resourceIDs[record.Group] = map[int]struct{}{}
		}
		key := endpointKey(record)
		acc, ok := grouped[record.Group][key]
		if !ok {
			acc = &endpointAccumulator{
				DNSName:    record.DNSName,
				RecordType: record.Type,
				TTL:        record.TTL,
				Targets:    map[string]struct{}{},
				IDs:        map[int]struct{}{},
				Tags:       map[string]struct{}{},
			}
			grouped[record.Group][key] = acc
		}
		acc.Targets[record.Target] = struct{}{}
		acc.IDs[record.ObjectID] = struct{}{}
		resourceIDs[record.Group][record.ObjectID] = struct{}{}
		for _, tag := range record.Tags {
			acc.Tags[tag] = struct{}{}
		}
	}

	groups := sortedKeys(grouped)
	resources := make([]desiredResource, 0, len(groups))
	for _, group := range groups {
		endpointKeys := sortedKeys(grouped[group])
		endpoints := make([]endpointSpec, 0, len(endpointKeys))
		for _, key := range endpointKeys {
			acc := grouped[group][key]
			endpoints = append(endpoints, endpointSpec{
				DNSName:    acc.DNSName,
				RecordTTL:  acc.TTL,
				RecordType: acc.RecordType,
				Targets:    sortedSet(acc.Targets),
				ProviderSpecific: []providerSpecific{
					{Name: "netbox-object-ids", Value: intsToCSV(acc.IDs)},
					{Name: "netbox-tags", Value: strings.Join(sortedSet(acc.Tags), ",")},
				},
			})
		}
		resources = append(resources, desiredResource{
			Name:      resourceNameForGroup(group),
			Namespace: namespace,
			Labels: map[string]string{
				managedByLabel:          managedByValue,
				"netbox-dns-sync/group": sanitizeName(group),
			},
			Annotations: map[string]string{
				"netbox.object.ids":   intsToCSV(resourceIDs[group]),
				"netbox.domain.group": group,
			},
			Endpoints: endpoints,
		})
	}

	return resources
}

type endpointAccumulator struct {
	DNSName    string
	RecordType string
	TTL        *int64
	Targets    map[string]struct{}
	IDs        map[int]struct{}
	Tags       map[string]struct{}
}

func endpointKey(record sourceRecord) string {
	ttl := ""
	if record.TTL != nil {
		ttl = fmt.Sprintf("%d", *record.TTL)
	}
	return strings.Join([]string{record.DNSName, record.Type, ttl}, "|")
}

func resourceNameForGroup(group string) string {
	base := "netbox-" + sanitizeName(group)
	if len(base) <= 63 {
		return base
	}
	hash := sha1.Sum([]byte(group))
	suffix := hex.EncodeToString(hash[:])[:8]
	return base[:54] + "-" + suffix
}

func sanitizeName(value string) string {
	replacer := strings.NewReplacer(".", "-", "_", "-", "*", "wildcard-")
	value = replacer.Replace(strings.ToLower(value))
	parts := strings.FieldsFunc(value, func(r rune) bool {
		return (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '-'
	})
	joined := strings.Join(parts, "-")
	joined = strings.Trim(joined, "-")
	if joined == "" {
		return "records"
	}
	return joined
}

func (r *reconciler) listManaged(ctx context.Context) (map[string]*unstructured.Unstructured, error) {
	list, err := r.dynamicClient.Resource(dnsEndpointGVR).Namespace(r.config.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: managedByLabel + "=" + managedByValue,
	})
	if err != nil {
		return nil, err
	}
	result := make(map[string]*unstructured.Unstructured, len(list.Items))
	for i := range list.Items {
		item := list.Items[i]
		result[item.GetName()] = item.DeepCopy()
	}
	return result, nil
}

func planChanges(desired []desiredResource, existing map[string]*unstructured.Unstructured, sourceHealthy bool, maxChanges int) ([]changePlan, reconcileSummary, error) {
	plans := make([]changePlan, 0)
	summary := reconcileSummary{}
	seen := make(map[string]struct{}, len(desired))

	for _, resource := range desired {
		seen[resource.Name] = struct{}{}
		current, found := existing[resource.Name]
		if !found {
			plans = append(plans, changePlan{Operation: "create", Resource: resource})
			summary.Created++
			continue
		}
		if equivalentResource(resource, current) {
			summary.Unchanged++
			continue
		}
		plans = append(plans, changePlan{Operation: "update", Resource: resource, Existing: current})
		summary.Updated++
	}

	for name, current := range existing {
		if _, ok := seen[name]; ok {
			continue
		}
		if !sourceHealthy {
			summary.SkippedDelete++
			continue
		}
		plans = append(plans, changePlan{Operation: "delete", Resource: desiredResource{Name: name, Namespace: current.GetNamespace()}, Existing: current})
		summary.Deleted++
	}

	if maxChanges > 0 && summary.Created+summary.Updated+summary.Deleted > maxChanges {
		return nil, summary, fmt.Errorf("planned changes exceed MAX_CHANGES_PER_RUN: %d > %d", summary.Created+summary.Updated+summary.Deleted, maxChanges)
	}

	sort.Slice(plans, func(i, j int) bool {
		if plans[i].Operation == plans[j].Operation {
			return plans[i].Resource.Name < plans[j].Resource.Name
		}
		return plans[i].Operation < plans[j].Operation
	})
	return plans, summary, nil
}

func equivalentResource(desired desiredResource, existing *unstructured.Unstructured) bool {
	if existing == nil {
		return false
	}
	current := desiredResource{
		Name:        existing.GetName(),
		Namespace:   existing.GetNamespace(),
		Labels:      subsetMap(existing.GetLabels(), desired.Labels),
		Annotations: subsetMap(existing.GetAnnotations(), desired.Annotations),
	}

	endpoints, found, err := unstructured.NestedSlice(existing.Object, "spec", "endpoints")
	if err != nil || !found {
		return false
	}
	current.Endpoints = normalizeExistingEndpoints(endpoints)
	return reflectJSON(normalizeDesiredResource(current)) == reflectJSON(normalizeDesiredResource(desired))
}

func normalizeExistingEndpoints(values []any) []endpointSpec {
	endpoints := make([]endpointSpec, 0, len(values))
	for _, value := range values {
		data, err := json.Marshal(value)
		if err != nil {
			continue
		}
		var endpoint endpointSpec
		if err := json.Unmarshal(data, &endpoint); err != nil {
			continue
		}
		sort.Strings(endpoint.Targets)
		sort.Slice(endpoint.ProviderSpecific, func(i, j int) bool {
			if endpoint.ProviderSpecific[i].Name == endpoint.ProviderSpecific[j].Name {
				return endpoint.ProviderSpecific[i].Value < endpoint.ProviderSpecific[j].Value
			}
			return endpoint.ProviderSpecific[i].Name < endpoint.ProviderSpecific[j].Name
		})
		endpoints = append(endpoints, endpoint)
	}
	sort.Slice(endpoints, func(i, j int) bool {
		if endpoints[i].DNSName == endpoints[j].DNSName {
			return endpoints[i].RecordType < endpoints[j].RecordType
		}
		return endpoints[i].DNSName < endpoints[j].DNSName
	})
	return endpoints
}

func reflectJSON(value any) string {
	data, _ := json.Marshal(value)
	return string(data)
}

func normalizeDesiredResource(resource desiredResource) desiredResource {
	normalized := resource
	normalized.Endpoints = append([]endpointSpec(nil), resource.Endpoints...)
	for index := range normalized.Endpoints {
		sort.Strings(normalized.Endpoints[index].Targets)
		sort.Slice(normalized.Endpoints[index].ProviderSpecific, func(i, j int) bool {
			if normalized.Endpoints[index].ProviderSpecific[i].Name == normalized.Endpoints[index].ProviderSpecific[j].Name {
				return normalized.Endpoints[index].ProviderSpecific[i].Value < normalized.Endpoints[index].ProviderSpecific[j].Value
			}
			return normalized.Endpoints[index].ProviderSpecific[i].Name < normalized.Endpoints[index].ProviderSpecific[j].Name
		})
	}
	sort.Slice(normalized.Endpoints, func(i, j int) bool {
		if normalized.Endpoints[i].DNSName == normalized.Endpoints[j].DNSName {
			return normalized.Endpoints[i].RecordType < normalized.Endpoints[j].RecordType
		}
		return normalized.Endpoints[i].DNSName < normalized.Endpoints[j].DNSName
	})
	return normalized
}

func subsetMap(actual, expected map[string]string) map[string]string {
	result := make(map[string]string, len(expected))
	for key := range expected {
		result[key] = actual[key]
	}
	return result
}

func (r *reconciler) applyChange(ctx context.Context, change changePlan) error {
	switch change.Operation {
	case "create":
		resource := toUnstructured(change.Resource)
		return retry(ctx, r.config.RetryCount, func() error {
			_, err := r.dynamicClient.Resource(dnsEndpointGVR).Namespace(change.Resource.Namespace).Create(ctx, resource, metav1.CreateOptions{})
			return err
		})
	case "update":
		resource := toUnstructured(change.Resource)
		resource.SetResourceVersion(change.Existing.GetResourceVersion())
		return retry(ctx, r.config.RetryCount, func() error {
			_, err := r.dynamicClient.Resource(dnsEndpointGVR).Namespace(change.Resource.Namespace).Update(ctx, resource, metav1.UpdateOptions{})
			return err
		})
	case "delete":
		return retry(ctx, r.config.RetryCount, func() error {
			err := r.dynamicClient.Resource(dnsEndpointGVR).Namespace(change.Resource.Namespace).Delete(ctx, change.Resource.Name, metav1.DeleteOptions{})
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		})
	default:
		return errors.New("unknown change operation")
	}
}

func toUnstructured(resource desiredResource) *unstructured.Unstructured {
	endpoints := make([]any, 0, len(resource.Endpoints))
	for _, endpoint := range resource.Endpoints {
		targets := make([]any, 0, len(endpoint.Targets))
		for _, target := range endpoint.Targets {
			targets = append(targets, target)
		}
		entry := map[string]any{
			"dnsName":    endpoint.DNSName,
			"recordType": endpoint.RecordType,
			"targets":    targets,
		}
		if endpoint.RecordTTL != nil {
			entry["recordTTL"] = *endpoint.RecordTTL
		}
		if len(endpoint.ProviderSpecific) > 0 {
			providerSpecific := make([]any, 0, len(endpoint.ProviderSpecific))
			for _, value := range endpoint.ProviderSpecific {
				if value.Value == "" {
					continue
				}
				providerSpecific = append(providerSpecific, map[string]any{"name": value.Name, "value": value.Value})
			}
			if len(providerSpecific) > 0 {
				entry["providerSpecific"] = providerSpecific
			}
		}
		endpoints = append(endpoints, entry)
	}
	object := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "externaldns.k8s.io/v1alpha1",
		"kind":       "DNSEndpoint",
		"spec": map[string]any{
			"endpoints": endpoints,
		},
	}}
	object.SetName(resource.Name)
	object.SetNamespace(resource.Namespace)
	object.SetLabels(resource.Labels)
	object.SetAnnotations(resource.Annotations)
	return object
}

func retry(ctx context.Context, attempts int, fn func() error) error {
	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		if err := fn(); err != nil {
			lastErr = err
			if attempt == attempts {
				break
			}
			wait := time.Duration(attempt*attempt) * 200 * time.Millisecond
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(wait):
			}
			continue
		}
		return nil
	}
	return lastErr
}

func sortedKeys[T any](input map[string]T) []string {
	keys := make([]string, 0, len(input))
	for key := range input {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func sortedSet(set map[string]struct{}) []string {
	values := make([]string, 0, len(set))
	for value := range set {
		if value == "" {
			continue
		}
		values = append(values, value)
	}
	sort.Strings(values)
	return values
}

func intsToCSV(values map[int]struct{}) string {
	if len(values) == 0 {
		return ""
	}
	ids := make([]int, 0, len(values))
	for value := range values {
		ids = append(ids, value)
	}
	sort.Ints(ids)
	parts := make([]string, 0, len(ids))
	for _, id := range ids {
		parts = append(parts, fmt.Sprintf("%d", id))
	}
	return strings.Join(parts, ",")
}

func sortBySpecificity(values []string) []string {
	result := append([]string(nil), values...)
	sort.Slice(result, func(i, j int) bool {
		if len(result[i]) == len(result[j]) {
			return result[i] < result[j]
		}
		return len(result[i]) > len(result[j])
	})
	return result
}
