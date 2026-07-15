package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
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
	Labels           map[string]string  `json:"labels,omitempty"`
	RecordTTL        *int64             `json:"recordTTL,omitempty"`
	RecordType       string             `json:"recordType"`
	SetIdentifier    string             `json:"setIdentifier,omitempty"`
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

type deviceAccumulator struct {
	DeviceID   int
	DeviceName string
	Groups     map[string]struct{}
	IDs        map[int]struct{}
	Endpoints  map[string]*endpointAccumulator
}

type endpointAccumulator struct {
	DNSName    string
	RecordType string
	TTL        *int64
	Targets    map[string]struct{}
	IDs        map[int]struct{}
	Group      string
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
	grouped := make(map[string]*deviceAccumulator)
	for _, record := range records {
		deviceKey := deviceKey(record)
		device, ok := grouped[deviceKey]
		if !ok {
			device = &deviceAccumulator{
				DeviceID:   record.DeviceID,
				DeviceName: record.DeviceName,
				Groups:     map[string]struct{}{},
				IDs:        map[int]struct{}{},
				Endpoints:  map[string]*endpointAccumulator{},
			}
			grouped[deviceKey] = device
		}
		device.Groups[record.Group] = struct{}{}
		device.IDs[record.ObjectID] = struct{}{}

		endpointKey := endpointKey(record)
		endpoint, ok := device.Endpoints[endpointKey]
		if !ok {
			endpoint = &endpointAccumulator{
				DNSName:    record.DNSName,
				RecordType: record.Type,
				TTL:        record.TTL,
				Targets:    map[string]struct{}{},
				IDs:        map[int]struct{}{},
				Group:      record.Group,
			}
			device.Endpoints[endpointKey] = endpoint
		}
		endpoint.Targets[record.Target] = struct{}{}
		endpoint.IDs[record.ObjectID] = struct{}{}
	}

	resourceKeys := sortedKeys(grouped)
	resources := make([]desiredResource, 0, len(resourceKeys))
	for _, key := range resourceKeys {
		device := grouped[key]
		endpointKeys := sortedKeys(device.Endpoints)
		endpoints := make([]endpointSpec, 0, len(endpointKeys))
		for _, endpointKey := range endpointKeys {
			endpoint := device.Endpoints[endpointKey]
			endpoints = append(endpoints, endpointSpec{
				DNSName:    endpoint.DNSName,
				RecordTTL:  endpoint.TTL,
				RecordType: endpoint.RecordType,
				Targets:    sortedSet(endpoint.Targets),
			})
		}
		resources = append(resources, desiredResource{
			Name:      resourceNameForDevice(device.DeviceName, device.DeviceID),
			Namespace: namespace,
			Labels: map[string]string{
				managedByLabel:           managedByValue,
				"netbox-dns-sync/device": sanitizeName(device.DeviceName),
			},
			Annotations: map[string]string{
				"netbox.device.id":     strconv.Itoa(device.DeviceID),
				"netbox.device.name":   device.DeviceName,
				"netbox.domain.groups": strings.Join(sortedSet(device.Groups), ","),
				"netbox.object.ids":    intsToCSV(device.IDs),
			},
			Endpoints: endpoints,
		})
	}

	return resources
}

func deviceKey(record sourceRecord) string {
	return fmt.Sprintf("%d|%s", record.DeviceID, strings.ToLower(strings.TrimSpace(record.DeviceName)))
}

func endpointKey(record sourceRecord) string {
	ttl := ""
	if record.TTL != nil {
		ttl = fmt.Sprintf("%d", *record.TTL)
	}
	return strings.Join([]string{record.DNSName, record.Type, ttl}, "|")
}

func resourceNameForDevice(deviceName string, deviceID int) string {
	normalizedName := strings.TrimSuffix(strings.TrimSpace(strings.ToLower(deviceName)), ".")
	hash := sha256.Sum256([]byte(fmt.Sprintf("%s/%d", normalizedName, deviceID)))
	suffix := fmt.Sprintf("-%d-%x", deviceID, hash[:2])

	base := sanitizeName(normalizedName)
	if maxBase := 253 - len(suffix); len(base) > maxBase {
		base = strings.Trim(base[:maxBase], "-")
	}
	if base == "" {
		base = "device"
	}
	return base + suffix
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

func normalizeExistingEndpoints(value []any) []endpointSpec {
	data, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	var endpoints []endpointSpec
	if err := json.Unmarshal(data, &endpoints); err != nil {
		return nil
	}
	for i := range endpoints {
		endpoints[i] = normalizeEndpoint(endpoints[i])
	}
	sort.Slice(endpoints, func(i, j int) bool {
		return endpointSortKey(endpoints[i]) < endpointSortKey(endpoints[j])
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
	for i := range normalized.Endpoints {
		normalized.Endpoints[i] = normalizeEndpoint(normalized.Endpoints[i])
	}
	sort.Slice(normalized.Endpoints, func(i, j int) bool {
		return endpointSortKey(normalized.Endpoints[i]) < endpointSortKey(normalized.Endpoints[j])
	})
	return normalized
}

func normalizeEndpoint(endpoint endpointSpec) endpointSpec {
	normalized := endpoint
	normalized.Targets = append([]string(nil), endpoint.Targets...)
	normalized.ProviderSpecific = append([]providerSpecific(nil), endpoint.ProviderSpecific...)
	sort.Strings(normalized.Targets)
	sort.Slice(normalized.ProviderSpecific, func(i, j int) bool {
		if normalized.ProviderSpecific[i].Name == normalized.ProviderSpecific[j].Name {
			return normalized.ProviderSpecific[i].Value < normalized.ProviderSpecific[j].Value
		}
		return normalized.ProviderSpecific[i].Name < normalized.ProviderSpecific[j].Name
	})
	return normalized
}

func endpointSortKey(endpoint endpointSpec) string {
	ttl := ""
	if endpoint.RecordTTL != nil {
		ttl = strconv.FormatInt(*endpoint.RecordTTL, 10)
	}
	return strings.Join([]string{endpoint.DNSName, endpoint.RecordType, ttl, strings.Join(endpoint.Targets, ",")}, "|")
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
	for _, spec := range resource.Endpoints {
		targets := make([]any, 0, len(spec.Targets))
		for _, target := range spec.Targets {
			targets = append(targets, target)
		}
		endpoint := map[string]any{
			"dnsName":    spec.DNSName,
			"recordType": spec.RecordType,
			"targets":    targets,
		}
		if len(spec.Labels) > 0 {
			endpoint["labels"] = spec.Labels
		}
		if spec.RecordTTL != nil {
			endpoint["recordTTL"] = *spec.RecordTTL
		}
		if spec.SetIdentifier != "" {
			endpoint["setIdentifier"] = spec.SetIdentifier
		}
		if len(spec.ProviderSpecific) > 0 {
			providerSpecific := make([]any, 0, len(spec.ProviderSpecific))
			for _, value := range spec.ProviderSpecific {
				if value.Value == "" {
					continue
				}
				providerSpecific = append(providerSpecific, map[string]any{"name": value.Name, "value": value.Value})
			}
			if len(providerSpecific) > 0 {
				endpoint["providerSpecific"] = providerSpecific
			}
		}
		endpoints = append(endpoints, endpoint)
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
