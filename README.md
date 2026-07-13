# netbox-to-external-dns-controller

Sync NetBox IP addresses into `externaldns.k8s.io/v1alpha1` `DNSEndpoint` resources.

## Overview

The controller reads NetBox IP address objects from `NETBOX_URL/api/ipam/ip-addresses/`,
filters them by `DOMAIN_ALLOWLIST`, groups them by assigned NetBox device, and
reconciles one `DNSEndpoint` per device into `TARGET_NAMESPACE`.

> [!IMPORTANT]
> `DNSEndpoint` is ExternalDNS's CRD source type. Configure ExternalDNS with
> `--registry=crd`.
> The `dnsendpoints.externaldns.k8s.io` CRD must also be installed in the cluster.

## Configuration

Required environment variables:

- `NETBOX_URL`
- `NETBOX_TOKEN`
- `DOMAIN_ALLOWLIST` - comma-separated domain suffixes that the controller is allowed to manage

Optional environment variables:

- `NETBOX_FILTERS` - raw query string appended to the NetBox IP address request
- `NETBOX_ENABLED_STATUSES` - comma-separated NetBox statuses to treat as enabled (`active` by default)
- `NETBOX_TTL_FIELD` - custom field used for per-record TTL overrides (`external_dns_ttl` by default)
- `TARGET_NAMESPACE` - namespace that contains managed `DNSEndpoint` objects (`external-records` by default)
- `SYNC_INTERVAL` - if set, run continuously on this interval; otherwise the tool runs once for CronJob use
- `DRY_RUN` - when `true`, log planned create/update/delete actions without writing to Kubernetes
- `MAX_CHANGES_PER_RUN` - guardrail that aborts a sync when too many changes are planned
- `DELETE_ON_PARTIAL_FAILURE` - when `true`, allow deletes even if some source records were malformed
- `METRICS_ADDRESS` - optional HTTP listen address for Prometheus metrics
- `KUBECONFIG` - optional path for out-of-cluster testing
- `NETBOX_PAGE_SIZE` - NetBox page size (`100` by default)
- `HTTP_TIMEOUT` - timeout for NetBox API requests (`30s` by default)
- `RETRY_COUNT` - retry attempts for NetBox and Kubernetes writes (`3` by default)

## Behavior

- Records are generated from NetBox IP addresses with a populated `dns_name`.
- Record type is derived from the IP family (`A` or `AAAA`).
- Only records whose names match `DOMAIN_ALLOWLIST` are managed.
- One `DNSEndpoint` is generated per NetBox device.
- Each `DNSEndpoint` includes one or more `spec.endpoints` entries, with repeated NetBox IPs for the same `dnsName` and record type merged into one `targets` list.
- Managed resources are labeled with:
  - `app.kubernetes.io/managed-by=netbox-dns-sync`
- Deletes are skipped when the source fetch is incomplete or unhealthy unless
  `DELETE_ON_PARTIAL_FAILURE=true`.

## Example usage

### Example controller configuration

```bash
export NETBOX_URL="https://netbox.example.com"
export NETBOX_TOKEN="replace-me"
export DOMAIN_ALLOWLIST="example.com,example.org"
export TARGET_NAMESPACE="external-dns"

./netbox-to-external-dns-controller
```

### Example ExternalDNS configuration

ExternalDNS must be configured for the CRD registry:

```yaml
args:
  - --registry=crd
  - --namespace=external-dns
```

### Example generated `DNSEndpoint`

```yaml
apiVersion: externaldns.k8s.io/v1alpha1
kind: DNSEndpoint
metadata:
  name: device-a-10-<hash>
  namespace: external-dns
  labels:
    app.kubernetes.io/managed-by: netbox-dns-sync
    netbox-dns-sync/device: device-a
  annotations:
    netbox.device.id: "10"
    netbox.device.name: device-a
    netbox.domain.groups: example.com
    netbox.object.ids: "1,2,3"
spec:
  endpoints:
    - dnsName: host.example.com
      recordType: A
      recordTTL: 300
      targets:
        - 192.0.2.10
        - 192.0.2.11
      providerSpecific:
        - name: netbox-group
          value: example.com
        - name: netbox-object-ids
          value: "1,3"
        - name: netbox-tags
          value: edge,prod
    - dnsName: host-ipmi.example.com
      recordType: A
      targets:
        - 192.0.2.20
```

## Development

```bash
go build ./...
go test ./...
golangci-lint run
docker build -t netbox-to-external-dns-controller .
```

## CI

- Pull requests run `go test ./...` and `golangci-lint`.
- Pushes to `main` build, sign, and publish a container image to `ghcr.io/<owner>/<repo>`.
