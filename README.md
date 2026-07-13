# netbox-to-external-dns-controller

Sync NetBox IP addresses into `externaldns.k8s.io/v1alpha1` `DNSRecord` resources.

## Overview

The controller reads NetBox IP address objects from `NETBOX_URL/api/ipam/ip-addresses/`,
filters them by `DOMAIN_ALLOWLIST`, and reconciles one `DNSRecord` per unique
`dnsName`/record type into `TARGET_NAMESPACE`.

This keeps the desired state easier to inspect with `kubectl get dnsrecords -n <namespace>`
than grouped `DNSEndpoint` objects.

> [!IMPORTANT]
> `DNSRecord` is ExternalDNS's CRD registry type. Configure ExternalDNS with
> `--registry=crd`, and make sure `--txt-owner-id` matches `EXTERNAL_DNS_OWNER_ID`.
> The `dnsrecords.externaldns.k8s.io` CRD must also be installed in the cluster.

## Configuration

Required environment variables:

- `NETBOX_URL`
- `NETBOX_TOKEN`
- `DOMAIN_ALLOWLIST` - comma-separated domain suffixes that the controller is allowed to manage

Optional environment variables:

- `NETBOX_FILTERS` - raw query string appended to the NetBox IP address request
- `NETBOX_ENABLED_STATUSES` - comma-separated NetBox statuses to treat as enabled (`active` by default)
- `NETBOX_TTL_FIELD` - custom field used for per-record TTL overrides (`external_dns_ttl` by default)
- `TARGET_NAMESPACE` - namespace that contains managed `DNSRecord` objects (`external-records` by default)
- `EXTERNAL_DNS_OWNER_ID` - owner label written to each `DNSRecord`; must match ExternalDNS `--txt-owner-id` (`netbox-dns-sync` by default)
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
- One `DNSRecord` is generated per unique `dnsName` and record type.
- Multiple NetBox IPs for the same `dnsName` and record type are merged into one `targets` list.
- Managed resources are labeled with:
  - `app.kubernetes.io/managed-by=netbox-dns-sync`
  - `externaldns.k8s.io/owner=<EXTERNAL_DNS_OWNER_ID>`
- Deletes are skipped when the source fetch is incomplete or unhealthy unless
  `DELETE_ON_PARTIAL_FAILURE=true`.

## Example usage

### Example controller configuration

```bash
export NETBOX_URL="https://netbox.example.com"
export NETBOX_TOKEN="replace-me"
export DOMAIN_ALLOWLIST="example.com,example.org"
export TARGET_NAMESPACE="external-dns"
export EXTERNAL_DNS_OWNER_ID="netbox-dns-sync"

./netbox-to-external-dns-controller
```

### Example ExternalDNS configuration

ExternalDNS must be configured for the CRD registry:

```yaml
args:
  - --registry=crd
  - --txt-owner-id=netbox-dns-sync
  - --namespace=external-dns
```

### Example generated `DNSRecord`

```yaml
apiVersion: externaldns.k8s.io/v1alpha1
kind: DNSRecord
metadata:
  name: a-example-com-<hash>
  namespace: external-dns
  labels:
    app.kubernetes.io/managed-by: netbox-dns-sync
    externaldns.k8s.io/owner: netbox-dns-sync
    netbox-dns-sync/group: example-com
  annotations:
    netbox.domain.group: example.com
    netbox.object.ids: "1,3"
spec:
  endpoint:
    dnsName: a.example.com
    labels:
      owner: netbox-dns-sync
    recordType: A
    recordTTL: 300
    targets:
      - 192.0.2.10
      - 192.0.2.11
    providerSpecific:
      - name: netbox-object-ids
        value: "1,3"
      - name: netbox-tags
        value: edge,prod
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
- Pushes to `main` build and publish a container image to `ghcr.io/<owner>/<repo>`.
