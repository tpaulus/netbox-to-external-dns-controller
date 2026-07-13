# netbox-to-external-dns-controller
Create ExternalDNS Sync Resources based off of NetBox Devices

## Configuration

The sync tool reads NetBox IP address objects from `NETBOX_URL/api/ipam/ip-addresses/` and
reconciles `externaldns.k8s.io/v1alpha1` `DNSEndpoint` resources in the `TARGET_NAMESPACE`
(defaults to `external-records`).

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

## Behavior

- Records are generated from NetBox IP addresses with a populated `dns_name`.
- Record type is derived from the IP family (`A` or `AAAA`).
- Only records whose names match `DOMAIN_ALLOWLIST` are managed.
- One `DNSEndpoint` is generated per matched domain suffix for readable diffs.
- Managed resources are labeled with `app.kubernetes.io/managed-by=netbox-dns-sync`.
- Deletes are skipped when the source fetch is incomplete or unhealthy unless
  `DELETE_ON_PARTIAL_FAILURE=true`.

## Development

```bash
go test ./...
```
