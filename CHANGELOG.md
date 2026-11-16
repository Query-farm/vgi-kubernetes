# Changelog

## 0.1.1 (2026-11-16)

- Bump vgi-go to 0.42.2.
- CI tooling (haybarn-unittest, lint helpers) runs on Python 3.14.

## 0.1.0 (unreleased)

Initial release: a live Kubernetes cluster's API as read-only SQL.

- Typed catalog tables for the core API (pods, pod_containers, nodes, workloads, networking,
  config, storage, RBAC with flattened rbac_rules / rbac_subjects, events), metrics-server usage,
  API discovery and the API server version.
- `resources(api_version, resource)` for any listable resource, custom resources included, and
  the `resource_tables` ATTACH option exposing them as tables.
- `pod_logs(namespace, pod, ...)` and the `quantity_value` scalar.
- Pushdown: namespace paths, field selectors, label selectors; lazy limit/continue paging.
- Credentials from a `kubernetes` secret (kubeconfig + context, or server + token + CA); the
  local kubeconfig only with `use_local_kubeconfig true`. Secret values are never returned.

Targets: vgi-go 0.42 · VGI protocol 3.3 · Haybarn 2.0.0rc11 · vgi-lint-check 2.x (unpinned)
