<p align="center">
  <img src="https://query.farm/media-kit/products/vgi/vgi-logo.png" alt="Vector Gateway Interface (VGI)" width="320">
</p>

# Query a Live Kubernetes Cluster's Pods, Nodes, Workloads and RBAC in DuckDB

> **vgi-kubernetes** · a [Query.Farm](https://query.farm) VGI worker

[![CI](https://github.com/Query-farm/vgi-kubernetes/actions/workflows/ci.yml/badge.svg)](https://github.com/Query-farm/vgi-kubernetes/actions/workflows/ci.yml)
[![vgi worker: query.farm](https://query.farm/media-kit/shields/vgi-worker.svg)](https://query.farm)

`kubectl get` answers one question about one kind of object at a time. This worker puts the
Kubernetes API behind SQL tables, so "which containers were OOM-killed", "how much CPU is
requested per node", "who can read secrets in this namespace, via which binding" or "which
custom resources are not ready" become a `JOIN` and a `GROUP BY`.

- **Typed tables for the core API**: pods (containers as `LIST(STRUCT)`), nodes, deployments,
  replica sets, stateful sets, daemon sets, jobs, cron jobs, autoscalers, services, endpoint
  slices, ingresses, network policies, config maps, secrets, service accounts, volumes, claims,
  storage classes, priority classes, roles and bindings (plus flattened `rbac_rules` /
  `rbac_subjects`), events. CPU and memory are numbers (`cpu_*_millicores`, `memory_*_bytes`),
  times are `TIMESTAMPTZ`, labels are `MAP(VARCHAR, VARCHAR)`.
- **Any other resource, custom resources included**: `resources(api_version, resource)`, or
  tables per resource with the `resource_tables` ATTACH option; `api_resources` lists what the
  cluster serves.
- **`pod_logs`**, **metrics-server usage** (`node_metrics`, `pod_metrics`) and the API server
  version.
- **Server-side filtering**: namespace, field selectors and label selectors (see
  [What pushes down](#what-pushes-down)); lists are paged with the API's `limit`/`continue`.
- **Read-only by construction**: the worker only issues `GET` requests (the get, list and watch
  verbs). **Secret values are never returned.**

## Quick start

```sql
INSTALL vgi FROM community;     -- Haybarn 2.0; on DuckDB 2.0 use the query_farm repository
LOAD vgi;
ATTACH 'kubernetes' (TYPE vgi, LOCATION 'github-auto://Query-farm/vgi-kubernetes@v0.2.0');

-- A ServiceAccount token (read-only RBAC is enough) ...
CREATE SECRET (TYPE kubernetes, SERVER 'https://10.0.0.1:6443', TOKEN '...',
               CERTIFICATE_AUTHORITY_DATA '<base64 or PEM CA>');
-- ... or a whole kubeconfig (YAML or base64) and a context:
-- CREATE SECRET (TYPE kubernetes, KUBECONFIG '...', CONTEXT 'prod');

SELECT namespace, name, c.name AS container, c.restart_count, c.last_termination_reason
FROM kubernetes.main.pods, UNNEST(containers) AS u(c)
WHERE c.restart_count > 0
ORDER BY c.restart_count DESC;
```

The secret type is registered by the worker, so run `CREATE SECRET` after `ATTACH`.

<details><summary>Other ways to run it</summary>

- **DuckDB 2.0**:
  `SET allow_extension_repositories = 'allowed'; CREATE EXTENSION REPOSITORY IF NOT EXISTS query_farm WITH PREFIX 'https://duckdb-extensions.query.farm'; INSTALL vgi FROM query_farm; LOAD vgi FROM query_farm;`
  then attach as above.
- **Docker** (HTTP): `docker run -p 8000:8000 ghcr.io/query-farm/vgi-kubernetes:0.2.0`, then
  `ATTACH 'kubernetes' (TYPE vgi, LOCATION 'http://localhost:8000')`.
- **From source**: `make build` (Go 1.26+ and a C toolchain, for cgo), then
  `ATTACH 'kubernetes' (TYPE vgi, LOCATION '/abs/path/to/bin/vgi-kubernetes-worker')`.
</details>

## Credentials

The worker never reads `~/.kube/config` or `$KUBECONFIG` on its own. Credentials come from a
DuckDB secret of `TYPE kubernetes`:

| Field | Meaning |
|---|---|
| `KUBECONFIG` | A kubeconfig file's contents, as YAML or base64. Exec / auth-provider plugins in it (EKS, GKE, ...) run on the worker's host, as with kubectl. Redacted. |
| `CONTEXT` | Context in `KUBECONFIG`; default its current-context. |
| `SERVER` | API server URL. Overrides the kubeconfig's server. |
| `TOKEN` | Bearer token (e.g. `kubectl create token <sa>`). Redacted. |
| `CERTIFICATE_AUTHORITY_DATA` | PEM or base64 PEM CA bundle; default the system roots. |
| `CLIENT_CERTIFICATE_DATA`, `CLIENT_KEY_DATA` | PEM or base64 client certificate and key (mTLS). The key is redacted. |
| `TLS_SERVER_NAME` | Name to verify in the server certificate. |
| `INSECURE_SKIP_TLS_VERIFY` | `'true'` to skip certificate verification (test clusters only). |

ATTACH options:

| Option | Meaning |
|---|---|
| `use_local_kubeconfig true` | **Opt in** to the local kubeconfig (`$KUBECONFIG`, else `~/.kube/config`) when no `kubernetes` secret is set. Off by default. |
| `context '<name>'` | Kubeconfig context to use (local or the secret's); overrides the secret's `CONTEXT`. |
| `secret '<name>'` | Which `kubernetes` secret to use when there are several: one ATTACH per cluster. |
| `resource_tables '<g>/<v>/<resource>, ...'` | Expose these resources as tables in schema `resources` (see below). `'all'` discovers every listable resource at ATTACH, which needs `use_local_kubeconfig` (DuckDB secrets are not available at ATTACH time). |
| `configmap_values true` | Fill `configmaps.data`; by default only the keys are returned. |
| `page_size <n>` | Objects per list page (`limit=`), default 500, max 5000. |

```sql
CREATE SECRET prod (TYPE kubernetes, KUBECONFIG '...', CONTEXT 'prod');
CREATE SECRET staging (TYPE kubernetes, KUBECONFIG '...', CONTEXT 'staging');
ATTACH 'kubernetes' AS prod (TYPE vgi, LOCATION '...', secret 'prod');
ATTACH 'kubernetes' AS staging (TYPE vgi, LOCATION '...', secret 'staging');
SELECT 'prod' AS cluster, count(*) FROM prod.main.pods UNION ALL
SELECT 'staging', count(*) FROM staging.main.pods;
```

The identity needs only `get`, `list` and `watch` (plus `get` on `pods/log` for `pod_logs`);
the built-in `view` ClusterRole covers everything except secrets, RBAC objects and logs.

**Secrets never leak.** `secrets` lists names, types and keys only. kubectl's
`last-applied-configuration` annotation (which embeds the data of an applied Secret) is
redacted on Secrets and ConfigMaps, and `resources('v1', 'secrets')` replaces every data
value with `<redacted>`.

## What's inside

Catalog `kubernetes`, schema `main`. Every table has `label_selector` and `field_selector`
pushdown columns (see below); resource tables start with `namespace` (if namespaced), `name`,
`uid`, `creation_timestamp`, `labels`, `annotations` and `owner_references`.

| Object | What it is |
|---|---|
| `pods` | Phase, node, IPs, QoS, readiness, restart count, summed requests/limits, images, `containers` (state, reason, exit codes, resources), conditions |
| `pod_containers` | One row per container (init, sidecar, app, ephemeral) with status and resources |
| `nodes` | Readiness, roles, addresses, versions, capacity/allocatable as numbers and maps, conditions, taints |
| `namespaces`, `priorityclasses`, `api_servers` | Namespace phase; priority classes; API server version |
| `deployments`, `replicasets`, `statefulsets`, `daemonsets` | Replica counts, rollout state, selectors, images and container specs |
| `jobs`, `cronjobs`, `horizontalpodautoscalers` | Job status/timing/owning CronJob; schedules; autoscaler bounds and metrics |
| `services`, `endpointslices`, `ingresses`, `networkpolicies` | Ports, selectors, backends, routes, default-deny flags |
| `configmaps`, `secrets`, `serviceaccounts` | Keys (values only on opt-in for ConfigMaps, never for Secrets) |
| `persistentvolumes`, `persistentvolumeclaims`, `storageclasses` | Capacity in bytes, phase, claims, default class |
| `roles`, `clusterroles`, `rolebindings`, `clusterrolebindings` | Rules and subjects as `LIST(STRUCT)` |
| `rbac_rules`, `rbac_subjects` | Every rule flattened to (api_group, resource, verb); every binding to one row per subject |
| `events` | Type, reason, message, count, involved object, `last_seen` |
| `node_metrics`, `pod_metrics` | metrics.k8s.io usage (needs metrics-server) |
| `api_resources` | Discovery: every served resource, verbs, short names |
| `resources(api_version, resource)` | Any listable resource with `spec`, `status` and `object` as `JSON` |
| `pod_logs(namespace, pod, container :=, previous :=, since_seconds :=, since_time :=, tail_lines :=, limit_bytes :=)` | Log lines with runtime timestamps |
| `quantity_value(quantity)` | `'250m'` → 0.25, `'64Mi'` → 67108864 |

### Custom resources as tables

```sql
ATTACH 'kubernetes' (TYPE vgi, LOCATION '...',
    resource_tables 'cert-manager.io/v1/certificates, policy/v1/poddisruptionbudgets');
SELECT namespace, name, status->>'$.conditions[0].reason' FROM kubernetes.resources.certificates;
```

Each table has the columns of `resources()` and is scanned by it. A resource name used by two
groups becomes `<resource>_<group>` (dots to underscores).

## What pushes down

DuckDB hands the worker the `WHERE` conjuncts; the worker narrows the API request with them and
then re-applies **every** filter itself, so results are exact either way.

| SQL | API request |
|---|---|
| `namespace = 'x'`, `namespace IN ('x', 'y')` | `/namespaces/x/...` (one list per namespace) |
| `name = 'x'` (and `role_name`, `pod_name`, ... on flattened tables) | `fieldSelector=metadata.name=x` |
| `pods.phase`, `node_name`, `pod_ip`, `service_account`, `restart_policy`, `host_network` | `status.phase`, `spec.nodeName`, ... (`=` and `!=`) |
| `events.type`, `reason`, `involved_kind/name/namespace/uid/field_path`, `source_component` | `type`, `reason`, `involvedObject.*`, `source` |
| `namespaces.phase`, `nodes.unschedulable`, `secrets.type`, `services.type`, `services.cluster_ip` | matching field selectors |
| `label_selector = 'app=web,tier in (a,b),!legacy'` | `labelSelector=...` (any kubectl syntax: `=`, `!=`, `in`, `notin`, exists, `!key`); the column echoes the selector |
| `field_selector = 'spec.schedulerName=x'` | a raw `fieldSelector` |

Everything else (`LIKE`, `labels['app'] = 'web'`, JSON paths, timestamp ranges) is evaluated
by the worker after listing. Pages are fetched lazily, and the `continue` token is kept in the
scan state, so a long scan resumes across HTTP-transport requests. 429 and 5xx responses are
retried with `Retry-After` or exponential backoff.

## Examples

```sql
-- CPU requested vs allocatable per node
SELECT n.name, sum(p.cpu_request_millicores) / n.allocatable_cpu_millicores AS cpu_requested
FROM kubernetes.main.nodes n LEFT JOIN kubernetes.main.pods p ON p.node_name = n.name AND p.phase = 'Running'
GROUP BY n.name, n.allocatable_cpu_millicores ORDER BY cpu_requested DESC;

-- Who can read secrets, and through which binding
SELECT s.subject_kind, s.subject_name, s.binding_kind, s.namespace, r.role_name
FROM kubernetes.main.rbac_rules r
JOIN kubernetes.main.rbac_subjects s ON s.role_kind = r.role_kind AND s.role_name = r.role_name
     AND (r.role_kind = 'ClusterRole' OR s.namespace = r.namespace)
WHERE r.resource IN ('secrets', '*') AND r.verb IN ('get', 'list', '*') AND r.api_group IN ('', '*')
GROUP BY ALL ORDER BY ALL;

-- Why is it crash-looping?
SELECT timestamp, message
FROM kubernetes.main.pod_logs('prod', 'api-7d9f8-abcde', container := 'api', previous := true)
ORDER BY line_number;

-- Containers using more memory than they request
SELECT m.namespace, m.pod_name, m.container_name, m.memory_usage_bytes, c.memory_request_bytes
FROM kubernetes.main.pod_metrics m
JOIN kubernetes.main.pod_containers c USING (namespace, pod_name, container_name)
WHERE m.memory_usage_bytes > c.memory_request_bytes;
```

## How it is tested

- **Unit tests** (`make test-unit`, no cluster): pushdown planning, credential resolution
  (kubeconfig YAML/base64, contexts, server/token/CA, the local-kubeconfig opt-in and that the
  worker never reads it otherwise), log parsing, secret sanitizing, `resource_tables` parsing.
  Table scans run in-process (`vgitest`, with the scan state gob round-tripped between batches as
  over HTTP) against a fake API server that replays responses recorded from the kind fixture
  (`internal/kubernetes/testdata`), paged two items at a time, with a 429 retry and a 401.
- **SQL tests** (`test/sql/*.test`, through the real `vgi` extension over stdio, launch and
  HTTP) run against a real **kind** cluster, `vgi-kubernetes-test`. `ci/kind-fixture.sh` creates
  it (kubeconfig in a temp file; `~/.kube/config` is untouched), applies `test/fixture/*.yaml`
  (a deployment with a sidecar, a crash-looping pod, a completed pod and job, a suspended cron
  job, a stateful set with a bound volume, a pending claim, a daemon set, a service, ingress,
  autoscaler, network policies, a ConfigMap and a Secret, RBAC, a synthetic event, a CRD with two
  instances and metrics-server), connects as a ServiceAccount bound to a get/list/watch-only
  ClusterRole, runs the suite and deletes the cluster. CI does the same on ubuntu runners.
- **Metadata** (`make lint-explain`): vgi-lint, 100/100 with zero findings at `info`. It runs
  static (`--no-execute`) because every table needs a cluster; `make lint-fixture` executes the
  examples against the kind fixture, and `make lint-ai` runs the agent tasks there.

`make check` runs all of it; one kind cluster is shared by the three transports
(`KIND_KEEP=1` keeps and reuses it).

## Known limitations

- **TIMESTAMPTZ filters and a non-UTC `TimeZone`**: in a session with ICU loaded and `TimeZone`
  set to anything but UTC, a pushed-down filter on a `TIMESTAMPTZ` column fails with
  "comparison operands have incompatible types". vgi-go 0.42.0 compares the column's
  `timestamp[us, UTC]` with the literal's session-zone type before the worker sees the filter.
  Run `SET TimeZone = 'UTC'` or compare `creation_timestamp::TIMESTAMP`.
- `labels['app'] = 'web'` is filtered after listing (the SDK does not expose map-subscript
  predicates for translation); use `label_selector = 'app=web'` to filter on the server.
  `label_selector` takes one equality: `label_selector IN (...)` is not pushed.
- `field_selector` and the mapped columns use the field selectors the API server supports for
  that resource; a raw `field_selector` the server does not support is a 400 error.
- `resource_tables 'all'` needs `use_local_kubeconfig`, because ATTACH runs before DuckDB secrets
  are available. Name the resources explicitly to use a secret.
- `pod_logs` reads at most `limit_bytes` (10 MiB by default) per container. Over the HTTP
  transport, a later batch served by another worker process fetches the log again and skips the
  lines already returned, so a growing log can shift by the lines written in between.
- Pod-level `cpu_request_millicores` / `memory_*_bytes` sum the app containers; the effective
  request including init containers and pod overhead is not computed.
- Tables are listed live on every query (no caching or watch-based informers). Very large
  clusters: filter by namespace or selector, or lower `page_size` for tighter memory use.

## Support

Open an issue on this repository. For VGI itself, see [query.farm](https://query.farm).

## Authorship & License

Copyright 2026 Query Farm LLC - https://query.farm

Released under the [BSD Zero Clause License](LICENSE) (0BSD). Kubernetes is a registered
trademark of The Linux Foundation; this worker is not affiliated with or endorsed by it. It
talks to the API server with the published `k8s.io/client-go` and `k8s.io/api` libraries.
