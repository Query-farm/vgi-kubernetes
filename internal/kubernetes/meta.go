// Discovery metadata: catalog and schema descriptions, categories, and the
// helpers every table uses to publish its docs.
//
// DuckDB, coding agents and the vgi-lint metadata checker all read these tags.

package kubernetes

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
)

// CatalogName is the name DuckDB attaches the worker under.
const CatalogName = "kubernetes"

// SourceURL is where this worker's code lives.
const SourceURL = "https://github.com/Query-farm/vgi-kubernetes"

// category is one entry of the schema's vgi.categories list.
type category struct {
	Name        string `json:"name"`
	Title       string `json:"title"`
	Description string `json:"description"`
}

// SchemaCategories group tables in docs and agent tools. Every table's
// vgi.category must name one of these.
var SchemaCategories = []category{
	{Name: "workloads", Title: "Workloads", Description: "Pods, their containers, and the controllers that run them: deployments, replica sets, stateful sets, daemon sets, jobs, cron jobs and autoscalers."},
	{Name: "cluster", Title: "Cluster", Description: "Nodes, namespaces, priority classes and the API server version."},
	{Name: "networking", Title: "Networking", Description: "Services, endpoint slices, ingresses and network policies."},
	{Name: "config", Title: "Configuration", Description: "ConfigMaps (keys, values on opt-in), Secrets (names, types and keys only) and service accounts."},
	{Name: "storage", Title: "Storage", Description: "Persistent volumes, persistent volume claims and storage classes."},
	{Name: "rbac", Title: "RBAC", Description: "Roles, cluster roles, their bindings, and flattened rule and subject tables."},
	{Name: "observability", Title: "Events, Logs and Metrics", Description: "Events, container logs and metrics.k8s.io resource usage."},
	{Name: "utilities", Title: "Utilities", Description: "Pure helpers that need no cluster, such as parsing resource quantities."},
	{Name: "discovery", Title: "API Discovery", Description: "The API resources a cluster serves, and a generic reader for any of them, custom resources included."},
}

// mustJSON marshals v, panicking on failure (only static data is passed).
func mustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(b)
}

// keywordsArray turns "a, b, c" into the JSON array string vgi.keywords requires.
func keywordsArray(keywords string) string {
	out := []string{}
	for _, k := range strings.Split(keywords, ",") {
		if k = strings.TrimSpace(k); k != "" {
			out = append(out, k)
		}
	}
	return mustJSON(out)
}

// fnDocs is the discovery text of one table or function.
type fnDocs struct {
	Title       string
	Category    string
	Keywords    string
	Description string
	DocLLM      string
	DocMD       string
	Examples    []vgi.CatalogExample
}

// objectTags builds the standard per-object discovery tags.
func objectTags(d fnDocs) map[string]string {
	known := false
	for _, c := range SchemaCategories {
		known = known || c.Name == d.Category
	}
	if !known {
		panic(fmt.Sprintf("unknown category %q; add it to SchemaCategories", d.Category))
	}
	return withExamples(map[string]string{
		"vgi.title":    d.Title,
		"vgi.category": d.Category,
		"vgi.keywords": keywordsArray(d.Keywords),
		"vgi.doc_llm":  d.DocLLM,
		"vgi.doc_md":   d.DocMD,
	}, d.Examples)
}

// resultColumn describes one output column for vgi.result_columns_schema.
type resultColumn struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Description string `json:"description"`
}

// column builds an Arrow field whose comment shows up in DuckDB's DESCRIBE and agent tools.
func column(name string, typ arrow.DataType, comment string, nullable bool) arrow.Field {
	return arrow.Field{Name: name, Type: typ, Nullable: nullable,
		Metadata: arrow.NewMetadata([]string{"comment"}, []string{comment})}
}

// duckType names an Arrow type the way DuckDB shows it.
func duckType(t arrow.DataType) string {
	switch x := t.(type) {
	case *arrow.StringType:
		return "VARCHAR"
	case *arrow.Int64Type:
		return "BIGINT"
	case *arrow.Int32Type:
		return "INTEGER"
	case *arrow.Float64Type:
		return "DOUBLE"
	case *arrow.BooleanType:
		return "BOOLEAN"
	case *arrow.TimestampType:
		return "TIMESTAMP WITH TIME ZONE"
	case *arrow.MapType:
		return "MAP(" + duckType(x.KeyType()) + ", " + duckType(x.ItemType()) + ")"
	case *arrow.ListType:
		return duckType(x.Elem()) + "[]"
	case *arrow.StructType:
		parts := make([]string, x.NumFields())
		for i, f := range x.Fields() {
			parts[i] = f.Name + " " + duckType(f.Type)
		}
		return "STRUCT(" + strings.Join(parts, ", ") + ")"
	case arrow.ExtensionType:
		return "JSON"
	}
	return t.String()
}

// schemaColumns lists a schema's columns for vgi.result_columns_schema.
func schemaColumns(s *arrow.Schema) []resultColumn {
	out := make([]resultColumn, s.NumFields())
	for i, f := range s.Fields() {
		c, _ := f.Metadata.GetValue("comment")
		out[i] = resultColumn{Name: f.Name, Type: duckType(f.Type), Description: c}
	}
	return out
}

// exampleQuery is one entry of vgi.example_queries.
type exampleQuery struct {
	Description string `json:"description"`
	SQL         string `json:"sql"`
}

// exampleQueries converts catalog examples to vgi.example_queries entries.
func exampleQueries(lists ...[]vgi.CatalogExample) []exampleQuery {
	out := []exampleQuery{}
	for _, examples := range lists {
		for _, ex := range examples {
			out = append(out, exampleQuery{Description: ex.Description, SQL: ex.SQL})
		}
	}
	return out
}

// withExamples publishes examples (with descriptions) as vgi.example_queries.
func withExamples(tags map[string]string, lists ...[]vgi.CatalogExample) map[string]string {
	tags["vgi.example_queries"] = mustJSON(exampleQueries(lists...))
	return tags
}

// ex builds one example.
func ex(desc, sql string) vgi.CatalogExample {
	return vgi.CatalogExample{Description: desc, SQL: sql}
}

// pushdownNote describes a table's server-side filters for its docs.
func pushdownNote(def *resDef) string {
	var parts []string
	if def.nsCol() != "" {
		parts = append(parts, def.nsCol()+" = / IN (one list request per namespace)")
	}
	fs := def.fieldSelectors()
	cols := make([]string, 0, len(fs))
	for c := range fs {
		cols = append(cols, c)
	}
	sort.Strings(cols)
	for _, c := range cols {
		parts = append(parts, c+" = / != (fieldSelector "+fs[c]+")")
	}
	parts = append(parts, "label_selector = '<selector>' (labelSelector)", "field_selector = '<selector>' (raw fieldSelector)")
	return "Server-side filters: " + strings.Join(parts, "; ") + ". Every other WHERE condition is applied by the worker."
}

// finishDocs fills a resource table's DocLLM / DocMD from its description,
// its own LLM text and its pushdown.
func finishDocs(def *resDef, llm, md string) {
	note := pushdownNote(def)
	cols := []string{}
	for _, f := range def.Schema().Fields() {
		cols = append(cols, f.Name)
	}
	def.Docs.DocLLM = llm + " Columns: " + strings.Join(cols, ", ") + ". " + note
	def.Docs.DocMD = "## " + def.Docs.Title + "\n\n" + md + "\n\n" + note
}

const catalogDocLLM = "Read-only SQL over a live Kubernetes cluster's API. Each table in schema main lists one " +
	"kind of object (pods, nodes, deployments, services, events, RBAC roles, ...) through the API server with " +
	"typed columns: CPU and memory requests/limits are numbers (cpu_*_millicores, memory_*_bytes), timestamps " +
	"are `TIMESTAMPTZ`, labels/annotations are `MAP(VARCHAR, VARCHAR)`, pod containers are `LIST(STRUCT)`. WHERE " +
	"namespace = '...' lists one namespace; name, status.phase, spec.nodeName and similar columns become field " +
	"selectors; WHERE label_selector = 'app=web' sends a label selector. pod_logs(namespace, pod) reads container " +
	"logs; node_metrics / pod_metrics read metrics.k8s.io usage; api_resources lists everything the cluster serves " +
	"and resources(api_version, resource) reads any resource, custom resources included, with spec and status as " +
	"JSON. Credentials come from a DuckDB secret of type kubernetes (KUBECONFIG with optional CONTEXT, or SERVER + " +
	"TOKEN + CERTIFICATE_AUTHORITY_DATA). Secret values are never returned: secrets lists names, types and keys only. " +
	"The worker only reads (get/list/watch); it cannot create, change or delete anything."

// CatalogDocMD is the catalog's long human-facing description.
const CatalogDocMD = "# Kubernetes\n\n" +
	"Query a live [Kubernetes](https://kubernetes.io) cluster from DuckDB: pods and their containers, nodes, " +
	"workloads, services, ingresses, events, storage, RBAC, container logs and metrics-server usage, with typed " +
	"columns and server-side namespace, field-selector and label-selector filtering. Any other API resource, custom " +
	"resources included, is readable through `resources(api_version, resource)` or as tables via the " +
	"`resource_tables` ATTACH option.\n\n" +
	"Connect with a `kubernetes` secret holding a `KUBECONFIG` (and optional `CONTEXT`) or `SERVER` + `TOKEN` + " +
	"`CERTIFICATE_AUTHORITY_DATA`. The local kubeconfig is read only when the ATTACH sets " +
	"`use_local_kubeconfig true`.\n\n" +
	"Read-only by construction: the worker issues GET requests only. Secret values are never returned."

// CatalogComment is the catalog's one-line description in directories.
const CatalogComment = "A live Kubernetes cluster's API as SQL: pods, nodes, workloads, services, events, RBAC, " +
	"storage, logs, metrics and custom resources, read-only."

// agentTask is one public analyst task for `vgi-lint simulate`.
type agentTask struct {
	Name   string `json:"name"`
	Prompt string `json:"prompt"`
}

// agentTestTasks are graded privately by vgi-agent-tests.yaml.
var agentTestTasks = []agentTask{
	{Name: "restarting_pods", Prompt: "Using the Kubernetes worker, which pods in namespace vgi-fixture have restarted at least once?"},
	{Name: "deployment_readiness", Prompt: "How many ready replicas does the deployment web in namespace vgi-fixture have, out of how many desired?"},
	{Name: "memory_requests", Prompt: "What is the total memory request, in bytes, of all containers of the pods labelled app=web in namespace vgi-fixture?"},
	{Name: "secret_keys", Prompt: "Which keys does the Secret db-credentials in namespace vgi-fixture contain?"},
	{Name: "crd_instances", Prompt: "List the names of the Widget custom resources (group fixture.vgi.query.farm) and the size in each one's spec."},
	{Name: "rbac_secret_readers", Prompt: "Which Roles in namespace vgi-fixture grant the get verb on secrets?"},
	{Name: "warning_events", Prompt: "Which event reasons of type Warning have been recorded in namespace vgi-fixture?"},
	{Name: "completed_job", Prompt: "Did the Job pi in namespace vgi-fixture complete, and how many pods succeeded?"},
	{Name: "pod_security_level", Prompt: "Which namespaces set a Pod Security Admission enforce level, and which level?"},
	{Name: "node_inventory", Prompt: "For each node, what kubelet version does it run and how many CPU millicores are allocatable?"},
	{Name: "web_replicaset", Prompt: "Which ReplicaSet currently backs the deployment web in vgi-fixture, and what rollout revision is it?"},
	{Name: "stateful_and_daemon", Prompt: "Are the StatefulSet db and the DaemonSet agent in namespace vgi-fixture fully ready? Give ready versus desired for each."},
	{Name: "suspended_cronjobs", Prompt: "Which CronJobs in vgi-fixture are suspended, and what is each one's schedule?"},
	{Name: "hpa_bounds", Prompt: "What are the minimum and maximum replica counts of the autoscaler that scales the deployment web?"},
	{Name: "service_backends", Prompt: "How many ready endpoints back the service web in vgi-fixture, and on which port does the service listen?"},
	{Name: "ingress_routes", Prompt: "Which host and path does the Ingress in vgi-fixture route to the service web?"},
	{Name: "default_deny", Prompt: "Which NetworkPolicies in vgi-fixture deny all inbound traffic by default?"},
	{Name: "configmap_keys", Prompt: "Which keys does the ConfigMap app-config in vgi-fixture define?"},
	{Name: "automount_optout", Prompt: "Which service accounts in vgi-fixture opt out of automounting their API token?"},
	{Name: "pending_claim", Prompt: "Which PersistentVolumeClaims in vgi-fixture are still Pending, how many bytes do they request, and is their storage class the cluster default?"},
	{Name: "bound_volume", Prompt: "Which PersistentVolume is bound to the claim storage-db-0 in vgi-fixture, and what is its capacity in bytes?"},
	{Name: "high_priority_pods", Prompt: "Which pods in vgi-fixture use the PriorityClass vgi-fixture-high, and what is that class's value?"},
	{Name: "role_rule_count", Prompt: "How many rules does the Role secret-reader in vgi-fixture have, and which resource is restricted to named objects?"},
	{Name: "binding_subjects", Prompt: "Which subjects does the RoleBinding reporter-reads-secrets grant its role to, and which role is it?"},
	{Name: "alice_roles", Prompt: "Which roles are granted to the user alice, and in which namespace?"},
	{Name: "sql_reader_verbs", Prompt: "Which ClusterRole is bound cluster-wide to the service account vgi-sql of namespace vgi-fixture, and which verbs does it grant on resources?"},
	{Name: "node_usage", Prompt: "How much memory is each node using right now, in bytes, according to metrics-server?"},
	{Name: "web_pod_usage", Prompt: "How much CPU and memory are the containers of the web pods in vgi-fixture using right now?"},
	{Name: "widget_api", Prompt: "Which API version and resource name serve the Widget kind, and is it namespaced?"},
	{Name: "cluster_version", Prompt: "Which Kubernetes version is the cluster's API server running?"},
	{Name: "crash_log", Prompt: "What did the crash-looping pod crashy in vgi-fixture print before it exited?"},
	{Name: "quantity_bytes", Prompt: "How many bytes is the Kubernetes memory quantity 1.5Gi?"},
}

// CatalogTags are the catalog-level discovery tags.
var CatalogTags = map[string]string{
	"vgi.title":              "Kubernetes Cluster API",
	"vgi.subtitle":           "A live Kubernetes cluster's pods, nodes, workloads, events, RBAC, logs and custom resources as SQL.",
	"vgi.doc_llm":            catalogDocLLM,
	"vgi.doc_md":             CatalogDocMD,
	"vgi.source_url":         SourceURL,
	"vgi.icon_url":           "https://query.farm/media-kit/products/vgi/vgi-logo.png",
	"vgi.license":            "0BSD",
	"vgi.author":             "Query.Farm",
	"vgi.copyright":          "Copyright 2026 Query Farm LLC - https://query.farm",
	"vgi.keywords":           keywordsArray("kubernetes, k8s, kubectl, pods, nodes, deployments, rbac, events, container logs, custom resources, cluster inventory"),
	"vgi.support_contact":    SourceURL + "/issues",
	"vgi.support_policy_url": SourceURL + "#support",
	"vgi.agent_test_tasks":   mustJSON(agentTestTasks),
}

const mainSchemaDocLLM = "Schema main has one table per core Kubernetes object kind plus a few functions. Tables " +
	"list objects through the API server on every query (nothing is cached), across all namespaces unless WHERE " +
	"namespace = '...' (or IN) narrows it. Use pods for scheduling/health questions (phase, node_name, ready, " +
	"restart_count, containers with waiting/terminated reasons such as CrashLoopBackOff), pod_containers for one row " +
	"per container, nodes for capacity/allocatable/conditions/taints, deployments/replicasets/statefulsets/daemonsets/" +
	"jobs/cronjobs for controllers, services/endpointslices/ingresses/networkpolicies for networking, configmaps/" +
	"secrets/serviceaccounts for configuration (secret values are never exposed), persistentvolumes/" +
	"persistentvolumeclaims/storageclasses for storage, roles/clusterroles/rolebindings/clusterrolebindings and the " +
	"flattened rbac_rules / rbac_subjects for access reviews, events for what happened recently. pod_logs(namespace, " +
	"pod) returns log lines with timestamps; node_metrics / pod_metrics need metrics-server; api_resources and " +
	"resources(api_version, resource) reach every other resource, custom resources included. Filter labels server-side " +
	"with WHERE label_selector = 'app=web' (any kubectl selector syntax); labels['app'] also works but is filtered by " +
	"the worker after listing."

// MainSchemaDocMD is the main schema's human-facing description.
const MainSchemaDocMD = "## main\n\nOne table per core Kubernetes object kind, read live from the API server, plus " +
	"`pod_logs`, metrics-server usage and API discovery.\n\n" +
	"Namespace, field-selector and label-selector filters run server-side; everything else is filtered by the worker."

// MainSchemaComment is the main schema's comment.
const MainSchemaComment = "Core Kubernetes objects, pod logs, metrics and API discovery, read live from the API server."

// ResourcesSchemaComment describes the per-ATTACH resource_tables schema.
const ResourcesSchemaComment = "API resources listed in the ATTACH's resource_tables option, one table each, with spec and status as JSON."

// schemaTags builds the main schema's tags.
func schemaTags(examples []exampleQuery) map[string]string {
	return map[string]string{
		"vgi.title":           "Kubernetes API",
		"domain":              "kubernetes",
		"vgi.keywords":        keywordsArray("kubernetes, pods, nodes, deployments, services, events, rbac, logs, metrics, custom resources"),
		"vgi.doc_llm":         mainSchemaDocLLM,
		"vgi.doc_md":          MainSchemaDocMD,
		"vgi.categories":      mustJSON(SchemaCategories),
		"vgi.example_queries": mustJSON(examples),
	}
}
