// API discovery and the generic resource reader.
//
//   - api_resources: every resource the cluster serves (discovery).
//   - resources(api_version, resource): any listable resource, custom
//     resources included, with the metadata columns plus spec, status and
//     the whole object as JSON.
//   - cluster_version: the API server's /version.

package kubernetes

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// discovered is one served API resource.
type discovered struct {
	Group, Version, Resource, Kind, Singular string
	Namespaced, Preferred                    bool
	Verbs, ShortNames, Categories            []string
}

func (d discovered) source() Source {
	return Source{Group: d.Group, Version: d.Version, Resource: d.Resource, Kind: d.Kind, Namespaced: d.Namespaced}
}

func (d discovered) listable() bool {
	for _, v := range d.Verbs {
		if v == "list" {
			return true
		}
	}
	return false
}

// groupVersionResources reads one group-version's resource list.
func groupVersionResources(ctx context.Context, c *conn, group, version string, preferred bool) ([]discovered, error) {
	path := "/api/" + version
	if group != "" {
		path = "/apis/" + group + "/" + version
	}
	body, err := c.get(ctx, path, nil)
	if err != nil {
		return nil, err
	}
	var rl metav1.APIResourceList
	if err := json.Unmarshal(body, &rl); err != nil {
		return nil, fmt.Errorf("kubernetes GET %s: decoding: %w", path, err)
	}
	var out []discovered
	for _, r := range rl.APIResources {
		if strings.Contains(r.Name, "/") {
			continue // subresources (pods/log, deployments/scale, ...)
		}
		out = append(out, discovered{Group: group, Version: version, Resource: r.Name, Kind: r.Kind, Singular: r.SingularName,
			Namespaced: r.Namespaced, Preferred: preferred, Verbs: r.Verbs, ShortNames: r.ShortNames, Categories: r.Categories})
	}
	return out, nil
}

var (
	discMu    sync.Mutex
	discCache = map[string]discCacheEntry{}
)

type discCacheEntry struct {
	at  time.Time
	all []discovered
}

// discoverAll lists every served resource of every group-version (cached 2 min).
func discoverAll(ctx context.Context, c *conn) ([]discovered, error) {
	discMu.Lock()
	if e, ok := discCache[c.key]; ok && time.Since(e.at) < 2*time.Minute {
		discMu.Unlock()
		return e.all, nil
	}
	discMu.Unlock()
	body, err := c.get(ctx, "/api", nil)
	if err != nil {
		return nil, err
	}
	var core metav1.APIVersions
	if err := json.Unmarshal(body, &core); err != nil {
		return nil, fmt.Errorf("kubernetes GET /api: decoding: %w", err)
	}
	var all []discovered
	for i, v := range core.Versions {
		rs, err := groupVersionResources(ctx, c, "", v, i == 0)
		if err != nil {
			return nil, err
		}
		all = append(all, rs...)
	}
	body, err = c.get(ctx, "/apis", nil)
	if err != nil {
		return nil, err
	}
	var groups metav1.APIGroupList
	if err := json.Unmarshal(body, &groups); err != nil {
		return nil, fmt.Errorf("kubernetes GET /apis: decoding: %w", err)
	}
	for _, g := range groups.Groups {
		for _, v := range g.Versions {
			rs, err := groupVersionResources(ctx, c, g.Name, v.Version, v.Version == g.PreferredVersion.Version)
			if err != nil {
				if isStatus(err, 404, 503) {
					continue // an unavailable aggregated API (e.g. metrics-server down)
				}
				return nil, err
			}
			all = append(all, rs...)
		}
	}
	discMu.Lock()
	discCache[c.key] = discCacheEntry{at: time.Now(), all: all}
	discMu.Unlock()
	return all, nil
}

// splitAPIVersion parses "v1" or "group/version".
func splitAPIVersion(apiVersion string) (string, string, error) {
	av := strings.Trim(strings.TrimSpace(apiVersion), "/")
	if av == "" {
		return "", "", fmt.Errorf("api_version must be 'v1' or '<group>/<version>', e.g. 'apps/v1'")
	}
	if i := strings.LastIndex(av, "/"); i >= 0 {
		return av[:i], av[i+1:], nil
	}
	return "", av, nil
}

// resolveResource finds resource (plural, singular, kind or short name) in apiVersion.
func resolveResource(ctx context.Context, c *conn, apiVersion, resource string) (Source, error) {
	group, version, err := splitAPIVersion(apiVersion)
	if err != nil {
		return Source{}, err
	}
	rs, err := groupVersionResources(ctx, c, group, version, true)
	if err != nil {
		if isStatus(err, 404) {
			return Source{}, fmt.Errorf("API version %q is not served by this cluster (see kubernetes.main.api_resources)", apiVersion)
		}
		return Source{}, err
	}
	want := strings.ToLower(strings.TrimSpace(resource))
	for _, r := range rs {
		match := strings.ToLower(r.Resource) == want || strings.ToLower(r.Singular) == want || strings.ToLower(r.Kind) == want
		for _, s := range r.ShortNames {
			match = match || strings.ToLower(s) == want
		}
		if match {
			if !r.listable() {
				return Source{}, fmt.Errorf("%s %s cannot be listed (verbs: %s)", apiVersion, r.Resource, strings.Join(r.Verbs, ", "))
			}
			return r.source(), nil
		}
	}
	return Source{}, fmt.Errorf("no resource %q in API version %q (see kubernetes.main.api_resources)", resource, apiVersion)
}

// ---------------------------------------------------------------- api_resources

var apiResourcesDef = &resDef{
	Name: "api_resources", Key: []string{"api_version", "resource"},
	Docs: fnDocs{Title: "API Resources", Category: "discovery",
		Keywords:    "kubernetes, api discovery, api-resources, crds, custom resources, api groups, verbs",
		Description: "Every API resource the cluster serves (like kubectl api-resources), custom resources included",
		Examples: []vgi.CatalogExample{
			ex("Custom resource types (dotted groups outside *.k8s.io)", "SELECT api_version, resource, kind FROM kubernetes.main.api_resources WHERE preferred AND \"group\" LIKE '%.%' AND \"group\" NOT LIKE '%k8s.io' ORDER BY ALL"),
		}},
	Cols: []arrow.Field{
		column("group", tString, "API group ('' for the core group).", false),
		column("version", tString, "API version within the group, e.g. v1.", false),
		column("api_version", tString, "group/version, or v1 for the core group: the first argument of resources().", false),
		column("resource", tString, "Plural resource name, e.g. deployments: the second argument of resources().", false),
		column("kind", tString, "Object kind, e.g. Deployment.", false),
		column("singular_name", tString, "Singular resource name.", true),
		column("namespaced", tBool, "Whether objects live in namespaces.", false),
		column("preferred", tBool, "Whether this is the group's preferred version.", false),
		column("listable", tBool, "Whether the list verb is supported (resources() needs it).", false),
		column("verbs", tStrList, "Supported verbs.", false),
		column("short_names", tStrList, "kubectl short names, e.g. po, deploy.", false),
		column("categories", tStrList, "Categories, e.g. all.", false),
	},
	Fetch: func(ctx context.Context, c *conn, st *ScanState) ([][]any, error) {
		all, err := discoverAll(ctx, c)
		if err != nil {
			return nil, err
		}
		rows := make([][]any, 0, len(all))
		for _, d := range all {
			rows = append(rows, []any{d.Group, d.Version, d.source().groupVersion(), d.Resource, d.Kind, nilIfEmpty(d.Singular),
				d.Namespaced, d.Preferred, d.listable(), strList(d.Verbs), strList(d.ShortNames), strList(d.Categories),
				strPtr(st.LabelEcho), strPtr(st.FieldEcho)})
		}
		return rows, nil
	},
}

// ---------------------------------------------------------------- cluster_version

var apiServersDef = &resDef{
	Name: "api_servers", Key: []string{"server"},
	Docs: fnDocs{Title: "Cluster Version", Category: "cluster",
		Keywords:    "kubernetes, version, api server, git version, platform, upgrade",
		Description: "The API server the worker connects to and its version (GET /version), one row",
		Examples: []vgi.CatalogExample{
			ex("Kubernetes version of the cluster", "SELECT git_version, platform FROM kubernetes.main.api_servers"),
		}},
	Cols: []arrow.Field{
		column("git_version", tString, "e.g. v1.31.2.", false),
		column("major", tString, "Major version.", true),
		column("minor", tString, "Minor version.", true),
		column("platform", tString, "e.g. linux/amd64.", true),
		column("go_version", tString, "Go version the server was built with.", true),
		column("build_date", tTS, "When the API server binary was built.", true),
		column("server", tString, "API server URL the worker connected to.", false),
	},
	Fetch: func(ctx context.Context, c *conn, st *ScanState) ([][]any, error) {
		body, err := c.get(ctx, "/version", nil)
		if err != nil {
			return nil, err
		}
		var v struct {
			Major, Minor, GitVersion, GoVersion, Platform, BuildDate string
		}
		if err := json.Unmarshal(body, &v); err != nil {
			return nil, fmt.Errorf("kubernetes GET /version: decoding: %w", err)
		}
		var bd any
		if t, err := time.Parse(time.RFC3339, v.BuildDate); err == nil {
			bd = t.UTC()
		}
		return [][]any{{v.GitVersion, nilIfEmpty(v.Major), nilIfEmpty(v.Minor), nilIfEmpty(v.Platform), nilIfEmpty(v.GoVersion), bd,
			c.base, strPtr(st.LabelEcho), strPtr(st.FieldEcho)}}, nil
	},
}

// ---------------------------------------------------------------- resources()

// genericCols are the columns of resources() and of resource_tables tables.
var genericCols = []arrow.Field{
	column("namespace", tString, "Namespace (NULL for cluster-scoped resources).", true),
	column("name", tString, "metadata.name.", false),
	column("uid", tString, "metadata.uid.", false),
	column("creation_timestamp", tTS, "When the object was created.", true),
	column("labels", tStrMap, "metadata.labels. To filter server-side, use the label_selector column.", false),
	column("annotations", tStrMap, "metadata.annotations.", false),
	column("owner_references", ownerRefType, "metadata.ownerReferences: api_version, kind, name, uid, controller.", false),
	column("api_version", tString, "apiVersion of the object.", false),
	column("kind", tString, "Kind of the object.", false),
	column("spec", tJSON, "The object's spec as JSON (NULL when it has none).", true),
	column("status", tJSON, "The object's status as JSON (NULL when it has none).", true),
	column("object", tJSON, "The whole object as JSON, without metadata.managedFields. Secret data is removed.", false),
}

// sanitizeObject removes managed fields and secret material from an object.
func sanitizeObject(obj map[string]any, src Source, opts attachOpts) {
	if md, ok := obj["metadata"].(map[string]any); ok {
		delete(md, "managedFields")
		if ann, ok := md["annotations"].(map[string]any); ok && src.Group == "" && (src.Resource == "secrets" || src.Resource == "configmaps") {
			if _, ok := ann[lastAppliedAnnotation]; ok {
				ann[lastAppliedAnnotation] = "<redacted>"
			}
		}
	}
	if src.Group == "" && src.Resource == "secrets" {
		for _, k := range []string{"data", "stringData"} {
			if m, ok := obj[k].(map[string]any); ok {
				keys := make(map[string]any, len(m))
				for kk := range m {
					keys[kk] = "<redacted>"
				}
				obj[k] = keys
			}
		}
	}
	if src.Group == "" && src.Resource == "configmaps" && !opts.ConfigMapValues {
		for _, k := range []string{"data", "binaryData"} {
			if m, ok := obj[k].(map[string]any); ok {
				keys := make(map[string]any, len(m))
				for kk := range m {
					keys[kk] = "<hidden: ATTACH with configmap_values true>"
				}
				obj[k] = keys
			}
		}
	}
}

func genericRow(raw []byte, meta *metav1.ObjectMeta, rc *rowCtx) ([][]any, error) {
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, fmt.Errorf("decoding %s: %w", rc.src.Kind, err)
	}
	sanitizeObject(obj, rc.src, rc.opts)
	redact := rc.src.Group == "" && (rc.src.Resource == "secrets" || rc.src.Resource == "configmaps")
	mv := metaValues(meta, true, redact)
	if !rc.src.Namespaced {
		mv[0] = nil
	}
	apiVersion, _ := obj["apiVersion"].(string)
	if apiVersion == "" {
		apiVersion = rc.src.groupVersion()
	}
	kind, _ := obj["kind"].(string)
	if kind == "" {
		kind = rc.src.Kind
	}
	if _, ok := obj["apiVersion"]; !ok {
		obj["apiVersion"] = apiVersion
	}
	if _, ok := obj["kind"]; !ok {
		obj["kind"] = kind
	}
	row := append(mv, apiVersion, kind, toJSON(obj["spec"]), toJSON(obj["status"]), toJSON(obj))
	return [][]any{row}, nil
}

type resourcesArgs struct {
	APIVersion string `vgi:"pos=0,pattern=^([a-z0-9.-]+/)?v[0-9]+[a-z0-9]*$,doc=API version of the resource as listed in api_resources.api_version (e.g. apps/v1)."`
	Resource   string `vgi:"pos=1,doc=Resource name as listed in api_resources.resource (e.g. deployments); its kind or short name also works."`
}

var resourcesDef = &resDef{
	Name: "resources", NamespaceCol: "namespace",
	Docs: fnDocs{Title: "Any API Resource", Category: "discovery",
		Keywords:    "kubernetes, custom resources, crd, generic, dynamic client, spec, status, json",
		Description: "Lists any listable API resource, custom resources included, with metadata columns and spec/status/object as JSON",
		Examples: []vgi.CatalogExample{
			ex("PodDisruptionBudgets and how many disruptions they allow", "SELECT namespace, name, (status->>'disruptionsAllowed')::INT AS allowed FROM kubernetes.main.resources('policy/v1', 'poddisruptionbudgets') ORDER BY allowed, namespace, name"),
			ex("cert-manager certificates that are not ready", "SELECT namespace, name, status->>'$.conditions[0].reason' AS reason FROM kubernetes.main.resources('cert-manager.io/v1', 'certificates') WHERE status->>'$.conditions[0].status' <> 'True' ORDER BY namespace, name"),
		}},
	Cols:           genericCols,
	FieldSelectors: map[string]string{"name": "metadata.name"},
	Row:            genericRow,
}

func newResourcesFunc() *tableFunc {
	return &tableFunc{def: resourcesDef, Args: resourcesArgs{}, Expose: true,
		Init: func(ctx context.Context, c *conn, p *vgi.ProcessParams, st *ScanState) error {
			var a resourcesArgs
			if err := vgi.BindArgs(p.Args, &a); err != nil {
				return err
			}
			src, err := resolveResource(ctx, c, a.APIVersion, a.Resource)
			if err != nil {
				return err
			}
			st.Sources = []Source{src}
			return nil
		}}
}

// resourceTableName names a resource_tables table: the plural, or plural_group on a clash.
func resourceTableNames(ds []discovered) map[int]string {
	count := map[string]int{}
	for _, d := range ds {
		count[d.Resource]++
	}
	out := map[int]string{}
	for i, d := range ds {
		name := d.Resource
		if count[d.Resource] > 1 && d.Group != "" {
			name += "_" + strings.NewReplacer(".", "_", "-", "_").Replace(d.Group)
		}
		out[i] = name
	}
	return out
}
