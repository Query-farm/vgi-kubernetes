// The per-ATTACH `resources` schema: the API resources named by the
// resource_tables ATTACH option, one table each.
//
//	ATTACH 'kubernetes' AS k8s (TYPE vgi, LOCATION '...',
//	    resource_tables 'stable.example.com/v1/crontabs, policy/v1/poddisruptionbudgets');
//	SELECT name, spec->>'cronSpec' FROM k8s.resources.crontabs;
//
// Every table has the columns of resources() and is scanned by
// kubernetes.main.resources(api_version, resource), so no credentials are
// needed at ATTACH. `resource_tables 'all'` instead discovers every listable
// resource at ATTACH, which needs credentials then: only the local kubeconfig
// (use_local_kubeconfig true) is available at that point.

package kubernetes

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-go/vgi/generated"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// resourcesSchema is the schema name of the resource_tables tables.
const resourcesSchema = "resources"

// attachTable is one resource_tables table.
type attachTable struct {
	Name       string `json:"name"`
	APIVersion string `json:"api_version"`
	Resource   string `json:"resource"`
	Kind       string `json:"kind,omitempty"`
}

// attachState is the worker's per-ATTACH data (attach_opaque_data). It holds
// no credentials.
type attachState struct {
	Marker string        `json:"vgi_kubernetes"`
	Tables []attachTable `json:"tables"`
}

const attachMarker = "v1"

func decodeAttachState(data []byte) (*attachState, bool) {
	if len(data) == 0 || data[0] != '{' {
		return nil, false
	}
	var st attachState
	if err := json.Unmarshal(data, &st); err != nil || st.Marker != attachMarker {
		return nil, false
	}
	return &st, true
}

func (st *attachState) table(name string) (*attachTable, bool) {
	for i := range st.Tables {
		if strings.EqualFold(st.Tables[i].Name, name) {
			return &st.Tables[i], true
		}
	}
	return nil, false
}

// rawAttachOptions reads the ATTACH options batch into Go values.
func rawAttachOptions(req *vgi.CatalogAttachRequestWire) (map[string]any, error) {
	out := map[string]any{}
	if req.Options == nil || len(*req.Options) == 0 {
		return out, nil
	}
	batch, err := vgi.DeserializeRecordBatch(*req.Options)
	if err != nil {
		return nil, fmt.Errorf("reading ATTACH options: %w", err)
	}
	defer batch.Release()
	if batch.NumRows() == 0 {
		return out, nil
	}
	for i, f := range batch.Schema().Fields() {
		col := batch.Column(i)
		if col.IsNull(0) {
			continue
		}
		switch c := col.(type) {
		case *array.String:
			out[strings.ToLower(f.Name)] = c.Value(0)
		case *array.Boolean:
			out[strings.ToLower(f.Name)] = c.Value(0)
		case *array.Int64:
			out[strings.ToLower(f.Name)] = c.Value(0)
		}
	}
	return out, nil
}

// parseResourceTables parses "group/version/resource, v1/pods" entries.
func parseResourceTables(spec string) ([]attachTable, error) {
	var out []attachTable
	for _, part := range strings.Split(spec, ",") {
		part = strings.Trim(strings.TrimSpace(part), "/")
		if part == "" {
			continue
		}
		i := strings.LastIndex(part, "/")
		if i <= 0 {
			return nil, fmt.Errorf("resource_tables entry %q: want <group>/<version>/<resource> (or v1/<resource> for the core group)", part)
		}
		out = append(out, attachTable{APIVersion: part[:i], Resource: strings.ToLower(part[i+1:])})
	}
	return out, nil
}

// nameTables assigns table names: the resource, or resource_<group> on a clash.
func nameTables(ts []attachTable) {
	ds := make([]discovered, len(ts))
	for i, t := range ts {
		g, _, _ := splitAPIVersion(t.APIVersion)
		ds[i] = discovered{Group: g, Resource: t.Resource}
	}
	names := resourceTableNames(ds)
	for i := range ts {
		ts[i].Name = names[i]
	}
}

// validateAttach resolves the resource_tables option.
func validateAttach(req *vgi.CatalogAttachRequestWire, _ *vgirpc.CallContext) (*vgi.AttachDecision, error) {
	opts, err := rawAttachOptions(req)
	if err != nil {
		return nil, err
	}
	st := &attachState{Marker: attachMarker}
	spec, _ := opts[optResourceTables].(string)
	spec = strings.TrimSpace(spec)
	switch {
	case spec == "":
	case strings.EqualFold(spec, "all"):
		useLocal, _ := opts[optUseLocalKubeconfig].(bool)
		if !useLocal {
			return nil, fmt.Errorf("resource_tables 'all' discovers resources at ATTACH time, when DuckDB secrets are " +
				"not available: set use_local_kubeconfig true, or list the resources explicitly " +
				"(resource_tables '<group>/<version>/<resource>, ...')")
		}
		ctxName, _ := opts[optContext].(string)
		cfg, key, err := restConfigFrom(nil, false, attachOpts{UseLocal: true, Context: ctxName})
		if err != nil {
			return nil, err
		}
		c, err := connFor(cfg, key, attachOpts{UseLocal: true, Context: ctxName, PageSize: 500})
		if err != nil {
			return nil, err
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		all, err := discoverAll(ctx, c)
		if err != nil {
			return nil, fmt.Errorf("resource_tables 'all': discovery: %w", err)
		}
		var ds []discovered
		for _, d := range all {
			if d.Preferred && d.listable() {
				ds = append(ds, d)
			}
		}
		sort.Slice(ds, func(i, j int) bool {
			if ds[i].Resource != ds[j].Resource {
				return ds[i].Resource < ds[j].Resource
			}
			return ds[i].Group < ds[j].Group
		})
		for _, d := range ds {
			st.Tables = append(st.Tables, attachTable{APIVersion: d.source().groupVersion(), Resource: d.Resource, Kind: d.Kind})
		}
		nameTables(st.Tables)
	default:
		ts, err := parseResourceTables(spec)
		if err != nil {
			return nil, err
		}
		nameTables(ts)
		seen := map[string]bool{}
		for _, t := range ts {
			if seen[t.Name] {
				return nil, fmt.Errorf("resource_tables lists %s/%s twice", t.APIVersion, t.Resource)
			}
			seen[t.Name] = true
		}
		st.Tables = ts
	}
	data, err := json.Marshal(st)
	if err != nil {
		return nil, err
	}
	return &vgi.AttachDecision{AttachOpaqueData: data}, nil
}

// resourceTableSchema is the column layout of every resources.<table>.
func resourceTableSchema() *arrow.Schema { return resourcesDef.Schema() }

func scanFunctionFor(t *attachTable) *vgi.ScanFunctionResult {
	main := []string{"main"}
	return &vgi.ScanFunctionResult{
		FunctionName: resourcesDef.Name,
		PositionalArguments: []vgi.ScanArg{
			{Value: t.APIVersion, Type: arrow.BinaryTypes.String},
			{Value: t.Resource, Type: arrow.BinaryTypes.String},
		},
		SchemaPath: &main,
	}
}

func resourceTableTags(t *attachTable) map[string]string {
	sql := fmt.Sprintf("SELECT namespace, name, creation_timestamp FROM kubernetes.%s.%s ORDER BY namespace, name", resourcesSchema, t.Name)
	return map[string]string{
		"vgi.title":    t.APIVersion + " " + t.Resource,
		"vgi.category": "discovery",
		"vgi.keywords": keywordsArray("kubernetes, custom resource, " + t.Resource + ", " + t.APIVersion),
		"vgi.doc_llm": fmt.Sprintf("Objects of the Kubernetes resource %s in API version %s (from the ATTACH's "+
			"resource_tables option), read with kubernetes.main.resources('%s', '%s'): metadata columns plus spec, status "+
			"and object as JSON. WHERE namespace / name / label_selector run server-side.", t.Resource, t.APIVersion, t.APIVersion, t.Resource),
		"vgi.doc_md":                fmt.Sprintf("## %s\n\nObjects of `%s` in `%s`, with spec and status as JSON.", t.Name, t.Resource, t.APIVersion),
		"vgi.example_queries":       mustJSON([]exampleQuery{{Description: "List the objects", SQL: sql}}),
		"vgi.result_columns_schema": mustJSON(schemaColumns(resourceTableSchema())),
	}
}

func tableItem(t *attachTable) ([]byte, error) {
	scanBytes, err := vgi.SerializeScanFunctionResult(scanFunctionFor(t))
	if err != nil {
		return nil, err
	}
	return vgi.SerializeTableInfo(&vgi.TableInfo{
		Name:         t.Name,
		SchemaPath:   []string{resourcesSchema},
		Comment:      fmt.Sprintf("Objects of %s (%s), read live from the API server.", t.Resource, t.APIVersion),
		Tags:         resourceTableTags(t),
		Columns:      resourceTableSchema(),
		ScanFunction: scanBytes,
	})
}

func resourcesSchemaContents(st *attachState) (generated.SchemaContents, error) {
	comment := ResourcesSchemaComment
	info, err := vgi.SerializeSchemaInfo(&vgi.SchemaInfo{
		Path: []string{resourcesSchema}, Comment: &comment,
		Tags: map[string]string{
			"vgi.title":    "Resource Tables",
			"domain":       "kubernetes",
			"vgi.keywords": keywordsArray("kubernetes, custom resources, crd, resource tables"),
			"vgi.doc_llm":  "Tables for the API resources named by the ATTACH's resource_tables option. " + ResourcesSchemaComment,
			"vgi.doc_md":   "## resources\n\n" + ResourcesSchemaComment,
		},
	})
	if err != nil {
		return generated.SchemaContents{}, err
	}
	tables := [][]byte{}
	for i := range st.Tables {
		item, err := tableItem(&st.Tables[i])
		if err != nil {
			return generated.SchemaContents{}, fmt.Errorf("table %s.%s: %w", resourcesSchema, st.Tables[i].Name, err)
		}
		tables = append(tables, item)
	}
	empty := func() [][]byte { return [][]byte{} }
	return generated.SchemaContents{
		Path: []string{resourcesSchema}, Schema: info, Tables: tables,
		Views: empty(), ScalarFunctions: empty(), AggregateFunctions: empty(), TableFunctions: empty(),
		ScalarMacros: empty(), TableMacros: empty(), Indexes: empty(),
	}, nil
}

// catalogContents serves the static main schema plus the resources schema.
func catalogContents(call *vgi.CatalogContentsCall) (vgi.CatalogContentsResult, error) {
	schemas, err := call.Contents()
	if err != nil {
		return vgi.CatalogContentsResult{}, err
	}
	if st, ok := decodeAttachState(call.AttachOpaqueData); ok && len(st.Tables) > 0 {
		sc, err := resourcesSchemaContents(st)
		if err != nil {
			return vgi.CatalogContentsResult{}, err
		}
		schemas = append(schemas, sc)
	}
	return vgi.CatalogContentsResult{Schemas: schemas}, nil
}

// schemaTables is the per-schema fallback (catalog_schema_contents_tables).
func schemaTables(attach []byte, path vgi.SchemaPath) ([]vgi.SerializedSchemaItem, bool) {
	st, ok := decodeAttachState(attach)
	if !ok || len(path) != 1 || path[0] != resourcesSchema || len(st.Tables) == 0 {
		return nil, false
	}
	sc, err := resourcesSchemaContents(st)
	if err != nil {
		return nil, false
	}
	items := make([]vgi.SerializedSchemaItem, len(sc.Tables))
	for i, t := range sc.Tables {
		items[i] = t
	}
	return items, true
}

// tableGet answers a lookup of one resource table by name.
func tableGet(attach []byte, path vgi.SchemaPath, name string, _, _ *string) ([]byte, bool, error) {
	st, ok := decodeAttachState(attach)
	if !ok || len(path) != 1 || path[0] != resourcesSchema {
		return nil, false, nil
	}
	t, ok := st.table(name)
	if !ok {
		return nil, false, nil
	}
	data, err := tableItem(t)
	return data, true, err
}

// scanFunctionGet names the scan of a resource table.
func scanFunctionGet(attach []byte, path vgi.SchemaPath, name string, _, _ *string) (*vgi.ScanFunctionResult, bool, error) {
	st, ok := decodeAttachState(attach)
	if !ok || len(path) != 1 || path[0] != resourcesSchema {
		return nil, false, nil
	}
	t, ok := st.table(name)
	if !ok {
		return nil, false, nil
	}
	return scanFunctionFor(t), true, nil
}
