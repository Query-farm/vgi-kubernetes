// The table framework every Kubernetes resource table uses.
//
// A resDef names one or more API resources (sources), the columns, the docs
// and a row mapper. The scan connects with the session's kubernetes secret,
// plans the list requests from the pushed filters (pushdown.go), fetches one
// page per call (limit + continue), maps objects to rows, re-applies every
// pushed filter exactly, projects the requested columns and emits a batch.
// The cursor lives in ScanState, whose exported fields the SDK carries in
// HTTP continuation tokens, so a long scan survives the HTTP transport.

package kubernetes

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Source is one API resource a table lists. Exported: it travels in ScanState.
type Source struct {
	Group      string
	Version    string
	Resource   string
	Kind       string
	Namespaced bool
}

// apiPath is the collection path, in one namespace or across all ("").
func (s Source) apiPath(ns string) string {
	p := "/api/" + s.Version
	if s.Group != "" {
		p = "/apis/" + s.Group + "/" + s.Version
	}
	if s.Namespaced && ns != "" {
		p += "/namespaces/" + url.PathEscape(ns)
	}
	return p + "/" + s.Resource
}

// groupVersion is the apiVersion string ("v1", "apps/v1").
func (s Source) groupVersion() string {
	if s.Group == "" {
		return s.Version
	}
	return s.Group + "/" + s.Version
}

// ScanState is the resumable cursor shared by every resource table. It never
// holds credentials: the secret is re-read on every call.
type ScanState struct {
	Started       bool
	Done          bool
	Sources       []Source
	SrcIdx        int
	Namespaces    []string
	NsIdx         int
	Continue      string
	LabelSelector string
	FieldSelector string
	LabelEcho     *string
	FieldEcho     *string
	// Optional per-scan values used by specific tables.
	Extra map[string]string
}

// rowCtx is what a row mapper may need besides the object.
type rowCtx struct {
	ctx  context.Context
	src  Source
	opts attachOpts
	conn *conn
}

// rowFunc maps one object (raw JSON) to rows of the table's own columns.
type rowFunc func(raw []byte, meta *metav1.ObjectMeta, rc *rowCtx) ([][]any, error)

// typed adapts a typed mapper: the object is decoded into T.
func typed[T any](fn func(obj *T, rc *rowCtx) [][]any) rowFunc {
	return func(raw []byte, _ *metav1.ObjectMeta, rc *rowCtx) ([][]any, error) {
		var obj T
		if err := json.Unmarshal(raw, &obj); err != nil {
			return nil, fmt.Errorf("decoding %s: %w", rc.src.Kind, err)
		}
		return fn(&obj, rc), nil
	}
}

// one wraps a single row.
func one(cols ...any) [][]any { return [][]any{cols} }

// resDef defines one table.
type resDef struct {
	Name    string
	Docs    fnDocs
	Sources []Source
	// Meta prepends the standard metadata columns (metaFields).
	Meta bool
	// Cols are the table's own columns (after the metadata columns).
	Cols []arrow.Field
	// FieldSelectors maps a column to the fieldSelector path that filters it
	// server-side (equality / inequality).
	FieldSelectors map[string]string
	// NamespaceCol is the column holding the object's namespace for namespace
	// pushdown ("namespace" when Meta is set).
	NamespaceCol string
	Row          rowFunc
	// Fetch, when set, replaces listing: it returns every full-width row
	// (selector columns included) in one call.
	Fetch func(ctx context.Context, c *conn, st *ScanState) ([][]any, error)
	// Key is the primary-key column(s) published as table metadata.
	Key []string
	// Scope, when set, reports how the rows are produced (doc only).
	schema *arrow.Schema
}

// Selector-echo columns: the pushed label / field selector, for pushdown.
const (
	colLabelSelector = "label_selector"
	colFieldSelector = "field_selector"
)

var ownerRefType = listOf(structOf(sf("api_version", tString), sf("kind", tString), sf("name", tString),
	sf("uid", tString), sf("controller", tBool)))

// metaFields are the standard metadata columns.
func metaFields(namespaced bool, kind string) []arrow.Field {
	var out []arrow.Field
	if namespaced {
		out = append(out, column("namespace", tString, "Namespace of the "+kind+".", false))
	}
	return append(out,
		column("name", tString, "Name of the "+kind+" (metadata.name).", false),
		column("uid", tString, "Unique id of the "+kind+" (metadata.uid).", false),
		column("creation_timestamp", tTS, "When the "+kind+" was created.", true),
		column("labels", tStrMap, "Labels (metadata.labels). To filter server-side, use the label_selector column.", false),
		column("annotations", tStrMap, "Annotations (metadata.annotations); kubectl's last-applied-configuration is "+
			"redacted on Secrets and ConfigMaps.", false),
		column("owner_references", ownerRefType, "Owners of this object (metadata.ownerReferences): api_version, "+
			"kind, name, uid, controller.", false),
	)
}

// selectorFields are the trailing pushdown-only columns.
func selectorFields() []arrow.Field {
	return []arrow.Field{
		column(colLabelSelector, tString, "Pushdown-only: WHERE label_selector = 'app=web,tier!=db' sends a Kubernetes "+
			"label selector (=, !=, in, notin, exists, !key) to the API server; the column echoes it. NULL otherwise.", true),
		column(colFieldSelector, tString, "Pushdown-only: WHERE field_selector = 'status.phase=Running' sends a raw "+
			"Kubernetes field selector to the API server; the column echoes it. NULL otherwise.", true),
	}
}

func metaValues(m *metav1.ObjectMeta, namespaced bool, redactLastApplied bool) []any {
	var out []any
	if namespaced {
		out = append(out, m.Namespace)
	}
	ann := m.Annotations
	if redactLastApplied {
		if _, ok := ann[lastAppliedAnnotation]; ok {
			cp := make(map[string]string, len(ann))
			for k, v := range ann {
				cp[k] = v
			}
			cp[lastAppliedAnnotation] = "<redacted>"
			ann = cp
		}
	}
	owners := make([]any, 0, len(m.OwnerReferences))
	for _, o := range m.OwnerReferences {
		owners = append(owners, structVal{o.APIVersion, o.Kind, o.Name, string(o.UID), boolPtr(o.Controller)})
	}
	return append(out, m.Name, string(m.UID), tsv(m.CreationTimestamp), strMap(m.Labels), strMap(ann), owners)
}

const lastAppliedAnnotation = "kubectl.kubernetes.io/last-applied-configuration"

// Schema is the table's full output schema.
func (d *resDef) Schema() *arrow.Schema {
	if d.schema != nil {
		return d.schema
	}
	var fields []arrow.Field
	if d.Meta {
		namespaced := false
		for _, s := range d.Sources {
			namespaced = namespaced || s.Namespaced
		}
		fields = append(fields, metaFields(namespaced, d.Sources[0].Kind)...)
	}
	fields = append(fields, d.Cols...)
	fields = append(fields, selectorFields()...)
	d.schema = arrow.NewSchema(fields, nil)
	return d.schema
}

func (d *resDef) namespaced() bool {
	for _, s := range d.Sources {
		if s.Namespaced {
			return true
		}
	}
	return false
}

func (d *resDef) nsCol() string {
	if d.NamespaceCol != "" {
		return d.NamespaceCol
	}
	if d.Meta && d.namespaced() {
		return "namespace"
	}
	return ""
}

func (d *resDef) fieldSelectors() map[string]string {
	out := map[string]string{}
	if d.Meta {
		out["name"] = "metadata.name"
	}
	for k, v := range d.FieldSelectors {
		out[k] = v
	}
	return out
}

// tableFunc is the scan function of a resource table.
type tableFunc struct {
	def *resDef
	// Init, when set, fills ScanState.Sources (generic resources()).
	Init func(ctx context.Context, c *conn, p *vgi.ProcessParams, st *ScanState) error
	// Args are the table function's arguments (nil for catalog tables).
	Args any
	Bind func(p *vgi.BindParams) (*arrow.Schema, error)
	// Expose makes the scan a callable table function.
	Expose bool
}

var _ vgi.TypedTableFunc[ScanState] = (*tableFunc)(nil)

func (t *tableFunc) Name() string { return t.def.Name }

func (t *tableFunc) Metadata() vgi.FunctionMetadata {
	d := t.def.Docs
	tags := objectTags(d)
	tags["vgi.result_columns_schema"] = mustJSON(schemaColumns(t.def.Schema()))
	return vgi.FunctionMetadata{
		Description:        d.Description,
		Stability:          vgi.StabilityVolatile,
		ProjectionPushdown: true,
		FilterPushdown:     true,
		RequiredSecrets:    []vgi.SecretRequirement{{SecretType: SecretType}},
		Tags:               tags,
		Examples:           d.Examples,
	}
}

func (t *tableFunc) ArgumentSpecs() []vgi.ArgSpec {
	if t.Args == nil {
		return nil
	}
	return vgi.DeriveArgSpecs(t.Args)
}

func (t *tableFunc) OnBind(p *vgi.BindParams) (*vgi.BindResponse, error) {
	s := t.def.Schema()
	if t.Bind != nil {
		var err error
		if s, err = t.Bind(p); err != nil {
			return nil, err
		}
	}
	return bindWithSecret(p, s)
}

// bindWithSecret binds schema s; when the ATTACH names a kubernetes secret
// (secret 'name'), the first bind asks DuckDB to resolve that secret by name
// (a two-phase bind), since only the default secret of a type is sent otherwise.
func bindWithSecret(p *vgi.BindParams, s *arrow.Schema) (*vgi.BindResponse, error) {
	resp, err := vgi.BindSchema(s)
	if err != nil {
		return nil, err
	}
	if ao := readAttachOpts(p.AttachOptions()); ao.Secret != "" && !p.ResolvedSecretsProvided {
		resp.SecretScopeRequest = []vgi.SecretLookup{{SecretType: SecretType, SecretName: ao.Secret}}
	}
	return resp, nil
}

func (t *tableFunc) NewState(*vgi.ProcessParams) (*ScanState, error) {
	return &ScanState{}, nil
}

func (t *tableFunc) Process(ctx context.Context, p *vgi.ProcessParams, st *ScanState, out *vgirpc.OutputCollector) error {
	if st.Done {
		return out.Finish()
	}
	c, err := connect(p.Secrets, p.AttachOptions())
	if err != nil {
		return err
	}
	full := p.BindOutputSchema
	if full == nil {
		full = t.def.Schema()
	}
	if !st.Started {
		st.Sources = append([]Source(nil), t.def.Sources...)
		if t.Init != nil {
			if err := t.Init(ctx, c, p, st); err != nil {
				return err
			}
		}
		pl := planPushdown(p.CurrentPushdownFilters, t.def.nsCol(), t.def.fieldSelectors())
		st.Namespaces = pl.Namespaces
		st.LabelSelector, st.FieldSelector = pl.Label, strings.Join(pl.Field, ",")
		st.LabelEcho, st.FieldEcho = pl.LabelEcho, pl.FieldEcho
		st.Started = true
		if pl.Empty || (len(st.Sources) == 0 && t.def.Fetch == nil) {
			st.Done = true
			return out.Finish()
		}
	}
	for {
		rows, err := t.page(ctx, c, st)
		if err != nil {
			return err
		}
		rows, err = applyFilters(ctx, p.CurrentPushdownFilters, full, rows)
		if err != nil {
			return err
		}
		if len(rows) > 0 {
			batch, err := buildBatch(p.OutputSchema, full, rows)
			if err != nil {
				return err
			}
			return out.Emit(batch)
		}
		if st.Done {
			return out.Finish()
		}
	}
}

// listPage is the envelope of a list response.
type listPage struct {
	Metadata metav1.ListMeta   `json:"metadata"`
	Items    []json.RawMessage `json:"items"`
}

// page fetches the next page and maps it to full-width rows.
func (t *tableFunc) page(ctx context.Context, c *conn, st *ScanState) ([][]any, error) {
	if t.def.Fetch != nil {
		st.Done = true
		return t.def.Fetch(ctx, c, st)
	}
	src := st.Sources[st.SrcIdx]
	ns := ""
	if src.Namespaced && len(st.Namespaces) > 0 {
		ns = st.Namespaces[st.NsIdx]
	}
	q := url.Values{}
	q.Set("limit", strconv.FormatInt(c.opts.PageSize, 10))
	if st.Continue != "" {
		q.Set("continue", st.Continue)
	}
	if st.LabelSelector != "" {
		q.Set("labelSelector", st.LabelSelector)
	}
	if st.FieldSelector != "" {
		q.Set("fieldSelector", st.FieldSelector)
	}
	path := src.apiPath(ns)
	body, err := c.get(ctx, path, q)
	if err != nil {
		if isStatus(err, 404) && src.Group != "" {
			return nil, fmt.Errorf("%w (is the %s API served by this cluster?)", err, src.groupVersion())
		}
		return nil, err
	}
	var pg listPage
	if err := json.Unmarshal(body, &pg); err != nil {
		return nil, fmt.Errorf("kubernetes GET %s: decoding list: %w", path, err)
	}
	rows, err := t.mapItems(ctx, c, src, st, pg.Items)
	if err != nil {
		return nil, err
	}
	st.Continue = pg.Metadata.Continue
	if st.Continue == "" {
		t.advance(st, src)
	}
	return rows, nil
}

// advance moves to the next namespace / source, or marks the scan done.
func (t *tableFunc) advance(st *ScanState, src Source) {
	if src.Namespaced && st.NsIdx+1 < len(st.Namespaces) {
		st.NsIdx++
		return
	}
	st.NsIdx = 0
	st.SrcIdx++
	if st.SrcIdx >= len(st.Sources) {
		st.Done = true
	}
}

func (t *tableFunc) mapItems(ctx context.Context, c *conn, src Source, st *ScanState, items []json.RawMessage) ([][]any, error) {
	rc := &rowCtx{ctx: ctx, src: src, opts: c.opts, conn: c}
	namespaced := t.def.Meta && t.def.namespaced()
	redact := src.Group == "" && (src.Resource == "secrets" || src.Resource == "configmaps")
	var rows [][]any
	for _, raw := range items {
		var env struct {
			Metadata metav1.ObjectMeta `json:"metadata"`
		}
		if err := json.Unmarshal(raw, &env); err != nil {
			return nil, fmt.Errorf("decoding %s metadata: %w", src.Kind, err)
		}
		own, err := t.def.Row(raw, &env.Metadata, rc)
		if err != nil {
			return nil, err
		}
		for _, r := range own {
			var row []any
			if t.def.Meta {
				row = metaValues(&env.Metadata, namespaced, redact)
				if namespaced && !src.Namespaced {
					row[0] = nil
				}
			}
			row = append(row, r...)
			row = append(row, strPtr(st.LabelEcho), strPtr(st.FieldEcho))
			rows = append(rows, row)
		}
	}
	return rows, nil
}

// newTable wraps a definition for registration.
func newTable(t *tableFunc) vgi.TableFunction {
	return vgi.AsTableFunction[ScanState](t)
}

// catalogTable registers def as a catalog table in schema main.
func catalogTable(def *resDef) vgi.CatalogTable {
	s := def.Schema()
	comments := map[string]string{}
	for _, f := range s.Fields() {
		c, _ := f.Metadata.GetValue("comment")
		comments[f.Name] = c
	}
	tags := objectTags(def.Docs)
	tags["domain"] = "kubernetes"
	return vgi.CatalogTable{
		Name:           def.Name,
		Comment:        def.Docs.Description,
		Columns:        s,
		Function:       newTable(&tableFunc{def: def}),
		ColumnComments: comments,
		Tags:           tags,
		Constraints:    keyConstraints(def),
	}
}

// keyConstraints declares the NOT NULL + primary key of a table.
func keyConstraints(def *resDef) [][]byte {
	if len(def.Key) == 0 {
		return nil
	}
	cons := func(id string, kind vgi.ConstraintKind, cols [][]string) []byte {
		rec, err := vgi.ConstraintRecord(vgi.ConstraintRecordSpec{SchemaPath: []string{"main"}, Table: def.Name, ID: id,
			Kind: kind, Columns: cols, Enforcement: vgi.ConstraintEnforced, Validation: vgi.ConstraintAllRows})
		if err != nil {
			panic(err)
		}
		return rec
	}
	var out [][]byte
	cols := make([][]string, len(def.Key))
	for i, k := range def.Key {
		cols[i] = []string{k}
		out = append(out, cons(k+"_not_null", vgi.ConstraintNotNull, [][]string{{k}}))
	}
	return append(out, cons("pk", vgi.ConstraintPrimaryKey, cols))
}
