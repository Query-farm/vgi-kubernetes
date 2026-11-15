package kubernetes

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-go/vgi/vgitest"
)

// ---------------------------------------------------------------- fake API server

// fakeAPI serves the recorded responses in testdata/ (captured from the kind
// fixture with `kubectl get --raw`). List responses are paged two items at a
// time with a continue token, whatever limit the client asks for, so every
// scan crosses page boundaries.
type fakeAPI struct {
	t        *testing.T
	mu       sync.Mutex
	requests []string
	// fail429 makes the first request to each path answer 429 + Retry-After.
	fail429 bool
	seen429 map[string]bool
}

var routes = map[string]string{
	"/api/v1/namespaces/vgi-fixture/pods":                             "pods.json",
	"/api/v1/pods":                                                    "pods.json",
	"/api/v1/nodes":                                                   "nodes.json",
	"/api/v1/namespaces/vgi-fixture/secrets":                          "secrets.json",
	"/api/v1/namespaces/vgi-fixture/events":                           "events.json",
	"/apis/fixture.vgi.query.farm/v1/widgets":                         "widgets.json",
	"/apis/fixture.vgi.query.farm/v1/namespaces/vgi-fixture/widgets":  "widgets.json",
	"/apis/fixture.vgi.query.farm/v1":                                 "widgets-discovery.json",
	"/apis/metrics.k8s.io/v1beta1/namespaces/vgi-fixture/pods":        "podmetrics.json",
	"/apis/rbac.authorization.k8s.io/v1/namespaces/vgi-fixture/roles": "roles.json",
	"/apis/apps/v1/namespaces/vgi-fixture/deployments":                "deployments.json",
	"/version": "version.json",
	"/api/v1/namespaces/vgi-fixture/pods/hello/log":                  "hello.log",
	"/apis/rbac.authorization.k8s.io/v1/clusterroles":                "",
	"/apis/fixture.vgi.query.farm/v1/namespaces/vgi-fixture/gadgets": "",
	"/api/v1/namespaces/vgi-fixture/pods/hello":                      "",
	"/api/v1/namespaces/vgi-fixture/pods/missing":                    "",
	"/apis/metrics.k8s.io/v1beta1/nodes":                             "",
	"/api/v1/secrets":                                                "secrets.json",
	"/api/v1/events":                                                 "events.json",
	"/apis/rbac.authorization.k8s.io/v1/roles":                       "roles.json",
	"/apis/metrics.k8s.io/v1beta1/pods":                              "podmetrics.json",
}

func (f *fakeAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.requests = append(f.requests, r.URL.RequestURI())
	if f.fail429 && !f.seen429[r.URL.Path] {
		f.seen429[r.URL.Path] = true
		f.mu.Unlock()
		w.Header().Set("Retry-After", "0")
		http.Error(w, `{"kind":"Status","message":"slow down","reason":"TooManyRequests","code":429}`, http.StatusTooManyRequests)
		return
	}
	f.mu.Unlock()
	if r.Method != http.MethodGet {
		f.t.Errorf("worker issued %s %s; only GET is allowed", r.Method, r.URL)
		http.Error(w, "read-only", http.StatusMethodNotAllowed)
		return
	}
	if r.Header.Get("Authorization") != "Bearer test-token" {
		http.Error(w, `{"kind":"Status","message":"Unauthorized","reason":"Unauthorized","code":401}`, http.StatusUnauthorized)
		return
	}
	file, ok := routes[r.URL.Path]
	if !ok || file == "" {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"kind":"Status","message":"the server could not find the requested resource","reason":"NotFound","code":404}`))
		return
	}
	body, err := os.ReadFile(filepath.Join("testdata", file))
	if err != nil {
		f.t.Fatal(err)
	}
	if strings.HasSuffix(file, ".log") || !strings.Contains(string(body), `"items"`) {
		_, _ = w.Write(body)
		return
	}
	var list map[string]any
	if err := json.Unmarshal(body, &list); err != nil {
		f.t.Fatal(err)
	}
	items, _ := list["items"].([]any)
	start, _ := strconv.Atoi(r.URL.Query().Get("continue"))
	end := min(start+2, len(items))
	md := map[string]any{"resourceVersion": "1"}
	if end < len(items) {
		md["continue"] = strconv.Itoa(end)
	}
	list["items"], list["metadata"] = items[start:end], md
	_ = json.NewEncoder(w).Encode(list)
}

func (f *fakeAPI) requestLog() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.requests...)
}

func newFake(t *testing.T) (*fakeAPI, vgi.Secrets) {
	t.Helper()
	f := &fakeAPI{t: t, seen429: map[string]bool{}}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	secrets := vgi.Secrets{"k8s": {"type": SecretType, "server": srv.URL, "token": "test-token"}}
	return f, secrets
}

// run drives a table function in-process (bind -> process, with the state
// gob round-tripped between batches as over HTTP) and returns its rows.
func run(t *testing.T, fn vgi.TableFunction, secrets vgi.Secrets, positional ...any) ([]string, [][]string) {
	t.Helper()
	res, err := vgitest.RunTable(fn, vgitest.Call{Positional: positional, Secrets: secrets})
	if err != nil {
		t.Fatal(err)
	}
	defer res.Release()
	var cols []string
	for _, f := range res.Schema.Fields() {
		cols = append(cols, f.Name)
	}
	var rows [][]string
	for _, b := range res.Batches {
		for r := 0; r < int(b.NumRows()); r++ {
			row := make([]string, b.NumCols())
			for c := 0; c < int(b.NumCols()); c++ {
				if b.Column(c).IsNull(r) {
					row[c] = "NULL"
				} else {
					row[c] = b.Column(c).ValueStr(r)
				}
			}
			rows = append(rows, row)
		}
	}
	return cols, rows
}

func col(t *testing.T, cols []string, name string) int {
	t.Helper()
	for i, c := range cols {
		if c == name {
			return i
		}
	}
	t.Fatalf("no column %q in %v", name, cols)
	return -1
}

func catalogFn(def *resDef) vgi.TableFunction { return newTable(&tableFunc{def: def}) }

// ---------------------------------------------------------------- scans

func TestPodsScanPagesAndMapsTypes(t *testing.T) {
	f, secrets := newFake(t)
	cols, rows := run(t, catalogFn(podsDef), secrets)
	if len(rows) != 7 {
		t.Fatalf("got %d pods, want 7 (4 pages of 2)", len(rows))
	}
	reqs := f.requestLog()
	if len(reqs) != 4 || !strings.Contains(reqs[1], "continue=2") || !strings.Contains(reqs[0], "limit=500") {
		t.Fatalf("unexpected requests %v", reqs)
	}
	byName := map[string][]string{}
	for _, r := range rows {
		byName[r[col(t, cols, "name")]] = r
	}
	crashy := byName["crashy"]
	if crashy == nil {
		t.Fatal("no crashy pod")
	}
	if crashy[col(t, cols, "priority_class_name")] != "vgi-fixture-high" || crashy[col(t, cols, "cpu_request_millicores")] != "5" ||
		crashy[col(t, cols, "memory_request_bytes")] != "8388608" || crashy[col(t, cols, "ready")] != "false" {
		t.Fatalf("crashy row: %v", crashy)
	}
	if !strings.Contains(crashy[col(t, cols, "containers")], "app") {
		t.Fatalf("containers: %s", crashy[col(t, cols, "containers")])
	}
	webs := 0
	for name, r := range byName {
		if strings.HasPrefix(name, "web-") {
			webs++
			if r[col(t, cols, "cpu_request_millicores")] != "60" || r[col(t, cols, "memory_limit_bytes")] != "67108864" ||
				r[col(t, cols, "controller_kind")] != "ReplicaSet" {
				t.Fatalf("web row: %v", r)
			}
		}
	}
	if webs != 2 {
		t.Fatalf("want 2 web pods, got %d", webs)
	}
}

func TestRetriesRateLimit(t *testing.T) {
	f, secrets := newFake(t)
	f.fail429 = true
	_, rows := run(t, catalogFn(apiServersDef), secrets)
	if len(rows) != 1 || !strings.HasPrefix(rows[0][0], "v1.") {
		t.Fatalf("rows %v", rows)
	}
	if n := len(f.requestLog()); n != 2 {
		t.Fatalf("want a retry after 429, got %d requests", n)
	}
}

func TestUnauthorizedIsAnError(t *testing.T) {
	_, secrets := newFake(t)
	secrets["k8s"]["token"] = "wrong"
	_, err := vgitest.RunTable(catalogFn(apiServersDef), vgitest.Call{Secrets: secrets})
	if err == nil || !strings.Contains(err.Error(), "401") {
		t.Fatalf("want a 401 error, got %v", err)
	}
}

func TestNoCredentialsNeverReadsLocalKubeconfig(t *testing.T) {
	dir := t.TempDir()
	kc := filepath.Join(dir, "config")
	if err := os.WriteFile(kc, []byte(testKubeconfig("https://127.0.0.1:1")), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("KUBECONFIG", kc)
	_, err := vgitest.RunTable(catalogFn(apiServersDef), vgitest.Call{})
	if err == nil || !strings.Contains(err.Error(), "no Kubernetes credentials") {
		t.Fatalf("want the no-credentials error, got %v", err)
	}
}

func TestSecretsNeverReturnValues(t *testing.T) {
	_, secrets := newFake(t)
	cols, rows := run(t, catalogFn(secretsDef), secrets)
	if len(rows) != 1 {
		t.Fatalf("rows %v", rows)
	}
	all := strings.Join(rows[0], "|")
	// The recorded Secret holds this password in data and in kubectl's last-applied annotation.
	for _, leak := range []string{"fixture-password", base64.StdEncoding.EncodeToString([]byte("fixture-password-do-not-leak"))} {
		if strings.Contains(all, leak) {
			t.Fatalf("secret value leaked: %s", all)
		}
	}
	if rows[0][col(t, cols, "keys")] != `["password","username"]` {
		t.Fatalf("keys %s", rows[0][col(t, cols, "keys")])
	}
}

func TestEventsAndRoles(t *testing.T) {
	_, secrets := newFake(t)
	cols, rows := run(t, catalogFn(eventsDef), secrets)
	found := false
	for _, r := range rows {
		if r[col(t, cols, "reason")] == "FixtureWarning" {
			found = r[col(t, cols, "count")] == "3" && r[col(t, cols, "involved_name")] == "crashy" &&
				strings.HasPrefix(r[col(t, cols, "last_seen")], "2026-01-01")
		}
	}
	if !found {
		t.Fatal("FixtureWarning event not mapped")
	}
	cols, rows = run(t, catalogFn(rolesDef), secrets)
	if len(rows) != 1 || rows[0][col(t, cols, "name")] != "secret-reader" {
		t.Fatalf("roles %v", rows)
	}
}

func TestRBACRulesFlatten(t *testing.T) {
	_, secrets := newFake(t)
	// The fake serves no clusterroles (404): the scan reports it.
	if _, err := vgitest.RunTable(catalogFn(rbacRulesDef), vgitest.Call{Secrets: secrets}); err == nil || !strings.Contains(err.Error(), "404") {
		t.Fatalf("want a 404 from clusterroles, got %v", err)
	}
	def := *rbacRulesDef
	def.Sources = def.Sources[:1]
	def.schema = nil
	cols, rows := run(t, catalogFn(&def), secrets)
	var got []string
	for _, r := range rows {
		got = append(got, r[col(t, cols, "resource")]+":"+r[col(t, cols, "verb")])
	}
	want := []string{"secrets:get", "secrets:list", "configmaps:get"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestResourcesGeneric(t *testing.T) {
	_, secrets := newFake(t)
	cols, rows := run(t, newTable(newResourcesFunc()), secrets, "fixture.vgi.query.farm/v1", "wdg")
	if len(rows) != 2 {
		t.Fatalf("rows %v", rows)
	}
	if rows[0][col(t, cols, "kind")] != "Widget" || !strings.Contains(rows[0][col(t, cols, "spec")], `"size":3`) {
		t.Fatalf("row %v", rows[0])
	}
	if _, err := vgitest.RunTable(newTable(newResourcesFunc()), vgitest.Call{Secrets: secrets,
		Positional: []any{"fixture.vgi.query.farm/v1", "gadgets"}}); err == nil || !strings.Contains(err.Error(), `no resource "gadgets"`) {
		t.Fatalf("want unknown resource error, got %v", err)
	}
}

func TestPodMetrics(t *testing.T) {
	_, secrets := newFake(t)
	cols, rows := run(t, catalogFn(podMetricsDef), secrets)
	if len(rows) == 0 {
		t.Fatal("no pod metrics rows")
	}
	for _, r := range rows {
		if r[col(t, cols, "memory_usage_bytes")] == "NULL" || r[col(t, cols, "window_seconds")] == "NULL" {
			t.Fatalf("row %v", r)
		}
	}
	if _, err := vgitest.RunTable(catalogFn(nodeMetricsDef), vgitest.Call{Secrets: secrets}); err == nil ||
		!strings.Contains(err.Error(), "metrics.k8s.io/v1beta1 API served") {
		t.Fatalf("want a metrics-server hint, got %v", err)
	}
}

func TestPodLogs(t *testing.T) {
	f, secrets := newFake(t)
	res, err := vgitest.RunTable(vgi.AsTableFunction[LogState](&podLogsFunc{}), vgitest.Call{Secrets: secrets,
		Positional: []any{"vgi-fixture", "hello"}, Named: map[string]any{"container": "hello", "tail_lines": int64(5)}})
	if err != nil {
		t.Fatal(err)
	}
	defer res.Release()
	if res.Rows() != 3 {
		t.Fatalf("got %d lines", res.Rows())
	}
	last := f.requestLog()[len(f.requestLog())-1]
	for _, q := range []string{"timestamps=true", "tailLines=5", "container=hello", "limitBytes=10485760"} {
		if !strings.Contains(last, q) {
			t.Fatalf("log request %s lacks %s", last, q)
		}
	}
	if _, err := vgitest.RunTable(vgi.AsTableFunction[LogState](&podLogsFunc{}), vgitest.Call{Secrets: secrets,
		Positional: []any{"vgi-fixture", "missing"}}); err == nil || !strings.Contains(err.Error(), "404") {
		t.Fatalf("want 404 for a missing pod, got %v", err)
	}
}

// ---------------------------------------------------------------- pure logic

func TestPlanPreds(t *testing.T) {
	podSel := podsDef.fieldSelectors()
	cases := []struct {
		name  string
		preds []pred
		want  plan
	}{
		{"none", nil, plan{}},
		{"namespace eq", []pred{{"namespace", "eq", []string{"a"}}}, plan{Namespaces: []string{"a"}}},
		{"namespace in", []pred{{"namespace", "in", []string{"b", "a"}}}, plan{Namespaces: []string{"a", "b"}}},
		{"namespace intersect empty", []pred{{"namespace", "eq", []string{"a"}}, {"namespace", "eq", []string{"b"}}}, plan{Empty: true}},
		{"namespace ne not pushed", []pred{{"namespace", "ne", []string{"a"}}}, plan{}},
		{"field eq + ne", []pred{{"phase", "eq", []string{"Running"}}, {"node_name", "ne", []string{"n1"}}},
			plan{Field: []string{"spec.nodeName!=n1", "status.phase=Running"}}},
		{"field escape", []pred{{"name", "eq", []string{"a,b=c"}}}, plan{Field: []string{`metadata.name=a\,b\=c`}}},
		{"in one value", []pred{{"phase", "in", []string{"Failed"}}}, plan{Field: []string{"status.phase=Failed"}}},
		{"in many not pushed", []pred{{"phase", "in", []string{"Failed", "Pending"}}}, plan{}},
		{"unmapped column", []pred{{"qos_class", "eq", []string{"BestEffort"}}}, plan{}},
		{"label selector", []pred{{colLabelSelector, "eq", []string{"app in (a,b),!legacy"}}},
			plan{Label: "app in (a,b),!legacy", LabelEcho: ptr("app in (a,b),!legacy")}},
		{"two label selectors", []pred{{colLabelSelector, "eq", []string{"a=1"}}, {colLabelSelector, "eq", []string{"b=2"}}},
			plan{Label: "a=1", LabelEcho: ptr("a=1"), Empty: true}},
		{"raw field selector", []pred{{colFieldSelector, "eq", []string{"spec.schedulerName=x"}}, {"phase", "eq", []string{"Running"}}},
			plan{Field: []string{"spec.schedulerName=x", "status.phase=Running"}, FieldEcho: ptr("spec.schedulerName=x")}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := planPreds(tc.preds, "namespace", podSel)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %+v want %+v", got, tc.want)
			}
		})
	}
}

func ptr(s string) *string { return &s }

func TestQuantityValue(t *testing.T) {
	for in, want := range map[string]float64{"250m": 0.25, "64Mi": 67108864, "1G": 1e9, "1.5Gi": 1610612736, "2": 2} {
		if got, ok := quantityValue(in); !ok || got != want {
			t.Errorf("%s: got %v %v want %v", in, got, ok, want)
		}
	}
	if _, ok := quantityValue("bogus"); ok {
		t.Error("bogus parsed")
	}
}

func testKubeconfig(server string) string {
	return fmt.Sprintf(`apiVersion: v1
kind: Config
clusters:
- name: c1
  cluster: {server: %s, insecure-skip-tls-verify: true}
- name: c2
  cluster: {server: https://other.example:6443}
users:
- name: u1
  user: {token: tok-1}
contexts:
- name: one
  context: {cluster: c1, user: u1}
- name: two
  context: {cluster: c2, user: u1}
current-context: one
`, server)
}

func TestRestConfigFrom(t *testing.T) {
	kc := testKubeconfig("https://10.0.0.1:6443")
	for name, sv := range map[string]secretValues{
		"yaml":   {"kubeconfig": kc},
		"base64": {"kubeconfig": base64.StdEncoding.EncodeToString([]byte(kc))},
	} {
		cfg, _, err := restConfigFrom(sv, true, attachOpts{})
		if err != nil || cfg.Host != "https://10.0.0.1:6443" || cfg.BearerToken != "tok-1" {
			t.Fatalf("%s: %v %+v", name, err, cfg)
		}
	}
	cfg, _, err := restConfigFrom(secretValues{"kubeconfig": kc, "context": "two"}, true, attachOpts{})
	if err != nil || cfg.Host != "https://other.example:6443" {
		t.Fatalf("context two: %v %v", err, cfg)
	}
	cfg, _, err = restConfigFrom(secretValues{"kubeconfig": kc, "context": "two"}, true, attachOpts{Context: "one"})
	if err != nil || cfg.Host != "https://10.0.0.1:6443" {
		t.Fatalf("attach context overrides the secret's: %v %v", err, cfg)
	}
	if _, _, err := restConfigFrom(secretValues{"kubeconfig": kc, "context": "nope"}, true, attachOpts{}); err == nil || !strings.Contains(err.Error(), "nope") {
		t.Fatalf("missing context: %v", err)
	}
	cfg, k1, err := restConfigFrom(secretValues{"server": "https://h:6443", "token": "t", "certificate_authority_data": base64.StdEncoding.EncodeToString([]byte("-----BEGIN CERTIFICATE-----\nx\n-----END CERTIFICATE-----\n"))}, true, attachOpts{})
	if err != nil || cfg.BearerToken != "t" || !strings.HasPrefix(string(cfg.CAData), "-----BEGIN") {
		t.Fatalf("server/token: %v %+v", err, cfg)
	}
	_, k2, _ := restConfigFrom(secretValues{"server": "https://h:6443", "token": "t2"}, true, attachOpts{})
	if k1 == k2 {
		t.Fatal("different tokens must give different connection keys")
	}
	if _, _, err := restConfigFrom(secretValues{"server": "https://h", "client_certificate_data": "x"}, true, attachOpts{}); err == nil {
		t.Fatal("cert without key accepted")
	}
	if _, _, err := restConfigFrom(secretValues{"server": "https://h", "insecure_skip_tls_verify": "maybe"}, true, attachOpts{}); err == nil {
		t.Fatal("bad boolean accepted")
	}
	if _, _, err := restConfigFrom(nil, false, attachOpts{}); err != errNoCredentials {
		t.Fatalf("want errNoCredentials, got %v", err)
	}
	// The local kubeconfig only with the explicit opt-in.
	dir := t.TempDir()
	path := filepath.Join(dir, "config")
	if err := os.WriteFile(path, []byte(kc), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("KUBECONFIG", path)
	cfg, _, err = restConfigFrom(nil, false, attachOpts{UseLocal: true, Context: "two"})
	if err != nil || cfg.Host != "https://other.example:6443" {
		t.Fatalf("use_local_kubeconfig: %v %v", err, cfg)
	}
}

func TestParseLog(t *testing.T) {
	lines := parseLog([]byte("2026-11-15T11:37:57.832974123Z hello world\n2026-11-15T11:37:58Z \nno timestamp here\n"))
	if len(lines) != 3 || lines[0].msg != "hello world" || lines[1].msg != "" || lines[2].ts != nil || lines[2].msg != "no timestamp here" {
		t.Fatalf("%+v", lines)
	}
	if ts := lines[0].ts.(time.Time); ts.Nanosecond() != 832974000 {
		t.Fatalf("timestamp not truncated to microseconds: %v", ts)
	}
}

func TestSanitizeObject(t *testing.T) {
	obj := map[string]any{
		"metadata":   map[string]any{"managedFields": []any{1}, "annotations": map[string]any{lastAppliedAnnotation: `{"stringData":{"p":"s3cret"}}`}},
		"data":       map[string]any{"p": "czNjcmV0"},
		"stringData": map[string]any{"p": "s3cret"},
	}
	sanitizeObject(obj, Source{Version: "v1", Resource: "secrets"}, attachOpts{})
	b, _ := json.Marshal(obj)
	if strings.Contains(string(b), "s3cret") || strings.Contains(string(b), "czNjcmV0") || strings.Contains(string(b), "managedFields") {
		t.Fatalf("not sanitized: %s", b)
	}
	cm := map[string]any{"data": map[string]any{"k": "v"}}
	sanitizeObject(cm, Source{Version: "v1", Resource: "configmaps"}, attachOpts{ConfigMapValues: true})
	if cm["data"].(map[string]any)["k"] != "v" {
		t.Fatal("configmap values hidden despite configmap_values")
	}
}

func TestParseResourceTables(t *testing.T) {
	ts, err := parseResourceTables("stable.example.com/v1/crontabs, v1/pods ,b.io/v1/crontabs")
	if err != nil {
		t.Fatal(err)
	}
	nameTables(ts)
	var names []string
	for _, x := range ts {
		names = append(names, x.Name+"="+x.APIVersion)
	}
	want := []string{"crontabs_stable_example_com=stable.example.com/v1", "pods=v1", "crontabs_b_io=b.io/v1"}
	if !reflect.DeepEqual(names, want) {
		t.Fatalf("got %v", names)
	}
	if _, err := parseResourceTables("crontabs"); err == nil {
		t.Fatal("bare resource accepted")
	}
}

func TestSchemasAndMetadata(t *testing.T) {
	for _, d := range append(catalogTables, resourcesDef) {
		s := d.Schema()
		seen := map[string]bool{}
		for _, f := range s.Fields() {
			if seen[f.Name] {
				t.Errorf("%s: duplicate column %s", d.Name, f.Name)
			}
			seen[f.Name] = true
			if c, _ := f.Metadata.GetValue("comment"); c == "" {
				t.Errorf("%s.%s has no comment", d.Name, f.Name)
			}
		}
		for _, k := range d.Key {
			if !seen[k] {
				t.Errorf("%s: key column %s missing", d.Name, k)
			}
		}
		if d.Docs.DocLLM == "" || d.Docs.DocMD == "" || len(d.Docs.Examples) == 0 {
			t.Errorf("%s: incomplete docs", d.Name)
		}
		if _, ok := s.FieldsByName(colLabelSelector); !ok {
			t.Errorf("%s: no label_selector column", d.Name)
		}
	}
	if w := NewWorker(); w == nil {
		t.Fatal("NewWorker returned nil")
	}
}
