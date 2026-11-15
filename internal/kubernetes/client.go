// Connection to a Kubernetes API server.
//
// Credentials come from a DuckDB secret of type `kubernetes`, either a whole
// kubeconfig (optionally with a context name) or a server URL with a bearer
// token or client certificate:
//
//	CREATE SECRET (TYPE kubernetes, KUBECONFIG '<kubeconfig YAML or base64>', CONTEXT 'prod');
//	CREATE SECRET (TYPE kubernetes, SERVER 'https://10.0.0.1:6443', TOKEN '...',
//	    CERTIFICATE_AUTHORITY_DATA '<PEM or base64 PEM>');
//
// The local ~/.kube/config (or $KUBECONFIG) is read only when the ATTACH sets
// `use_local_kubeconfig true`; it is never read implicitly. The secret is
// resolved on every call and never stored in scan state (which travels to
// DuckDB in HTTP continuation tokens).
//
// The worker only issues GET requests (the get, list and watch verbs).

package kubernetes

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// SecretType is the DuckDB secret type holding the connection settings.
const SecretType = "kubernetes"

// userAgent identifies the worker to the API server.
const userAgent = "vgi-kubernetes-worker/" + Version

// requestTimeout bounds every API request attempt (pod logs get longer).
const requestTimeout = 60 * time.Second

// maxAttempts bounds retries of 429 / 5xx responses.
const maxAttempts = 5

// ATTACH option names.
const (
	optUseLocalKubeconfig = "use_local_kubeconfig"
	optContext            = "context"
	optSecret             = "secret"
	optResourceTables     = "resource_tables"
	optConfigMapValues    = "configmap_values"
	optPageSize           = "page_size"
)

func secretField(name, doc string, redact bool) arrow.Field {
	keys, vals := []string{"vgi_doc"}, []string{doc}
	if redact {
		keys, vals = append(keys, "redact"), append(vals, "true")
	}
	return arrow.Field{Name: name, Type: arrow.BinaryTypes.String, Nullable: true, Metadata: arrow.NewMetadata(keys, vals)}
}

var secretSchemaMetadata = arrow.NewMetadata(
	[]string{"vgi_doc_url"},
	[]string{SourceURL + "#credentials"})

// KubernetesSecretType registers CREATE SECRET (TYPE kubernetes, ...).
var KubernetesSecretType = vgi.SecretTypeSpec{
	Name: SecretType,
	Description: "How to reach a Kubernetes API server: a kubeconfig (with an optional context) or a server URL " +
		"with a bearer token or client certificate. Read-only access (get/list/watch) is all the worker needs.",
	Schema: arrow.NewSchema([]arrow.Field{
		secretField("kubeconfig", "A whole kubeconfig file's contents, as YAML or base64-encoded YAML. Exec and "+
			"auth-provider plugins in it run on the worker's host, as with kubectl.", true),
		secretField("context", "Context in KUBECONFIG to use; default its current-context.", false),
		secretField("server", "API server URL, e.g. https://10.0.0.1:6443. Overrides the kubeconfig's server.", false),
		secretField("token", "Bearer token, e.g. a ServiceAccount token from `kubectl create token`.", true),
		secretField("certificate_authority_data", "PEM (or base64 PEM) CA bundle that signed the API server's "+
			"certificate; default the system roots.", false),
		secretField("client_certificate_data", "PEM (or base64 PEM) client certificate for mutual TLS.", false),
		secretField("client_key_data", "PEM (or base64 PEM) private key of CLIENT_CERTIFICATE_DATA.", true),
		secretField("tls_server_name", "Server name to verify in the API server's certificate, when it differs "+
			"from the SERVER host.", false),
		secretField("insecure_skip_tls_verify", "'true' to skip verifying the server certificate (test clusters only).", false),
	}, &secretSchemaMetadata),
}

// AttachOptionSpecs are the ATTACH options this worker accepts.
var AttachOptionSpecs = []vgi.AttachOptionSpec{
	{Name: optUseLocalKubeconfig, Type: arrow.FixedWidthTypes.Boolean, Description: "Opt in to reading the local " +
		"kubeconfig ($KUBECONFIG, else ~/.kube/config) when no kubernetes secret is set. Off by default: the worker " +
		"never reads local credentials unless this is true."},
	{Name: optContext, Type: arrow.BinaryTypes.String, Description: "Kubeconfig context to use (the local kubeconfig's " +
		"or the secret's KUBECONFIG); overrides the secret's CONTEXT. Default the current-context."},
	{Name: optSecret, Type: arrow.BinaryTypes.String, Description: "Name of the kubernetes secret to use when several " +
		"exist (one ATTACH per cluster); default the unscoped kubernetes secret."},
	{Name: optResourceTables, Type: arrow.BinaryTypes.String, Description: "Comma-separated API resources to expose as " +
		"tables in schema `resources`, each `group/version/resource` (e.g. `stable.example.com/v1/crontabs`, " +
		"`v1/pods`), or `all` to expose every listable resource found by discovery (needs use_local_kubeconfig, " +
		"since secrets are not available at ATTACH time). Each table has the metadata columns plus spec, status and " +
		"object as JSON."},
	{Name: optConfigMapValues, Type: arrow.FixedWidthTypes.Boolean, Description: "true to fill configmaps.data with " +
		"the ConfigMap values; default false, which lists only the keys. Secret values are never returned."},
	{Name: optPageSize, Type: arrow.PrimitiveTypes.Int64, Description: "Objects requested per list page (limit=), 1 to " +
		"5000; default 500. Pages are fetched lazily with the API's continue token."},
}

// conn is a resolved connection. Never stored in scan state.
type conn struct {
	key  string
	cfg  *rest.Config
	http *http.Client
	base string
	opts attachOpts
}

// attachOpts are the ATTACH options a scan needs.
type attachOpts struct {
	UseLocal        bool
	Context         string
	Secret          string
	ConfigMapValues bool
	PageSize        int64
}

func readAttachOpts(o *vgi.AttachOptions) attachOpts {
	out := attachOpts{PageSize: 500}
	if o == nil {
		return out
	}
	if v, ok := o.Bool(optUseLocalKubeconfig); ok {
		out.UseLocal = v
	}
	if v, ok := o.String(optContext); ok {
		out.Context = strings.TrimSpace(v)
	}
	if v, ok := o.String(optSecret); ok {
		out.Secret = strings.TrimSpace(v)
	}
	if v, ok := o.Bool(optConfigMapValues); ok {
		out.ConfigMapValues = v
	}
	if v, ok := o.Int64(optPageSize); ok && v > 0 {
		out.PageSize = min(v, 5000)
	}
	return out
}

// decodeMaybeBase64 returns s, or its base64 decoding when s is base64 and
// does not already look like PEM / YAML.
func decodeMaybeBase64(s string) []byte {
	t := strings.TrimSpace(s)
	if t == "" || strings.Contains(t, "-----BEGIN") || strings.ContainsAny(t, ":\n") {
		return []byte(s)
	}
	clean := strings.Join(strings.Fields(t), "")
	for _, enc := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding} {
		if b, err := enc.DecodeString(clean); err == nil {
			return b
		}
	}
	return []byte(s)
}

// secretValues reads the kubernetes secret's fields ("" when unset).
type secretValues map[string]string

func (s secretValues) get(k string) string { return strings.TrimSpace(s[k]) }

// lookupSecret picks the secret: the ATTACH's named one, else the default.
func lookupSecret(secrets vgi.Secrets, name string) (secretValues, bool, error) {
	var raw map[string]any
	var ok bool
	if name != "" {
		raw, ok = secrets.LookupNamed(SecretType, name)
		if !ok {
			return nil, false, fmt.Errorf("the ATTACH names kubernetes secret %q, but no such secret of TYPE kubernetes is visible", name)
		}
	} else {
		raw, ok = secrets.Lookup(SecretType)
	}
	if !ok {
		return nil, false, nil
	}
	out := secretValues{}
	for k, v := range raw {
		out[strings.ToLower(k)] = vgi.RenderSecretValue(v)
	}
	return out, true, nil
}

// errNoCredentials explains how to connect.
var errNoCredentials = errors.New("no Kubernetes credentials: CREATE SECRET (TYPE kubernetes, KUBECONFIG '...') or " +
	"(TYPE kubernetes, SERVER 'https://...', TOKEN '...', CERTIFICATE_AUTHORITY_DATA '...'), or ATTACH with " +
	"use_local_kubeconfig true to read the local kubeconfig")

// restConfigFrom builds a client config from the secret (or, if opted in, the local kubeconfig).
func restConfigFrom(sv secretValues, haveSecret bool, ao attachOpts) (*rest.Config, string, error) {
	h := sha256.New()
	ctxName := ao.Context
	var cfg *rest.Config
	switch {
	case haveSecret:
		if ctxName == "" {
			ctxName = sv.get("context")
		}
		if kc := sv.get("kubeconfig"); kc != "" {
			raw := decodeMaybeBase64(kc)
			apiCfg, err := clientcmd.Load(raw)
			if err != nil {
				return nil, "", fmt.Errorf("kubernetes secret: KUBECONFIG is not a kubeconfig: %w", err)
			}
			overrides := &clientcmd.ConfigOverrides{CurrentContext: ctxName}
			cc := clientcmd.NewNonInteractiveClientConfig(*apiCfg, ctxName, overrides, nil)
			cfg, err = cc.ClientConfig()
			if err != nil {
				return nil, "", fmt.Errorf("kubernetes secret: KUBECONFIG: %w", err)
			}
			h.Write(raw)
		} else {
			if sv.get("server") == "" {
				return nil, "", errors.New("kubernetes secret: set KUBECONFIG, or SERVER with TOKEN or CLIENT_CERTIFICATE_DATA")
			}
			cfg = &rest.Config{}
		}
		if s := sv.get("server"); s != "" {
			cfg.Host = s
		}
		if t := sv.get("token"); t != "" {
			cfg.BearerToken = t
			cfg.BearerTokenFile = ""
		}
		if ca := sv.get("certificate_authority_data"); ca != "" {
			cfg.CAData = decodeMaybeBase64(ca)
			cfg.CAFile = ""
		}
		if cc := sv.get("client_certificate_data"); cc != "" {
			cfg.CertData = decodeMaybeBase64(cc)
			cfg.CertFile = ""
		}
		if ck := sv.get("client_key_data"); ck != "" {
			cfg.KeyData = decodeMaybeBase64(ck)
			cfg.KeyFile = ""
		}
		if (len(cfg.CertData) == 0) != (len(cfg.KeyData) == 0) && sv.get("kubeconfig") == "" {
			return nil, "", errors.New("kubernetes secret: CLIENT_CERTIFICATE_DATA and CLIENT_KEY_DATA must be set together")
		}
		if n := sv.get("tls_server_name"); n != "" {
			cfg.ServerName = n
		}
		switch strings.ToLower(sv.get("insecure_skip_tls_verify")) {
		case "true", "1", "yes", "on":
			cfg.Insecure = true
			cfg.CAData, cfg.CAFile = nil, ""
		case "", "false", "0", "no", "off":
		default:
			return nil, "", fmt.Errorf("kubernetes secret: INSECURE_SKIP_TLS_VERIFY must be 'true' or 'false', got %q", sv.get("insecure_skip_tls_verify"))
		}
		for _, k := range []string{"server", "token", "certificate_authority_data", "client_certificate_data",
			"client_key_data", "tls_server_name", "insecure_skip_tls_verify"} {
			h.Write([]byte("\x00" + sv.get(k)))
		}
	case ao.UseLocal:
		rules := clientcmd.NewDefaultClientConfigLoadingRules()
		overrides := &clientcmd.ConfigOverrides{CurrentContext: ctxName}
		var err error
		cfg, err = clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, overrides).ClientConfig()
		if err != nil {
			return nil, "", fmt.Errorf("use_local_kubeconfig: %w", err)
		}
		h.Write([]byte("local\x00" + strings.Join(rules.Precedence, ":")))
	default:
		return nil, "", errNoCredentials
	}
	if cfg.Host == "" {
		return nil, "", errors.New("kubernetes: no API server URL (set SERVER, or a kubeconfig cluster server)")
	}
	h.Write([]byte("\x00" + ctxName + "\x00" + cfg.Host))
	cfg.UserAgent = userAgent
	cfg.QPS, cfg.Burst = 50, 100
	return cfg, hex.EncodeToString(h.Sum(nil)), nil
}

var (
	connMu sync.Mutex
	conns  = map[string]*conn{}
)

// connect resolves the call's credentials into a (cached) connection.
func connect(secrets vgi.Secrets, o *vgi.AttachOptions) (*conn, error) {
	ao := readAttachOpts(o)
	sv, have, err := lookupSecret(secrets, ao.Secret)
	if err != nil {
		return nil, err
	}
	cfg, key, err := restConfigFrom(sv, have, ao)
	if err != nil {
		return nil, err
	}
	return connFor(cfg, key, ao)
}

// connFor returns the cached connection for a resolved config.
func connFor(cfg *rest.Config, key string, ao attachOpts) (*conn, error) {
	connMu.Lock()
	defer connMu.Unlock()
	if c, ok := conns[key]; ok {
		return &conn{key: c.key, cfg: c.cfg, http: c.http, base: c.base, opts: ao}, nil
	}
	hc, err := rest.HTTPClientFor(cfg)
	if err != nil {
		return nil, fmt.Errorf("kubernetes: building the HTTP client: %w", err)
	}
	c := &conn{key: key, cfg: cfg, http: hc, base: strings.TrimRight(cfg.Host, "/"), opts: ao}
	if !strings.HasPrefix(c.base, "http://") && !strings.HasPrefix(c.base, "https://") {
		c.base = "https://" + c.base
	}
	conns[key] = c
	return &conn{key: c.key, cfg: c.cfg, http: c.http, base: c.base, opts: ao}, nil
}

// apiStatus is the Kubernetes Status object returned with errors.
type apiStatus struct {
	Kind    string `json:"kind"`
	Message string `json:"message"`
	Reason  string `json:"reason"`
	Code    int    `json:"code"`
}

// APIError is a non-2xx response from the API server.
type APIError struct {
	Path    string
	Code    int
	Reason  string
	Message string
}

func (e *APIError) Error() string {
	msg := e.Message
	if msg == "" {
		msg = http.StatusText(e.Code)
	}
	return fmt.Sprintf("kubernetes GET %s: %d %s: %s", e.Path, e.Code, e.Reason, msg)
}

// isStatus reports whether err is an APIError with one of the codes.
func isStatus(err error, codes ...int) bool {
	var ae *APIError
	if !errors.As(err, &ae) {
		return false
	}
	for _, c := range codes {
		if ae.Code == c {
			return true
		}
	}
	return false
}

// sleep is replaced in tests so retries don't wait.
var sleep = func(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

// get issues a GET for path?query and returns the body. 429 and 5xx are
// retried with Retry-After or exponential backoff.
func (c *conn) get(ctx context.Context, path string, q url.Values) ([]byte, error) {
	return c.getWithTimeout(ctx, path, q, requestTimeout)
}

func (c *conn) getWithTimeout(ctx context.Context, path string, q url.Values, timeout time.Duration) ([]byte, error) {
	u := c.base + path
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	for attempt := 1; ; attempt++ {
		body, retryAfter, err := c.once(ctx, u, path, timeout)
		if err == nil {
			return body, nil
		}
		retryable := retryAfter >= 0
		if !retryable || attempt >= maxAttempts || ctx.Err() != nil {
			return nil, err
		}
		wait := time.Duration(250<<attempt) * time.Millisecond
		if retryAfter > 0 {
			wait = retryAfter
		}
		sleep(ctx, min(wait, 30*time.Second))
	}
}

// once performs one attempt. retryAfter < 0 means not retryable.
func (c *conn) once(ctx context.Context, u, path string, timeout time.Duration) ([]byte, time.Duration, error) {
	actx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(actx, http.MethodGet, u, nil)
	if err != nil {
		return nil, -1, err
	}
	req.Header.Set("Accept", "application/json")
	slog.Debug("kubernetes GET", "url", strings.TrimPrefix(u, c.base))
	resp, err := c.http.Do(req)
	if err != nil {
		// Connection-level failures are retried.
		return nil, 0, fmt.Errorf("kubernetes GET %s: %w", path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, fmt.Errorf("kubernetes GET %s: reading response: %w", path, err)
	}
	if resp.StatusCode/100 == 2 {
		return body, -1, nil
	}
	ae := &APIError{Path: path, Code: resp.StatusCode, Reason: http.StatusText(resp.StatusCode)}
	var st apiStatus
	if json.Unmarshal(body, &st) == nil && st.Kind == "Status" {
		ae.Message = st.Message
		if st.Reason != "" {
			ae.Reason = st.Reason
		}
	} else if len(body) > 0 && len(body) < 1024 {
		ae.Message = strings.TrimSpace(string(body))
	}
	retry := time.Duration(-1)
	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
		retry = 0
		if s := resp.Header.Get("Retry-After"); s != "" {
			if n, err := strconv.Atoi(s); err == nil && n >= 0 {
				retry = time.Duration(n) * time.Second
			}
		}
	}
	return nil, retry, ae
}
