// pod_logs(namespace, pod, ...): container log lines with timestamps.
//
// The log of each container is read with GET .../pods/<pod>/log?timestamps=true
// (bounded by limit_bytes) and emitted in batches. The cursor (container
// index, line offset) lives in exported LogState fields; the fetched log is
// kept in memory for the scan and fetched again (skipping the lines already
// emitted) if a later batch is served by another worker process.

package kubernetes

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	corev1 "k8s.io/api/core/v1"
)

// logBatchLines bounds the lines per emitted batch.
const logBatchLines = 8192

// logTimeout bounds one log read.
const logTimeout = 3 * time.Minute

type podLogsArgs struct {
	Namespace    string     `vgi:"pos=0,doc=Namespace of the pod."`
	Pod          string     `vgi:"pos=1,doc=Pod name."`
	Container    *string    `vgi:"name=container,doc=Container name; default every app container of the pod (init containers must be named)."`
	Previous     bool       `vgi:"name=previous,default=false,doc=true to read the previous (crashed or restarted) instance of the container, like kubectl logs --previous."`
	SinceSeconds *int64     `vgi:"name=since_seconds,ge=1,doc=Only lines from the last N seconds."`
	SinceTime    *time.Time `vgi:"name=since_time,doc=Only lines at or after this time."`
	TailLines    *int64     `vgi:"name=tail_lines,ge=0,doc=Only the last N lines of each container."`
	LimitBytes   int64      `vgi:"name=limit_bytes,default=10485760,ge=1,doc=Maximum bytes read per container (default 10 MiB); the log is cut at this size."`
}

var podLogsSchema = arrow.NewSchema([]arrow.Field{
	column("namespace", tString, "Namespace of the pod.", false),
	column("pod", tString, "Pod name.", false),
	column("container", tString, "Container name.", false),
	column("line_number", tInt64, "1-based line number within this container's returned log.", false),
	column("timestamp", tTS, "Time the container runtime recorded the line.", true),
	column("message", tString, "The log line, without the timestamp prefix.", false),
}, nil)

var podLogsDocs = fnDocs{Title: "Kubernetes Pod Logs", Category: "observability",
	Keywords:    "kubernetes, logs, kubectl logs, container logs, previous logs, crash logs, troubleshooting",
	Description: "Log lines of a pod's containers, with the runtime's timestamps (kubectl logs --timestamps)",
	DocLLM: "pod_logs(namespace, pod, container := NULL, previous := false, since_seconds := NULL, since_time := NULL, " +
		"tail_lines := NULL, limit_bytes := 10485760) returns one row per log line: namespace, pod, container, " +
		"line_number (1-based per container), timestamp (`TIMESTAMPTZ` recorded by the container runtime), message. " +
		"With container NULL every app container of the pod is read (name init containers explicitly). previous := true " +
		"reads the last terminated instance, which is what explains a CrashLoopBackOff; containers without one are " +
		"skipped. Use tail_lines or since_seconds to keep reads small; at most limit_bytes are read per container. " +
		"Filter the lines with WHERE message LIKE / regexp_matches; those filters run in DuckDB after the read.",
	DocMD: "## Pod Logs\n\n`pod_logs(namespace, pod)` reads container logs like `kubectl logs --timestamps`: one row per " +
		"line with the container, a line number, the runtime timestamp and the message.\n\n" +
		"Named arguments: `container`, `previous` (the crashed instance), `since_seconds`, `since_time`, `tail_lines` " +
		"and `limit_bytes` (default 10 MiB per container).",
	Examples: []vgi.CatalogExample{
		ex("Last 50 lines of a pod's logs", "SELECT timestamp, container, message FROM kubernetes.main.pod_logs('default', 'web-0', tail_lines := 50) ORDER BY timestamp"),
		ex("Error lines from the crashed instance of a container", "SELECT timestamp, message FROM kubernetes.main.pod_logs('default', 'worker-1', container := 'app', previous := true) WHERE message ILIKE '%error%' ORDER BY line_number"),
	},
}

// LogState is the resumable cursor of a pod_logs scan.
type LogState struct {
	Started    bool
	Done       bool
	ScanID     string
	Namespace  string
	Pod        string
	Containers []string
	Explicit   bool
	CIdx       int
	LineOffset int64
	Query      string
}

type logLine struct {
	ts  any
	msg string
}

var (
	logCacheMu sync.Mutex
	logCache   = map[string][]logLine{}
)

type podLogsFunc struct{}

var _ vgi.TypedTableFunc[LogState] = (*podLogsFunc)(nil)

func (*podLogsFunc) Name() string { return "pod_logs" }

func (*podLogsFunc) Metadata() vgi.FunctionMetadata {
	tags := objectTags(podLogsDocs)
	tags["vgi.result_columns_schema"] = mustJSON(schemaColumns(podLogsSchema))
	return vgi.FunctionMetadata{
		Description:     podLogsDocs.Description,
		Stability:       vgi.StabilityVolatile,
		RequiredSecrets: []vgi.SecretRequirement{{SecretType: SecretType}},
		Tags:            tags,
		Examples:        podLogsDocs.Examples,
	}
}

func (*podLogsFunc) ArgumentSpecs() []vgi.ArgSpec { return vgi.DeriveArgSpecs(podLogsArgs{}) }

func (*podLogsFunc) OnBind(p *vgi.BindParams) (*vgi.BindResponse, error) {
	return bindWithSecret(p, podLogsSchema)
}

func (*podLogsFunc) NewState(*vgi.ProcessParams) (*LogState, error) { return &LogState{}, nil }

func newScanID() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// logQuery builds the log request's query string.
func logQuery(a *podLogsArgs) url.Values {
	q := url.Values{}
	q.Set("timestamps", "true")
	if a.Previous {
		q.Set("previous", "true")
	}
	if a.SinceSeconds != nil {
		q.Set("sinceSeconds", strconv.FormatInt(*a.SinceSeconds, 10))
	}
	if a.SinceTime != nil {
		q.Set("sinceTime", a.SinceTime.UTC().Format(time.RFC3339))
	}
	if a.TailLines != nil {
		q.Set("tailLines", strconv.FormatInt(*a.TailLines, 10))
	}
	q.Set("limitBytes", strconv.FormatInt(a.LimitBytes, 10))
	return q
}

func (*podLogsFunc) Process(ctx context.Context, p *vgi.ProcessParams, st *LogState, out *vgirpc.OutputCollector) error {
	if st.Done {
		return out.Finish()
	}
	c, err := connect(p.Secrets, p.AttachOptions())
	if err != nil {
		return err
	}
	if !st.Started {
		var a podLogsArgs
		if err := vgi.BindArgs(p.Args, &a); err != nil {
			return err
		}
		if a.SinceSeconds != nil && a.SinceTime != nil {
			return fmt.Errorf("pod_logs: set since_seconds or since_time, not both")
		}
		st.Namespace, st.Pod, st.ScanID = a.Namespace, a.Pod, newScanID()
		st.Query = logQuery(&a).Encode()
		if a.Container != nil && *a.Container != "" {
			st.Containers, st.Explicit = []string{*a.Container}, true
		} else {
			body, err := c.get(ctx, "/api/v1/namespaces/"+url.PathEscape(a.Namespace)+"/pods/"+url.PathEscape(a.Pod), nil)
			if err != nil {
				return err
			}
			var pod corev1.Pod
			if err := json.Unmarshal(body, &pod); err != nil {
				return fmt.Errorf("decoding pod %s/%s: %w", a.Namespace, a.Pod, err)
			}
			for _, ct := range pod.Spec.Containers {
				st.Containers = append(st.Containers, ct.Name)
			}
		}
		st.Started = true
	}
	for st.CIdx < len(st.Containers) {
		name := st.Containers[st.CIdx]
		lines, err := containerLog(ctx, c, st, name)
		if err != nil {
			return err
		}
		if st.LineOffset >= int64(len(lines)) {
			forgetLog(st, name)
			st.CIdx++
			st.LineOffset = 0
			continue
		}
		end := min(st.LineOffset+logBatchLines, int64(len(lines)))
		rows := make([][]any, 0, end-st.LineOffset)
		for i := st.LineOffset; i < end; i++ {
			rows = append(rows, []any{st.Namespace, st.Pod, name, i + 1, lines[i].ts, lines[i].msg})
		}
		st.LineOffset = end
		batch, err := buildBatch(p.OutputSchema, podLogsSchema, rows)
		if err != nil {
			return err
		}
		return out.Emit(batch)
	}
	st.Done = true
	return out.Finish()
}

func forgetLog(st *LogState, container string) {
	logCacheMu.Lock()
	delete(logCache, st.ScanID+"/"+container)
	logCacheMu.Unlock()
}

// containerLog returns a container's parsed log, from the scan cache or the API.
func containerLog(ctx context.Context, c *conn, st *LogState, container string) ([]logLine, error) {
	key := st.ScanID + "/" + container
	logCacheMu.Lock()
	lines, ok := logCache[key]
	logCacheMu.Unlock()
	if ok {
		return lines, nil
	}
	q, _ := url.ParseQuery(st.Query)
	q.Set("container", container)
	path := "/api/v1/namespaces/" + url.PathEscape(st.Namespace) + "/pods/" + url.PathEscape(st.Pod) + "/log"
	body, err := c.getWithTimeout(ctx, path, q, logTimeout)
	if err != nil {
		if !st.Explicit && q.Get("previous") == "true" && isStatus(err, 400) {
			body = nil // this container has no previous instance; skip it
		} else {
			return nil, err
		}
	}
	lines = parseLog(body)
	logCacheMu.Lock()
	logCache[key] = lines
	logCacheMu.Unlock()
	return lines, nil
}

// parseLog splits a timestamps=true log into lines.
func parseLog(body []byte) []logLine {
	var out []logLine
	sc := bufio.NewScanner(bytes.NewReader(body))
	sc.Buffer(make([]byte, 64*1024), 16*1024*1024)
	for sc.Scan() {
		line := sc.Text()
		var t any
		if i := strings.IndexByte(line, ' '); i > 0 {
			if pt, err := time.Parse(time.RFC3339Nano, line[:i]); err == nil {
				t, line = pt.UTC().Truncate(time.Microsecond), line[i+1:]
			}
		} else if pt, err := time.Parse(time.RFC3339Nano, line); err == nil {
			t, line = pt.UTC().Truncate(time.Microsecond), ""
		}
		out = append(out, logLine{ts: t, msg: line})
	}
	return out
}
