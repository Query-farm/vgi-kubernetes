// metrics.k8s.io resource usage (metrics-server): node_metrics and pod_metrics.

package kubernetes

import (
	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// usage is one metrics.k8s.io usage sample (NodeMetrics / ContainerMetrics).
type usage = corev1.ResourceList

type nodeMetrics struct {
	Metadata  metav1.ObjectMeta `json:"metadata"`
	Timestamp metav1.Time       `json:"timestamp"`
	Window    metav1.Duration   `json:"window"`
	Usage     usage             `json:"usage"`
}

type podMetrics struct {
	Metadata   metav1.ObjectMeta `json:"metadata"`
	Timestamp  metav1.Time       `json:"timestamp"`
	Window     metav1.Duration   `json:"window"`
	Containers []struct {
		Name  string `json:"name"`
		Usage usage  `json:"usage"`
	} `json:"containers"`
}

// cpuUsageMilli is CPU usage in millicores with sub-millicore precision.
func cpuUsageMilli(u usage) any {
	q, ok := u[corev1.ResourceCPU]
	if !ok {
		return nil
	}
	return float64(q.ScaledValue(-9)) / 1e6
}

const metricsNote = " Needs metrics-server (the metrics.k8s.io API); without it the query fails with a 404 naming the API."

var nodeMetricsDef = &resDef{
	Name: "node_metrics", Key: []string{"node_name"},
	Sources: groupSrc("metrics.k8s.io", "v1beta1", "nodes", "NodeMetrics", false),
	Docs: fnDocs{Title: "Node Metrics", Category: "observability",
		Keywords:    "kubernetes, metrics-server, node usage, cpu usage, memory usage, kubectl top nodes",
		Description: "Current CPU and memory usage per node from metrics.k8s.io (kubectl top nodes)",
		Examples: []vgi.CatalogExample{
			ex("Node CPU and memory utilisation against allocatable", "SELECT n.name, m.cpu_usage_millicores / n.allocatable_cpu_millicores AS cpu_util, m.memory_usage_bytes / n.allocatable_memory_bytes AS mem_util FROM kubernetes.main.node_metrics m JOIN kubernetes.main.nodes n ON n.name = m.node_name ORDER BY cpu_util DESC"),
		}},
	Cols: []arrow.Field{
		column("node_name", tString, "Node the sample is for (joins nodes.name).", false),
		column("timestamp", tTS, "When the sample was taken.", true),
		column("window_seconds", tFloat64, "Sampling window in seconds.", true),
		column("cpu_usage_millicores", tFloat64, "CPU in use, in millicores.", true),
		column("memory_usage_bytes", tInt64, "Working-set memory in bytes.", true),
		column("labels", tStrMap, "Labels of the node.", false),
	},
	FieldSelectors: map[string]string{"node_name": "metadata.name"},
	Row: typed(func(o *nodeMetrics, _ *rowCtx) [][]any {
		return one(o.Metadata.Name, tsv(o.Timestamp), o.Window.Seconds(), cpuUsageMilli(o.Usage), bytesOf(o.Usage, corev1.ResourceMemory),
			strMap(o.Metadata.Labels))
	}),
}

var podMetricsDef = &resDef{
	Name: "pod_metrics", NamespaceCol: "namespace", Key: []string{"namespace", "pod_name", "container_name"},
	Sources: groupSrc("metrics.k8s.io", "v1beta1", "pods", "PodMetrics", true),
	Docs: fnDocs{Title: "Pod Metrics", Category: "observability",
		Keywords:    "kubernetes, metrics-server, container usage, cpu usage, memory usage, kubectl top pods",
		Description: "Current CPU and memory usage per container from metrics.k8s.io (kubectl top pods --containers)",
		Examples: []vgi.CatalogExample{
			ex("Containers using more memory than they request", "SELECT m.namespace, m.pod_name, m.container_name, m.memory_usage_bytes, c.memory_request_bytes FROM kubernetes.main.pod_metrics m JOIN kubernetes.main.pod_containers c USING (namespace, pod_name, container_name) WHERE m.memory_usage_bytes > c.memory_request_bytes ORDER BY m.memory_usage_bytes DESC"),
			ex("Top CPU consumers per namespace", "SELECT namespace, sum(cpu_usage_millicores) AS millicores FROM kubernetes.main.pod_metrics GROUP BY namespace ORDER BY millicores DESC"),
		}},
	Cols: []arrow.Field{
		column("namespace", tString, "Namespace of the pod.", false),
		column("pod_name", tString, "Pod the sample is for (joins pods.name).", false),
		column("container_name", tString, "Container within the pod (joins pod_containers.container_name).", false),
		column("timestamp", tTS, "When the sample was taken.", true),
		column("window_seconds", tFloat64, "Sampling window in seconds.", true),
		column("cpu_usage_millicores", tFloat64, "CPU in use, in millicores.", true),
		column("memory_usage_bytes", tInt64, "Working-set memory in bytes.", true),
		column("labels", tStrMap, "Labels of the pod.", false),
	},
	FieldSelectors: map[string]string{"pod_name": "metadata.name"},
	Row: typed(func(o *podMetrics, _ *rowCtx) [][]any {
		var rows [][]any
		for _, c := range o.Containers {
			rows = append(rows, []any{o.Metadata.Namespace, o.Metadata.Name, c.Name, tsv(o.Timestamp), o.Window.Seconds(),
				cpuUsageMilli(c.Usage), bytesOf(c.Usage, corev1.ResourceMemory), strMap(o.Metadata.Labels)})
		}
		return rows
	}),
}
