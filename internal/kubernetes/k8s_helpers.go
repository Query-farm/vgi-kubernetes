// Helpers shared by the resource row mappers: quantities, containers,
// conditions and selectors.

package kubernetes

import (
	"encoding/json"
	"sort"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// cpuMilli is a CPU quantity of rl in millicores (nil when absent).
func cpuMilli(rl corev1.ResourceList) any {
	q, ok := rl[corev1.ResourceCPU]
	if !ok {
		return nil
	}
	return q.MilliValue()
}

// bytesOf is a byte quantity (memory, storage) of rl (nil when absent).
func bytesOf(rl corev1.ResourceList, name corev1.ResourceName) any {
	q, ok := rl[name]
	if !ok {
		return nil
	}
	return q.Value()
}

// countOf is an integer quantity (pods) of rl.
func countOf(rl corev1.ResourceList, name corev1.ResourceName) any {
	q, ok := rl[name]
	if !ok {
		return nil
	}
	return q.Value()
}

// quantityMap renders a ResourceList as MAP(VARCHAR, VARCHAR) of canonical strings.
func quantityMap(rl corev1.ResourceList) any {
	out := make(map[string]string, len(rl))
	for k, q := range rl {
		out[string(k)] = q.String()
	}
	return out
}

// containerSpecType is a container of a pod template (no runtime status).
var containerSpecType = structOf(
	sf("name", tString), sf("kind", tString), sf("image", tString),
	sf("cpu_request_millicores", tInt64), sf("cpu_limit_millicores", tInt64),
	sf("memory_request_bytes", tInt64), sf("memory_limit_bytes", tInt64),
	sf("ports", listOf(tInt64)), sf("command", tStrList),
)

const containerSpecDoc = "Containers of the pod template: name, kind (init, sidecar or app), image, " +
	"cpu_request_millicores, cpu_limit_millicores, memory_request_bytes, memory_limit_bytes, ports (container " +
	"ports), command."

func containerSpecs(ps *corev1.PodSpec) []any {
	var out []any
	add := func(c *corev1.Container, kind string) {
		ports := make([]any, 0, len(c.Ports))
		for _, p := range c.Ports {
			ports = append(ports, int64(p.ContainerPort))
		}
		out = append(out, structVal{c.Name, kind, c.Image,
			cpuMilli(c.Resources.Requests), cpuMilli(c.Resources.Limits),
			bytesOf(c.Resources.Requests, corev1.ResourceMemory), bytesOf(c.Resources.Limits, corev1.ResourceMemory),
			ports, strList(c.Command)})
	}
	for i := range ps.InitContainers {
		kind := "init"
		if ps.InitContainers[i].RestartPolicy != nil && *ps.InitContainers[i].RestartPolicy == corev1.ContainerRestartPolicyAlways {
			kind = "sidecar"
		}
		add(&ps.InitContainers[i], kind)
	}
	for i := range ps.Containers {
		add(&ps.Containers[i], "app")
	}
	return out
}

// images lists the distinct images of a pod spec, init containers first.
func images(ps *corev1.PodSpec) []any {
	seen := map[string]bool{}
	var out []any
	for _, cs := range [][]corev1.Container{ps.InitContainers, ps.Containers} {
		for _, c := range cs {
			if !seen[c.Image] {
				seen[c.Image] = true
				out = append(out, c.Image)
			}
		}
	}
	if out == nil {
		out = []any{}
	}
	return out
}

// conditionType is the common shape of status conditions.
var conditionType = listOf(structOf(sf("type", tString), sf("status", tString), sf("reason", tString),
	sf("message", tString), sf("last_transition_time", tTS)))

const conditionsDoc = "Status conditions: type, status (True/False/Unknown), reason, message, last_transition_time."

func cond(typ, status, reason, message string, t metav1.Time) structVal {
	return structVal{typ, status, nilIfEmpty(reason), nilIfEmpty(message), tsv(t)}
}

// conditionTrue reports a condition's status as BOOLEAN (nil when absent).
func conditionTrue(statuses map[string]string, typ string) any {
	s, ok := statuses[typ]
	if !ok {
		return nil
	}
	return s == "True"
}

// matchLabels returns a label selector's matchLabels as a MAP (nil selector -> NULL).
func matchLabels(s *metav1.LabelSelector) any {
	if s == nil {
		return nil
	}
	return strMap(s.MatchLabels)
}

// selectorString renders a label selector in kubectl syntax (nil -> NULL).
func selectorString(s *metav1.LabelSelector) any {
	if s == nil {
		return nil
	}
	sel, err := metav1.LabelSelectorAsSelector(s)
	if err != nil {
		return nil
	}
	return sel.String()
}

// sortedKeys returns the keys of maps, sorted, as a LIST value.
func sortedKeys[V any](ms ...map[string]V) []any {
	var keys []string
	for _, m := range ms {
		for k := range m {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return strList(keys)
}

// controllerOf returns the controlling owner (kind, name) of an object.
func controllerOf(m *metav1.ObjectMeta) (any, any) {
	for _, o := range m.OwnerReferences {
		if o.Controller != nil && *o.Controller {
			return o.Kind, o.Name
		}
	}
	return nil, nil
}

// nodeRoles derives node roles from node-role.kubernetes.io/<role> labels.
func nodeRoles(labels map[string]string) []any {
	var roles []string
	for k := range labels {
		if r, ok := strings.CutPrefix(k, "node-role.kubernetes.io/"); ok && r != "" {
			roles = append(roles, r)
		}
	}
	if v := labels["kubernetes.io/role"]; v != "" {
		roles = append(roles, v)
	}
	sort.Strings(roles)
	return strList(roles)
}

// jsonUnmarshal decodes an API object.
func jsonUnmarshal(raw []byte, v any) error { return json.Unmarshal(raw, v) }

// strconvItoa formats an integer.
func strconvItoa(n int64) string { return strconv.FormatInt(n, 10) }
