// Core (v1) resources: namespaces, nodes, pods, pod_containers, services,
// configmaps, secrets, service accounts, persistent volumes and claims, events.

package kubernetes

import (
	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"

	corev1 "k8s.io/api/core/v1"
)

func coreSrc(resource, kind string, namespaced bool) []Source {
	return []Source{{Version: "v1", Resource: resource, Kind: kind, Namespaced: namespaced}}
}

// ---------------------------------------------------------------- namespaces

var namespacesDef = &resDef{
	Name: "namespaces", Meta: true, Key: []string{"uid"},
	Sources: coreSrc("namespaces", "Namespace", false),
	Docs: fnDocs{Title: "Namespaces", Category: "cluster",
		Keywords:    "kubernetes, namespaces, tenancy, pod security admission, labels",
		Description: "Namespaces of the cluster, one row each",
		Examples: []vgi.CatalogExample{
			ex("Namespaces with their pod security enforcement level", "SELECT name, phase, labels['pod-security.kubernetes.io/enforce'] AS enforce FROM kubernetes.main.namespaces ORDER BY name"),
		}},
	Cols: []arrow.Field{
		column("phase", tString, "Active or Terminating.", true),
	},
	FieldSelectors: map[string]string{"phase": "status.phase"},
	Row: typed(func(o *corev1.Namespace, _ *rowCtx) [][]any {
		return one(nilIfEmpty(string(o.Status.Phase)))
	}),
}

// ---------------------------------------------------------------- nodes

var nodeAddressType = listOf(structOf(sf("type", tString), sf("address", tString)))
var taintType = listOf(structOf(sf("key", tString), sf("value", tString), sf("effect", tString), sf("time_added", tTS)))

var nodesDef = &resDef{
	Name: "nodes", Meta: true, Key: []string{"uid"},
	Sources: coreSrc("nodes", "Node", false),
	Docs: fnDocs{Title: "Nodes", Category: "cluster",
		Keywords:    "kubernetes, nodes, capacity, allocatable, taints, kubelet, node conditions, cordon",
		Description: "Cluster nodes with capacity, allocatable resources, conditions, taints and versions",
		Examples: []vgi.CatalogExample{
			ex("Allocatable CPU cores and memory GiB per ready node", "SELECT name, allocatable_cpu_millicores / 1000.0 AS cpu_cores, allocatable_memory_bytes / 2^30 AS memory_gib FROM kubernetes.main.nodes WHERE ready ORDER BY name"),
			ex("Nodes with a NoSchedule taint", "SELECT name, t.key, t.effect FROM kubernetes.main.nodes, UNNEST(taints) AS u(t) WHERE t.effect = 'NoSchedule' ORDER BY name"),
		}},
	Cols: []arrow.Field{
		column("ready", tBool, "Whether the Ready condition is True.", true),
		column("unschedulable", tBool, "Whether the node is cordoned (spec.unschedulable).", false),
		column("roles", tStrList, "Roles from node-role.kubernetes.io/<role> labels, e.g. control-plane.", false),
		column("internal_ip", tString, "First InternalIP address.", true),
		column("external_ip", tString, "First ExternalIP address.", true),
		column("addresses", nodeAddressType, "All addresses: type (InternalIP, ExternalIP, Hostname, ...) and address.", false),
		column("pod_cidr", tString, "Pod CIDR assigned to the node.", true),
		column("provider_id", tString, "Cloud provider id of the machine.", true),
		column("kubelet_version", tString, "Kubelet version, e.g. v1.31.2.", true),
		column("container_runtime_version", tString, "Container runtime, e.g. containerd://1.7.18.", true),
		column("os_image", tString, "Operating system image.", true),
		column("kernel_version", tString, "Linux (or Windows) kernel release of the node, from uname -r.", true),
		column("architecture", tString, "CPU architecture, e.g. amd64, arm64.", true),
		column("operating_system", tString, "linux or windows.", true),
		column("capacity_cpu_millicores", tInt64, "Total CPU in millicores.", true),
		column("capacity_memory_bytes", tInt64, "Total memory in bytes.", true),
		column("capacity_pods", tInt64, "Maximum count of pods the node can run.", true),
		column("capacity_ephemeral_storage_bytes", tInt64, "Total ephemeral storage in bytes.", true),
		column("allocatable_cpu_millicores", tInt64, "CPU available to pods, in millicores.", true),
		column("allocatable_memory_bytes", tInt64, "Memory available to pods, in bytes.", true),
		column("allocatable_pods", tInt64, "Count of pods that can be scheduled on the node.", true),
		column("allocatable_ephemeral_storage_bytes", tInt64, "Ephemeral storage available to pods, in bytes.", true),
		column("capacity", tStrMap, "Every capacity quantity (cpu, memory, pods, hugepages-*, extended resources) as text.", false),
		column("allocatable", tStrMap, "Every allocatable quantity as text.", false),
		column("conditions", conditionType, conditionsDoc+" Types: Ready, MemoryPressure, DiskPressure, PIDPressure, NetworkUnavailable.", false),
		column("taints", taintType, "Taints: key, value, effect (NoSchedule, PreferNoSchedule, NoExecute), time_added.", false),
	},
	FieldSelectors: map[string]string{"unschedulable": "spec.unschedulable"},
	Row: typed(func(o *corev1.Node, _ *rowCtx) [][]any {
		st := map[string]string{}
		conds := make([]any, 0, len(o.Status.Conditions))
		for _, c := range o.Status.Conditions {
			st[string(c.Type)] = string(c.Status)
			conds = append(conds, cond(string(c.Type), string(c.Status), c.Reason, c.Message, c.LastTransitionTime))
		}
		var internal, external any
		addrs := make([]any, 0, len(o.Status.Addresses))
		for _, a := range o.Status.Addresses {
			addrs = append(addrs, structVal{string(a.Type), a.Address})
			if a.Type == corev1.NodeInternalIP && internal == nil {
				internal = a.Address
			}
			if a.Type == corev1.NodeExternalIP && external == nil {
				external = a.Address
			}
		}
		taints := make([]any, 0, len(o.Spec.Taints))
		for _, t := range o.Spec.Taints {
			taints = append(taints, structVal{t.Key, nilIfEmpty(t.Value), string(t.Effect), ts(t.TimeAdded)})
		}
		ni := o.Status.NodeInfo
		cp, al := o.Status.Capacity, o.Status.Allocatable
		return one(conditionTrue(st, "Ready"), o.Spec.Unschedulable, nodeRoles(o.Labels), internal, external, addrs,
			nilIfEmpty(o.Spec.PodCIDR), nilIfEmpty(o.Spec.ProviderID), nilIfEmpty(ni.KubeletVersion),
			nilIfEmpty(ni.ContainerRuntimeVersion), nilIfEmpty(ni.OSImage), nilIfEmpty(ni.KernelVersion),
			nilIfEmpty(ni.Architecture), nilIfEmpty(ni.OperatingSystem),
			cpuMilli(cp), bytesOf(cp, corev1.ResourceMemory), countOf(cp, corev1.ResourcePods), bytesOf(cp, corev1.ResourceEphemeralStorage),
			cpuMilli(al), bytesOf(al, corev1.ResourceMemory), countOf(al, corev1.ResourcePods), bytesOf(al, corev1.ResourceEphemeralStorage),
			quantityMap(cp), quantityMap(al), conds, taints)
	}),
}

// ---------------------------------------------------------------- pods

var podContainerType = structOf(
	sf("name", tString), sf("kind", tString), sf("image", tString), sf("image_id", tString),
	sf("ready", tBool), sf("started", tBool), sf("restart_count", tInt64),
	sf("state", tString), sf("state_reason", tString), sf("state_message", tString), sf("exit_code", tInt64),
	sf("started_at", tTS), sf("last_termination_reason", tString), sf("last_termination_exit_code", tInt64),
	sf("cpu_request_millicores", tInt64), sf("cpu_limit_millicores", tInt64),
	sf("memory_request_bytes", tInt64), sf("memory_limit_bytes", tInt64),
)

const podContainerDoc = "Containers with their status: name, kind (init, sidecar, app or ephemeral), image, " +
	"image_id, ready, started, restart_count, state (running, waiting, terminated), state_reason (e.g. " +
	"CrashLoopBackOff, ImagePullBackOff, Completed, OOMKilled), state_message, exit_code, started_at, " +
	"last_termination_reason, last_termination_exit_code, cpu_request_millicores, cpu_limit_millicores, " +
	"memory_request_bytes, memory_limit_bytes."

// podContainers lists a pod's containers (init, app, ephemeral) with status.
func podContainers(o *corev1.Pod) []structVal {
	status := map[string]corev1.ContainerStatus{}
	for _, cs := range [][]corev1.ContainerStatus{o.Status.InitContainerStatuses, o.Status.ContainerStatuses, o.Status.EphemeralContainerStatuses} {
		for _, s := range cs {
			status[s.Name] = s
		}
	}
	var out []structVal
	add := func(name, kind, image string, res corev1.ResourceRequirements) {
		s, ok := status[name]
		var ready, started, restarts, state, reason, msg, exit, startedAt, lastReason, lastExit any
		if ok {
			ready, restarts = s.Ready, int64(s.RestartCount)
			started = boolPtr(s.Started)
			switch {
			case s.State.Running != nil:
				state, startedAt = "running", tsv(s.State.Running.StartedAt)
			case s.State.Waiting != nil:
				state, reason, msg = "waiting", nilIfEmpty(s.State.Waiting.Reason), nilIfEmpty(s.State.Waiting.Message)
			case s.State.Terminated != nil:
				t := s.State.Terminated
				state, reason, msg, exit, startedAt = "terminated", nilIfEmpty(t.Reason), nilIfEmpty(t.Message), int64(t.ExitCode), tsv(t.StartedAt)
			}
			if t := s.LastTerminationState.Terminated; t != nil {
				lastReason, lastExit = nilIfEmpty(t.Reason), int64(t.ExitCode)
			}
			if s.Image != "" {
				image = s.Image
			}
		}
		var imageID any
		if ok {
			imageID = nilIfEmpty(s.ImageID)
		}
		out = append(out, structVal{name, kind, image, imageID, ready, started, restarts, state, reason, msg, exit,
			startedAt, lastReason, lastExit,
			cpuMilli(res.Requests), cpuMilli(res.Limits), bytesOf(res.Requests, corev1.ResourceMemory), bytesOf(res.Limits, corev1.ResourceMemory)})
	}
	for _, c := range o.Spec.InitContainers {
		kind := "init"
		if c.RestartPolicy != nil && *c.RestartPolicy == corev1.ContainerRestartPolicyAlways {
			kind = "sidecar"
		}
		add(c.Name, kind, c.Image, c.Resources)
	}
	for _, c := range o.Spec.Containers {
		add(c.Name, "app", c.Image, c.Resources)
	}
	for _, c := range o.Spec.EphemeralContainers {
		add(c.Name, "ephemeral", c.Image, c.Resources)
	}
	return out
}

// sumApp sums a quantity over the app containers (nil when none set it).
func sumApp(o *corev1.Pod, get func(corev1.ResourceRequirements) any) any {
	var total int64
	set := false
	for _, c := range o.Spec.Containers {
		if v, ok := get(c.Resources).(int64); ok {
			total += v
			set = true
		}
	}
	if !set {
		return nil
	}
	return total
}

var podsDef = &resDef{
	Name: "pods", Meta: true, Key: []string{"uid"},
	Sources: coreSrc("pods", "Pod", true),
	Docs: fnDocs{Title: "Pods", Category: "workloads",
		Keywords:    "kubernetes, pods, containers, crashloopbackoff, restarts, scheduling, requests, limits, images",
		Description: "Pods with phase, node, readiness, restarts, resource requests/limits and containers",
		Examples: []vgi.CatalogExample{
			ex("Pods not running or succeeded, with their reason", "SELECT namespace, name, phase, reason FROM kubernetes.main.pods WHERE phase NOT IN ('Running', 'Succeeded') ORDER BY namespace, name"),
			ex("Containers waiting in CrashLoopBackOff", "SELECT namespace, name, c.name AS container, c.restart_count FROM kubernetes.main.pods, UNNEST(containers) AS u(c) WHERE c.state_reason = 'CrashLoopBackOff' ORDER BY namespace, name"),
			ex("CPU requested per node, in cores", "SELECT node_name, sum(cpu_request_millicores) / 1000.0 AS cpu_cores FROM kubernetes.main.pods WHERE phase = 'Running' GROUP BY node_name ORDER BY cpu_cores DESC"),
		}},
	Cols: []arrow.Field{
		column("phase", tString, "Pending, Running, Succeeded, Failed or Unknown.", true),
		column("ready", tBool, "Whether the Ready condition is True.", true),
		column("node_name", tString, "Node the pod is scheduled on (NULL while unscheduled).", true),
		column("pod_ip", tString, "Pod IP address.", true),
		column("host_ip", tString, "IP of the node hosting the pod.", true),
		column("service_account", tString, "ServiceAccount the pod runs as.", true),
		column("qos_class", tString, "Guaranteed, Burstable or BestEffort.", true),
		column("priority_class_name", tString, "PriorityClass the pod requests (see priorityclasses).", true),
		column("priority", tInt64, "Resolved scheduling priority score (integer from the PriorityClass).", true),
		column("restart_policy", tString, "Always, OnFailure or Never.", true),
		column("host_network", tBool, "Whether the pod uses the host network namespace.", false),
		column("start_time", tTS, "When the kubelet acknowledged the pod.", true),
		column("deletion_timestamp", tTS, "When deletion was requested (NULL unless terminating).", true),
		column("reason", tString, "Pod-level status reason, e.g. Evicted.", true),
		column("message", tString, "Pod-level status message.", true),
		column("controller_kind", tString, "Kind of the controlling owner (ReplicaSet, StatefulSet, DaemonSet, Job, ...).", true),
		column("controller_name", tString, "Name of the controlling owner.", true),
		column("restart_count", tInt64, "Count of container restarts, summed over all containers.", false),
		column("container_count", tInt64, "Count of app containers.", false),
		column("ready_container_count", tInt64, "Count of ready app containers.", false),
		column("cpu_request_millicores", tInt64, "CPU requests summed over app containers, in millicores.", true),
		column("cpu_limit_millicores", tInt64, "CPU limits summed over app containers, in millicores.", true),
		column("memory_request_bytes", tInt64, "Memory requests summed over app containers, in bytes.", true),
		column("memory_limit_bytes", tInt64, "Memory limits summed over app containers, in bytes.", true),
		column("images", tStrList, "Distinct container images, init containers first.", false),
		column("containers", listOf(podContainerType), podContainerDoc, false),
		column("conditions", conditionType, conditionsDoc+" Types: PodScheduled, Initialized, ContainersReady, Ready.", false),
		column("node_selector", tStrMap, "spec.nodeSelector.", false),
	},
	FieldSelectors: map[string]string{
		"phase": "status.phase", "node_name": "spec.nodeName", "pod_ip": "status.podIP",
		"service_account": "spec.serviceAccountName", "restart_policy": "spec.restartPolicy",
		"host_network": "spec.hostNetwork",
	},
	Row: typed(func(o *corev1.Pod, _ *rowCtx) [][]any {
		st := map[string]string{}
		conds := make([]any, 0, len(o.Status.Conditions))
		for _, c := range o.Status.Conditions {
			st[string(c.Type)] = string(c.Status)
			conds = append(conds, cond(string(c.Type), string(c.Status), c.Reason, c.Message, c.LastTransitionTime))
		}
		var restarts, total, ready int64
		containers := podContainers(o)
		cs := make([]any, len(containers))
		for i, c := range containers {
			cs[i] = c
			if n, ok := c[6].(int64); ok {
				restarts += n
			}
			if c[1] == "app" {
				total++
				if r, ok := c[4].(bool); ok && r {
					ready++
				}
			}
		}
		ck, cn := controllerOf(&o.ObjectMeta)
		return one(nilIfEmpty(string(o.Status.Phase)), conditionTrue(st, "Ready"), nilIfEmpty(o.Spec.NodeName),
			nilIfEmpty(o.Status.PodIP), nilIfEmpty(o.Status.HostIP), nilIfEmpty(o.Spec.ServiceAccountName),
			nilIfEmpty(string(o.Status.QOSClass)), nilIfEmpty(o.Spec.PriorityClassName), i32Ptr(o.Spec.Priority),
			nilIfEmpty(string(o.Spec.RestartPolicy)), o.Spec.HostNetwork, ts(o.Status.StartTime), ts(o.DeletionTimestamp),
			nilIfEmpty(o.Status.Reason), nilIfEmpty(o.Status.Message), ck, cn, restarts, total, ready,
			sumApp(o, func(r corev1.ResourceRequirements) any { return cpuMilli(r.Requests) }),
			sumApp(o, func(r corev1.ResourceRequirements) any { return cpuMilli(r.Limits) }),
			sumApp(o, func(r corev1.ResourceRequirements) any { return bytesOf(r.Requests, corev1.ResourceMemory) }),
			sumApp(o, func(r corev1.ResourceRequirements) any { return bytesOf(r.Limits, corev1.ResourceMemory) }),
			images(&o.Spec), cs, conds, strMap(o.Spec.NodeSelector))
	}),
}

// podContainersDef flattens pods to one row per container.
var podContainersDef = &resDef{
	Name: "pod_containers", Meta: false, NamespaceCol: "namespace", Key: []string{"pod_uid", "container_name"},
	Sources: coreSrc("pods", "Pod", true),
	Docs: fnDocs{Title: "Pod Containers", Category: "workloads",
		Keywords:    "kubernetes, containers, images, restarts, oomkilled, crashloopbackoff, requests, limits",
		Description: "One row per container of every pod (init, sidecar, app and ephemeral), with its status and resources",
		Examples: []vgi.CatalogExample{
			ex("Containers that were OOM-killed last time they stopped", "SELECT namespace, pod_name, container_name, restart_count FROM kubernetes.main.pod_containers WHERE last_termination_reason = 'OOMKilled' ORDER BY restart_count DESC"),
			ex("Running containers without a memory limit", "SELECT namespace, pod_name, container_name, image FROM kubernetes.main.pod_containers WHERE state = 'running' AND memory_limit_bytes IS NULL ORDER BY namespace, pod_name"),
		}},
	Cols: append([]arrow.Field{
		column("namespace", tString, "Namespace of the pod.", false),
		column("pod_name", tString, "Name of the pod.", false),
		column("pod_uid", tString, "UID of the pod.", false),
		column("node_name", tString, "Node the pod runs on.", true),
		column("phase", tString, "Phase of the pod.", true),
		column("container_name", tString, "Name of the container.", false),
	}, containerColumns()...),
	FieldSelectors: map[string]string{"pod_name": "metadata.name", "node_name": "spec.nodeName", "phase": "status.phase"},
	Row: typed(func(o *corev1.Pod, _ *rowCtx) [][]any {
		var rows [][]any
		for _, c := range podContainers(o) {
			row := []any{o.Namespace, o.Name, string(o.UID), nilIfEmpty(o.Spec.NodeName), nilIfEmpty(string(o.Status.Phase)), c[0]}
			rows = append(rows, append(row, c[1:]...))
		}
		return rows
	}),
}

// containerColumns are podContainerType's fields after name, as columns.
func containerColumns() []arrow.Field {
	docs := map[string]string{
		"kind": "init, sidecar, app or ephemeral.", "image": "Image reference.", "image_id": "Resolved image digest.",
		"ready": "Whether the container passes its readiness probe.", "started": "Whether the startup probe passed.",
		"restart_count": "Count of restarts of this container.", "state": "running, waiting or terminated.",
		"state_reason":  "Why it is waiting/terminated, e.g. CrashLoopBackOff, ImagePullBackOff, Completed, Error.",
		"state_message": "Detail for state_reason.", "exit_code": "Process exit status (0 = success) when terminated.",
		"started_at":                 "When the current state started (running or terminated).",
		"last_termination_reason":    "Reason the previous instance terminated, e.g. OOMKilled, Error.",
		"last_termination_exit_code": "Process exit status (0 = success) of the previous instance.",
		"cpu_request_millicores":     "CPU request in millicores.", "cpu_limit_millicores": "CPU limit in millicores.",
		"memory_request_bytes": "Memory request in bytes.", "memory_limit_bytes": "Memory limit in bytes.",
	}
	var out []arrow.Field
	for _, f := range podContainerType.Fields()[1:] {
		out = append(out, column(f.Name, f.Type, docs[f.Name], f.Name != "kind" && f.Name != "image"))
	}
	return out
}

// ---------------------------------------------------------------- services

var servicePortType = listOf(structOf(sf("name", tString), sf("protocol", tString), sf("port", tInt64),
	sf("target_port", tString), sf("node_port", tInt64), sf("app_protocol", tString)))

var servicesDef = &resDef{
	Name: "services", Meta: true, Key: []string{"uid"},
	Sources: coreSrc("services", "Service", true),
	Docs: fnDocs{Title: "Services", Category: "networking",
		Keywords:    "kubernetes, services, clusterip, nodeport, loadbalancer, ports, selectors",
		Description: "Services with type, cluster IPs, ports, selector and load-balancer addresses",
		Examples: []vgi.CatalogExample{
			ex("LoadBalancer services and their external addresses", "SELECT namespace, name, load_balancer_ingress FROM kubernetes.main.services WHERE type = 'LoadBalancer' ORDER BY namespace, name"),
			ex("Every exposed port", "SELECT namespace, name, p.port, p.protocol, p.target_port FROM kubernetes.main.services, UNNEST(ports) AS u(p) ORDER BY namespace, name, p.port"),
		}},
	Cols: []arrow.Field{
		column("type", tString, "ClusterIP, NodePort, LoadBalancer or ExternalName.", true),
		column("cluster_ip", tString, "Primary cluster IP (None for headless).", true),
		column("cluster_ips", tStrList, "All cluster IPs (dual stack).", false),
		column("external_ips", tStrList, "spec.externalIPs.", false),
		column("external_name", tString, "DNS name for ExternalName services.", true),
		column("load_balancer_ingress", tStrList, "Load-balancer IPs or hostnames.", false),
		column("ports", servicePortType, "Ports: name, protocol, port, target_port (number or name), node_port, app_protocol.", false),
		column("selector_labels", tStrMap, "Pod selector (spec.selector); empty for services without one.", false),
		column("session_affinity", tString, "None or ClientIP.", true),
		column("external_traffic_policy", tString, "Cluster or Local.", true),
		column("internal_traffic_policy", tString, "Cluster or Local.", true),
		column("ip_families", tStrList, "IPv4 and/or IPv6.", false),
	},
	FieldSelectors: map[string]string{"type": "spec.type", "cluster_ip": "spec.clusterIP"},
	Row: typed(func(o *corev1.Service, _ *rowCtx) [][]any {
		ports := make([]any, 0, len(o.Spec.Ports))
		for _, p := range o.Spec.Ports {
			var np any
			if p.NodePort != 0 {
				np = int64(p.NodePort)
			}
			ports = append(ports, structVal{nilIfEmpty(p.Name), string(p.Protocol), int64(p.Port), nilIfEmpty(p.TargetPort.String()), np, strPtr(p.AppProtocol)})
		}
		lb := []any{}
		for _, in := range o.Status.LoadBalancer.Ingress {
			if in.IP != "" {
				lb = append(lb, in.IP)
			} else if in.Hostname != "" {
				lb = append(lb, in.Hostname)
			}
		}
		fam := make([]string, len(o.Spec.IPFamilies))
		for i, f := range o.Spec.IPFamilies {
			fam[i] = string(f)
		}
		var itp any
		if o.Spec.InternalTrafficPolicy != nil {
			itp = string(*o.Spec.InternalTrafficPolicy)
		}
		return one(nilIfEmpty(string(o.Spec.Type)), nilIfEmpty(o.Spec.ClusterIP), strList(o.Spec.ClusterIPs),
			strList(o.Spec.ExternalIPs), nilIfEmpty(o.Spec.ExternalName), lb, ports, strMap(o.Spec.Selector),
			nilIfEmpty(string(o.Spec.SessionAffinity)), nilIfEmpty(string(o.Spec.ExternalTrafficPolicy)), itp, strList(fam))
	}),
}

// ---------------------------------------------------------------- configmaps / secrets

var configMapsDef = &resDef{
	Name: "configmaps", Meta: true, Key: []string{"uid"},
	Sources: coreSrc("configmaps", "ConfigMap", true),
	Docs: fnDocs{Title: "ConfigMaps", Category: "config",
		Keywords:    "kubernetes, configmaps, configuration, keys",
		Description: "ConfigMaps with their keys; values only when the ATTACH sets configmap_values true",
		Examples: []vgi.CatalogExample{
			ex("ConfigMaps and how many keys each holds", "SELECT namespace, name, len(keys) AS key_count FROM kubernetes.main.configmaps ORDER BY key_count DESC, namespace, name"),
		}},
	Cols: []arrow.Field{
		column("keys", tStrList, "Keys of data and binaryData, sorted.", false),
		column("binary_keys", tStrList, "Keys of binaryData only.", false),
		column("immutable", tBool, "Whether the ConfigMap is immutable.", true),
		column("data", tStrMap, "data values; NULL unless the ATTACH sets configmap_values true.", true),
	},
	Row: typed(func(o *corev1.ConfigMap, rc *rowCtx) [][]any {
		var data any
		if rc.opts.ConfigMapValues {
			data = strMap(o.Data)
		}
		return one(sortedKeys(stringKeys(o.Data), toAnyMap(o.BinaryData)), sortedKeys(toAnyMap(o.BinaryData)), boolPtr(o.Immutable), data)
	}),
}

func toAnyMap(m map[string][]byte) map[string]any {
	out := make(map[string]any, len(m))
	for k := range m {
		out[k] = nil
	}
	return out
}

func stringKeys(m map[string]string) map[string]any {
	out := make(map[string]any, len(m))
	for k := range m {
		out[k] = nil
	}
	return out
}

var secretsDef = &resDef{
	Name: "secrets", Meta: true, Key: []string{"uid"},
	Sources: coreSrc("secrets", "Secret", true),
	Docs: fnDocs{Title: "Secrets", Category: "config",
		Keywords:    "kubernetes, secrets, tls certificates, service account tokens, keys, inventory",
		Description: "Secrets by name, type and keys; values are never returned",
		Examples: []vgi.CatalogExample{
			ex("Secrets by type", "SELECT type, count(*) AS secrets FROM kubernetes.main.secrets GROUP BY type ORDER BY secrets DESC"),
			ex("TLS secrets in one namespace", "SELECT name, keys FROM kubernetes.main.secrets WHERE namespace = 'default' AND type = 'kubernetes.io/tls' ORDER BY name"),
		}},
	Cols: []arrow.Field{
		column("type", tString, "Secret type, e.g. Opaque, kubernetes.io/tls, kubernetes.io/dockerconfigjson.", true),
		column("keys", tStrList, "Keys present in data, sorted. Values are never returned.", false),
		column("immutable", tBool, "Whether the Secret is immutable.", true),
	},
	FieldSelectors: map[string]string{"type": "type"},
	Row: typed(func(o *corev1.Secret, _ *rowCtx) [][]any {
		keys := toAnyMap(o.Data)
		for k := range o.StringData {
			keys[k] = nil
		}
		return one(nilIfEmpty(string(o.Type)), sortedKeys(keys), boolPtr(o.Immutable))
	}),
}

var serviceAccountsDef = &resDef{
	Name: "serviceaccounts", Meta: true, Key: []string{"uid"},
	Sources: coreSrc("serviceaccounts", "ServiceAccount", true),
	Docs: fnDocs{Title: "Service Accounts", Category: "config",
		Keywords:    "kubernetes, service accounts, workload identity, image pull secrets, automount",
		Description: "ServiceAccounts with their secrets, image pull secrets and token automount setting",
		Examples: []vgi.CatalogExample{
			ex("Service accounts that do not opt out of token automount", "SELECT namespace, name FROM kubernetes.main.serviceaccounts WHERE automount_service_account_token IS DISTINCT FROM false ORDER BY namespace, name"),
		}},
	Cols: []arrow.Field{
		column("secrets", tStrList, "Names of secrets listed on the ServiceAccount.", false),
		column("image_pull_secrets", tStrList, "Names of image pull secrets.", false),
		column("automount_service_account_token", tBool, "automountServiceAccountToken (NULL = default true).", true),
	},
	Row: typed(func(o *corev1.ServiceAccount, _ *rowCtx) [][]any {
		var s, ips []string
		for _, r := range o.Secrets {
			s = append(s, r.Name)
		}
		for _, r := range o.ImagePullSecrets {
			ips = append(ips, r.Name)
		}
		return one(strList(s), strList(ips), boolPtr(o.AutomountServiceAccountToken))
	}),
}

// ---------------------------------------------------------------- storage

func accessModes(ms []corev1.PersistentVolumeAccessMode) []any {
	out := make([]string, len(ms))
	for i, m := range ms {
		out[i] = string(m)
	}
	return strList(out)
}

// pvSourceType names the volume plugin of a PersistentVolume.
func pvSourceType(s *corev1.PersistentVolumeSpec) any {
	switch {
	case s.CSI != nil:
		return "csi"
	case s.HostPath != nil:
		return "hostPath"
	case s.Local != nil:
		return "local"
	case s.NFS != nil:
		return "nfs"
	case s.AWSElasticBlockStore != nil:
		return "awsElasticBlockStore"
	case s.GCEPersistentDisk != nil:
		return "gcePersistentDisk"
	case s.AzureDisk != nil:
		return "azureDisk"
	case s.AzureFile != nil:
		return "azureFile"
	case s.ISCSI != nil:
		return "iscsi"
	case s.FC != nil:
		return "fc"
	}
	return nil
}

var persistentVolumesDef = &resDef{
	Name: "persistentvolumes", Meta: true, Key: []string{"uid"},
	Sources: coreSrc("persistentvolumes", "PersistentVolume", false),
	Docs: fnDocs{Title: "Persistent Volumes", Category: "storage",
		Keywords:    "kubernetes, persistent volumes, pv, storage, capacity, reclaim policy, csi",
		Description: "PersistentVolumes with capacity, phase, claim, storage class and volume source",
		Examples: []vgi.CatalogExample{
			ex("Released or failed volumes still holding storage", "SELECT name, phase, capacity_bytes / 2^30 AS gib, claim_namespace, claim_name FROM kubernetes.main.persistentvolumes WHERE phase IN ('Released', 'Failed') ORDER BY gib DESC"),
		}},
	Cols: []arrow.Field{
		column("phase", tString, "Available, Bound, Released, Failed or Pending.", true),
		column("capacity_bytes", tInt64, "Storage capacity in bytes.", true),
		column("access_modes", tStrList, "ReadWriteOnce, ReadOnlyMany, ReadWriteMany, ReadWriteOncePod.", false),
		column("reclaim_policy", tString, "Retain, Delete or Recycle.", true),
		column("storage_class", tString, "StorageClass name.", true),
		column("volume_mode", tString, "Filesystem or Block.", true),
		column("claim_namespace", tString, "Namespace of the bound claim.", true),
		column("claim_name", tString, "Name of the bound claim.", true),
		column("source_type", tString, "Volume plugin: csi, hostPath, local, nfs, ...", true),
		column("csi_driver", tString, "CSI driver name.", true),
		column("csi_volume_handle", tString, "The CSI driver's id of the backing volume, e.g. a cloud disk id.", true),
		column("reason", tString, "Status reason.", true),
	},
	Row: typed(func(o *corev1.PersistentVolume, _ *rowCtx) [][]any {
		var cns, cn, drv, handle, vm any
		if r := o.Spec.ClaimRef; r != nil {
			cns, cn = nilIfEmpty(r.Namespace), nilIfEmpty(r.Name)
		}
		if c := o.Spec.CSI; c != nil {
			drv, handle = c.Driver, c.VolumeHandle
		}
		if o.Spec.VolumeMode != nil {
			vm = string(*o.Spec.VolumeMode)
		}
		return one(nilIfEmpty(string(o.Status.Phase)), bytesOf(o.Spec.Capacity, corev1.ResourceStorage), accessModes(o.Spec.AccessModes),
			nilIfEmpty(string(o.Spec.PersistentVolumeReclaimPolicy)), nilIfEmpty(o.Spec.StorageClassName), vm, cns, cn,
			pvSourceType(&o.Spec), drv, handle, nilIfEmpty(o.Status.Reason))
	}),
}

var persistentVolumeClaimsDef = &resDef{
	Name: "persistentvolumeclaims", Meta: true, Key: []string{"uid"},
	Sources: coreSrc("persistentvolumeclaims", "PersistentVolumeClaim", true),
	Docs: fnDocs{Title: "Volume Claims", Category: "storage",
		Keywords:    "kubernetes, persistent volume claims, pvc, storage requests, pending claims",
		Description: "PersistentVolumeClaims with phase, requested and bound capacity, storage class and volume",
		Examples: []vgi.CatalogExample{
			ex("Requested storage per storage class, in GiB", "SELECT storage_class, sum(requested_bytes) / 2^30 AS gib FROM kubernetes.main.persistentvolumeclaims GROUP BY storage_class ORDER BY gib DESC"),
		}},
	Cols: []arrow.Field{
		column("phase", tString, "Pending, Bound or Lost.", true),
		column("storage_class", tString, "Requested StorageClass.", true),
		column("volume_name", tString, "Bound PersistentVolume.", true),
		column("volume_mode", tString, "Filesystem or Block.", true),
		column("access_modes", tStrList, "Requested access modes.", false),
		column("requested_bytes", tInt64, "Requested storage in bytes.", true),
		column("capacity_bytes", tInt64, "Actual bound capacity in bytes.", true),
	},
	Row: typed(func(o *corev1.PersistentVolumeClaim, _ *rowCtx) [][]any {
		var vm any
		if o.Spec.VolumeMode != nil {
			vm = string(*o.Spec.VolumeMode)
		}
		return one(nilIfEmpty(string(o.Status.Phase)), strPtr(o.Spec.StorageClassName), nilIfEmpty(o.Spec.VolumeName), vm,
			accessModes(o.Spec.AccessModes), bytesOf(o.Spec.Resources.Requests, corev1.ResourceStorage),
			bytesOf(o.Status.Capacity, corev1.ResourceStorage))
	}),
}

// ---------------------------------------------------------------- events

var eventsDef = &resDef{
	Name: "events", Meta: false, NamespaceCol: "namespace", Key: []string{"uid"},
	Sources: coreSrc("events", "Event", true),
	Docs: fnDocs{Title: "Events", Category: "observability",
		Keywords:    "kubernetes, events, warnings, backoff, failedscheduling, oomkilling, troubleshooting",
		Description: "Events: what happened to which object, when, how often and why",
		Examples: []vgi.CatalogExample{
			ex("Most frequent warning reasons", "SELECT reason, sum(count) AS occurrences FROM kubernetes.main.events WHERE type = 'Warning' GROUP BY reason ORDER BY occurrences DESC"),
			ex("Recent events for one pod", "SELECT last_seen, type, reason, message FROM kubernetes.main.events WHERE namespace = 'default' AND involved_kind = 'Pod' AND involved_name = 'web-0' ORDER BY last_seen"),
		}},
	Cols: []arrow.Field{
		column("namespace", tString, "Namespace of the event.", false),
		column("name", tString, "Event object name.", false),
		column("uid", tString, "Event object uid.", false),
		column("type", tString, "Normal or Warning.", true),
		column("reason", tString, "Short machine-readable reason, e.g. BackOff, FailedScheduling, Pulled, Killing.", true),
		column("message", tString, "Human-readable description.", true),
		column("count", tInt64, "Count of occurrences (series count for new-style events).", true),
		column("involved_kind", tString, "Kind of the object the event is about.", true),
		column("involved_namespace", tString, "Namespace of that object.", true),
		column("involved_name", tString, "Name of that object.", true),
		column("involved_uid", tString, "UID of that object.", true),
		column("involved_field_path", tString, "Part of the object, e.g. spec.containers{web}.", true),
		column("source_component", tString, "Reporting component, e.g. kubelet, default-scheduler.", true),
		column("source_host", tString, "Node that reported it.", true),
		column("first_timestamp", tTS, "First occurrence (older events API).", true),
		column("last_timestamp", tTS, "Latest occurrence (older events API).", true),
		column("event_time", tTS, "Time of the first observation (events.k8s.io).", true),
		column("last_seen", tTS, "Latest of last_timestamp, series last observed time, event_time and creation time.", true),
		column("creation_timestamp", tTS, "When the Event object was created.", true),
	},
	FieldSelectors: map[string]string{
		"name": "metadata.name", "type": "type", "reason": "reason",
		"involved_kind": "involvedObject.kind", "involved_namespace": "involvedObject.namespace",
		"involved_name": "involvedObject.name", "involved_uid": "involvedObject.uid",
		"involved_field_path": "involvedObject.fieldPath", "source_component": "source",
	},
	Row: typed(func(o *corev1.Event, _ *rowCtx) [][]any {
		count := any(nil)
		if o.Count > 0 {
			count = int64(o.Count)
		}
		last := ts(&o.LastTimestamp)
		if o.Series != nil {
			count = int64(o.Series.Count)
			if l := microTS(&o.Series.LastObservedTime); l != nil {
				last = l
			}
		}
		if last == nil {
			last = microTS(&o.EventTime)
		}
		if last == nil {
			last = tsv(o.CreationTimestamp)
		}
		src := nilIfEmpty(o.Source.Component)
		if src == nil {
			src = nilIfEmpty(o.ReportingController)
		}
		io := o.InvolvedObject
		return one(o.Namespace, o.Name, string(o.UID), nilIfEmpty(o.Type), nilIfEmpty(o.Reason), nilIfEmpty(o.Message), count,
			nilIfEmpty(io.Kind), nilIfEmpty(io.Namespace), nilIfEmpty(io.Name), nilIfEmpty(string(io.UID)), nilIfEmpty(io.FieldPath),
			src, nilIfEmpty(o.Source.Host), ts(&o.FirstTimestamp), ts(&o.LastTimestamp), microTS(&o.EventTime), last,
			tsv(o.CreationTimestamp))
	}),
}
