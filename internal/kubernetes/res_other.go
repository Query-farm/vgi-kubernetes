// Networking (endpoint slices, ingresses, network policies), storage
// classes, priority classes and RBAC.

package kubernetes

import (
	"fmt"
	"sort"
	"strings"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	discoveryv1 "k8s.io/api/discovery/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ---------------------------------------------------------------- endpointslices

var endpointType = listOf(structOf(sf("addresses", tStrList), sf("ready", tBool), sf("serving", tBool),
	sf("terminating", tBool), sf("hostname", tString), sf("node_name", tString), sf("zone", tString),
	sf("target_kind", tString), sf("target_namespace", tString), sf("target_name", tString)))

var endpointPortType = listOf(structOf(sf("name", tString), sf("protocol", tString), sf("port", tInt64), sf("app_protocol", tString)))

var endpointSlicesDef = &resDef{
	Name: "endpointslices", Meta: true, Key: []string{"uid"},
	Sources: groupSrc("discovery.k8s.io", "v1", "endpointslices", "EndpointSlice", true),
	Docs: fnDocs{Title: "Endpoint Slices", Category: "networking",
		Keywords:    "kubernetes, endpointslices, endpoints, service backends, readiness",
		Description: "EndpointSlices: the backend addresses of each service, with readiness and the pod behind each",
		Examples: []vgi.CatalogExample{
			ex("Ready and total endpoints per service", "SELECT namespace, service_name, sum(ready_count) AS ready, sum(endpoint_count) AS total FROM kubernetes.main.endpointslices GROUP BY ALL ORDER BY namespace, service_name"),
		}},
	Cols: []arrow.Field{
		column("service_name", tString, "Service the slice belongs to (kubernetes.io/service-name label).", true),
		column("address_type", tString, "IPv4, IPv6 or FQDN.", false),
		column("endpoint_count", tInt64, "Count of endpoints in the slice.", false),
		column("ready_count", tInt64, "Count of endpoints whose ready condition is true.", false),
		column("endpoints", endpointType, "Endpoints: addresses, ready, serving, terminating, hostname, node_name, zone, target_kind, target_namespace, target_name.", false),
		column("ports", endpointPortType, "Ports: name, protocol, port (TCP/UDP port number), app_protocol.", false),
	},
	Row: typed(func(o *discoveryv1.EndpointSlice, _ *rowCtx) [][]any {
		eps := make([]any, 0, len(o.Endpoints))
		var ready int64
		for _, e := range o.Endpoints {
			var tk, tns, tn any
			if r := e.TargetRef; r != nil {
				tk, tns, tn = nilIfEmpty(r.Kind), nilIfEmpty(r.Namespace), nilIfEmpty(r.Name)
			}
			if e.Conditions.Ready != nil && *e.Conditions.Ready {
				ready++
			}
			eps = append(eps, structVal{strList(e.Addresses), boolPtr(e.Conditions.Ready), boolPtr(e.Conditions.Serving),
				boolPtr(e.Conditions.Terminating), strPtr(e.Hostname), strPtr(e.NodeName), strPtr(e.Zone), tk, tns, tn})
		}
		ports := make([]any, 0, len(o.Ports))
		for _, p := range o.Ports {
			var proto any
			if p.Protocol != nil {
				proto = string(*p.Protocol)
			}
			ports = append(ports, structVal{strPtr(p.Name), proto, i32Ptr(p.Port), strPtr(p.AppProtocol)})
		}
		return one(nilIfEmpty(o.Labels["kubernetes.io/service-name"]), string(o.AddressType), int64(len(o.Endpoints)), ready, eps, ports)
	}),
}

// ---------------------------------------------------------------- ingresses

var ingressRuleType = listOf(structOf(sf("host", tString), sf("path", tString), sf("path_type", tString),
	sf("service_name", tString), sf("service_port", tString)))
var ingressTLSType = listOf(structOf(sf("hosts", tStrList), sf("secret_name", tString)))

func backendService(b *networkingv1.IngressBackend) (any, any) {
	if b == nil || b.Service == nil {
		return nil, nil
	}
	port := any(nil)
	if b.Service.Port.Name != "" {
		port = b.Service.Port.Name
	} else if b.Service.Port.Number != 0 {
		port = strings.TrimSpace(strconvItoa(int64(b.Service.Port.Number)))
	}
	return b.Service.Name, port
}

var ingressesDef = &resDef{
	Name: "ingresses", Meta: true, Key: []string{"uid"},
	Sources: groupSrc("networking.k8s.io", "v1", "ingresses", "Ingress", true),
	Docs: fnDocs{Title: "Ingresses", Category: "networking",
		Keywords:    "kubernetes, ingress, http routing, hosts, tls, ingress class",
		Description: "Ingresses with class, host/path rules and their backend services, TLS hosts and addresses",
		Examples: []vgi.CatalogExample{
			ex("Every routed host and path with its backend service", "SELECT namespace, name, r.host, r.path, r.service_name, r.service_port FROM kubernetes.main.ingresses, UNNEST(rules) AS u(r) ORDER BY r.host, r.path"),
			ex("Hosts served without TLS", "SELECT namespace, name, unnest(hosts) AS host FROM kubernetes.main.ingresses WHERE len(tls) = 0 ORDER BY host"),
		}},
	Cols: []arrow.Field{
		column("ingress_class_name", tString, "IngressClass.", true),
		column("hosts", tStrList, "Distinct hosts of the rules.", false),
		column("rules", ingressRuleType, "One entry per host/path: host, path, path_type, service_name, service_port.", false),
		column("tls", ingressTLSType, "TLS blocks: hosts and secret_name.", false),
		column("default_backend_service", tString, "Service of spec.defaultBackend.", true),
		column("default_backend_port", tString, "Port (number or name) of spec.defaultBackend.", true),
		column("load_balancer_ingress", tStrList, "Load-balancer IPs or hostnames.", false),
	},
	Row: typed(func(o *networkingv1.Ingress, _ *rowCtx) [][]any {
		rules := []any{}
		hostSet := map[string]bool{}
		for _, r := range o.Spec.Rules {
			if r.Host != "" {
				hostSet[r.Host] = true
			}
			if r.HTTP == nil {
				rules = append(rules, structVal{nilIfEmpty(r.Host), nil, nil, nil, nil})
				continue
			}
			for _, p := range r.HTTP.Paths {
				var pt any
				if p.PathType != nil {
					pt = string(*p.PathType)
				}
				sn, sp := backendService(&p.Backend)
				rules = append(rules, structVal{nilIfEmpty(r.Host), nilIfEmpty(p.Path), pt, sn, sp})
			}
		}
		var hosts []string
		for h := range hostSet {
			hosts = append(hosts, h)
		}
		sort.Strings(hosts)
		tls := make([]any, 0, len(o.Spec.TLS))
		for _, t := range o.Spec.TLS {
			tls = append(tls, structVal{strList(t.Hosts), nilIfEmpty(t.SecretName)})
		}
		dn, dp := backendService(o.Spec.DefaultBackend)
		lb := []any{}
		for _, in := range o.Status.LoadBalancer.Ingress {
			if in.IP != "" {
				lb = append(lb, in.IP)
			} else if in.Hostname != "" {
				lb = append(lb, in.Hostname)
			}
		}
		return one(strPtr(o.Spec.IngressClassName), strList(hosts), rules, tls, dn, dp, lb)
	}),
}

// ---------------------------------------------------------------- networkpolicies

var networkPoliciesDef = &resDef{
	Name: "networkpolicies", Meta: true, Key: []string{"uid"},
	Sources: groupSrc("networking.k8s.io", "v1", "networkpolicies", "NetworkPolicy", true),
	Docs: fnDocs{Title: "Network Policies", Category: "networking",
		Keywords:    "kubernetes, network policies, default deny, ingress rules, egress rules, segmentation",
		Description: "NetworkPolicies with the pods they select, policy types, rule counts and default-deny flags",
		Examples: []vgi.CatalogExample{
			ex("Namespaces with a default-deny ingress policy", "SELECT DISTINCT namespace FROM kubernetes.main.networkpolicies WHERE default_deny_ingress ORDER BY namespace"),
		}},
	Cols: []arrow.Field{
		column("pod_selector", tString, "Selected pods, kubectl selector syntax ('' selects every pod in the namespace).", true),
		column("pod_selector_labels", tStrMap, "matchLabels of the pod selector.", true),
		column("selects_all_pods", tBool, "Whether the pod selector is empty (applies to every pod).", false),
		column("policy_types", tStrList, "Ingress and/or Egress.", false),
		column("ingress_rule_count", tInt64, "Count of ingress rules.", false),
		column("egress_rule_count", tInt64, "Count of egress rules.", false),
		column("default_deny_ingress", tBool, "Policy type Ingress with no ingress rules: denies all inbound traffic to the selected pods.", false),
		column("default_deny_egress", tBool, "Policy type Egress with no egress rules: denies all outbound traffic.", false),
		column("ingress", tJSON, "spec.ingress rules (from peers and ports) as JSON.", true),
		column("egress", tJSON, "spec.egress rules (to peers and ports) as JSON.", true),
	},
	Row: typed(func(o *networkingv1.NetworkPolicy, _ *rowCtx) [][]any {
		types := map[string]bool{}
		var pt []string
		for _, t := range o.Spec.PolicyTypes {
			types[string(t)] = true
			pt = append(pt, string(t))
		}
		if len(pt) == 0 { // API default: Ingress, plus Egress when egress rules exist
			types["Ingress"] = true
			pt = append(pt, "Ingress")
			if len(o.Spec.Egress) > 0 {
				types["Egress"] = true
				pt = append(pt, "Egress")
			}
		}
		sel := o.Spec.PodSelector
		all := len(sel.MatchLabels) == 0 && len(sel.MatchExpressions) == 0
		var in, eg any
		if len(o.Spec.Ingress) > 0 {
			in = toJSON(o.Spec.Ingress)
		}
		if len(o.Spec.Egress) > 0 {
			eg = toJSON(o.Spec.Egress)
		}
		return one(selectorString(&sel), matchLabels(&sel), all, strList(pt), int64(len(o.Spec.Ingress)), int64(len(o.Spec.Egress)),
			types["Ingress"] && len(o.Spec.Ingress) == 0, types["Egress"] && len(o.Spec.Egress) == 0, in, eg)
	}),
}

// ---------------------------------------------------------------- storageclasses / priorityclasses

var storageClassesDef = &resDef{
	Name: "storageclasses", Meta: true, Key: []string{"uid"},
	Sources: groupSrc("storage.k8s.io", "v1", "storageclasses", "StorageClass", false),
	Docs: fnDocs{Title: "Storage Classes", Category: "storage",
		Keywords:    "kubernetes, storage classes, provisioner, default storage class, volume expansion",
		Description: "StorageClasses with provisioner, reclaim policy, binding mode, expansion and default flag",
		Examples: []vgi.CatalogExample{
			ex("The default storage class", "SELECT name, provisioner, volume_binding_mode FROM kubernetes.main.storageclasses WHERE is_default ORDER BY name"),
		}},
	Cols: []arrow.Field{
		column("provisioner", tString, "Volume provisioner, e.g. ebs.csi.aws.com, rancher.io/local-path.", false),
		column("is_default", tBool, "Whether it is the default class (storageclass.kubernetes.io/is-default-class).", false),
		column("reclaim_policy", tString, "Delete or Retain.", true),
		column("volume_binding_mode", tString, "Immediate or WaitForFirstConsumer.", true),
		column("allow_volume_expansion", tBool, "Whether claims can grow.", true),
		column("parameters", tStrMap, "Provisioner parameters.", false),
		column("mount_options", tStrList, "Options passed to mount for volumes of this class, e.g. debug, nfsvers=4.1.", false),
	},
	Row: typed(func(o *storagev1.StorageClass, _ *rowCtx) [][]any {
		var rp, bm any
		if o.ReclaimPolicy != nil {
			rp = string(*o.ReclaimPolicy)
		}
		if o.VolumeBindingMode != nil {
			bm = string(*o.VolumeBindingMode)
		}
		def := o.Annotations["storageclass.kubernetes.io/is-default-class"] == "true" ||
			o.Annotations["storageclass.beta.kubernetes.io/is-default-class"] == "true"
		return one(o.Provisioner, def, rp, bm, boolPtr(o.AllowVolumeExpansion), strMap(o.Parameters), strList(o.MountOptions))
	}),
}

var priorityClassesDef = &resDef{
	Name: "priorityclasses", Meta: true, Key: []string{"uid"},
	Sources: groupSrc("scheduling.k8s.io", "v1", "priorityclasses", "PriorityClass", false),
	Docs: fnDocs{Title: "Priority Classes", Category: "cluster",
		Keywords:    "kubernetes, priority classes, preemption, scheduling priority",
		Description: "PriorityClasses with their value, global default flag and preemption policy",
		Examples: []vgi.CatalogExample{
			ex("Priority classes from highest to lowest", "SELECT name, value, preemption_policy FROM kubernetes.main.priorityclasses ORDER BY value DESC"),
		}},
	Cols: []arrow.Field{
		column("value", tInt64, "Priority number (unitless integer score); higher schedules first and may preempt lower.", false),
		column("global_default", tBool, "Whether pods without a class get this one.", false),
		column("preemption_policy", tString, "PreemptLowerPriority or Never.", true),
		column("description", tString, "Free-text purpose of the class, set by its author.", true),
	},
	Row: typed(func(o *schedulingv1.PriorityClass, _ *rowCtx) [][]any {
		var pp any
		if o.PreemptionPolicy != nil {
			pp = string(*o.PreemptionPolicy)
		}
		return one(int64(o.Value), o.GlobalDefault, pp, nilIfEmpty(o.Description))
	}),
}

// ---------------------------------------------------------------- RBAC

var policyRuleType = listOf(structOf(sf("api_groups", tStrList), sf("resources", tStrList), sf("resource_names", tStrList),
	sf("verbs", tStrList), sf("non_resource_urls", tStrList)))

const rulesDoc = "Rules: api_groups ('' = core), resources (with subresources such as pods/exec), resource_names, verbs, non_resource_urls."

func ruleList(rs []rbacv1.PolicyRule) []any {
	out := make([]any, 0, len(rs))
	for _, r := range rs {
		out = append(out, structVal{strList(r.APIGroups), strList(r.Resources), strList(r.ResourceNames), strList(r.Verbs), strList(r.NonResourceURLs)})
	}
	return out
}

var subjectType = listOf(structOf(sf("kind", tString), sf("name", tString), sf("namespace", tString), sf("api_group", tString)))

func subjectList(ss []rbacv1.Subject) []any {
	out := make([]any, 0, len(ss))
	for _, s := range ss {
		out = append(out, structVal{s.Kind, s.Name, nilIfEmpty(s.Namespace), nilIfEmpty(s.APIGroup)})
	}
	return out
}

var rbacSrc = func(resource, kind string, namespaced bool) []Source {
	return groupSrc("rbac.authorization.k8s.io", "v1", resource, kind, namespaced)
}

var rolesDef = &resDef{
	Name: "roles", Meta: true, Key: []string{"uid"},
	Sources: rbacSrc("roles", "Role", true),
	Docs: fnDocs{Title: "Roles", Category: "rbac",
		Keywords:    "kubernetes, rbac, roles, permissions, policy rules",
		Description: "Namespaced RBAC Roles with their rules",
		Examples: []vgi.CatalogExample{
			ex("Roles with the most rules", "SELECT namespace, name, len(rules) AS rule_count FROM kubernetes.main.roles ORDER BY rule_count DESC, namespace, name"),
		}},
	Cols: []arrow.Field{column("rules", policyRuleType, rulesDoc, false)},
	Row: typed(func(o *rbacv1.Role, _ *rowCtx) [][]any {
		return one(ruleList(o.Rules))
	}),
}

var clusterRolesDef = &resDef{
	Name: "clusterroles", Meta: true, Key: []string{"uid"},
	Sources: rbacSrc("clusterroles", "ClusterRole", false),
	Docs: fnDocs{Title: "Cluster Roles", Category: "rbac",
		Keywords:    "kubernetes, rbac, cluster roles, aggregation, permissions",
		Description: "ClusterRoles with their rules and aggregation selectors",
		Examples: []vgi.CatalogExample{
			ex("Cluster roles that grant every verb on every resource", "SELECT name FROM kubernetes.main.clusterroles, UNNEST(rules) AS u(r) WHERE list_contains(r.verbs, '*') AND list_contains(r.resources, '*') ORDER BY name"),
		}},
	Cols: []arrow.Field{
		column("rules", policyRuleType, rulesDoc, false),
		column("aggregated", tBool, "Whether the rules are aggregated from other ClusterRoles (aggregationRule).", false),
		column("aggregation_selectors", tStrList, "Label selectors of the aggregationRule, kubectl syntax.", false),
	},
	Row: typed(func(o *rbacv1.ClusterRole, _ *rowCtx) [][]any {
		sels := []any{}
		if o.AggregationRule != nil {
			for i := range o.AggregationRule.ClusterRoleSelectors {
				if s := selectorString(&o.AggregationRule.ClusterRoleSelectors[i]); s != nil {
					sels = append(sels, s)
				}
			}
		}
		return one(ruleList(o.Rules), o.AggregationRule != nil, sels)
	}),
}

var roleBindingsDef = &resDef{
	Name: "rolebindings", Meta: true, Key: []string{"uid"},
	Sources: rbacSrc("rolebindings", "RoleBinding", true),
	Docs: fnDocs{Title: "Role Bindings", Category: "rbac",
		Keywords:    "kubernetes, rbac, role bindings, subjects, service accounts, groups",
		Description: "Namespaced RoleBindings: which Role or ClusterRole is granted to which subjects",
		Examples: []vgi.CatalogExample{
			ex("Bindings that grant cluster-admin inside a namespace", "SELECT namespace, name, subjects FROM kubernetes.main.rolebindings WHERE role_name = 'cluster-admin' ORDER BY namespace, name"),
		}},
	Cols: []arrow.Field{
		column("role_kind", tString, "Role or ClusterRole.", false),
		column("role_name", tString, "Name of the granted role.", false),
		column("subjects", subjectType, "Subjects: kind (User, Group, ServiceAccount), name, namespace, api_group.", false),
	},
	Row: typed(func(o *rbacv1.RoleBinding, _ *rowCtx) [][]any {
		return one(o.RoleRef.Kind, o.RoleRef.Name, subjectList(o.Subjects))
	}),
}

var clusterRoleBindingsDef = &resDef{
	Name: "clusterrolebindings", Meta: true, Key: []string{"uid"},
	Sources: rbacSrc("clusterrolebindings", "ClusterRoleBinding", false),
	Docs: fnDocs{Title: "Cluster Role Bindings", Category: "rbac",
		Keywords:    "kubernetes, rbac, cluster role bindings, cluster-admin, subjects",
		Description: "ClusterRoleBindings: which ClusterRole is granted cluster-wide to which subjects",
		Examples: []vgi.CatalogExample{
			ex("Who is bound to cluster-admin", "SELECT name, s.kind, s.name AS subject FROM kubernetes.main.clusterrolebindings, UNNEST(subjects) AS u(s) WHERE role_name = 'cluster-admin' ORDER BY name"),
		}},
	Cols: []arrow.Field{
		column("role_kind", tString, "Always ClusterRole.", false),
		column("role_name", tString, "Name of the granted ClusterRole.", false),
		column("subjects", subjectType, "Subjects: kind (User, Group, ServiceAccount), name, namespace, api_group.", false),
	},
	Row: typed(func(o *rbacv1.ClusterRoleBinding, _ *rowCtx) [][]any {
		return one(o.RoleRef.Kind, o.RoleRef.Name, subjectList(o.Subjects))
	}),
}

// rbacRulesDef flattens Roles and ClusterRoles to one row per
// (api_group, resource, verb) or (non_resource_url, verb).
var rbacRulesDef = &resDef{
	Name: "rbac_rules", NamespaceCol: "namespace", Key: []string{"id"},
	Sources: append(rbacSrc("roles", "Role", true), rbacSrc("clusterroles", "ClusterRole", false)...),
	Docs: fnDocs{Title: "RBAC Rules", Category: "rbac",
		Keywords:    "kubernetes, rbac, permissions, verbs, access review, secrets access, wildcard",
		Description: "Every Role and ClusterRole rule flattened to one row per api_group, resource and verb (or non-resource URL and verb)",
		Examples: []vgi.CatalogExample{
			ex("Roles and cluster roles that can read secrets", "SELECT role_kind, namespace, role_name FROM kubernetes.main.rbac_rules WHERE resource IN ('secrets', '*') AND verb IN ('get', 'list', 'watch', '*') AND api_group IN ('', '*') GROUP BY ALL ORDER BY ALL"),
			ex("Who can exec into pods, via which binding", "SELECT b.subject_kind, b.subject_name, r.role_name FROM kubernetes.main.rbac_rules r JOIN kubernetes.main.rbac_subjects b ON b.role_name = r.role_name AND b.role_kind = r.role_kind WHERE r.resource = 'pods/exec' AND r.verb IN ('create', '*') ORDER BY ALL"),
		}},
	Cols: []arrow.Field{
		column("id", tString, "Row identity: role_kind/namespace/role_name#rule_index/api_group/resource/verb (or the URL).", false),
		column("role_kind", tString, "Role or ClusterRole.", false),
		column("namespace", tString, "Namespace of a Role; NULL for a ClusterRole.", true),
		column("role_name", tString, "Name of the role.", false),
		column("rule_index", tInt64, "0-based position (index) of the rule in the role.", false),
		column("api_group", tString, "API group ('' = core, '*' = all); NULL for non-resource rules.", true),
		column("resource", tString, "Resource, possibly with subresource (pods/exec) or '*'; NULL for non-resource rules.", true),
		column("resource_names", tStrList, "Restricts the rule to these object names; empty = all.", false),
		column("verb", tString, "One verb: get, list, watch, create, update, patch, delete, deletecollection, escalate, bind, impersonate or '*'.", false),
		column("non_resource_url", tString, "Non-resource URL such as /healthz or /metrics*; NULL for resource rules.", true),
	},
	FieldSelectors: map[string]string{"role_name": "metadata.name"},
	Row: func(raw []byte, meta *metav1.ObjectMeta, rc *rowCtx) ([][]any, error) {
		var o struct {
			Rules []rbacv1.PolicyRule `json:"rules"`
		}
		if err := jsonUnmarshal(raw, &o); err != nil {
			return nil, err
		}
		ns := nilIfEmpty(meta.Namespace)
		var rows [][]any
		for i, r := range o.Rules {
			names := strList(r.ResourceNames)
			seen := map[string]bool{}
			add := func(key string, row []any) {
				id := fmt.Sprintf("%s/%s/%s#%d/%s", rc.src.Kind, meta.Namespace, meta.Name, i, key)
				if !seen[id] {
					seen[id] = true
					rows = append(rows, append([]any{id}, row...))
				}
			}
			for _, verb := range r.Verbs {
				for _, g := range r.APIGroups {
					for _, res := range r.Resources {
						add(g+"/"+res+"/"+verb, []any{rc.src.Kind, ns, meta.Name, int64(i), g, res, names, verb, nil})
					}
				}
				for _, u := range r.NonResourceURLs {
					add(u+"/"+verb, []any{rc.src.Kind, ns, meta.Name, int64(i), nil, nil, names, verb, u})
				}
			}
		}
		return rows, nil
	},
}

// rbacSubjectsDef flattens RoleBindings and ClusterRoleBindings to one row per subject.
var rbacSubjectsDef = &resDef{
	Name: "rbac_subjects", NamespaceCol: "namespace", Key: []string{"id"},
	Sources: append(rbacSrc("rolebindings", "RoleBinding", true), rbacSrc("clusterrolebindings", "ClusterRoleBinding", false)...),
	Docs: fnDocs{Title: "RBAC Subjects", Category: "rbac",
		Keywords:    "kubernetes, rbac, subjects, service accounts, users, groups, bindings, access review",
		Description: "Every RoleBinding and ClusterRoleBinding flattened to one row per subject, with the role it grants",
		Examples: []vgi.CatalogExample{
			ex("Roles granted to one service account", "SELECT binding_kind, namespace, binding_name, role_kind, role_name FROM kubernetes.main.rbac_subjects WHERE subject_kind = 'ServiceAccount' AND subject_namespace = 'kube-system' AND subject_name = 'default' ORDER BY ALL"),
		}},
	Cols: []arrow.Field{
		column("id", tString, "Row identity: binding_kind/namespace/binding_name#subject_index.", false),
		column("binding_kind", tString, "RoleBinding or ClusterRoleBinding.", false),
		column("namespace", tString, "Namespace of a RoleBinding (where the grant applies); NULL for a ClusterRoleBinding.", true),
		column("binding_name", tString, "Name of the binding.", false),
		column("role_kind", tString, "Role or ClusterRole.", false),
		column("role_name", tString, "Granted role.", false),
		column("subject_kind", tString, "User, Group or ServiceAccount.", false),
		column("subject_name", tString, "User name, group name or ServiceAccount name.", false),
		column("subject_namespace", tString, "Namespace of a ServiceAccount subject.", true),
	},
	FieldSelectors: map[string]string{"binding_name": "metadata.name"},
	Row: func(raw []byte, meta *metav1.ObjectMeta, rc *rowCtx) ([][]any, error) {
		var o struct {
			RoleRef  rbacv1.RoleRef   `json:"roleRef"`
			Subjects []rbacv1.Subject `json:"subjects"`
		}
		if err := jsonUnmarshal(raw, &o); err != nil {
			return nil, err
		}
		var rows [][]any
		for i, s := range o.Subjects {
			id := fmt.Sprintf("%s/%s/%s#%d", rc.src.Kind, meta.Namespace, meta.Name, i)
			rows = append(rows, []any{id, rc.src.Kind, nilIfEmpty(meta.Namespace), meta.Name, o.RoleRef.Kind, o.RoleRef.Name,
				s.Kind, s.Name, nilIfEmpty(s.Namespace)})
		}
		return rows, nil
	},
}
