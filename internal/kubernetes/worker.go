// Package kubernetes exposes a live Kubernetes cluster's API to DuckDB as
// read-only SQL: catalog kubernetes, schema main (core objects, logs,
// metrics, discovery) and, per ATTACH, schema resources (resource_tables).
//
// Run it:
//
//	vgi-kubernetes-worker                      # stdio: what DuckDB launches by default
//	vgi-kubernetes-worker --http --port 8000   # HTTP: for Docker / hosted deploys
//
// Attach it from DuckDB:
//
//	CREATE SECRET (TYPE kubernetes, KUBECONFIG '...', CONTEXT 'prod');
//	ATTACH 'kubernetes' (TYPE vgi, LOCATION '/abs/path/to/bin/vgi-kubernetes-worker');
package kubernetes

import (
	"github.com/Query-farm/vgi-go/vgi"
)

// catalogTables are the resource tables of schema main.
var catalogTables = []*resDef{
	namespacesDef, nodesDef, podsDef, podContainersDef, deploymentsDef, replicaSetsDef, statefulSetsDef,
	daemonSetsDef, jobsDef, cronJobsDef, hpasDef, servicesDef, endpointSlicesDef, ingressesDef,
	networkPoliciesDef, configMapsDef, secretsDef, serviceAccountsDef, persistentVolumesDef,
	persistentVolumeClaimsDef, storageClassesDef, priorityClassesDef, rolesDef, clusterRolesDef,
	roleBindingsDef, clusterRoleBindingsDef, rbacRulesDef, rbacSubjectsDef, eventsDef,
	nodeMetricsDef, podMetricsDef, apiResourcesDef, apiServersDef,
}

func init() {
	for _, d := range append(catalogTables, resourcesDef) {
		d.Docs.Title = "Kubernetes " + d.Docs.Title
		llm := d.Docs.Description + "."
		if d.Docs.DocLLM != "" {
			llm = d.Docs.DocLLM
		}
		md := d.Docs.Description + "."
		if d.Fetch != nil {
			d.Docs.DocLLM = llm + " Columns: " + columnList(d) + "."
			d.Docs.DocMD = "## " + d.Docs.Title + "\n\n" + md
			continue
		}
		finishDocs(d, llm, md)
	}
	podMetricsDef.Docs.DocLLM += metricsNote
	nodeMetricsDef.Docs.DocLLM += metricsNote
	resourcesDef.Docs.DocLLM = "resources(api_version, resource) lists every object of any listable API resource, " +
		"custom resources included: api_version is 'v1' for the core group or '<group>/<version>' (see " +
		"api_resources.api_version), resource is the plural (crontabs), singular, kind (CronTab) or a short name. " +
		"Spec and status are JSON: use ->> / json_extract to read fields. " + resourcesDef.Docs.DocLLM
}

func columnList(d *resDef) string {
	s := ""
	for i, f := range d.Schema().Fields() {
		if i > 0 {
			s += ", "
		}
		s += f.Name
	}
	return s
}

// allExamples lists every object's examples for the schema-level tag.
func allExamples() []exampleQuery {
	// One example per table keeps the schema-level list within agent context limits.
	var lists [][]vgi.CatalogExample
	for _, d := range catalogTables {
		lists = append(lists, d.Docs.Examples[:1])
	}
	return exampleQueries(append(lists, resourcesDef.Docs.Examples[:1], podLogsDocs.Examples[:1])...)
}

// NewWorker builds the worker. Extra options (logging, storage) are appended
// after the catalog options.
func NewWorker(opts ...vgi.WorkerOption) *vgi.Worker {
	sourceURL, version := SourceURL, Version
	base := []vgi.WorkerOption{
		vgi.WithCatalogName(CatalogName),
		vgi.WithCatalogComment(CatalogComment),
		vgi.WithCatalogTags(CatalogTags),
		vgi.WithCatalogInfo(vgi.CatalogInfo{SourceURL: &sourceURL, ImplementationVersion: &version}),
		vgi.WithSchemaComments(map[string]string{"main": MainSchemaComment}),
		vgi.WithSchemaTags(map[string]map[string]string{"main": schemaTags(allExamples())}),
		vgi.WithSecretTypes(KubernetesSecretType),
		vgi.WithAttachOptions(AttachOptionSpecs...),
		vgi.WithAttachValidator(validateAttach),
		vgi.WithCatalogContentsHandler(catalogContents),
		vgi.WithSchemaContentsHandler(schemaTables),
		vgi.WithAttachTableGetHandler(tableGet),
		vgi.WithAttachScanFunctionGetHandler(scanFunctionGet),
	}
	w := vgi.NewWorker(append(base, opts...)...)
	for _, d := range catalogTables {
		w.RegisterCatalogTable("main", catalogTable(d))
	}
	w.RegisterTable(newTable(newResourcesFunc()))
	w.RegisterTable(vgi.AsTableFunction[LogState](&podLogsFunc{}))
	w.RegisterScalar(vgi.AsScalarFunction[quantityArgs](&quantityFn{}))
	return w
}
