// Workload controllers: deployments, replica sets, stateful sets, daemon
// sets, jobs, cron jobs and horizontal pod autoscalers.

package kubernetes

import (
	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	batchv1 "k8s.io/api/batch/v1"
)

func groupSrc(group, version, resource, kind string, namespaced bool) []Source {
	return []Source{{Group: group, Version: version, Resource: resource, Kind: kind, Namespaced: namespaced}}
}

func appsConditions(n int, each func(i int) (string, string, string, string, any)) ([]any, map[string]string) {
	out := make([]any, 0, n)
	st := map[string]string{}
	for i := 0; i < n; i++ {
		typ, status, reason, msg, t := each(i)
		st[typ] = status
		out = append(out, structVal{typ, status, nilIfEmpty(reason), nilIfEmpty(msg), t})
	}
	return out, st
}

var templateCols = []arrow.Field{
	column("images", tStrList, "Distinct container images of the pod template.", false),
	column("containers", listOf(containerSpecType), containerSpecDoc, false),
}

// ---------------------------------------------------------------- deployments

var deploymentsDef = &resDef{
	Name: "deployments", Meta: true, Key: []string{"uid"},
	Sources: groupSrc("apps", "v1", "deployments", "Deployment", true),
	Docs: fnDocs{Title: "Deployments", Category: "workloads",
		Keywords:    "kubernetes, deployments, rollout, replicas, availability, images",
		Description: "Deployments with desired, ready, available and updated replicas, strategy, images and conditions",
		Examples: []vgi.CatalogExample{
			ex("Deployments with fewer ready replicas than desired", "SELECT namespace, name, replicas, ready_replicas FROM kubernetes.main.deployments WHERE coalesce(ready_replicas, 0) < replicas ORDER BY namespace, name"),
			ex("Images deployed per namespace", "SELECT namespace, image, count(*) AS deployments FROM kubernetes.main.deployments, UNNEST(images) AS u(image) GROUP BY ALL ORDER BY namespace, image"),
		}},
	Cols: append([]arrow.Field{
		column("replicas", tInt64, "Desired count of pods (spec.replicas).", true),
		column("ready_replicas", tInt64, "Count of pods passing readiness.", true),
		column("available_replicas", tInt64, "Count of pods available for at least minReadySeconds.", true),
		column("updated_replicas", tInt64, "Count of pods running the latest template.", true),
		column("unavailable_replicas", tInt64, "Count of pods still needed for full availability.", true),
		column("available", tBool, "Whether the Available condition is True.", true),
		column("progressing", tBool, "Whether the Progressing condition is True.", true),
		column("paused", tBool, "Whether the rollout is paused.", false),
		column("strategy", tString, "RollingUpdate or Recreate.", true),
		column("max_surge", tString, "Rolling update maxSurge (number or percent).", true),
		column("max_unavailable", tString, "Rolling update maxUnavailable (number or percent).", true),
		column("min_ready_seconds", tInt64, "Seconds a new pod must be ready before it counts as available.", true),
		column("revision", tString, "Current rollout revision (deployment.kubernetes.io/revision).", true),
		column("generation", tInt64, "Spec revision counter (metadata.generation), incremented on every spec change.", true),
		column("observed_generation", tInt64, "Spec revision number (integer counter) the controller last acted on; below generation = rollout pending.", true),
		column("selector", tString, "Pod label selector in kubectl syntax.", true),
		column("service_account", tString, "ServiceAccount of the pod template.", true),
	}, append(templateCols, column("conditions", conditionType, conditionsDoc, false))...),
	Row: typed(func(o *appsv1.Deployment, _ *rowCtx) [][]any {
		conds, st := appsConditions(len(o.Status.Conditions), func(i int) (string, string, string, string, any) {
			c := o.Status.Conditions[i]
			return string(c.Type), string(c.Status), c.Reason, c.Message, tsv(c.LastTransitionTime)
		})
		var surge, unavail any
		if ru := o.Spec.Strategy.RollingUpdate; ru != nil {
			if ru.MaxSurge != nil {
				surge = ru.MaxSurge.String()
			}
			if ru.MaxUnavailable != nil {
				unavail = ru.MaxUnavailable.String()
			}
		}
		s := o.Status
		return one(i32Ptr(o.Spec.Replicas), int64(s.ReadyReplicas), int64(s.AvailableReplicas), int64(s.UpdatedReplicas),
			int64(s.UnavailableReplicas), conditionTrue(st, "Available"), conditionTrue(st, "Progressing"), o.Spec.Paused,
			nilIfEmpty(string(o.Spec.Strategy.Type)), surge, unavail, int64(o.Spec.MinReadySeconds),
			nilIfEmpty(o.Annotations["deployment.kubernetes.io/revision"]), o.Generation, s.ObservedGeneration,
			selectorString(o.Spec.Selector), nilIfEmpty(o.Spec.Template.Spec.ServiceAccountName),
			images(&o.Spec.Template.Spec), containerSpecs(&o.Spec.Template.Spec), conds)
	}),
}

// ---------------------------------------------------------------- replicasets

var replicaSetsDef = &resDef{
	Name: "replicasets", Meta: true, Key: []string{"uid"},
	Sources: groupSrc("apps", "v1", "replicasets", "ReplicaSet", true),
	Docs: fnDocs{Title: "Replica Sets", Category: "workloads",
		Keywords:    "kubernetes, replicasets, rollout history, revisions, replicas",
		Description: "ReplicaSets with replica counts, owning deployment, revision and images",
		Examples: []vgi.CatalogExample{
			ex("Old, scaled-down ReplicaSets kept per deployment", "SELECT namespace, owner_references[1].name AS deployment, count(*) AS old_replicasets FROM kubernetes.main.replicasets WHERE replicas = 0 GROUP BY ALL ORDER BY old_replicasets DESC"),
		}},
	Cols: append([]arrow.Field{
		column("replicas", tInt64, "Desired count of pods.", true),
		column("current_replicas", tInt64, "Observed count of pods (status.replicas).", true),
		column("ready_replicas", tInt64, "Count of ready pods.", true),
		column("available_replicas", tInt64, "Count of available pods.", true),
		column("revision", tString, "Deployment revision of this ReplicaSet.", true),
		column("selector", tString, "Pod label selector in kubectl syntax.", true),
	}, templateCols...),
	Row: typed(func(o *appsv1.ReplicaSet, _ *rowCtx) [][]any {
		s := o.Status
		return one(i32Ptr(o.Spec.Replicas), int64(s.Replicas), int64(s.ReadyReplicas), int64(s.AvailableReplicas),
			nilIfEmpty(o.Annotations["deployment.kubernetes.io/revision"]), selectorString(o.Spec.Selector),
			images(&o.Spec.Template.Spec), containerSpecs(&o.Spec.Template.Spec))
	}),
}

// ---------------------------------------------------------------- statefulsets

var statefulSetsDef = &resDef{
	Name: "statefulsets", Meta: true, Key: []string{"uid"},
	Sources: groupSrc("apps", "v1", "statefulsets", "StatefulSet", true),
	Docs: fnDocs{Title: "Stateful Sets", Category: "workloads",
		Keywords:    "kubernetes, statefulsets, databases, volume claim templates, ordered rollout",
		Description: "StatefulSets with replica counts, revisions, governing service and volume claim templates",
		Examples: []vgi.CatalogExample{
			ex("StatefulSets mid-rollout", "SELECT namespace, name, current_revision, update_revision FROM kubernetes.main.statefulsets WHERE current_revision <> update_revision ORDER BY namespace, name"),
		}},
	Cols: append([]arrow.Field{
		column("replicas", tInt64, "Desired count of pods.", true),
		column("ready_replicas", tInt64, "Count of ready pods.", true),
		column("current_replicas", tInt64, "Count of pods at current_revision.", true),
		column("updated_replicas", tInt64, "Count of pods at update_revision.", true),
		column("available_replicas", tInt64, "Count of available pods.", true),
		column("service_name", tString, "Governing headless service.", true),
		column("pod_management_policy", tString, "OrderedReady or Parallel.", true),
		column("update_strategy", tString, "RollingUpdate or OnDelete.", true),
		column("current_revision", tString, "Revision of the current pods.", true),
		column("update_revision", tString, "Revision being rolled out.", true),
		column("volume_claim_templates", tStrList, "Names of the volume claim templates.", false),
		column("selector", tString, "Pod label selector in kubectl syntax.", true),
	}, templateCols...),
	Row: typed(func(o *appsv1.StatefulSet, _ *rowCtx) [][]any {
		s := o.Status
		var vct []string
		for _, t := range o.Spec.VolumeClaimTemplates {
			vct = append(vct, t.Name)
		}
		return one(i32Ptr(o.Spec.Replicas), int64(s.ReadyReplicas), int64(s.CurrentReplicas), int64(s.UpdatedReplicas),
			int64(s.AvailableReplicas), nilIfEmpty(o.Spec.ServiceName), nilIfEmpty(string(o.Spec.PodManagementPolicy)),
			nilIfEmpty(string(o.Spec.UpdateStrategy.Type)), nilIfEmpty(s.CurrentRevision), nilIfEmpty(s.UpdateRevision),
			strList(vct), selectorString(o.Spec.Selector), images(&o.Spec.Template.Spec), containerSpecs(&o.Spec.Template.Spec))
	}),
}

// ---------------------------------------------------------------- daemonsets

var daemonSetsDef = &resDef{
	Name: "daemonsets", Meta: true, Key: []string{"uid"},
	Sources: groupSrc("apps", "v1", "daemonsets", "DaemonSet", true),
	Docs: fnDocs{Title: "Daemon Sets", Category: "workloads",
		Keywords:    "kubernetes, daemonsets, node agents, scheduling, misscheduled",
		Description: "DaemonSets with desired, scheduled, ready, available and misscheduled pod counts",
		Examples: []vgi.CatalogExample{
			ex("DaemonSets not ready on every node", "SELECT namespace, name, desired_number_scheduled, number_ready FROM kubernetes.main.daemonsets WHERE number_ready < desired_number_scheduled ORDER BY namespace, name"),
		}},
	Cols: append([]arrow.Field{
		column("desired_number_scheduled", tInt64, "Count of nodes that should run the pod.", false),
		column("current_number_scheduled", tInt64, "Count of nodes running at least one pod.", false),
		column("number_ready", tInt64, "Count of nodes with a ready pod.", false),
		column("number_available", tInt64, "Count of nodes with an available pod.", false),
		column("number_unavailable", tInt64, "Count of nodes without an available pod.", false),
		column("number_misscheduled", tInt64, "Count of nodes running the pod that should not.", false),
		column("updated_number_scheduled", tInt64, "Count of nodes running the updated pod.", false),
		column("update_strategy", tString, "RollingUpdate or OnDelete.", true),
		column("node_selector", tStrMap, "Pod template nodeSelector.", false),
		column("selector", tString, "Pod label selector in kubectl syntax.", true),
	}, templateCols...),
	Row: typed(func(o *appsv1.DaemonSet, _ *rowCtx) [][]any {
		s := o.Status
		return one(int64(s.DesiredNumberScheduled), int64(s.CurrentNumberScheduled), int64(s.NumberReady), int64(s.NumberAvailable),
			int64(s.NumberUnavailable), int64(s.NumberMisscheduled), int64(s.UpdatedNumberScheduled),
			nilIfEmpty(string(o.Spec.UpdateStrategy.Type)), strMap(o.Spec.Template.Spec.NodeSelector), selectorString(o.Spec.Selector),
			images(&o.Spec.Template.Spec), containerSpecs(&o.Spec.Template.Spec))
	}),
}

// ---------------------------------------------------------------- jobs

var jobsDef = &resDef{
	Name: "jobs", Meta: true, Key: []string{"uid"},
	Sources: groupSrc("batch", "v1", "jobs", "Job", true),
	Docs: fnDocs{Title: "Jobs", Category: "workloads",
		Keywords:    "kubernetes, jobs, batch, completions, failures, backoff, cronjob runs",
		Description: "Jobs with status, completions, active/succeeded/failed pods, timing and owning CronJob",
		Examples: []vgi.CatalogExample{
			ex("Failed jobs in the last day", "SELECT namespace, name, failed, start_time FROM kubernetes.main.jobs WHERE status = 'Failed' AND start_time > now() - INTERVAL 1 DAY ORDER BY start_time DESC"),
			ex("Average duration of completed jobs per CronJob", "SELECT namespace, cron_job, avg(duration_seconds) AS avg_seconds FROM kubernetes.main.jobs WHERE status = 'Complete' AND cron_job IS NOT NULL GROUP BY ALL ORDER BY avg_seconds DESC"),
		}},
	Cols: append([]arrow.Field{
		column("status", tString, "Complete, Failed, Suspended or Running (from the conditions).", false),
		column("completions", tInt64, "Desired count of successful pods.", true),
		column("parallelism", tInt64, "Maximum count of pods running at once.", true),
		column("active", tInt64, "Count of pods running now.", false),
		column("ready_pods", tInt64, "Count of active pods that are ready.", true),
		column("succeeded", tInt64, "Count of pods that succeeded.", false),
		column("failed", tInt64, "Count of pods that failed.", false),
		column("backoff_limit", tInt64, "Count of pod retries before the job is marked failed.", true),
		column("completion_mode", tString, "NonIndexed or Indexed.", true),
		column("suspend", tBool, "Whether the job is suspended.", true),
		column("start_time", tTS, "When the job started.", true),
		column("completion_time", tTS, "When the job completed (successful jobs only).", true),
		column("duration_seconds", tFloat64, "completion_time - start_time, in seconds.", true),
		column("cron_job", tString, "Name of the owning CronJob.", true),
	}, append(templateCols, column("conditions", conditionType, conditionsDoc+" Types: Complete, Failed, Suspended, FailureTarget, SuccessCriteriaMet.", false))...),
	Row: typed(func(o *batchv1.Job, _ *rowCtx) [][]any {
		conds, st := appsConditions(len(o.Status.Conditions), func(i int) (string, string, string, string, any) {
			c := o.Status.Conditions[i]
			return string(c.Type), string(c.Status), c.Reason, c.Message, tsv(c.LastTransitionTime)
		})
		status := "Running"
		switch {
		case st["Complete"] == "True":
			status = "Complete"
		case st["Failed"] == "True":
			status = "Failed"
		case st["Suspended"] == "True":
			status = "Suspended"
		}
		var dur any
		if o.Status.StartTime != nil && o.Status.CompletionTime != nil {
			dur = o.Status.CompletionTime.Sub(o.Status.StartTime.Time).Seconds()
		}
		var cron any
		for _, r := range o.OwnerReferences {
			if r.Kind == "CronJob" {
				cron = r.Name
			}
		}
		var mode any
		if o.Spec.CompletionMode != nil {
			mode = string(*o.Spec.CompletionMode)
		}
		s := o.Status
		return one(status, i32Ptr(o.Spec.Completions), i32Ptr(o.Spec.Parallelism), int64(s.Active), i32Ptr(s.Ready),
			int64(s.Succeeded), int64(s.Failed), i32Ptr(o.Spec.BackoffLimit), mode, boolPtr(o.Spec.Suspend),
			ts(s.StartTime), ts(s.CompletionTime), dur, cron, images(&o.Spec.Template.Spec), containerSpecs(&o.Spec.Template.Spec), conds)
	}),
}

// ---------------------------------------------------------------- cronjobs

var cronJobsDef = &resDef{
	Name: "cronjobs", Meta: true, Key: []string{"uid"},
	Sources: groupSrc("batch", "v1", "cronjobs", "CronJob", true),
	Docs: fnDocs{Title: "Cron Jobs", Category: "workloads",
		Keywords:    "kubernetes, cronjobs, schedules, batch, last run, suspended",
		Description: "CronJobs with schedule, time zone, suspension, concurrency policy and last runs",
		Examples: []vgi.CatalogExample{
			ex("CronJobs whose last scheduled run did not succeed", "SELECT namespace, name, schedule, last_schedule_time, last_successful_time FROM kubernetes.main.cronjobs WHERE last_successful_time IS NULL OR last_successful_time < last_schedule_time ORDER BY namespace, name"),
		}},
	Cols: append([]arrow.Field{
		column("schedule", tString, "Cron schedule, e.g. */5 * * * *.", false),
		column("time_zone", tString, "IANA time zone of the schedule (NULL = controller's).", true),
		column("suspend", tBool, "Whether new runs are suspended.", true),
		column("concurrency_policy", tString, "Allow, Forbid or Replace.", true),
		column("starting_deadline_seconds", tInt64, "Seconds after a missed schedule time within which the run may still start.", true),
		column("successful_jobs_history_limit", tInt64, "Count of completed jobs kept.", true),
		column("failed_jobs_history_limit", tInt64, "Count of failed jobs kept.", true),
		column("last_schedule_time", tTS, "When a job was last scheduled.", true),
		column("last_successful_time", tTS, "When a job last completed successfully.", true),
		column("active_jobs", tStrList, "Names of running jobs.", false),
	}, templateCols...),
	Row: typed(func(o *batchv1.CronJob, _ *rowCtx) [][]any {
		var active []string
		for _, r := range o.Status.Active {
			active = append(active, r.Name)
		}
		ps := &o.Spec.JobTemplate.Spec.Template.Spec
		return one(o.Spec.Schedule, strPtr(o.Spec.TimeZone), boolPtr(o.Spec.Suspend), nilIfEmpty(string(o.Spec.ConcurrencyPolicy)),
			i64Ptr(o.Spec.StartingDeadlineSeconds), i32Ptr(o.Spec.SuccessfulJobsHistoryLimit), i32Ptr(o.Spec.FailedJobsHistoryLimit),
			ts(o.Status.LastScheduleTime), ts(o.Status.LastSuccessfulTime), strList(active), images(ps), containerSpecs(ps))
	}),
}

// ---------------------------------------------------------------- horizontal pod autoscalers

var hpasDef = &resDef{
	Name: "horizontalpodautoscalers", Meta: true, Key: []string{"uid"},
	Sources: groupSrc("autoscaling", "v2", "horizontalpodautoscalers", "HorizontalPodAutoscaler", true),
	Docs: fnDocs{Title: "Pod Autoscalers", Category: "workloads",
		Keywords:    "kubernetes, hpa, autoscaling, replicas, scaling limits, metrics",
		Description: "HorizontalPodAutoscalers with target, replica bounds, current/desired replicas and metrics",
		Examples: []vgi.CatalogExample{
			ex("Autoscalers pinned at their maximum", "SELECT namespace, name, target_kind, target_name, max_replicas FROM kubernetes.main.horizontalpodautoscalers WHERE current_replicas >= max_replicas ORDER BY namespace, name"),
		}},
	Cols: []arrow.Field{
		column("target_api_version", tString, "apiVersion of the scaled object.", true),
		column("target_kind", tString, "Kind of the scaled object, e.g. Deployment.", true),
		column("target_name", tString, "Name of the scaled object.", true),
		column("min_replicas", tInt64, "Minimum count of pods.", true),
		column("max_replicas", tInt64, "Maximum count of pods.", false),
		column("current_replicas", tInt64, "Count of pods the target runs now.", true),
		column("desired_replicas", tInt64, "Count of pods the autoscaler wants.", true),
		column("last_scale_time", tTS, "When it last scaled.", true),
		column("metrics", tJSON, "spec.metrics: what it scales on (resource, pods, object, external), as JSON.", true),
		column("current_metrics", tJSON, "status.currentMetrics as JSON.", true),
		column("conditions", conditionType, conditionsDoc+" Types: AbleToScale, ScalingActive, ScalingLimited.", false),
	},
	Row: typed(func(o *autoscalingv2.HorizontalPodAutoscaler, _ *rowCtx) [][]any {
		conds, _ := appsConditions(len(o.Status.Conditions), func(i int) (string, string, string, string, any) {
			c := o.Status.Conditions[i]
			return string(c.Type), string(c.Status), c.Reason, c.Message, tsv(c.LastTransitionTime)
		})
		var m, cm any
		if len(o.Spec.Metrics) > 0 {
			m = toJSON(o.Spec.Metrics)
		}
		if len(o.Status.CurrentMetrics) > 0 {
			cm = toJSON(o.Status.CurrentMetrics)
		}
		r := o.Spec.ScaleTargetRef
		return one(nilIfEmpty(r.APIVersion), nilIfEmpty(r.Kind), nilIfEmpty(r.Name), i32Ptr(o.Spec.MinReplicas), int64(o.Spec.MaxReplicas),
			int64(o.Status.CurrentReplicas), int64(o.Status.DesiredReplicas), ts(o.Status.LastScaleTime), m, cm, conds)
	}),
}
