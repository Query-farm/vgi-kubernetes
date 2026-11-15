// Translating pushed SQL filters into Kubernetes list requests.
//
// Only the top-level conjuncts of the WHERE clause are translated; every
// filter is still re-applied to the returned rows (filters.go), so a
// translation only has to return a superset of the matching objects:
//
//   - namespace = 'x' / namespace IN (...)  -> /namespaces/x/... list paths
//   - name = 'x'                            -> fieldSelector metadata.name=x
//   - <col> = v / <col> != v for the columns a table maps to API field
//     selectors (pods.phase -> status.phase, pods.node_name -> spec.nodeName,
//     events.reason -> reason, ...)       -> fieldSelector
//   - label_selector = '<selector>'         -> labelSelector (the column echoes it)
//   - field_selector = '<selector>'         -> fieldSelector (the column echoes it)

package kubernetes

import (
	"fmt"
	"sort"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow/scalar"
	"k8s.io/apimachinery/pkg/fields"
)

// plan is the list request a scan issues.
type plan struct {
	// Namespaces to list; nil means all namespaces in one request.
	Namespaces []string
	// Field is the fieldSelector terms, Label the labelSelector.
	Field []string
	Label string
	// LabelEcho / FieldEcho are the values of the selector columns.
	LabelEcho *string
	FieldEcho *string
	// Empty means the filters can match nothing (e.g. namespace IN ()).
	Empty bool
}

// pred is a translatable top-level conjunct: col = v, col != v or col IN (vals).
type pred struct {
	Col  string
	Op   string // "eq", "ne" or "in"
	Vals []string
}

// selectorValue renders a literal as a selector value.
func selectorValue(s scalar.Scalar) (string, bool) {
	v, ok := scalarGo(s)
	if !ok || v == nil {
		return "", false
	}
	switch x := v.(type) {
	case string:
		return x, true
	case bool:
		return fmt.Sprint(x), true
	case int64:
		return fmt.Sprint(x), true
	}
	return "", false
}

// conjuncts flattens top-level AND filters.
func conjuncts(fs []vgi.Filter) []vgi.Filter {
	var out []vgi.Filter
	for _, f := range fs {
		if a, ok := f.(*vgi.AndFilter); ok {
			out = append(out, conjuncts(a.Children)...)
			continue
		}
		out = append(out, f)
	}
	return out
}

// predsOf extracts the translatable conjuncts of the pushed filters.
func predsOf(pf *vgi.PushdownFilters) []pred {
	if pf == nil {
		return nil
	}
	var out []pred
	for _, f := range conjuncts(pf.Filters) {
		switch ft := f.(type) {
		case *vgi.ConstantFilter:
			v, ok := selectorValue(ft.Value)
			if !ok {
				continue
			}
			switch ft.Op {
			case vgi.OpEQ:
				out = append(out, pred{Col: f.ColumnName(), Op: "eq", Vals: []string{v}})
			case vgi.OpNE:
				out = append(out, pred{Col: f.ColumnName(), Op: "ne", Vals: []string{v}})
			}
		case *vgi.InFilter:
			if ft.Values == nil {
				continue
			}
			vals := []string{}
			all := true
			for i := 0; i < ft.Values.Len(); i++ {
				s, err := scalar.GetScalar(ft.Values, i)
				if err != nil {
					all = false
					break
				}
				if !s.IsValid() {
					continue // NULL never matches IN
				}
				v, ok := selectorValue(s)
				if !ok {
					all = false
					break
				}
				vals = append(vals, v)
			}
			if all {
				out = append(out, pred{Col: f.ColumnName(), Op: "in", Vals: vals})
			}
		}
	}
	return out
}

// planPushdown derives the list request from the pushed filters.
func planPushdown(pf *vgi.PushdownFilters, nsCol string, fieldSel map[string]string) plan {
	return planPreds(predsOf(pf), nsCol, fieldSel)
}

// planPreds derives the list request from translatable conjuncts.
func planPreds(preds []pred, nsCol string, fieldSel map[string]string) plan {
	var p plan
	var nsSet map[string]bool
	restrictNS := func(vals []string) {
		next := map[string]bool{}
		for _, v := range vals {
			if nsSet == nil || nsSet[v] {
				next[v] = true
			}
		}
		nsSet = next
	}
	fieldTerms := map[string]bool{}
	echo := func(cur **string, v string) {
		if *cur == nil {
			vv := v
			*cur = &vv
		} else if **cur != v {
			p.Empty = true
		}
	}
	for _, pr := range preds {
		eq := pr.Op == "eq" || (pr.Op == "in" && len(pr.Vals) == 1)
		switch {
		case pr.Col == nsCol && nsCol != "" && (pr.Op == "eq" || pr.Op == "in"):
			restrictNS(pr.Vals)
		case pr.Col == colLabelSelector:
			if eq {
				echo(&p.LabelEcho, pr.Vals[0])
				p.Label = *p.LabelEcho
			}
		case pr.Col == colFieldSelector:
			if eq {
				echo(&p.FieldEcho, pr.Vals[0])
				fieldTerms[*p.FieldEcho] = true
			}
		default:
			path, ok := fieldSel[pr.Col]
			if !ok {
				continue
			}
			switch {
			case eq:
				fieldTerms[path+"="+fields.EscapeValue(pr.Vals[0])] = true
			case pr.Op == "ne":
				fieldTerms[path+"!="+fields.EscapeValue(pr.Vals[0])] = true
			}
		}
	}
	if nsSet != nil {
		if len(nsSet) == 0 {
			p.Empty = true
		}
		for ns := range nsSet {
			p.Namespaces = append(p.Namespaces, ns)
		}
		sort.Strings(p.Namespaces)
	}
	for t := range fieldTerms {
		p.Field = append(p.Field, t)
	}
	sort.Strings(p.Field)
	return p
}
