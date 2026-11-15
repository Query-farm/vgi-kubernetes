// quantity_value(quantity): a Kubernetes resource quantity string ('250m',
// '64Mi', '1.5Gi', '2e3') as a number. Needs no cluster, so it carries the
// worker's executable examples, and is useful on the text quantities of
// nodes.capacity / nodes.allocatable and of resources() JSON.

package kubernetes

import (
	"context"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"k8s.io/apimachinery/pkg/api/resource"
)

type quantityArgs struct {
	Quantity *array.String `vgi:"pos=0,const=false,doc=A Kubernetes quantity such as '250m' (CPU), '64Mi' or '1.5Gi' (bytes), '2k' or '1e3'."`
}

// quantityFn is quantity_value(quantity).
type quantityFn struct{}

// executableExample is one vgi.executable_examples entry.
type executableExample struct {
	Description string  `json:"description"`
	SQL         string  `json:"sql"`
	Expected    [][]any `json:"expected_result"`
}

var quantityDocs = fnDocs{
	Title: "Kubernetes Quantity Parser", Category: "utilities",
	Keywords:    "kubernetes, quantity, resource quantity, millicores, mebibytes, gibibytes, parse units",
	Description: "Parse a Kubernetes resource quantity ('250m', '64Mi', '1.5Gi') into a number",
	DocLLM: "quantity_value(quantity `VARCHAR`) returns the numeric value of a Kubernetes resource quantity as a " +
		"`DOUBLE`, the way the API server parses it: '250m' is 0.25 (CPU cores), '64Mi' is 67108864 (bytes, binary " +
		"suffixes Ki Mi Gi Ti Pi Ei), '1G' is 1000000000 (decimal suffixes k M G T P E), '1e3' is 1000. Multiply a CPU " +
		"value by 1000 for millicores. Invalid quantities and NULL give NULL. Use it on text quantities such as " +
		"nodes.capacity['nvidia.com/gpu'] or values read from resources() JSON; the typed tables already expose " +
		"cpu_*_millicores and memory_*_bytes columns. Needs no cluster connection.",
	DocMD: "## quantity_value(quantity)\n\nA Kubernetes quantity string as a number: `'250m'` → 0.25, `'64Mi'` → " +
		"67108864, `'1G'` → 1000000000.\n\nInvalid quantities and NULL give NULL.",
	Examples: []vgi.CatalogExample{
		ex("Hugepages capacity per node, in bytes", "SELECT name, kubernetes.main.quantity_value(capacity['hugepages-2Mi']) AS hugepages_bytes FROM kubernetes.main.nodes ORDER BY name"),
	},
}

var quantityExecutable = []executableExample{
	{Description: "Parse CPU, memory and decimal quantities",
		SQL: "SELECT q, kubernetes.main.quantity_value(q) AS value " +
			"FROM (VALUES ('250m'), ('64Mi'), ('1G'), ('1.5Gi'), ('2'), ('bogus')) t(q) ORDER BY q",
		Expected: [][]any{{"1.5Gi", "1610612736.0"}, {"1G", "1000000000.0"}, {"2", "2.0"}, {"250m", "0.25"},
			{"64Mi", "67108864.0"}, {"bogus", nil}}},
}

// quantityValue parses one quantity (ok false when invalid).
func quantityValue(s string) (float64, bool) {
	q, err := resource.ParseQuantity(s)
	if err != nil {
		return 0, false
	}
	return q.AsApproximateFloat64(), true
}

func (*quantityFn) Name() string { return "quantity_value" }

func (*quantityFn) Metadata() vgi.FunctionMetadata {
	tags := objectTags(quantityDocs)
	tags["vgi.executable_examples"] = mustJSON(quantityExecutable)
	return vgi.FunctionMetadata{
		Description: quantityDocs.Description,
		Stability:   vgi.StabilityConsistent,
		Tags:        tags,
		Examples:    quantityDocs.Examples,
	}
}

func (*quantityFn) OnBindTyped(*quantityArgs, *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindResult(arrow.PrimitiveTypes.Float64)
}

func (*quantityFn) ProcessTyped(_ context.Context, _ *quantityArgs, _ *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	col := batch.Column(0)
	b := array.NewFloat64Builder(memory.NewGoAllocator())
	defer b.Release()
	for i := 0; i < int(batch.NumRows()); i++ {
		if col.IsNull(i) {
			b.AppendNull()
			continue
		}
		if v, ok := quantityValue(vgi.GetStringValue(col, i)); ok {
			b.Append(v)
		} else {
			b.AppendNull()
		}
	}
	arr := b.NewArray()
	defer arr.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "result", Type: arrow.PrimitiveTypes.Float64, Nullable: true}}, nil)
	return array.NewRecordBatch(schema, []arrow.Array{arr}, batch.NumRows()), nil
}
