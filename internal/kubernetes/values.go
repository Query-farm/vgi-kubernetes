// Row values and Arrow batch building shared by every table.
//
// Each table builds full-width rows ([]any in schema order), the pushed
// filters are re-applied to them (filters.go), and the surviving rows are
// written into the projected output schema.
//
// Go value per Arrow type: string (VARCHAR), int64 (BIGINT/INTEGER), float64
// (DOUBLE), bool (BOOLEAN), time.Time (TIMESTAMPTZ), jsonText or any
// JSON-marshalable value (JSON), []any (LIST), structVal (STRUCT, fields in
// declaration order) and map[string]string (MAP(VARCHAR, VARCHAR)). nil is NULL.

package kubernetes

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Arrow types used by the output schemas.
var (
	tString  = arrow.BinaryTypes.String
	tInt64   = arrow.PrimitiveTypes.Int64
	tFloat64 = arrow.PrimitiveTypes.Float64
	tBool    = arrow.FixedWidthTypes.Boolean
	tTS      = &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
	tStrList = arrow.ListOf(arrow.BinaryTypes.String)
	tStrMap  = arrow.MapOf(arrow.BinaryTypes.String, arrow.BinaryTypes.String)
	tJSON    = mustJSONType()
)

func mustJSONType() arrow.DataType {
	t, err := extensions.NewJSONType(arrow.BinaryTypes.String)
	if err != nil {
		panic(err)
	}
	return t
}

// structOf builds a STRUCT type from fields.
func structOf(fields ...arrow.Field) *arrow.StructType { return arrow.StructOf(fields...) }

// listOf builds a LIST type.
func listOf(t arrow.DataType) *arrow.ListType { return arrow.ListOf(t) }

// sf is a nullable struct field.
func sf(name string, t arrow.DataType) arrow.Field {
	return arrow.Field{Name: name, Type: t, Nullable: true}
}

// jsonText is already-encoded JSON destined for a JSON column.
type jsonText string

// structVal is a STRUCT value, one entry per field in declaration order.
type structVal []any

// ts converts a Kubernetes timestamp to a TIMESTAMPTZ value (nil when unset).
func ts(t *metav1.Time) any {
	if t == nil || t.IsZero() {
		return nil
	}
	return t.UTC().Truncate(time.Microsecond)
}

// tsv is ts for a non-pointer time.
func tsv(t metav1.Time) any { return ts(&t) }

// microTS converts a MicroTime (events.eventTime) to TIMESTAMPTZ.
func microTS(t *metav1.MicroTime) any {
	if t == nil || t.IsZero() {
		return nil
	}
	return t.UTC().Truncate(time.Microsecond)
}

// nilIfEmpty turns "" into NULL.
func nilIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// strPtr dereferences an optional string.
func strPtr(s *string) any {
	if s == nil {
		return nil
	}
	return *s
}

// i32Ptr dereferences an optional int32 to BIGINT.
func i32Ptr(v *int32) any {
	if v == nil {
		return nil
	}
	return int64(*v)
}

// i64Ptr dereferences an optional int64.
func i64Ptr(v *int64) any {
	if v == nil {
		return nil
	}
	return *v
}

// boolPtr dereferences an optional bool.
func boolPtr(v *bool) any {
	if v == nil {
		return nil
	}
	return *v
}

// strList converts a string slice to a LIST value (empty slice stays empty).
func strList(ss []string) []any {
	out := make([]any, len(ss))
	for i, s := range ss {
		out[i] = s
	}
	return out
}

// strMap returns a MAP value; nil maps become an empty map.
func strMap(m map[string]string) any {
	if m == nil {
		return map[string]string{}
	}
	return m
}

// toJSON marshals an open-shaped value for a JSON column (nil for nil).
func toJSON(v any) any {
	if v == nil {
		return nil
	}
	b, err := json.Marshal(v)
	if err != nil || string(b) == "null" {
		return nil
	}
	return jsonText(b)
}

// buildBatch writes rows (full-width, in full's order) into a batch with the
// out schema, which may be a projection of full.
func buildBatch(out, full *arrow.Schema, rows [][]any) (arrow.RecordBatch, error) {
	b := array.NewRecordBuilder(memory.NewGoAllocator(), out)
	defer b.Release()
	idx := make([]int, out.NumFields())
	for i, f := range out.Fields() {
		ix := full.FieldIndices(f.Name)
		if len(ix) == 0 {
			return nil, fmt.Errorf("internal: projected column %q not in schema", f.Name)
		}
		idx[i] = ix[0]
	}
	for _, row := range rows {
		for i := range idx {
			if err := appendValue(b.Field(i), row[idx[i]]); err != nil {
				return nil, fmt.Errorf("column %s: %w", out.Field(i).Name, err)
			}
		}
	}
	return b.NewRecordBatch(), nil
}

func appendValue(fb array.Builder, v any) error {
	if v == nil {
		fb.AppendNull()
		return nil
	}
	switch b := fb.(type) {
	case *array.StringBuilder:
		switch x := v.(type) {
		case string:
			b.Append(x)
		case jsonText:
			b.Append(string(x))
		case time.Time:
			b.Append(x.UTC().Format("2006-01-02 15:04:05.999999+00"))
		case map[string]string, []any, structVal:
			enc, err := json.Marshal(x)
			if err != nil {
				return err
			}
			b.Append(string(enc))
		default:
			b.Append(fmt.Sprint(x))
		}
	case *array.ExtensionBuilder:
		sb, ok := b.StorageBuilder().(*array.StringBuilder)
		if !ok {
			return fmt.Errorf("unsupported extension storage")
		}
		switch x := v.(type) {
		case jsonText:
			sb.Append(string(x))
		default:
			enc, err := json.Marshal(x)
			if err != nil {
				return err
			}
			sb.Append(string(enc))
		}
	case *array.Int64Builder:
		x, ok := asInt64(v)
		if !ok {
			return fmt.Errorf("want integer, got %T", v)
		}
		b.Append(x)
	case *array.Int32Builder:
		x, ok := asInt64(v)
		if !ok {
			return fmt.Errorf("want integer, got %T", v)
		}
		b.Append(int32(x))
	case *array.Float64Builder:
		switch x := v.(type) {
		case float64:
			b.Append(x)
		case int64:
			b.Append(float64(x))
		default:
			return fmt.Errorf("want float64, got %T", v)
		}
	case *array.BooleanBuilder:
		x, ok := v.(bool)
		if !ok {
			return fmt.Errorf("want bool, got %T", v)
		}
		b.Append(x)
	case *array.TimestampBuilder:
		x, ok := v.(time.Time)
		if !ok {
			return fmt.Errorf("want time, got %T", v)
		}
		b.Append(arrow.Timestamp(x.UnixMicro()))
	case *array.MapBuilder:
		m, ok := v.(map[string]string)
		if !ok {
			return fmt.Errorf("want map, got %T", v)
		}
		keys := make([]string, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		b.Append(true)
		kb := b.KeyBuilder().(*array.StringBuilder)
		ib := b.ItemBuilder().(*array.StringBuilder)
		for _, k := range keys {
			kb.Append(k)
			ib.Append(m[k])
		}
	case *array.ListBuilder:
		items, ok := v.([]any)
		if !ok {
			return fmt.Errorf("want list, got %T", v)
		}
		b.Append(true)
		for _, it := range items {
			if err := appendValue(b.ValueBuilder(), it); err != nil {
				return err
			}
		}
	case *array.StructBuilder:
		fields, ok := v.(structVal)
		if !ok || len(fields) != b.NumField() {
			return fmt.Errorf("want struct of %d fields, got %T", b.NumField(), v)
		}
		b.Append(true)
		for i, f := range fields {
			if err := appendValue(b.FieldBuilder(i), f); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unsupported builder %T", fb)
	}
	return nil
}

func asInt64(v any) (int64, bool) {
	switch x := v.(type) {
	case int64:
		return x, true
	case int32:
		return int64(x), true
	case int:
		return int64(x), true
	}
	return 0, false
}

// stagingSchema is full with TIMESTAMPTZ and JSON columns as VARCHAR: the
// embedded DuckDB filter evaluator does not accept those Arrow types.
func stagingSchema(full *arrow.Schema) *arrow.Schema {
	fields := make([]arrow.Field, full.NumFields())
	for i, f := range full.Fields() {
		switch f.Type.ID() {
		case arrow.TIMESTAMP, arrow.EXTENSION:
			f.Type = tString
		}
		fields[i] = f
	}
	return arrow.NewSchema(fields, nil)
}
