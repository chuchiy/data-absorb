package converter

import (
	"database/sql"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type SchemaBuilder struct {
	mapper *TypeMapper
}

func NewSchemaBuilder() *SchemaBuilder {
	return &SchemaBuilder{
		mapper: NewTypeMapper(),
	}
}

func (b *SchemaBuilder) Build(columns []*sql.ColumnType) (*arrow.Schema, error) {
	fields := make([]arrow.Field, 0, len(columns))

	for _, col := range columns {
		name := col.Name()
		dbTypeName := col.DatabaseTypeName()

		dt, err := b.mapper.ToArrowType(dbTypeName, 0, 0)
		if err != nil {
			return nil, fmt.Errorf("列 %s 类型映射失败: %w", name, err)
		}

		fields = append(fields, arrow.Field{
			Name:     name,
			Type:     dt,
			Nullable: true,
		})
	}

	return arrow.NewSchema(fields, nil), nil
}

type RowConverter struct {
	schema *arrow.Schema
	mapper *TypeMapper
}

func NewRowConverter(schema *arrow.Schema) *RowConverter {
	return &RowConverter{
		schema: schema,
		mapper: NewTypeMapper(),
	}
}

func (c *RowConverter) ConvertRow(values []interface{}) ([]array.Builder, error) {
	if len(values) != len(c.schema.Fields()) {
		return nil, fmt.Errorf("列数不匹配: expected %d, got %d", len(c.schema.Fields()), len(values))
	}

	mem := memory.NewGoAllocator()
	builders := make([]array.Builder, len(c.schema.Fields()))

	for i, field := range c.schema.Fields() {
		builders[i] = c.mapper.NewBuilder(mem, field.Type)
	}

	for i, val := range values {
		if val == nil {
			builders[i].AppendNull()
			continue
		}

		switch b := builders[i].(type) {
		case *array.Int8Builder:
			b.Append(convToInt8(val))
		case *array.Int16Builder:
			b.Append(convToInt16(val))
		case *array.Int32Builder:
			b.Append(convToInt32(val))
		case *array.Int64Builder:
			b.Append(convToInt64(val))
		case *array.Uint8Builder:
			b.Append(convToUint8(val))
		case *array.Uint16Builder:
			b.Append(convToUint16(val))
		case *array.Uint32Builder:
			b.Append(convToUint32(val))
		case *array.Uint64Builder:
			b.Append(convToUint64(val))
		case *array.Float32Builder:
			b.Append(convToFloat32(val))
		case *array.Float64Builder:
			b.Append(convToFloat64(val))
		case *array.BooleanBuilder:
			b.Append(convToBool(val))
		case *array.StringBuilder:
			b.Append(fmt.Sprintf("%v", val))
	case *array.BinaryBuilder:
		b.Append(convToBytes(val))
	case *array.Decimal128Builder:
		b.Append(convToDecimal128(val))
	default:
		b.AppendNull()
	}
	}

	return builders, nil
}

func (c *RowConverter) BuildRecord(builders []array.Builder, n int64) arrow.Record {
	arrays := make([]arrow.Array, len(builders))

	for i, b := range builders {
		arrays[i] = b.NewArray()
		b.Release()
	}

	record := array.NewRecord(c.schema, arrays, n)
	for _, arr := range arrays {
		arr.Release()
	}

	return record
}

func convToInt8(v interface{}) int8 {
	switch n := v.(type) {
	case int8:
		return n
	case int16:
		return int8(n)
	case int32:
		return int8(n)
	case int64:
		return int8(n)
	case int:
		return int8(n)
	case uint8:
		return int8(n)
	case uint16:
		return int8(n)
	case uint32:
		return int8(n)
	case uint64:
		return int8(n)
	case uint:
		return int8(n)
	default:
		return 0
	}
}

func convToInt16(v interface{}) int16 {
	switch n := v.(type) {
	case int8:
		return int16(n)
	case int16:
		return n
	case int32:
		return int16(n)
	case int64:
		return int16(n)
	case int:
		return int16(n)
	case uint8:
		return int16(n)
	case uint16:
		return int16(n)
	case uint32:
		return int16(n)
	case uint64:
		return int16(n)
	case uint:
		return int16(n)
	default:
		return 0
	}
}

func convToInt32(v interface{}) int32 {
	switch n := v.(type) {
	case int8:
		return int32(n)
	case int16:
		return int32(n)
	case int32:
		return n
	case int64:
		return int32(n)
	case int:
		return int32(n)
	case uint8:
		return int32(n)
	case uint16:
		return int32(n)
	case uint32:
		return int32(n)
	case uint64:
		return int32(n)
	case uint:
		return int32(n)
	default:
		return 0
	}
}

func convToInt64(v interface{}) int64 {
	switch n := v.(type) {
	case int8:
		return int64(n)
	case int16:
		return int64(n)
	case int32:
		return int64(n)
	case int64:
		return n
	case int:
		return int64(n)
	case uint8:
		return int64(n)
	case uint16:
		return int64(n)
	case uint32:
		return int64(n)
	case uint64:
		return int64(n)
	case uint:
		return int64(n)
	default:
		return 0
	}
}

func convToUint8(v interface{}) uint8 {
	switch n := v.(type) {
	case int8:
		return uint8(n)
	case int16:
		return uint8(n)
	case int32:
		return uint8(n)
	case int64:
		return uint8(n)
	case int:
		return uint8(n)
	case uint8:
		return n
	case uint16:
		return uint8(n)
	case uint32:
		return uint8(n)
	case uint64:
		return uint8(n)
	case uint:
		return uint8(n)
	default:
		return 0
	}
}

func convToUint16(v interface{}) uint16 {
	switch n := v.(type) {
	case int8:
		return uint16(n)
	case int16:
		return uint16(n)
	case int32:
		return uint16(n)
	case int64:
		return uint16(n)
	case int:
		return uint16(n)
	case uint8:
		return uint16(n)
	case uint16:
		return n
	case uint32:
		return uint16(n)
	case uint64:
		return uint16(n)
	case uint:
		return uint16(n)
	default:
		return 0
	}
}

func convToUint32(v interface{}) uint32 {
	switch n := v.(type) {
	case int8:
		return uint32(n)
	case int16:
		return uint32(n)
	case int32:
		return uint32(n)
	case int64:
		return uint32(n)
	case int:
		return uint32(n)
	case uint8:
		return uint32(n)
	case uint16:
		return uint32(n)
	case uint32:
		return n
	case uint64:
		return uint32(n)
	case uint:
		return uint32(n)
	default:
		return 0
	}
}

func convToUint64(v interface{}) uint64 {
	switch n := v.(type) {
	case int8:
		return uint64(n)
	case int16:
		return uint64(n)
	case int32:
		return uint64(n)
	case int64:
		return uint64(n)
	case int:
		return uint64(n)
	case uint8:
		return uint64(n)
	case uint16:
		return uint64(n)
	case uint32:
		return uint64(n)
	case uint64:
		return n
	case uint:
		return uint64(n)
	default:
		return 0
	}
}

func convToFloat32(v interface{}) float32 {
	switch n := v.(type) {
	case float32:
		return n
	case float64:
		return float32(n)
	case int8:
		return float32(n)
	case int16:
		return float32(n)
	case int32:
		return float32(n)
	case int64:
		return float32(n)
	case int:
		return float32(n)
	case uint8:
		return float32(n)
	case uint16:
		return float32(n)
	case uint32:
		return float32(n)
	case uint64:
		return float32(n)
	case uint:
		return float32(n)
	default:
		return 0
	}
}

func convToFloat64(v interface{}) float64 {
	switch n := v.(type) {
	case float32:
		return float64(n)
	case float64:
		return n
	case int8:
		return float64(n)
	case int16:
		return float64(n)
	case int32:
		return float64(n)
	case int64:
		return float64(n)
	case int:
		return float64(n)
	case uint8:
		return float64(n)
	case uint16:
		return float64(n)
	case uint32:
		return float64(n)
	case uint64:
		return float64(n)
	case uint:
		return float64(n)
	default:
		return 0
	}
}

func convToBool(v interface{}) bool {
	switch n := v.(type) {
	case bool:
		return n
	case int8:
		return n != 0
	case int16:
		return n != 0
	case int32:
		return n != 0
	case int64:
		return n != 0
	case int:
		return n != 0
	case uint8:
		return n != 0
	case uint16:
		return n != 0
	case uint32:
		return n != 0
	case uint64:
		return n != 0
	case uint:
		return n != 0
	case string:
		return n == "1" || n == "true" || n == "TRUE"
	default:
		return false
	}
}

func convToBytes(v interface{}) []byte {
	switch n := v.(type) {
	case []byte:
		return n
	case string:
		return []byte(n)
	default:
		return nil
	}
}

func convToDecimal128(v interface{}) decimal.Decimal128 {
	switch n := v.(type) {
	case int8:
		return decimal.NewDecimal128FromInt(int64(n) * 10000)
	case int16:
		return decimal.NewDecimal128FromInt(int64(n) * 10000)
	case int32:
		return decimal.NewDecimal128FromInt(int64(n) * 10000)
	case int64:
		return decimal.NewDecimal128FromInt(n * 10000)
	case int:
		return decimal.NewDecimal128FromInt(int64(n) * 10000)
	case uint8:
		return decimal.NewDecimal128FromInt(int64(n) * 10000)
	case uint16:
		return decimal.NewDecimal128FromInt(int64(n) * 10000)
	case uint32:
		return decimal.NewDecimal128FromInt(int64(n) * 10000)
	case uint64:
		return decimal.NewDecimal128FromInt(int64(n) * 10000)
	case uint:
		return decimal.NewDecimal128FromInt(int64(n) * 10000)
	case float32:
		r, _ := decimal.Decimal128FromFloat(float64(n), 38, 10)
		return r
	case float64:
		r, _ := decimal.Decimal128FromFloat(n, 38, 10)
		return r
	case string:
		r, _ := decimal.Decimal128FromString(n, 38, 10)
		return r
	default:
		return decimal.NewDecimal128FromInt(0)
	}
}
