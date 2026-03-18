package converter

import (
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type TypeMapper struct {
	typeMap          map[string]arrow.DataType
	decimalPrecision int32
	decimalScale     int32
}

func NewTypeMapper() *TypeMapper {
	return &TypeMapper{
		typeMap: map[string]arrow.DataType{
			"INT":       arrow.PrimitiveTypes.Int64,
			"INTEGER":   arrow.PrimitiveTypes.Int64,
			"BIGINT":    arrow.PrimitiveTypes.Int64,
			"BIGSERIAL": arrow.PrimitiveTypes.Int64,
			"SMALLINT":  arrow.PrimitiveTypes.Int32,
			"TINYINT":   arrow.PrimitiveTypes.Int8,
			"INT8":      arrow.PrimitiveTypes.Int8,
			"INT16":     arrow.PrimitiveTypes.Int16,
			"INT32":     arrow.PrimitiveTypes.Int32,
			"INT64":     arrow.PrimitiveTypes.Int64,
			"FLOAT":     arrow.PrimitiveTypes.Float32,
			"FLOAT4":    arrow.PrimitiveTypes.Float32,
			"FLOAT8":    arrow.PrimitiveTypes.Float64,
			"DOUBLE":    arrow.PrimitiveTypes.Float64,
			"REAL":      arrow.PrimitiveTypes.Float32,
			"DECIMAL":   arrow.PrimitiveTypes.Float64,
			"NUMERIC":   arrow.PrimitiveTypes.Float64,
			"NUMBER":    arrow.PrimitiveTypes.Float64,
			"VARCHAR":   &arrow.StringType{},
			"CHAR":      &arrow.StringType{},
			"CHARACTER": &arrow.StringType{},
			"TEXT":      &arrow.StringType{},
			"NVARCHAR":  &arrow.StringType{},
			"NCHAR":     &arrow.StringType{},
			"BOOL":      &arrow.BooleanType{},
			"BOOLEAN":   &arrow.BooleanType{},
			"DATE":      arrow.PrimitiveTypes.Date32,
			"TIME":      &arrow.Time32Type{Unit: arrow.Microsecond},
			"TIMESTAMP": &arrow.TimestampType{Unit: arrow.Microsecond},
			"DATETIME":  &arrow.TimestampType{Unit: arrow.Microsecond},
			"BLOB":      &arrow.BinaryType{},
			"BYTEA":     &arrow.BinaryType{},
			"BINARY":    &arrow.BinaryType{},
			"VARBINARY": &arrow.BinaryType{},
			"JSON":      &arrow.StringType{},
			"JSONB":     &arrow.StringType{},
			"UUID":      &arrow.StringType{},
			"XML":       &arrow.StringType{},
		},
		decimalPrecision: 38,
		decimalScale:     10,
	}
}

func (m *TypeMapper) ToArrowType(sqlType string, precision, scale int) (arrow.DataType, error) {
	normalized := strings.ToUpper(strings.TrimSpace(sqlType))

	if dt, ok := m.typeMap[normalized]; ok {
		if normalized == "DECIMAL" || normalized == "NUMERIC" || strings.HasPrefix(normalized, "DECIMAL") || strings.HasPrefix(normalized, "NUMERIC") {
			p := m.decimalPrecision
			s := m.decimalScale
			if precision > 0 {
				p = int32(precision)
			}
			if scale > 0 {
				s = int32(scale)
			}
			return &arrow.Decimal128Type{
				Precision: p,
				Scale:     s,
			}, nil
		}
		return dt, nil
	}

	if strings.HasPrefix(normalized, "DECIMAL") || strings.HasPrefix(normalized, "NUMERIC") {
		p := m.decimalPrecision
		s := m.decimalScale
		if precision > 0 {
			p = int32(precision)
		}
		if scale > 0 {
			s = int32(scale)
		}
		return &arrow.Decimal128Type{
			Precision: p,
			Scale:     s,
		}, nil
	}

	if strings.HasPrefix(normalized, "TIMESTAMP") {
		return &arrow.TimestampType{
			Unit:     arrow.Microsecond,
			TimeZone: "",
		}, nil
	}

	return &arrow.StringType{}, nil
}

func (m *TypeMapper) NewBuilder(mem memory.Allocator, dt arrow.DataType) array.Builder {
	switch dt.ID() {
	case arrow.INT8:
		return array.NewInt8Builder(mem)
	case arrow.INT16:
		return array.NewInt16Builder(mem)
	case arrow.INT32:
		return array.NewInt32Builder(mem)
	case arrow.INT64:
		return array.NewInt64Builder(mem)
	case arrow.UINT8:
		return array.NewUint8Builder(mem)
	case arrow.UINT16:
		return array.NewUint16Builder(mem)
	case arrow.UINT32:
		return array.NewUint32Builder(mem)
	case arrow.UINT64:
		return array.NewUint64Builder(mem)
	case arrow.FLOAT32:
		return array.NewFloat32Builder(mem)
	case arrow.FLOAT64:
		return array.NewFloat64Builder(mem)
	case arrow.STRING:
		return array.NewStringBuilder(mem)
	case arrow.BOOL:
		return array.NewBooleanBuilder(mem)
	case arrow.DATE32:
		return array.NewDate32Builder(mem)
	case arrow.DATE64:
		return array.NewDate64Builder(mem)
	case arrow.TIME32:
		return array.NewTime32Builder(mem, dt.(*arrow.Time32Type))
	case arrow.TIME64:
		return array.NewTime64Builder(mem, dt.(*arrow.Time64Type))
	case arrow.TIMESTAMP:
		return array.NewTimestampBuilder(mem, dt.(*arrow.TimestampType))
	case arrow.BINARY:
		return array.NewBinaryBuilder(mem, dt.(*arrow.BinaryType))
	case arrow.DECIMAL:
		dt := dt.(*arrow.Decimal128Type)
		return array.NewDecimal128Builder(mem, dt)
	default:
		return array.NewStringBuilder(mem)
	}
}
