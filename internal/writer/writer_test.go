package writer

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestParquetWriter(t *testing.T) {
	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "test.parquet")

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: &arrow.StringType{}},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	writer, err := NewParquetWriter(outputPath, schema)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	mem := memory.NewGoAllocator()

	idBuilder := array.NewInt64Builder(mem)
	idBuilder.AppendValues([]int64{1, 2, 3}, nil)
	nameBuilder := array.NewStringBuilder(mem)
	nameBuilder.AppendValues([]string{"a", "b", "c"}, nil)
	valueBuilder := array.NewFloat64Builder(mem)
	valueBuilder.AppendValues([]float64{1.1, 2.2, 3.3}, nil)

	record := array.NewRecord(schema, []arrow.Array{
		idBuilder.NewArray(),
		nameBuilder.NewArray(),
		valueBuilder.NewArray(),
	}, 3)

	if err := writer.Write(record); err != nil {
		t.Fatalf("Write error: %v", err)
	}

	record.Release()
	if err := writer.Close(); err != nil {
		t.Fatalf("Close error: %v", err)
	}

	if _, err := os.Stat(outputPath); err != nil {
		t.Errorf("Output file not created: %v", err)
	}
}

func TestArrowWriter(t *testing.T) {
	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "test.arrow")

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: &arrow.StringType{}},
	}, nil)

	writer, err := NewArrowWriter(outputPath, schema)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	mem := memory.NewGoAllocator()

	idBuilder := array.NewInt64Builder(mem)
	idBuilder.AppendValues([]int64{1, 2, 3}, nil)
	nameBuilder := array.NewStringBuilder(mem)
	nameBuilder.AppendValues([]string{"hello", "world", "test"}, nil)

	record := array.NewRecord(schema, []arrow.Array{
		idBuilder.NewArray(),
		nameBuilder.NewArray(),
	}, 3)

	if err := writer.Write(record); err != nil {
		t.Fatalf("Write error: %v", err)
	}

	record.Release()
	if err := writer.Close(); err != nil {
		t.Fatalf("Close error: %v", err)
	}

	if _, err := os.Stat(outputPath); err != nil {
		t.Errorf("Output file not created: %v", err)
	}
}

func TestWriterFactory(t *testing.T) {
	tmpDir := t.TempDir()
	factory := NewWriterFactory(tmpDir, false)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	writer, err := factory.Create("test.parquet", "parquet", schema)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}
	writer.Close()

	outputPath := filepath.Join(tmpDir, "test.parquet")
	if _, err := os.Stat(outputPath); err != nil {
		t.Errorf("Output file not created: %v", err)
	}
}

func TestWriterFactory_Overwrite(t *testing.T) {
	tmpDir := t.TempDir()
	factory := NewWriterFactory(tmpDir, true)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	writer, err := factory.Create("test.parquet", "parquet", schema)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}
	writer.Close()

	writer2, err := factory.Create("test.parquet", "parquet", schema)
	if err != nil {
		t.Fatalf("Failed to create writer (overwrite): %v", err)
	}
	writer2.Close()
}

func TestWriterFactory_NoOverwrite(t *testing.T) {
	tmpDir := t.TempDir()
	factory := NewWriterFactory(tmpDir, false)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	writer, err := factory.Create("test.parquet", "parquet", schema)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}
	writer.Close()

	_, err = factory.Create("test.parquet", "parquet", schema)
	if err == nil {
		t.Error("Should fail when file exists and overwrite is false")
	}
}
