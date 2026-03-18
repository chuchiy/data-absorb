package writer

import (
	"fmt"
	"os"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/apache/arrow/go/v12/parquet"
	"github.com/apache/arrow/go/v12/parquet/compress"
	"github.com/apache/arrow/go/v12/parquet/pqarrow"
)

type ParquetWriter struct {
	writer *pqarrow.FileWriter
	file   *os.File
}

func NewParquetWriter(path string, schema *arrow.Schema) (*ParquetWriter, error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("创建文件失败: %w", err)
	}

	props := parquet.NewWriterProperties(
		parquet.WithVersion(parquet.V2_LATEST),
		parquet.WithCompression(compress.Codecs.Zstd),
	)

	arrowProps := pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(memory.NewGoAllocator()))

	writer, err := pqarrow.NewFileWriter(schema, file, props, arrowProps)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("创建 Parquet 写入器失败: %w", err)
	}

	return &ParquetWriter{
		writer: writer,
		file:   file,
	}, nil
}

func (w *ParquetWriter) Write(record arrow.Record) error {
	return w.writer.Write(record)
}

func (w *ParquetWriter) Close() error {
	if w.writer != nil {
		err := w.writer.Close()
		w.writer = nil
		if err != nil {
			return fmt.Errorf("关闭 Parquet 写入器失败: %w", err)
		}
	}
	return nil
}
