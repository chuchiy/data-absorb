package writer

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

type ParquetWriter struct {
	writer    *pqarrow.FileWriter
	targetPath string
	tmpPath   string
}

func NewParquetWriter(path string, schema *arrow.Schema) (*ParquetWriter, error) {
	dir := filepath.Dir(path)
	tmpPath := filepath.Join(dir, fmt.Sprintf("%s.%d.tmp", filepath.Base(path), time.Now().UnixNano()))

	file, err := os.Create(tmpPath)
	if err != nil {
		return nil, fmt.Errorf("创建临时文件失败: %w", err)
	}

	props := parquet.NewWriterProperties(
		parquet.WithVersion(parquet.V2_LATEST),
		parquet.WithCompression(compress.Codecs.Zstd),
	)

	arrowProps := pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(memory.NewGoAllocator()))

	writer, err := pqarrow.NewFileWriter(schema, file, props, arrowProps)
	if err != nil {
		file.Close()
		os.Remove(tmpPath)
		return nil, fmt.Errorf("创建 Parquet 写入器失败: %w", err)
	}

	return &ParquetWriter{
		writer:    writer,
		targetPath: path,
		tmpPath:   tmpPath,
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
			os.Remove(w.tmpPath)
			return fmt.Errorf("关闭 Parquet 写入器失败: %w", err)
		}
	}

	if err := os.Rename(w.tmpPath, w.targetPath); err != nil {
		os.Remove(w.tmpPath)
		return fmt.Errorf("重命名文件失败: %w", err)
	}

	return nil
}
