package writer

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/apache/arrow/go/v12/arrow"
)

type Writer interface {
	Write(record arrow.Record) error
	Close() error
}

type WriterFactory struct {
	outputDir string
	overwrite bool
}

func NewWriterFactory(outputDir string, overwrite bool) *WriterFactory {
	return &WriterFactory{
		outputDir: outputDir,
		overwrite: overwrite,
	}
}

func (f *WriterFactory) Create(output string, format string, schema *arrow.Schema) (Writer, error) {
	if output == "" {
		return nil, fmt.Errorf("输出文件名不能为空")
	}

	outputPath := filepath.Join(f.outputDir, output)

	if !f.overwrite {
		if _, err := os.Stat(outputPath); err == nil {
			return nil, fmt.Errorf("文件已存在: %s", outputPath)
		}
	}

	dir := filepath.Dir(outputPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("创建输出目录失败: %w", err)
	}

	switch format {
	case "parquet":
		return NewParquetWriter(outputPath, schema)
	case "arrow":
		return NewArrowWriter(outputPath, schema)
	default:
		return nil, fmt.Errorf("不支持的格式: %s", format)
	}
}
