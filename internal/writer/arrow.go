package writer

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type ArrowWriter struct {
	writer    *ipc.FileWriter
	targetPath string
	tmpPath   string
}

func NewArrowWriter(path string, schema *arrow.Schema) (*ArrowWriter, error) {
	dir := filepath.Dir(path)
	tmpPath := filepath.Join(dir, fmt.Sprintf("%s.%d.tmp", filepath.Base(path), time.Now().UnixNano()))

	file, err := os.Create(tmpPath)
	if err != nil {
		return nil, fmt.Errorf("创建临时文件失败: %w", err)
	}

	writer, err := ipc.NewFileWriter(file,
		ipc.WithSchema(schema),
		ipc.WithAllocator(memory.NewGoAllocator()),
	)
	if err != nil {
		file.Close()
		os.Remove(tmpPath)
		return nil, fmt.Errorf("创建 Arrow 写入器失败: %w", err)
	}

	return &ArrowWriter{
		writer:    writer,
		targetPath: path,
		tmpPath:   tmpPath,
	}, nil
}

func (w *ArrowWriter) Write(record arrow.Record) error {
	return w.writer.Write(record)
}

func (w *ArrowWriter) Close() error {
	if err := w.writer.Close(); err != nil {
		os.Remove(w.tmpPath)
		return fmt.Errorf("关闭 Arrow 写入器失败: %w", err)
	}

	if err := os.Rename(w.tmpPath, w.targetPath); err != nil {
		os.Remove(w.tmpPath)
		return fmt.Errorf("重命名文件失败: %w", err)
	}

	return nil
}
