package writer

import (
	"fmt"
	"os"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type ArrowWriter struct {
	writer *ipc.FileWriter
	file   *os.File
}

func NewArrowWriter(path string, schema *arrow.Schema) (*ArrowWriter, error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("创建文件失败: %w", err)
	}

	writer, err := ipc.NewFileWriter(file,
		ipc.WithSchema(schema),
		ipc.WithAllocator(memory.NewGoAllocator()),
	)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("创建 Arrow 写入器失败: %w", err)
	}

	return &ArrowWriter{
		writer: writer,
		file:   file,
	}, nil
}

func (w *ArrowWriter) Write(record arrow.Record) error {
	return w.writer.Write(record)
}

func (w *ArrowWriter) Close() error {
	if err := w.writer.Close(); err != nil {
		w.file.Close()
		return fmt.Errorf("关闭 Arrow 写入器失败: %w", err)
	}
	return w.file.Close()
}
