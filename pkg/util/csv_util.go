package util

import (
	"encoding/csv"
	"os"
	"sync"
)

var (
	csvFile   *os.File
	csvWriter *csv.Writer
	csvMutex  sync.Mutex
)

// InitCSV 初始化CSV文件，支持自定义header
func InitCSV(path string, header []string) error {
	var err error
	csvFile, err = os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	csvWriter = csv.NewWriter(csvFile)
	if header != nil && len(header) > 0 {
		_ = csvWriter.Write(header)
		csvWriter.Flush()
	}
	return nil
}

// WriteCSV 写入一行数据到CSV，支持任意字段
func WriteCSV(record []string) {
	csvMutex.Lock()
	defer csvMutex.Unlock()
	if csvWriter != nil {
		_ = csvWriter.Write(record)
		csvWriter.Flush()
	}
}

// CloseCSV 关闭CSV文件
func CloseCSV() {
	csvMutex.Lock()
	defer csvMutex.Unlock()
	if csvWriter != nil {
		csvWriter.Flush()
	}
	if csvFile != nil {
		_ = csvFile.Close()
	}
}
