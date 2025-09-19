package csv_util

import (
	"encoding/csv"
	"fmt"
	"os"
	"sync"
)

var (
	csvFile   *os.File
	csvWriter *csv.Writer
	csvMutex  sync.Mutex
)

// InitCSV 初始化CSV文件
func InitCSV(path string) error {
	var err error
	csvFile, err = os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	csvWriter = csv.NewWriter(csvFile)
	_ = csvWriter.Write([]string{"timestamp", "lag", "wait_ms"})
	csvWriter.Flush()
	return nil
}

// WriteCSV 写入一行数据到CSV
func WriteCSV(ts string, lag int64, waitMs int64) {
	csvMutex.Lock()
	defer csvMutex.Unlock()
	if csvWriter != nil {
		_ = csvWriter.Write([]string{ts, fmt.Sprintf("%d", lag), fmt.Sprintf("%d", waitMs)})
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
