package util

import (
	"bufio"
	"io"
	"log"
	"os"

	"go.opentelemetry.io/collector/pdata/ptrace"
)

func LoadData(path string) ([]ptrace.Traces, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	unmarshaler := &ptrace.JSONUnmarshaler{}
	var traces []ptrace.Traces

	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
		}

		trace, err := unmarshaler.UnmarshalTraces(line)
		if err != nil {
			log.Printf("反序列化trace失败: %v", err)
			continue
		}
		traces = append(traces, trace)
	}
	return traces, nil
}
