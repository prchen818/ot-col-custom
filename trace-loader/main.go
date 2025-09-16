package main

import (
	"bufio"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"io"
	"log"
	"os"
)

func main() {
	path := "traces"
	traces, err := LoadData(path)
	if err != nil {
		log.Fatalf("加载trace数据失败: %v", err)
	}
	log.Printf("成功加载 %d 条trace", len(traces))
}

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
