package main

import (
	"bufio"
	"io"
	"log"
	"os"

	"go.opentelemetry.io/collector/pdata/ptrace"
)

func main() {
	traces, err := LoadData("traces")
	if err != nil {
		log.Fatalf("加载数据失败: %v", err)
	}

	for i, trace := range traces {
		allSame := true
		var firstTraceID []byte
		resourceSpans := trace.ResourceSpans()
		for j := 0; j < resourceSpans.Len(); j++ {
			rs := resourceSpans.At(j)
			scopeSpans := rs.ScopeSpans()
			for k := 0; k < scopeSpans.Len(); k++ {
				ss := scopeSpans.At(k)
				spans := ss.Spans()
				for l := 0; l < spans.Len(); l++ {
					span := spans.At(l)
					traceID := span.TraceID()
					if l == 0 && k == 0 && j == 0 {
						firstTraceID = traceID[:]
					} else {
						if string(traceID[:]) != string(firstTraceID) {
							allSame = false
							break
						}
					}
				}
				if !allSame {
					break
				}
			}
			if !allSame {
				break
			}
		}
		if allSame {
			log.Printf("trace[%d] 所有span的traceID一致: %x", i, firstTraceID)
		} else {
			log.Printf("trace[%d] 存在不同的traceID", i)
		}
	}
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
