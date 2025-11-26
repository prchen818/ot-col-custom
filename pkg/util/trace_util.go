package util

import (
	"bufio"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
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

func Encode(trace ptrace.Traces) (typeID string, isAbnormal bool) {
	var rootID pcommon.SpanID
	labels := make(map[pcommon.SpanID]string)
	children := make(map[pcommon.SpanID][]pcommon.SpanID)
	spansMap := make(map[pcommon.SpanID]ptrace.Span)

	// 收集所有 span 信息
	rsList := trace.ResourceSpans()
	for i := 0; i < rsList.Len(); i++ {
		rs := rsList.At(i)
		serviceNameAttr, ok := rs.Resource().Attributes().Get("service.name")
		serviceName := "unknown"
		if ok {
			serviceName = serviceNameAttr.Str()
		}
		scopeSpans := rs.ScopeSpans()
		for j := 0; j < scopeSpans.Len(); j++ {
			ss := scopeSpans.At(j)
			spans := ss.Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				sid := span.SpanID()
				spansMap[sid] = span
				label := fmt.Sprintf("%s:%s", serviceName, span.Name())
				labels[sid] = label
				parentID := span.ParentSpanID()
				if parentID.IsEmpty() {
					if !rootID.IsEmpty() {
						isAbnormal = true // 多根
					}
					rootID = sid
				} else {
					children[parentID] = append(children[parentID], sid)
				}
			}
		}
	}

	if rootID.IsEmpty() {
		return "empty_root", true
	}

	// 修改 BFS：不再按整层排序；按宽度优先依次弹出节点，对其子节点单独按 label 排序后入队。
	var path []string
	queue := []pcommon.SpanID{rootID}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		path = append(path, labels[cur])
		childIDs := children[cur]
		if len(childIDs) > 0 {
			sort.SliceStable(childIDs, func(i, j int) bool { return labels[childIDs[i]] < labels[childIDs[j]] })
			queue = append(queue, childIDs...)
		}
	}

	pathString := strings.Join(path, "->")
	h := sha1.New()
	h.Write([]byte(pathString))
	typeID = hex.EncodeToString(h.Sum(nil))
	return typeID, isAbnormal
}
