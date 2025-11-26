package main

import (
	"log"
	"math"
	"time"

	"github.com/prchen818/ot-col-custom/pkg/util"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func main() {
	traces, err := util.LoadData("data/trainticket/ot_trace.txt")
	if err != nil {
		log.Fatalf("加载数据失败: %v", err)
	}
	log.Printf("traces: %v", len(traces))
	//checkErrorTrace(traces)
	//checkGroupBy(traces)
	//checkErrorSpan(traces)
	checkTypes(traces)
}

func checkTypes(traces []ptrace.Traces) {
	typeCount := make(map[string]int)
	for _, trace := range traces {
		id, abnormal := util.Encode(trace)
		if abnormal {
			id = "abnormal"
		}
		if _, ok := typeCount[id]; !ok {
			typeCount[id] = 1
		} else {
			typeCount[id]++
		}
	}
	log.Printf("检测到的类型 (%d)", len(typeCount))
	log.Printf("----------------")
	for k, v := range typeCount {
		log.Printf("数量: %d, 类型: %s", v, k)
	}
}

func checkErrorSpan(traces []ptrace.Traces) bool {
	totalSpanCount := 0
	errorSpanCount := 0
	spanLatenciesByName := make(map[string][]time.Duration)

	for _, trace := range traces {
		resourceSpans := trace.ResourceSpans()
		for j := 0; j < resourceSpans.Len(); j++ {
			rs := resourceSpans.At(j)
			scopeSpans := rs.ScopeSpans()
			for k := 0; k < scopeSpans.Len(); k++ {
				ss := scopeSpans.At(k)
				spans := ss.Spans()
				for l := 0; l < spans.Len(); l++ {
					span := spans.At(l)
					totalSpanCount++

					if span.Status().Code() == ptrace.StatusCodeError {
						errorSpanCount++
					}

					latency := span.EndTimestamp().AsTime().Sub(span.StartTimestamp().AsTime())
					spanLatenciesByName[span.Name()] = append(spanLatenciesByName[span.Name()], latency)
				}
			}
		}
	}

	if totalSpanCount > 0 {
		log.Printf("错误Span占比: %.2f%% (%d/%d)", float64(errorSpanCount)*100/float64(totalSpanCount), errorSpanCount, totalSpanCount)
	} else {
		log.Println("没有找到任何Span。")
		return true
	}

	log.Printf("--- 按 span.name 划分的延迟分布 %d ---", len(spanLatenciesByName))
	totalOutliers3Sigma := 0
	for name, latencies := range spanLatenciesByName {
		count := len(latencies)
		if count < 2 { // 需要至少2个点来计算标准差
			log.Printf("Span Name: %s (数量: %d) - 数据点不足，跳过正态分布计算", name, count)
			continue
		}

		var sum time.Duration
		for _, lat := range latencies {
			sum += lat
		}
		// 均值 (ns)
		mean := float64(sum) / float64(count)

		// 计算标准差 (ns)
		var sumOfSquares float64
		for _, lat := range latencies {
			diff := float64(lat) - mean
			sumOfSquares += diff * diff
		}
		variance := sumOfSquares / float64(count)
		stdDev := math.Sqrt(variance)

		// 计算超出3-sigma的span数量
		threshold3Sigma := mean + 3*stdDev
		outliers3Sigma := 0
		for _, lat := range latencies {
			if float64(lat) > threshold3Sigma {
				outliers3Sigma++
			}
		}
		totalOutliers3Sigma += outliers3Sigma

		log.Printf("Span Name: %s (数量: %d)", name, count)
		log.Printf("  延迟正态分布: 均值=%v, 标准差=%v", time.Duration(mean), time.Duration(stdDev))
		log.Printf("  大于3-sigma的Span数量: %d", outliers3Sigma)
	}
	log.Printf("--- 统计汇总 ---")
	log.Printf("所有Span中大于各自组内3-sigma的Span总个数: %d", totalOutliers3Sigma)
	return errorSpanCount > 0
}

func checkErrorTrace(traces []ptrace.Traces) {
	errorCount := 0
	// 统计在假设每条trace中所有span的traceID相同的前提下，缺少根span的trace数量
	missingRootCount := 0

	// firstSeen: traceIDHex -> first trace index where it appeared
	firstSeen := make(map[pcommon.TraceID]int)
	// duplicates: set of traceIDHex that appear in multiple top-level traces
	duplicates := make(map[pcommon.TraceID]struct{})

	for i, trace := range traces {
		errFlag := false
		// seenInThisTrace 用于避免同一 trace 内重复计数同一 traceID
		seenInThisTrace := make(map[pcommon.TraceID]struct{})
		// hasRoot 表示本条 trace 中是否存在根span（ParentSpanID 为空）
		hasRoot := 0

		resourceSpans := trace.ResourceSpans()
		for j := 0; j < resourceSpans.Len(); j++ {
			rs := resourceSpans.At(j)
			scopeSpans := rs.ScopeSpans()
			for k := 0; k < scopeSpans.Len(); k++ {
				ss := scopeSpans.At(k)
				spans := ss.Spans()
				for l := 0; l < spans.Len(); l++ {
					span := spans.At(l)

					if span.Status().Code() == ptrace.StatusCodeError {
						errFlag = true
						log.Printf("trace[%d] 存在错误状态的span: %s", i, span.SpanID())
					}

					// 如果 ParentSpanID 为空（零值），说明这是一个根span
					if span.ParentSpanID().IsEmpty() {
						hasRoot++
					}

					// 使用十六进制表示 TraceID，更直观且唯一
					traceID := span.TraceID()
					if _, ok := seenInThisTrace[traceID]; ok {
						continue
					}
					seenInThisTrace[traceID] = struct{}{}

					if firstIdx, ok := firstSeen[traceID]; ok {
						if firstIdx != i {
							log.Printf("trace[%d] 与 trace[%d] 存在相同的TraceID: %s", i, firstIdx, traceID)
							duplicates[traceID] = struct{}{}
						}
					} else {
						firstSeen[traceID] = i
					}
				}
			}
		}

		// 如果本条 trace 中所有 span 的 traceID 相同（seenInThisTrace 长度为 1），
		// 并且没有检测到任何根 span，则统计为缺少根span的trace
		if len(seenInThisTrace) == 1 {
			if hasRoot == 0 {
				missingRootCount++
				log.Printf("trace[%d] 缺少 root span", i)
			} else if hasRoot > 1 {
				log.Printf("trace[%d] 存在多个 root span (%d 个)", i, hasRoot)
			}
		}

		if errFlag {
			errorCount++
		}
	}

	log.Printf("总共有 %d/%d 条trace包含错误状态的span", errorCount, len(traces))
	log.Printf("检测到 %d 个跨trace重复的TraceID", len(duplicates))
	log.Printf("缺少根span的trace数量: %d", missingRootCount)
}

func checkGroupBy(traces []ptrace.Traces) {
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
		if !allSame {
			log.Printf("trace[%d] 存在不同的traceID", i)
		}
	}
}
