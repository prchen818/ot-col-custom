// file: processor/tailsamplingprocessor/internal/tracepicker/encoder.go

package tracepicker

import (
	"container/list"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"go.opentelemetry.io/collector/pdata/pcommon" // 确保引入 pcommon 包
	"sort"
	"strings"
	"sync"
	"time"
	"math"

	"go.opentelemetry.io/collector/pdata/ptrace"
)

// --- HistPool 实现 (这部分没有改动) ---

// stat 保存延迟的均值和标准差
type stat struct {
	mu, std float64
}

// HistPool 存储每个操作的历史延迟数据。
type HistPool struct {
	limit    int
	mutex    sync.RWMutex
	data     map[string]*list.List // key: label, value: 历史延迟列表
	db       map[string]stat       // key: label, value: 统计数据
	recalcTh int                   // 重新计算统计数据的阈值
	count    int                   // 全局计数器
}

// NewHistPool 是 HistPool 的构造函数。
func NewHistPool(height uint64) *HistPool {
	return &HistPool{
		limit:    int(height),
		data:     make(map[string]*list.List),
		db:       make(map[string]stat),
		recalcTh: 100, // 初始阈值
	}
}

// Add 添加一条延迟记录，并可能触发统计更新。
func (p *HistPool) Add(label string, duration time.Duration) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if _, ok := p.data[label]; !ok {
		p.data[label] = list.New()
	}
	p.data[label].PushBack(duration.Seconds() * 1000) // 存为毫秒
	if p.data[label].Len() > p.limit {
		p.data[label].Remove(p.data[label].Front())
	}

	p.count++
	if p.count >= p.recalcTh {
		p.recalculateAll()
		p.count = 0
		if p.recalcTh < 2000 { // 避免阈值无限增长
			p.recalcTh += 100
		}
	}
}

// getMuStd 安全地获取一个操作的延迟统计数据。
func (p *HistPool) getMuStd(label string) (mu, std float64) {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	if s, ok := p.db[label]; ok {
		return s.mu, s.std
	}
	return 0, 0
}

// recalculateAll 更新所有操作的统计数据。
func (p *HistPool) recalculateAll() {
	for label, l := range p.data {
		var sum, sumSq float64
		var count float64
		for e := l.Front(); e != nil; e = e.Next() {
			val := e.Value.(float64)
			sum += val
			sumSq += val * val
			count++
		}
		mu := sum / count
		variance := (sumSq / count) - (mu * mu) // 方差
		if variance < 0 {
			variance = 0 // 避免浮点数精度问题导致负数
		}
		std := math.Sqrt(variance) // 标准差
		p.db[label] = stat{mu: mu, std: std}
	}
}

// --- BFSEncoder 实现 ---

// BFSEncoder 负责将 trace 编码为 typeID 并检测异常。
type BFSEncoder struct {
	pool *HistPool
}

// NewBFSEncoder 是 BFSEncoder 的构造函数。
func NewBFSEncoder(pool *HistPool) *BFSEncoder {
	return &BFSEncoder{pool: pool}
}

// Encode 将 ptrace.Traces 对象编码为 typeID 并判断其是否异常。
func (e *BFSEncoder) Encode(trace ptrace.Traces) (typeID string, isAbnormal bool) {
	// 修正：将 ptrace.SpanID 改为 pcommon.SpanID
	spanMap := make(map[pcommon.SpanID]ptrace.Span)
	childrenMap := make(map[pcommon.SpanID][]pcommon.SpanID)
	var rootID pcommon.SpanID
	var spans []ptrace.Span

	rs := trace.ResourceSpans()
	for i := 0; i < rs.Len(); i++ {
		// 在这里获取 Resource 级别的 service.name
		resource := rs.At(i).Resource()
		serviceNameAttr, ok := resource.Attributes().Get("service.name")
		serviceName := "unknown.service"
		if ok {
			serviceName = serviceNameAttr.Str()
		}

		ils := rs.At(i).ScopeSpans()
		for j := 0; j < ils.Len(); j++ {
			sps := ils.At(j).Spans()
			for k := 0; k < sps.Len(); k++ {
				span := sps.At(k)
				
				// 将 resource 的 service.name 附加到 span attributes 中，方便后续处理
				// 这是一个简化处理，更好的方式是直接传递 resource 对象或 serviceName
				span.Attributes().PutStr("service.name", serviceName)

				spans = append(spans, span)
				spanMap[span.SpanID()] = span
				if span.ParentSpanID().IsEmpty() {
					rootID = span.SpanID()
				} else {
					childrenMap[span.ParentSpanID()] = append(childrenMap[span.ParentSpanID()], span.SpanID())
				}
			}
		}
	}

	// 1. 异常检测
	var expectedDurationMs, trueDurationMs float64
	var hasError bool
	for _, span := range spans {
		if span.Status().Code() == ptrace.StatusCodeError {
			hasError = true
		}
		label := getSpanLabel(span)
		duration := span.EndTimestamp().AsTime().Sub(span.StartTimestamp().AsTime())
		e.pool.Add(label, duration)
		mu, std := e.pool.getMuStd(label)
		expectedDurationMs += mu + 5*std // 对应 Python 的 mu + 5 * std
		trueDurationMs += float64(duration.Milliseconds())
	}
	isAbnormal = hasError || (trueDurationMs > expectedDurationMs && expectedDurationMs > 0)

	// 2. BFS 编码生成 typeID
	if rootID.IsEmpty() {
		return "empty_root", isAbnormal
	}

	var path []string
	queue := []pcommon.SpanID{rootID}

	for len(queue) > 0 {
		levelSize := len(queue)
		levelNodes := []ptrace.Span{}
		for i := 0; i < levelSize; i++ {
			if node, ok := spanMap[queue[i]]; ok {
				levelNodes = append(levelNodes, node)
			}
		}
		queue = queue[levelSize:]

		sort.Slice(levelNodes, func(i, j int) bool {
			return getSpanLabel(levelNodes[i]) < getSpanLabel(levelNodes[j])
		})

		for _, node := range levelNodes {
			path = append(path, getSpanLabel(node))
			if children, ok := childrenMap[node.SpanID()]; ok {
				queue = append(queue, children...)
			}
		}
	}

	pathString := strings.Join(path, "->")
	h := sha1.New()
	h.Write([]byte(pathString))
	typeID = hex.EncodeToString(h.Sum(nil))

	return typeID, isAbnormal
}

// getSpanLabel 从 span 中提取 "service:operation" 标签。
func getSpanLabel(span ptrace.Span) string {
	serviceName := "unknown.service"
	if val, ok := span.Attributes().Get("service.name"); ok {
		serviceName = val.Str()
	}
	return fmt.Sprintf("%s:%s", serviceName, span.Name())
}