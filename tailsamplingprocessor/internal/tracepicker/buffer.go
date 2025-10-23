// file: processor/tailsamplingprocessor/internal/tracepicker/buffer.go

package tracepicker

import (
	"sync"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// SharedBuffer 缓存追踪数据，直到达到批处理大小。
// 它会区分正常和异常追踪。
type SharedBuffer struct {
	limit          uint64
	mutex          sync.Mutex
	typeMap        map[string][]ptrace.Traces // Key: typeID, Value: 该类型下的正常追踪列表
	abnormalTraces []ptrace.Traces          // 异常追踪列表
	count          uint64                   // 缓冲区中的总追踪数
}

// NewSharedBuffer 是 SharedBuffer 的构造函数。
func NewSharedBuffer(limit uint64) *SharedBuffer {
	return &SharedBuffer{
		limit:          limit,
		typeMap:        make(map[string][]ptrace.Traces),
		abnormalTraces: make([]ptrace.Traces, 0),
	}
}

// Add 将一条追踪添加到缓冲区。
// 它根据 isAbnormal 参数将追踪放入不同的存储区。
func (b *SharedBuffer) Add(typeID string, trace ptrace.Traces, isAbnormal bool) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if isAbnormal {
		b.abnormalTraces = append(b.abnormalTraces, trace)
	} else {
		b.typeMap[typeID] = append(b.typeMap[typeID], trace)
	}
	b.count++
}

// IsFull 检查缓冲区是否已满。
func (b *SharedBuffer) IsFull() bool {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.count >= b.limit
}

// IsEmpty 检查缓冲区是否为空。
func (b *SharedBuffer) IsEmpty() bool {
    b.mutex.Lock()
    defer b.mutex.Unlock()
    return b.count == 0
}

// Count 返回缓冲区中当前的追踪总数。
func (b *SharedBuffer) Count() uint64 {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.count
}

// 这个方法持有锁的时间极短，只在交换指针和计数器时加锁。
func (b *SharedBuffer) SwapAndClear() (map[string][]ptrace.Traces, []ptrace.Traces, uint64) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	// 复制当前数据
	currentNormalTraces := b.typeMap
	currentAbnormalTraces := b.abnormalTraces
	currentCount := b.count

	// 立即清空原缓冲区，使其可以接收新的数据
	b.typeMap = make(map[string][]ptrace.Traces)
	b.abnormalTraces = make([]ptrace.Traces, 0)
	b.count = 0

	return currentNormalTraces, currentAbnormalTraces, currentCount
}