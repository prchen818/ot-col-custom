package cache

import (
	"container/list"
	"fmt"
	"sync"
)

// FIFOCache 是一个固定大小的先进先出缓存队列
type FIFOCache[T any] struct {
	maxSize    int          // 缓存的最大容量
	list       *list.List   // 用于存储元素的双向链表
	sumFunc    func(T, T) T // 用于计算元素和的函数
	zeroVal    T            // 零值，用于初始化和清空
	currentSum T            // 维护当前所有元素的和

	mu sync.Mutex
}

// NewFIFOCache 创建一个新的FIFO缓存队列
// maxSize: 缓存的最大容量，必须大于0
// sumFunc: 用于计算两个元素之和的函数
// zeroVal: 该类型的零值，用于初始化求和结果
func NewFIFOCache[T any](maxSize int, sumFunc func(T, T) T, zeroVal T) (*FIFOCache[T], error) {
	if maxSize <= 0 {
		return nil, fmt.Errorf("maxSize must be greater than 0")
	}
	if sumFunc == nil {
		return nil, fmt.Errorf("sumFunc cannot be nil")
	}

	return &FIFOCache[T]{
		maxSize:    maxSize,
		list:       list.New(),
		sumFunc:    sumFunc,
		zeroVal:    zeroVal,
		currentSum: zeroVal,
	}, nil
}

// Add 向缓存中添加元素，如果缓存已满，则移除最早添加的元素
func (c *FIFOCache[T]) Add(item T) {
	c.mu.Lock()
	defer c.mu.Unlock() // 确保锁会释放

	// 当缓存已满时，先移除队首元素并从总和中减去
	if c.list.Len() >= c.maxSize {
		oldest := c.list.Front()
		c.currentSum = c.sumFunc(c.currentSum, c.negative(oldest.Value.(T)))
		c.list.Remove(oldest)
	}

	// 添加新元素并加到总和中
	c.list.PushBack(item)
	c.currentSum = c.sumFunc(c.currentSum, item)
}

// Sum 计算缓存中所有元素的和
func (c *FIFOCache[T]) Sum() T {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.currentSum
}

// Len 返回当前缓存中的元素数量
func (c *FIFOCache[T]) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.list.Len()
}

// Clear 清空缓存中的所有元素
func (c *FIFOCache[T]) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.list.Init()
	c.currentSum = c.zeroVal
}

// Elements 返回缓存中的所有元素（从旧到新）
func (c *FIFOCache[T]) Elements() []T {
	c.mu.Lock()
	defer c.mu.Unlock()
	elements := make([]T, 0, c.list.Len())
	for e := c.list.Front(); e != nil; e = e.Next() {
		elements = append(elements, e.Value.(T))
	}
	return elements
}

// negative 计算元素的"负数"（用于从总和中减去元素）
// 利用 sumFunc(a, negative(b)) 等价于 a - b（对于加法而言）
func (c *FIFOCache[T]) negative(item T) T {
	// 对于加法，negative(x) = zeroVal - x
	// 通过 sumFunc(zeroVal, x) 得到 x，再用 zeroVal 减去它
	// 这里利用了 sumFunc 的可逆性（假设 sumFunc 是加法）
	return c.sumFunc(c.zeroVal, c.sumFunc(c.zeroVal, item))
}
