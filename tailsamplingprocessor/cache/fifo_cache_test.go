package cache

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// float64Sum 用于计算两个float64的和，作为sumFunc传入
func float64Sum(a, b float64) float64 {
	return a + b
}

// TestNewFIFOCache 测试FIFOCache的创建
func TestNewFIFOCache(t *testing.T) {
	// 测试无效参数
	_, err := NewFIFOCache[float64](0, float64Sum, 0)
	assert.ErrorContains(t, err, "maxSize must be greater than 0")

	_, err = NewFIFOCache[float64](5, nil, 0)
	assert.ErrorContains(t, err, "sumFunc cannot be nil")

	// 测试有效参数
	cache, err := NewFIFOCache[float64](5, float64Sum, 0)
	require.NoError(t, err)
	assert.Equal(t, 5, cache.maxSize)
	assert.NotNil(t, cache.list)
	assert.Equal(t, 0, cache.Len())
}

// TestFIFOCache_Add 测试添加元素功能
func TestFIFOCache_Add(t *testing.T) {
	cache, err := NewFIFOCache[float64](3, float64Sum, 0)
	require.NoError(t, err)

	// 测试添加元素未达最大容量
	cache.Add(1.1)
	cache.Add(2.2)
	assert.Equal(t, 2, cache.Len())
	assert.Equal(t, []float64{1.1, 2.2}, cache.Elements())

	// 测试添加元素达到最大容量
	cache.Add(3.3)
	assert.Equal(t, 3, cache.Len())
	assert.Equal(t, []float64{1.1, 2.2, 3.3}, cache.Elements())

	// 测试添加元素超过最大容量（触发FIFO淘汰）
	cache.Add(4.4)
	assert.Equal(t, 3, cache.Len())
	assert.Equal(t, []float64{2.2, 3.3, 4.4}, cache.Elements()) // 最早的1.1被淘汰

	cache.Add(5.5)
	assert.Equal(t, []float64{3.3, 4.4, 5.5}, cache.Elements()) // 2.2被淘汰
}

// TestFIFOCache_Sum 测试元素求和功能
func TestFIFOCache_Sum(t *testing.T) {
	cache, err := NewFIFOCache[float64](5, float64Sum, 0)
	require.NoError(t, err)

	// 测试空缓存求和
	assert.Equal(t, 0.0, cache.Sum())

	// 测试单个元素求和
	cache.Add(10.5)
	assert.Equal(t, 10.5, cache.Sum())

	// 测试多个元素求和
	cache.Add(20.3)
	cache.Add(30.2)
	assert.Equal(t, 10.5+20.3+30.2, cache.Sum()) // 61.0

	// 测试超过容量后求和
	cache.Add(40.1)
	cache.Add(50.0)
	cache.Add(60.9)                                        // 触发淘汰最早的10.5
	assert.Equal(t, 20.3+30.2+40.1+50.0+60.9, cache.Sum()) // 201.5
}

// TestFIFOCache_Len 测试长度获取功能
func TestFIFOCache_Len(t *testing.T) {
	cache, err := NewFIFOCache[float64](2, float64Sum, 0)
	require.NoError(t, err)

	assert.Equal(t, 0, cache.Len())

	cache.Add(1.0)
	assert.Equal(t, 1, cache.Len())

	cache.Add(2.0)
	assert.Equal(t, 2, cache.Len())

	cache.Add(3.0)
	assert.Equal(t, 2, cache.Len()) // 容量不变
}

// TestFIFOCache_Clear 测试清空缓存功能
func TestFIFOCache_Clear(t *testing.T) {
	cache, err := NewFIFOCache[float64](3, float64Sum, 0)
	require.NoError(t, err)

	cache.Add(1.0)
	cache.Add(2.0)
	assert.Equal(t, 2, cache.Len())

	cache.Clear()
	assert.Equal(t, 0, cache.Len())
	assert.Empty(t, cache.Elements())
	assert.Equal(t, 0.0, cache.Sum())

	// 清空后添加元素应正常工作
	cache.Add(3.0)
	assert.Equal(t, 1, cache.Len())
	assert.Equal(t, []float64{3.0}, cache.Elements())
}

// TestFIFOCache_Elements 测试元素列表获取功能
func TestFIFOCache_Elements(t *testing.T) {
	cache, err := NewFIFOCache[float64](4, float64Sum, 0)
	require.NoError(t, err)

	// 空缓存
	assert.Empty(t, cache.Elements())

	// 添加元素后验证顺序（从旧到新）
	elements := []float64{1.1, 2.2, 3.3, 4.4, 5.5}
	for _, e := range elements {
		cache.Add(e)
	}

	// 超过容量后，应保留最后4个元素
	expected := []float64{2.2, 3.3, 4.4, 5.5}
	assert.Equal(t, expected, cache.Elements())
}
