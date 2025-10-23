// file: processor/tailsamplingprocessor/internal/tracepicker/quota_allocator.go

package tracepicker

import (
	"math"
)

// AllocateQuota 使用动态规划为每个类型分配采样配额。
// typeCounts: 本批次中每个 typeID 的追踪数量。
// historicalCounts: 历史上已采样的每个 typeID 的追踪数量。
// totalQuota: 本次批处理总共要采样的数量。
func AllocateQuota(typeCounts map[string]int, historicalCounts map[string]int, totalQuota int) map[string]int {
	if totalQuota <= 0 {
		return make(map[string]int)
	}
	
	allCodesSet := make(map[string]struct{})
	for code := range typeCounts {
		allCodesSet[code] = struct{}{}
	}
	for code := range historicalCounts {
		allCodesSet[code] = struct{}{}
	}

	codes := make([]string, 0, len(allCodesSet))
	for code := range allCodesSet {
		codes = append(codes, code)
	}

	upperBounds := make([]int, len(codes))
	bases := make([]int, len(codes))
	for i, code := range codes {
		upperBounds[i] = typeCounts[code]
		bases[i] = historicalCounts[code]
	}

	n := len(codes)
	if n == 0 {
		return make(map[string]int)
	}

	totalBase := 0
	for _, b := range bases {
		totalBase += b
	}
	average := float64(totalQuota+totalBase) / float64(n)

	dp := make([][]float64, n+1)
	for i := range dp {
		dp[i] = make([]float64, totalQuota+1)
		for j := range dp[i] {
			dp[i][j] = math.Inf(1)
		}
	}
	dp[0][0] = 0

	for i := 1; i <= n; i++ {
		for s := 0; s <= totalQuota; s++ {
			for x := 0; x <= min(upperBounds[i-1], s); x++ {
				cost := math.Pow(float64(x+bases[i-1])-average, 2)
				dp[i][s] = math.Min(dp[i][s], dp[i-1][s-x]+cost)
			}
		}
	}

	solution := make([]int, n)
	s := totalQuota
	for i := n; i > 0; i-- {
		for x := 0; x <= min(upperBounds[i-1], s); x++ {
			cost := math.Pow(float64(x+bases[i-1])-average, 2)
			if math.Abs(dp[i][s]-(dp[i-1][s-x]+cost)) < 1e-9 {
				solution[i-1] = x
				s -= x
				break
			}
		}
	}

	quotas := make(map[string]int)
	for i, code := range codes {
		if solution[i] > 0 {
			if _, ok := typeCounts[code]; ok {
				quotas[code] = solution[i]
			}
		}
	}

	return quotas
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}