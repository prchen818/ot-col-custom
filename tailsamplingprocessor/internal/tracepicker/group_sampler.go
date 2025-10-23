package tracepicker

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"time"

	"github.com/MaxHalford/eaopt"
)

// Combination 表示一个组合
type Combination struct {
	Comb []int
}

// NewCombination 创建新的组合
func NewCombination(comb []int) *Combination {
	result := make([]int, len(comb))
	copy(result, comb)
	return &Combination{Comb: result}
}

// SampleProblem 分组采样优化问题
type SampleProblem struct {
	// 基本参数
	Quotas   []int       // 每个代码的采样配额
	Bases    []int       // 每个代码的存储数量
	RawDist  [][]float64 // 候选数据的延迟分布 (numTrace, numLabel)
	AbDist   [][]float64 // 异常数据的延迟分布 (numAbTrace, numLabel)

	// 计算后的属性
	Splits   []int
	C        int // 总配额
	NumLabel int
	AllCombs [][]*Combination // (combCount, numCode)

	// 问题定义参数
	Name      string
	M         int   // 目标数量
	MaxOrMins []int // -1: 最大化; 1: 最小化
	Dim       int   // 决策变量维度
	VarTypes  []int // 0: 连续; 1: 离散
	Lb        []int // 下界
	Ub        []int // 上界
	Lbin      []int // 1: 包含下界
	Ubin      []int // 1: 包含上界

	// 百分位数相关
	Ps      []float64   // 百分位数点
	OriginP [][]float64 // 原始数据的百分位数 (numLabel, 8)
	MaxV    []float64   // 每个标签的最大值
	MinV    []float64   // 每个标签的最小值

	// 当前批次大小
	Np int
}

// NewSampleProblem 创建新的SampleProblem实例
func NewSampleProblem(rawDist, abDist [][]float64, quotas, bases []int, combCount, M int) (*SampleProblem, error) {
	if combCount < 2 {
		return nil, fmt.Errorf("combCount must be larger than 2")
	}

	sp := &SampleProblem{
		Quotas:  quotas,
		Bases:   bases,
		RawDist: rawDist,
		AbDist:  abDist,
		M:       M,
	}

	// 计算累积和
	sp.Splits = make([]int, len(bases))
	sum := 0
	for i, base := range bases {
		sum += base
		sp.Splits[i] = sum
	}

	// 计算总配额
	sp.C = 0
	for _, quota := range quotas {
		sp.C += quota
	}

	sp.NumLabel = len(rawDist[0])

	// 初始化组合
	initStart := time.Now()
	sp.AllCombs = make([][]*Combination, combCount)

	for combIdx := 0; combIdx < combCount; combIdx++ {
		sp.AllCombs[combIdx] = make([]*Combination, len(quotas))

		for i, quota := range quotas {
			var start, end int
			if i == 0 {
				start = 0
			} else {
				start = sp.Splits[i-1]
			}
			end = sp.Splits[i]

			// 随机采样
			indices := randomSample(start, end, quota)
			sp.AllCombs[combIdx][i] = NewCombination(indices)
		}
	}

	initEnd := time.Now()
	fmt.Printf("[SAMPLE] the time consuming of init is %.2f seconds\n", initEnd.Sub(initStart).Seconds())

	// 设置问题参数
	sp.Name = "SampleProblem"
	sp.MaxOrMins = []int{1} // 最小化
	sp.Dim = len(quotas)
	sp.VarTypes = make([]int, sp.Dim)
	sp.Lb = make([]int, sp.Dim)
	sp.Ub = make([]int, sp.Dim)
	sp.Lbin = make([]int, sp.Dim)
	sp.Ubin = make([]int, sp.Dim)

	for i := 0; i < sp.Dim; i++ {
		sp.VarTypes[i] = 1        // 离散变量
		sp.Lb[i] = 0             // 下界
		sp.Ub[i] = combCount - 1 // 上界
		sp.Lbin[i] = 1           // 包含下界
		sp.Ubin[i] = 1           // 包含上界
	}

	// 计算百分位数数据
	sp.Ps = []float64{0, 25, 50, 75, 90, 95, 99, 100}

	var origin [][]float64
	if len(abDist) > 0 {
		origin = append(rawDist, abDist...)
	} else {
		origin = rawDist
	}

	// 转置数据 (numTrace, numLabel) -> (numLabel, numTrace)
	originT := transpose(origin)

	// 计算百分位数
	sp.OriginP = make([][]float64, sp.NumLabel)
	sp.MaxV = make([]float64, sp.NumLabel)
	sp.MinV = make([]float64, sp.NumLabel)

	for i := 0; i < sp.NumLabel; i++ {
		sp.OriginP[i] = make([]float64, len(sp.Ps))
		for j, p := range sp.Ps {
			sp.OriginP[i][j] = percentile(originT[i], p)
		}
		sp.MaxV[i] = max(originT[i])
		sp.MinV[i] = minFloat(originT[i])
	}

	// 标准化 OriginP
	for i := 0; i < sp.NumLabel; i++ {
		for j := 0; j < len(sp.Ps); j++ {
			sp.OriginP[i][j] = (sp.OriginP[i][j] - sp.MinV[i]) / (sp.MaxV[i] - sp.MinV[i] + 1e-7)
		}
	}

	return sp, nil
}

// RandomPhen 生成随机表型
func (sp *SampleProblem) RandomPhen(n int) [][]int {
	phen := make([][]int, n)
	for i := 0; i < n; i++ {
		phen[i] = make([]int, len(sp.AllCombs[0]))
		for j := 0; j < len(sp.AllCombs[0]); j++ {
			phen[i][j] = rand.Intn(len(sp.AllCombs))
		}
	}
	return phen
}

// Consistency 计算一致性
func (sp *SampleProblem) Consistency(matrix [][]float64) []float64 {
	// matrix: (Np * numLabel, C)
	var sample [][]float64

	if len(sp.AbDist) > 0 {
		// 转置 AbDist
		abDistT := transpose(sp.AbDist)
		// 复制 abDistT Np 次
		tileAbDist := make([][]float64, sp.Np*sp.NumLabel)
		for i := 0; i < sp.Np; i++ {
			for j := 0; j < sp.NumLabel; j++ {
				tileAbDist[i*sp.NumLabel+j] = make([]float64, len(abDistT[j]))
				copy(tileAbDist[i*sp.NumLabel+j], abDistT[j])
			}
		}

		// 水平拼接 matrix 和 tileAbDist
		sample = make([][]float64, len(matrix))
		for i := 0; i < len(matrix); i++ {
			sample[i] = append(matrix[i], tileAbDist[i]...)
		}
	} else {
		sample = matrix
	}

	// 计算百分位数
	sampleP := make([][]float64, len(sample))
	for i := 0; i < len(sample); i++ {
		sampleP[i] = make([]float64, len(sp.Ps))
		for j, p := range sp.Ps {
			sampleP[i][j] = percentile(sample[i], p)
		}
	}

	// 复制 originP Np 次
	originP := make([][]float64, sp.Np*sp.NumLabel)
	for i := 0; i < sp.Np; i++ {
		for j := 0; j < sp.NumLabel; j++ {
			originP[i*sp.NumLabel+j] = make([]float64, len(sp.OriginP[j]))
			copy(originP[i*sp.NumLabel+j], sp.OriginP[j])
		}
	}

	// 标准化 sampleP
	for i := 0; i < len(sampleP); i++ {
		labelIdx := i % sp.NumLabel
		for j := 0; j < len(sampleP[i]); j++ {
			sampleP[i][j] = (sampleP[i][j] - sp.MinV[labelIdx]) / (sp.MaxV[labelIdx] - sp.MinV[labelIdx] + 1e-7)
		}
	}

	// 计算 RMSE
	mse := make([]float64, len(sampleP))
	for i := 0; i < len(sampleP); i++ {
		sum := 0.0
		for j := 0; j < len(sampleP[i]); j++ {
			diff := sampleP[i][j] - originP[i][j]
			sum += diff * diff
		}
		mse[i] = sum / float64(len(sampleP[i]))
	}

	// 重新组织结果 (Np, numLabel)
	result := make([]float64, sp.Np)
	for i := 0; i < sp.Np; i++ {
		sum := 0.0
		for j := 0; j < sp.NumLabel; j++ {
			sum += mse[i*sp.NumLabel+j]
		}
		result[i] = sum
	}

	return result
}

// EvalVars 评估变量
func (sp *SampleProblem) EvalVars(vars [][]int) []float64 {
	sp.Np = len(vars)

	// 选择组合
	selectIdxs := make([][]int, sp.Np)
	for i := 0; i < sp.Np; i++ {
		var allIdxs []int
		for j, varVal := range vars[i] {
			comb := sp.AllCombs[varVal][j]
			allIdxs = append(allIdxs, comb.Comb...)
		}
		selectIdxs[i] = allIdxs
	}

	// 构造采样数据 (Np, C, numLabel) -> (Np * numLabel, C)
	sampleData := make([][]float64, sp.Np*sp.NumLabel)
	for i := 0; i < sp.Np; i++ {
		for j := 0; j < sp.NumLabel; j++ {
			sampleData[i*sp.NumLabel+j] = make([]float64, sp.C)
			for k, idx := range selectIdxs[i] {
				sampleData[i*sp.NumLabel+j][k] = sp.RawDist[idx][j]
			}
		}
	}

	return sp.Consistency(sampleData)
}

// GetIdxsByVar 根据变量获取索引
func (sp *SampleProblem) GetIdxsByVar(varVal []int) []int {
	var selectIdxs []int
	for j, val := range varVal {
		comb := sp.AllCombs[val][j]
		selectIdxs = append(selectIdxs, comb.Comb...)
	}
	return selectIdxs
}

// SampleVector 实现 eaopt.Genome 接口的采样向量
type SampleVector struct {
	Genes   []int
	problem *SampleProblem
}

// NewSampleVector 创建新的采样向量
func NewSampleVector(genes []int, problem *SampleProblem) *SampleVector {
	return &SampleVector{
		Genes:   genes,
		problem: problem,
	}
}

// Evaluate 实现适应度评估
func (sv *SampleVector) Evaluate() (float64, error) {
	if sv.problem == nil {
		return 0, fmt.Errorf("problem not initialized")
	}

	vars := [][]int{sv.Genes}
	fitness := sv.problem.EvalVars(vars)
	return fitness[0], nil
}

// Mutate 实现变异操作
func (sv *SampleVector) Mutate(rng *rand.Rand) {
	if sv.problem == nil {
		return
	}

	// 随机选择一个基因进行变异
	if len(sv.Genes) > 0 {
		idx := rng.Intn(len(sv.Genes))
		sv.Genes[idx] = rng.Intn(sv.problem.Ub[idx]-sv.problem.Lb[idx]+1) + sv.problem.Lb[idx]
	}
}

// Crossover 实现交叉操作
func (sv *SampleVector) Crossover(other eaopt.Genome, rng *rand.Rand) {
	otherSv, ok := other.(*SampleVector)
	if !ok || len(sv.Genes) != len(otherSv.Genes) {
		return
	}

	// 单点交叉
	if len(sv.Genes) > 1 {
		crossPoint := rng.Intn(len(sv.Genes))
		for i := crossPoint; i < len(sv.Genes); i++ {
			sv.Genes[i], otherSv.Genes[i] = otherSv.Genes[i], sv.Genes[i]
		}
	}
}

// Clone 实现克隆操作
func (sv *SampleVector) Clone() eaopt.Genome {
	clone := &SampleVector{
		Genes:   make([]int, len(sv.Genes)),
		problem: sv.problem,
	}
	copy(clone.Genes, sv.Genes)
	return clone
}

// SampleOptimizer 采样优化器
type SampleOptimizer struct {
	Problem *SampleProblem
	Config  *OptimizeConfig
}

// OptimizeConfig 优化配置
type OptimizeConfig struct {
	PopSize      uint    // 种群大小
	NGenerations uint    // 代数
	CrossRate    float64 // 交叉率
	MutRate      float64 // 变异率
	HofSize      uint    // 名人堂大小
	Seed         int64   // 随机种子
}

// DefaultOptimizeConfig 默认优化配置
func DefaultOptimizeConfig() *OptimizeConfig {
	return &OptimizeConfig{
		PopSize:      50,
		NGenerations: 100,
		CrossRate:    0.8,
		MutRate:      0.1,
		HofSize:      10,
		Seed:         time.Now().UnixNano(),
	}
}

// NewSampleOptimizer 创建新的采样优化器
func NewSampleOptimizer(problem *SampleProblem, config *OptimizeConfig) *SampleOptimizer {
	if config == nil {
		config = DefaultOptimizeConfig()
	}
	return &SampleOptimizer{
		Problem: problem,
		Config:  config,
	}
}

// createSampleVectorFactory 创建SampleVector工厂函数
func (so *SampleOptimizer) createSampleVectorFactory() func(*rand.Rand) eaopt.Genome {
	return func(rng *rand.Rand) eaopt.Genome {
		genes := make([]int, so.Problem.Dim)
		for i := 0; i < so.Problem.Dim; i++ {
			genes[i] = rng.Intn(so.Problem.Ub[i]-so.Problem.Lb[i]+1) + so.Problem.Lb[i]
		}
		return NewSampleVector(genes, so.Problem)
	}
}

// Optimize 执行优化
func (so *SampleOptimizer) Optimize() (*SampleVector, error) {
	// 配置遗传算法
	config := eaopt.NewDefaultGAConfig()
	
	// 设置参数
	config.PopSize = so.Config.PopSize
	config.NGenerations = so.Config.NGenerations
	config.HofSize = so.Config.HofSize
	config.ParallelEval = false
	
	// 设置模型 - 使用带交叉率和变异率的代际模型
	config.Model = eaopt.ModGenerational{
		Selector:  eaopt.SelTournament{NContestants: 3},
		CrossRate: so.Config.CrossRate,
		MutRate:   so.Config.MutRate,
	}

	// 设置随机种子
	config.RNG = rand.New(rand.NewSource(so.Config.Seed))

	ga, err := config.NewGA()
	if err != nil {
		return nil, fmt.Errorf("failed to create GA: %w", err)
	}

	// 运行优化
	err = ga.Minimize(so.createSampleVectorFactory())
	if err != nil {
		return nil, fmt.Errorf("failed to minimize: %w", err)
	}

	// 获取最优解
	if len(ga.HallOfFame) == 0 {
		return nil, fmt.Errorf("no solution found")
	}

	bestGenome, ok := ga.HallOfFame[0].Genome.(*SampleVector)
	if !ok {
		return nil, fmt.Errorf("invalid genome type")
	}

	return bestGenome, nil
}

// OptimizeWithCallback 带回调的优化
func (so *SampleOptimizer) OptimizeWithCallback(callback func(generation uint, bestFitness float64)) (*SampleVector, error) {
	// 配置遗传算法
	config := eaopt.NewDefaultGAConfig()
	
	// 设置参数
	config.PopSize = so.Config.PopSize
	config.NGenerations = so.Config.NGenerations
	config.HofSize = so.Config.HofSize
	config.ParallelEval = false
	
	// 设置模型 - 使用带交叉率和变异率的代际模型
	config.Model = eaopt.ModGenerational{
		Selector:  eaopt.SelTournament{NContestants: 3},
		CrossRate: so.Config.CrossRate,
		MutRate:   so.Config.MutRate,
	}

	// 设置随机种子
	config.RNG = rand.New(rand.NewSource(so.Config.Seed))

	// 设置回调
	if callback != nil {
		config.Callback = func(ga *eaopt.GA) {
			if len(ga.HallOfFame) > 0 {
				callback(ga.Generations, ga.HallOfFame[0].Fitness)
			}
		}
	}

	ga, err := config.NewGA()
	if err != nil {
		return nil, fmt.Errorf("failed to create GA: %w", err)
	}

	// 运行优化
	err = ga.Minimize(so.createSampleVectorFactory())
	if err != nil {
		return nil, fmt.Errorf("failed to minimize: %w", err)
	}

	// 获取最优解
	if len(ga.HallOfFame) == 0 {
		return nil, fmt.Errorf("no solution found")
	}

	bestGenome, ok := ga.HallOfFame[0].Genome.(*SampleVector)
	if !ok {
		return nil, fmt.Errorf("invalid genome type")
	}

	return bestGenome, nil
}

// 辅助函数

// randomSample 从 [start, end) 范围内随机采样 n 个不重复的整数
func randomSample(start, end, n int) []int {
	if n > end-start {
		panic("sample size larger than population")
	}

	population := make([]int, end-start)
	for i := 0; i < end-start; i++ {
		population[i] = start + i
	}

	// Fisher-Yates shuffle
	for i := len(population) - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		population[i], population[j] = population[j], population[i]
	}

	return population[:n]
}

// transpose 转置二维切片
func transpose(matrix [][]float64) [][]float64 {
	if len(matrix) == 0 {
		return [][]float64{}
	}

	rows := len(matrix)
	cols := len(matrix[0])
	result := make([][]float64, cols)

	for i := 0; i < cols; i++ {
		result[i] = make([]float64, rows)
		for j := 0; j < rows; j++ {
			result[i][j] = matrix[j][i]
		}
	}

	return result
}

// percentile 计算百分位数
func percentile(data []float64, p float64) float64 {
	if len(data) == 0 {
		return math.NaN()
	}

	// 创建副本并排序
	sorted := make([]float64, len(data))
	copy(sorted, data)
	sort.Float64s(sorted)

	if p == 0 {
		return sorted[0]
	}
	if p == 100 {
		return sorted[len(sorted)-1]
	}

	// 线性插值计算百分位数
	n := float64(len(sorted))
	index := p / 100 * (n - 1)
	lower := int(math.Floor(index))
	upper := int(math.Ceil(index))

	if lower == upper {
		return sorted[lower]
	}

	weight := index - float64(lower)
	return sorted[lower]*(1-weight) + sorted[upper]*weight
}

// max 返回切片中的最大值
func max(data []float64) float64 {
	if len(data) == 0 {
		return math.NaN()
	}

	maxVal := data[0]
	for _, v := range data[1:] {
		if v > maxVal {
			maxVal = v
		}
	}
	return maxVal
}

// minFloat 返回切片中的最小值
func minFloat(data []float64) float64 {
	if len(data) == 0 {
		return math.NaN()
	}

	minVal := data[0]
	for _, v := range data[1:] {
		if v < minVal {
			minVal = v
		}
	}
	return minVal
}