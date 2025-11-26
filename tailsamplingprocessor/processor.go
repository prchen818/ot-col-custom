package tailsamplingprocessor

import (
	"context"
	"encoding/csv"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/cache"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/encoder"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
)

type tailSamplingProcessor struct {
	ctx            context.Context
	set            processor.Settings
	logger         *zap.Logger
	nextConsumer   consumer.Traces
	currSampleRate float64
	config         Config
	csvFile        *os.File
	csvWriter      *csv.Writer

	// 新增统计窗口相关字段
	traceCount   int64
	sampledCount int64
	controller   *Controller
	buffer       *cache.SharedBuffer
	encoder      *encoder.BFSEncoder
	encodeCost   float64
	sampleCost   float64
	encodeCount  int64
	sampleCount  int64
}

func newTracesProcessor(
	ctx context.Context,
	set processor.Settings,
	nextConsumer consumer.Traces,
	cfg Config,
) (processor.Traces, error) {
	controller, err := NewController(cfg, set.Logger)
	if err != nil {
		return nil, err
	}
	histPool := encoder.NewHistPool(1000)
	tsp := &tailSamplingProcessor{
		ctx:            ctx,
		set:            set,
		logger:         set.Logger,
		nextConsumer:   nextConsumer,
		config:         cfg,
		currSampleRate: cfg.SampleRate,
		controller:     controller,
		buffer:         cache.NewSharedBuffer(cfg.BatchSize),
		encoder:        encoder.NewBFSEncoder(histPool),
	}
	tsp.StartSampleRateUpdater()
	return tsp, nil
}

func (tsp *tailSamplingProcessor) StartSampleRateUpdater() {
	// 启动一个 goroutine 定期更新采样率
	go func() {
		ticker := time.NewTicker(time.Duration(tsp.config.UpdateInterval) * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				tsp.updateSampleRate()
			case <-tsp.ctx.Done():
				tsp.logger.Info("Sample rate updater stopped")
				return
			}
		}
	}()
}

func (tsp *tailSamplingProcessor) updateSampleRate() {

	// 计算实际采样率
	actualRate := 0.0
	if tsp.traceCount > 0 {
		actualRate = float64(tsp.sampledCount) / float64(tsp.traceCount)
	} else {
		tsp.logger.Info("没有追踪数据，跳过采样率调整")
		return
	}
	tsp.logger.Info("采样率统计", zap.Int64("trace_count", tsp.traceCount), zap.Int64("sampled_count", tsp.sampledCount), zap.Float64("actual_rate", actualRate))
	if !tsp.config.Dynamic {
		return
	}
	targetRate := tsp.config.SampleRate
	delta := tsp.controller.Update(tsp.currSampleRate)
	tsp.currSampleRate = max(0, tsp.currSampleRate+delta)

	// 重置窗口计数
	tsp.logger.Info("PID采样率调整",
		zap.Float64("target", targetRate),
		zap.Float64("actual", actualRate),
		zap.Float64("new", tsp.currSampleRate),
		zap.Float64("delta", delta),
		zap.Float64("encode_cost_ms", tsp.encodeCost),
		zap.Float64("sample_cost_ms", tsp.sampleCost),
	)

}

func (tsp *tailSamplingProcessor) ConsumeTraces(_ context.Context, td ptrace.Traces) error {
	if tsp.config.Mode == "random" {
		tsp.traceCount++
		if rand.Float64() < tsp.currSampleRate {
			tsp.sampledCount++
			tsp.exportTraces(td)
		}
		return nil
	}
	since := time.Now()
	typeId, isAbnormal := tsp.encoder.Encode(td)
	encodeDuration := time.Since(since)
	// 使用累计移动平均值算法
	tsp.encodeCount++
	tsp.encodeCost = tsp.encodeCost + (float64(encodeDuration.Milliseconds())-tsp.encodeCost)/float64(tsp.encodeCount)
	tsp.logger.Info("Trace encoded", zap.String("typeId", typeId), zap.Bool("isAbnormal", isAbnormal), zap.Duration("encodeDuration", encodeDuration))
	tsp.buffer.Add(typeId, td, isAbnormal)
	if tsp.buffer.IsFull() {
		tsp.logger.Info("Buffer is full, starting batch sampling")
		normalTraces, abnormalTraces, count := tsp.buffer.SwapAndClear()
		go tsp.BatchSampling(normalTraces, abnormalTraces, count)
	}
	return nil
}

func (tsp *tailSamplingProcessor) BatchSampling(
	normalTracesByType map[string][]ptrace.Traces,
	abnormalTraces []ptrace.Traces,
	bufferCount uint64,
) {
	since := time.Now()
	rate := tsp.currSampleRate
	budget := max(len(abnormalTraces), int(float64(bufferCount)*rate))
	sampledTraces := make([]ptrace.Traces, 0, budget)
	defer func() {
		duration := time.Since(since)
		// 使用累计移动平均值算法
		tsp.sampleCount++
		tsp.sampleCost = tsp.sampleCost + (float64(duration.Milliseconds())-tsp.sampleCost)/float64(tsp.sampleCount)
		tsp.logger.Info("Batch sampling completed", zap.Duration("duration", duration))

		// --- 每批次写 CSV ---
		if tsp.csvWriter != nil {
			actualRateBatch := 0.0
			if bufferCount > 0 {
				actualRateBatch = float64(len(sampledTraces)) / float64(bufferCount)
			}
			cumulativeActualRate := 0.0
			if tsp.traceCount > 0 {
				cumulativeActualRate = float64(tsp.sampledCount) / float64(tsp.traceCount)
			}
			record := []string{
				fmt.Sprintf("%d", time.Now().Unix()),
				strconv.FormatFloat(actualRateBatch, 'f', 6, 64),      // actual_rate（该批次实际采样率）
				strconv.FormatFloat(cumulativeActualRate, 'f', 6, 64), // cumulative_actual_rate（累计实际采样率）
				strconv.FormatFloat(tsp.currSampleRate, 'f', 6, 64),   // current_sample_rate
				strconv.FormatFloat(tsp.encodeCost, 'f', 2, 64),       // encode_cost_ms（移动平均）
				strconv.FormatFloat(tsp.sampleCost, 'f', 2, 64),       // sample_cost_ms（移动平均）
				strconv.Itoa(len(abnormalTraces)),                     // abnormal_count（该批次异常数量）
				fmt.Sprintf("%d", tsp.traceCount),                     // trace_count（当前累计）
				fmt.Sprintf("%d", tsp.sampledCount),                   // sampled_count（当前累计采样数）
			}
			if err := tsp.csvWriter.Write(record); err != nil {
				tsp.logger.Error("Failed to write to CSV", zap.Error(err))
			}
			tsp.csvWriter.Flush()
		}
		// --- End ---
	}()
	// 1. 保证所有异常追踪都被采样
	sampledTraces = append(sampledTraces, abnormalTraces...)

	// 2. 对正常追踪按类型进行采样
	remainingBudget := budget - len(sampledTraces)
	if remainingBudget > 0 && len(normalTracesByType) > 0 {
		type traceTypeInfo struct {
			typeID string
			count  int
		}
		sortedTypes := make([]traceTypeInfo, 0, len(normalTracesByType))
		for typeID, traces := range normalTracesByType {
			sortedTypes = append(sortedTypes, traceTypeInfo{typeID: typeID, count: len(traces)})
		}
		sort.Slice(sortedTypes, func(i, j int) bool {
			return sortedTypes[i].count < sortedTypes[j].count
		})

		remainingTypesCount := len(sortedTypes)
		for _, typeInfo := range sortedTypes {
			if remainingBudget <= 0 {
				break
			}
			avgBudget := remainingBudget / remainingTypesCount
			if avgBudget <= 0 { // 确保至少尝试采样一个
				avgBudget = 1
			}

			traces := normalTracesByType[typeInfo.typeID]
			numToSample := 0
			if typeInfo.count <= avgBudget {
				numToSample = typeInfo.count
			} else {
				numToSample = avgBudget
			}

			// 随机采样
			rand.Shuffle(len(traces), func(i, j int) { traces[i], traces[j] = traces[j], traces[i] })
			sampledTraces = append(sampledTraces, traces[:numToSample]...)

			remainingBudget -= numToSample
			remainingTypesCount--
		}
	}

	tsp.traceCount += int64(bufferCount)
	tsp.controller.totalCount += int64(bufferCount)
	tsp.sampledCount += int64(len(sampledTraces))
	tsp.controller.sampledCount += int64(len(sampledTraces))
	// Export sampled traces
	for _, td := range sampledTraces {
		tsp.exportTraces(td)
	}
}

// exportTraces sends the traces to the next consumer in the pipeline.
func (tsp *tailSamplingProcessor) exportTraces(td ptrace.Traces) {
	if err := tsp.nextConsumer.ConsumeTraces(tsp.ctx, td); err != nil {
		tsp.logger.Error("Failed to send traces to next consumer", zap.Error(err))
	}
}

func (tsp *tailSamplingProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (tsp *tailSamplingProcessor) Start(_ context.Context, _ component.Host) error {
	// --- Initialize CSV File ---
	var err error
	tsp.csvFile, err = os.OpenFile(fmt.Sprintf("data/sampling_rate_log_%s.csv", os.Getenv("DATASET")), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		return fmt.Errorf("failed to open CSV file: %w", err)
	}
	tsp.csvWriter = csv.NewWriter(tsp.csvFile)
	if err := tsp.csvWriter.Write([]string{
		"timestamp",
		"actual_rate",
		"cumulative_actual_rate",
		"current_sample_rate",
		"encode_cost_ms",
		"sample_cost_ms",
		"abnormal_count",
		"trace_count",
		"sampled_count",
	}); err != nil {
		return fmt.Errorf("failed to write CSV header: %w", err)
	}
	// --- End CSV Initialization ---
	return nil
}

func (tsp *tailSamplingProcessor) Shutdown(_ context.Context) error {
	tsp.logger.Info("Processor is shutting down, processing remaining traces in the buffer...")
	if tsp.csvWriter != nil {
		tsp.csvWriter.Flush()
	}
	if tsp.csvFile != nil {
		return tsp.csvFile.Close()
	}
	return nil
}
