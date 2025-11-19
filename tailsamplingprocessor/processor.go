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

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/tracepicker"
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
	buffer       *tracepicker.SharedBuffer
	encoder      *tracepicker.BFSEncoder
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
	histPool := tracepicker.NewHistPool(1000)
	tsp := &tailSamplingProcessor{
		ctx:            ctx,
		set:            set,
		logger:         set.Logger,
		nextConsumer:   nextConsumer,
		config:         cfg,
		currSampleRate: cfg.SampleRate,
		controller:     controller,
		buffer:         tracepicker.NewSharedBuffer(cfg.BatchSize),
		encoder:        tracepicker.NewBFSEncoder(histPool),
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

	// --- Write to CSV ---
	record := []string{
		fmt.Sprintf("%d", time.Now().Unix()),
		strconv.FormatFloat(actualRate, 'f', 6, 64),
		strconv.FormatFloat(tsp.currSampleRate, 'f', 6, 64),
		strconv.FormatFloat(delta, 'f', 6, 64),
		strconv.FormatFloat(tsp.encodeCost, 'f', 2, 64),
		strconv.FormatFloat(tsp.sampleCost, 'f', 2, 64),
	}
	if err := tsp.csvWriter.Write(record); err != nil {
		tsp.logger.Error("Failed to write to CSV", zap.Error(err))
	}
	tsp.csvWriter.Flush()
	// --- End Write to CSV ---

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
	defer func() {
		duration := time.Since(since)
		// 使用累计移动平均值算法
		tsp.sampleCount++
		tsp.sampleCost = tsp.sampleCost + (float64(duration.Milliseconds())-tsp.sampleCost)/float64(tsp.sampleCount)
		tsp.logger.Info("Batch sampling completed", zap.Duration("duration", duration))
	}()
	rate := tsp.currSampleRate
	budget := max(len(abnormalTraces), int(float64(bufferCount)*rate))
	sampledTraces := make([]ptrace.Traces, 0, budget)
	sampledIndicesByType := make(map[string]map[int]struct{})

	// 1. 保证所有异常追踪都被采样
	sampledTraces = append(sampledTraces, abnormalTraces...)

	// 2. 保证每个正常追踪类型至少有一条被采样
	for typeID, traces := range normalTracesByType {
		if len(traces) > 0 {
			// 随机选择一个进行采样
			idx := rand.Intn(len(traces))
			sampledTraces = append(sampledTraces, traces[idx])
			sampledIndicesByType[typeID] = map[int]struct{}{idx: {}}
		}
	}

	// 3. 如果预算仍有剩余，再按照现有策略均分预算采样
	remainingBudget := budget - len(sampledTraces)
	if remainingBudget > 0 {
		// 收集需要进一步采样的类型
		typesToSample := make([]struct {
			typeID string
			traces []ptrace.Traces
		}, 0, len(normalTracesByType))

		for typeID, traces := range normalTracesByType {
			// 如果该类型还有未被采样的追踪，则加入列表
			if len(traces) > len(sampledIndicesByType[typeID]) {
				typesToSample = append(typesToSample, struct {
					typeID string
					traces []ptrace.Traces
				}{typeID: typeID, traces: traces})
			}
		}

		// 按类型包含的追踪数量升序排序，优先采样数量少的类型
		sort.Slice(typesToSample, func(i, j int) bool {
			return len(typesToSample[i].traces) < len(typesToSample[j].traces)
		})

		remaining := remainingBudget
		for i, item := range typesToSample {
			if remaining <= 0 {
				break
			}

			typeID := item.typeID
			traces := item.traces
			sampledIndices := sampledIndicesByType[typeID]
			unsampledCount := len(traces) - len(sampledIndices)

			// 计算分配给当前类型的预算
			numTypesLeft := len(typesToSample) - i
			alloc := remaining / numTypesLeft
			if alloc > unsampledCount {
				alloc = unsampledCount
			}

			if alloc > 0 {
				// 找出未被采样的追踪的索引
				unsampledIndices := make([]int, 0, unsampledCount)
				for k := range traces {
					if _, ok := sampledIndices[k]; !ok {
						unsampledIndices = append(unsampledIndices, k)
					}
				}

				// 随机打乱并选择 alloc 个进行采样
				rand.Shuffle(len(unsampledIndices), func(a, b int) {
					unsampledIndices[a], unsampledIndices[b] = unsampledIndices[b], unsampledIndices[a]
				})

				for k := 0; k < alloc; k++ {
					idx := unsampledIndices[k]
					sampledTraces = append(sampledTraces, traces[idx])
					sampledIndices[idx] = struct{}{}
				}
				remaining -= alloc
			}
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
	tsp.csvFile, err = os.OpenFile(fmt.Sprintf("data/sampling_rate_log_%s.csv", time.Now().Format("01021504")), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		return fmt.Errorf("failed to open CSV file: %w", err)
	}
	tsp.csvWriter = csv.NewWriter(tsp.csvFile)
	// Write CSV header
	if err := tsp.csvWriter.Write([]string{"timestamp", "actual_rate", "current_sample_rate", "delta", "encode_cost_ms", "sample_cost_ms"}); err != nil {
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
