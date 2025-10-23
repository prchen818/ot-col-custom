// file: processor/tailsamplingprocessor/processor.go

// SPDX-License-Identifier: Apache-2.0

// file: processor/tailsamplingprocessor/processor.go

// SPDX-License-Identifier: Apache-2.0

package tailsamplingprocessor

import (
	"context"
	"math/rand"
	"time"

	"github.com/IBM/sarama"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
)

type tailSamplingSpanProcessor struct {
	ctx            context.Context
	set            processor.Settings
	logger         *zap.Logger
	nextConsumer   consumer.Traces
	currSampleRate float64
	config         Config
	kafkaClient    sarama.Client

	// 新增统计窗口相关字段
	traceCount      int64
	sampledCount    int64
	windowStartTime time.Time

	controller *Controller
}

func newTracesProcessor(
	ctx context.Context,
	set processor.Settings,
	nextConsumer consumer.Traces,
	cfg Config,
) (processor.Traces, error) {

	tsp := &tailSamplingSpanProcessor{
		ctx:             ctx,
		set:             set,
		logger:          set.Logger,
		nextConsumer:    nextConsumer,
		config:          cfg,
		currSampleRate:  cfg.SampleRate,
		windowStartTime: time.Now(),
		controller:      NewController(cfg, set.Logger),
	}
	//if cfg.Dynamic {
	tsp.StartSampleRateUpdater()
	//}
	return tsp, nil
}

func (tsp *tailSamplingSpanProcessor) StartSampleRateUpdater() {
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

func (tsp *tailSamplingSpanProcessor) updateSampleRate() {

	// 计算实际采样率
	actualRate := 0.0
	if tsp.traceCount > 0 {
		actualRate = float64(tsp.sampledCount) / float64(tsp.traceCount)
	} else {
		tsp.logger.Info("没有追踪数据，跳过采样率调整")
		return // 没有追踪数据，跳过调整
	}
	tsp.logger.Info("采样率统计", zap.Int64("trace_count", tsp.traceCount), zap.Int64("sampled_count", tsp.sampledCount), zap.Float64("actual_rate", actualRate))
	if !tsp.config.Dynamic {
		return
	}
	targetRate := tsp.config.SampleRate
	delta := tsp.controller.Update(targetRate, actualRate)
	tsp.currSampleRate = max(0, tsp.currSampleRate+delta)
	// 重置窗口计数
	tsp.logger.Info("PID采样率调整",
		zap.Float64("target", targetRate),
		zap.Float64("actual", actualRate),
		zap.Float64("new", tsp.currSampleRate),
		zap.Float64("delta", delta),
	)
	//tsp.traceCount = 0
	//tsp.sampledCount = 0
	tsp.windowStartTime = time.Now()
}

func (tsp *tailSamplingSpanProcessor) ConsumeTraces(_ context.Context, td ptrace.Traces) error {
	tsp.traceCount++ // 统计总数
	isSampled := false
	if rand.Float64() < tsp.currSampleRate {
		// 采样该追踪
		tsp.sampledCount++
		isSampled = true
		tsp.exportTraces(td)
		//tsp.logger.Info("✅️ Trace sampled and exported")
	}
	if !isSampled {
		//tsp.logger.Info("❌️ Trace not sampled")
	}
	return nil
}

// exportTraces 辅助函数保持不变。
func (tsp *tailSamplingSpanProcessor) exportTraces(td ptrace.Traces) {
	if err := tsp.nextConsumer.ConsumeTraces(tsp.ctx, td); err != nil {
		tsp.logger.Error("Failed to send traces to next consumer", zap.Error(err))
	}
}

func (tsp *tailSamplingSpanProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (tsp *tailSamplingSpanProcessor) Start(_ context.Context, _ component.Host) error {
	return nil
}

func (tsp *tailSamplingSpanProcessor) Shutdown(_ context.Context) error {
	tsp.logger.Info("Processor is shutting down, processing remaining traces in the buffer...")
	// 在关闭时，同步处理最后一批数据
	//normalTraces, abnormalTraces, count := tsp.buffer.SwapAndClear()
	//if count > 0 {
	//	tsp.runBatchSampling(normalTraces, abnormalTraces, count)
	//}
	return nil
}

// --- 新增的辅助函数 ---

// getSpanLabel 从 span 中提取 "service:operation" 标签。
func getSpanLabel(span ptrace.Span) string {
	serviceName := "unknown.service"
	if val, ok := span.Attributes().Get("service.name"); ok {
		serviceName = val.Str()
	}
	return serviceName + ":" + span.Name()
}
