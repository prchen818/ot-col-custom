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

	pidController *PIDController // PID 控制器
}

func newTracesProcessor(
	ctx context.Context,
	set processor.Settings,
	nextConsumer consumer.Traces,
	cfg Config,
) (processor.Traces, error) {
	// 初始化 Sarama Kafka 客户端
	kafkaCfg := sarama.NewConfig()
	client, err := sarama.NewClient(cfg.Kafka.Brokers, kafkaCfg)
	if err != nil {
		set.Logger.Error("Failed to create Kafka client", zap.Error(err))
		return nil, err
	}

	tsp := &tailSamplingSpanProcessor{
		ctx:             ctx,
		set:             set,
		logger:          set.Logger,
		nextConsumer:    nextConsumer,
		config:          cfg,
		currSampleRate:  cfg.SampleRate,
		kafkaClient:     client,
		windowStartTime: time.Now(),
		pidController:   NewPIDController(0.5, 0.1, 0.1), // PID 参数可调
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
				tsp.getMQLag()
				tsp.updateSampleRate()
			case <-tsp.ctx.Done():
				tsp.logger.Info("Sample rate updater stopped")
				return
			}
		}
	}()
}

func (tsp *tailSamplingSpanProcessor) updateSampleRate() {
	//windowDuration := time.Duration(tsp.config.UpdateInterval) * time.Second
	//now := time.Now()
	//if now.Sub(tsp.windowStartTime) < windowDuration {
	//	return // 窗口未结束，不调整采样率
	//}
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
	newRate := tsp.pidController.Update(targetRate, actualRate)
	tsp.logger.Info("PID采样率调整", zap.Float64("target", targetRate), zap.Float64("actual", actualRate), zap.Float64("new", newRate))
	tsp.currSampleRate = newRate
	// 重置窗口计数
	tsp.traceCount = 0
	tsp.sampledCount = 0
	tsp.windowStartTime = time.Now()
}

func (tsp *tailSamplingSpanProcessor) getMQLag() int64 {
	if tsp.kafkaClient == nil {
		tsp.logger.Warn("Kafka client not initialized")
		return -1
	}
	topic := tsp.config.Kafka.Topic
	group := tsp.config.Kafka.Group // 需在 config 里加 Group 字段
	partitions, err := tsp.kafkaClient.Partitions(topic)
	if err != nil {
		tsp.logger.Error("Failed to get partitions", zap.Error(err))
		return -1
	}
	// 创建 OffsetManager
	om, err := sarama.NewOffsetManagerFromClient(group, tsp.kafkaClient)
	if err != nil {
		tsp.logger.Error("Failed to create OffsetManager", zap.Error(err))
		return -1
	}
	defer om.Close()

	var totalLag int64
	for _, partition := range partitions {
		latestOffset, err := tsp.kafkaClient.GetOffset(topic, partition, sarama.OffsetNewest)
		if err != nil {
			tsp.logger.Error("Failed to get latest offset", zap.Error(err))
			continue
		}
		pom, err := om.ManagePartition(topic, partition)
		if err != nil {
			tsp.logger.Error("Failed to manage partition", zap.Error(err))
			continue
		}
		committedOffset, _ := pom.NextOffset()
		pom.Close()
		if committedOffset < 0 {
			committedOffset = 0 // 未提交时视为0
		}
		lag := latestOffset - committedOffset
		if lag < 0 {
			lag = 0
		}
		totalLag += lag
	}
	tsp.logger.Info("Kafka topic lag", zap.String("topic", topic), zap.Int64("total_lag", totalLag))
	return totalLag
}

func (tsp *tailSamplingSpanProcessor) ConsumeTraces(_ context.Context, td ptrace.Traces) error {
	tsp.traceCount++ // 统计总数
	isSampled := false
	if rand.Float64() < tsp.currSampleRate {
		// 采样该追踪
		tsp.sampledCount++
		isSampled = true
		tsp.exportTraces(td)
		tsp.logger.Info("✅️ Trace sampled and exported")
	}
	if !isSampled {
		tsp.logger.Info("❌️ Trace not sampled")
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
