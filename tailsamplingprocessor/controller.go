package tailsamplingprocessor

import (
	"log"

	"github.com/IBM/sarama"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/cache"
	"go.uber.org/zap"
)

// PID 控制器结构体
// 目标：让实际采样率逼近 config.SampleRate
// 采样率调整范围 [0,1]
type Controller struct {
	client  sarama.Client
	admin   sarama.ClusterAdmin
	logger  *zap.Logger
	topic   string
	groupID string

	cfg ControllerConfig

	totalCount   int64
	sampledCount int64
	target       float64
	integral     *cache.FIFOCache[float64]
	prevError    float64
}

func NewController(cfg Config, logger *zap.Logger) (*Controller, error) {
	// 初始化 Sarama Kafka 客户端
	kafkaCfg := sarama.NewConfig()
	client, err := sarama.NewClient(cfg.Kafka.Brokers, kafkaCfg)
	if err != nil {
		logger.Error(err.Error())
		return nil, err
	}
	clusterAdmin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		log.Fatalf("无法创建 sarama cluster admin: %v", err)
	}
	buffer, err := cache.NewFIFOCache[float64](cfg.Controller.BufferSize, func(f float64, f2 float64) float64 {
		return f + f2
	}, 0)
	if err != nil {
		logger.Error(err.Error())

	}
	return &Controller{
		cfg:      cfg.Controller,
		client:   client,
		logger:   logger,
		topic:    cfg.Kafka.Topic,
		groupID:  cfg.Kafka.Group,
		admin:    clusterAdmin,
		integral: buffer,
		target:   cfg.SampleRate,
	}, nil
}

func (c *Controller) Update(current float64) float64 {
	if c.totalCount > c.cfg.ShrinkThreshold {
		c.ShrinkCounts()
	}
	actual := float64(c.sampledCount) / float64(c.totalCount)
	error := c.target - actual
	c.integral.Add(error)
	derivative := error - c.prevError
	c.prevError = error
	deltaPid := c.cfg.Kp*error + c.cfg.Ki*c.integral.Sum() + c.cfg.Kd*derivative

	deltaFlow := -c.cfg.Alpha * current

	lag, _ := c.GetLag()
	lagWeight := max(0.0, float64(lag-c.cfg.LagThreshold)/float64(lag+c.cfg.LagBase)) // 简单平滑
	c.logger.Info("偏差计算", zap.Float64("deltaPid", deltaPid), zap.Float64("deltaFlow", deltaFlow), zap.Float64("lagWeight", lagWeight), zap.Int64("lag", lag))
	return deltaPid*(1-lagWeight) + deltaFlow*lagWeight
}

func (c *Controller) ShrinkCounts() {
	scaleSize := int64(float64(c.sampledCount) * c.cfg.ShrinkFactor)
	c.sampledCount -= scaleSize
	c.totalCount -= int64(float64(scaleSize) / c.target)
}

func (c *Controller) GetLag() (int64, error) {
	partitions, err := c.client.Partitions(c.topic)
	if err != nil {
		log.Printf("获取分区失败: %v", err)
		return 0, err
	}

	topicPartitions := make(map[string][]int32)
	topicPartitions[c.topic] = partitions

	groupOffsets, err := c.admin.ListConsumerGroupOffsets(c.groupID, topicPartitions)
	if err != nil {
		log.Printf("获取消费者组offset失败: %v", err)
		return 0, err
	}

	var totalLag int64
	for _, p := range partitions {
		latestOffset, err := c.client.GetOffset(c.topic, p, sarama.OffsetNewest)
		if err != nil {
			log.Printf("获取分区 %d 的最新offset失败: %v", p, err)
			continue
		}

		consumerGroupOffset := groupOffsets.Blocks[c.topic][p].Offset
		var lag int64
		if consumerGroupOffset == -1 {
			// No offset committed yet, so lag is the latest offset
			lag = latestOffset
		} else {
			lag = latestOffset - consumerGroupOffset
		}
		totalLag += lag
	}
	return totalLag, nil
}
