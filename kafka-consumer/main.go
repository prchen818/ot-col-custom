package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/prchen818/ot-col-custom/pkg/csv_util"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Kafka struct {
		Brokers []string `yaml:"brokers"`
	} `yaml:"kafka"`
	Consumer struct {
		Topic string `yaml:"topic"`
		Rate  int    `yaml:"rate"`
	} `yaml:"consumer"`
}

func loadConfig(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() {
		if cerr := f.Close(); cerr != nil {
			log.Printf("关闭配置文件出错: %v", cerr)
		}
	}()
	var cfg Config
	dec := yaml.NewDecoder(f)
	if err := dec.Decode(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

var (
	saramaConsumer sarama.ConsumerGroup
	msgCount       atomic.Int64 // 每秒读取消息计数器
	cachedLag      atomic.Int64 // 缓存的lag
)

// lagInfo 类型提升为全局类型
// Sarama consumer group handler

type lagInfo struct {
	lag          int64
	waitMs       int64
	readDuration time.Duration
}

type consumerGroupHandler struct {
	lagCh  chan lagInfo
	client sarama.Client // 用于获取最新offset
	rate   int           // 每秒读取数量限制
}

func (h *consumerGroupHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *consumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }
func (h *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	lastReadTime := time.Now()
	for {
		// 限制每秒读取数量
		if msgCount.Load() >= int64(h.rate) {
			log.Printf("sleeping ...")
			now := time.Now()
			// 计算距离下一秒的时间
			sleepMs := 1000 - now.Sub(now.Truncate(time.Second)).Milliseconds()
			if sleepMs > 0 {
				time.Sleep(time.Duration(sleepMs) * time.Millisecond)
			}
			continue
		}
		select {
		case msg := <-claim.Messages():
			waitMs := time.Since(msg.Timestamp).Milliseconds()
			readDuration := time.Since(lastReadTime)
			lastReadTime = time.Now()
			h.lagCh <- lagInfo{waitMs: waitMs, readDuration: readDuration}
			session.MarkMessage(msg, "")
			msgCount.Add(1) // 计数器递增
		case <-session.Context().Done():
			return nil
		}
	}
}

func consume(ctx context.Context, handler *consumerGroupHandler) {
	select {
	case info := <-handler.lagCh:
		ts := time.Now()
		currentLag := cachedLag.Load()
		csv_util.WriteCSV([]string{ts.Format("2006-01-02 15:04:05.000"), fmt.Sprintf("%d", currentLag), fmt.Sprintf("%d", info.waitMs)})
		log.Printf("[worker] 读取耗时: %v，lag: %d，waitMs: %d, 写入耗时: %v", info.readDuration, currentLag, info.waitMs, time.Since(ts))
	case <-ctx.Done():
		log.Println("接收到退出信号，停止消费")
		return
	case <-time.After(5 * time.Second):
		log.Println("消费超时")
		return
	}
}

func monitorLag(ctx context.Context, client sarama.Client, admin sarama.ClusterAdmin, topic string, groupID string) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			partitions, err := client.Partitions(topic)
			if err != nil {
				log.Printf("获取分区失败: %v", err)
				continue
			}

			topicPartitions := make(map[string][]int32)
			topicPartitions[topic] = partitions

			groupOffsets, err := admin.ListConsumerGroupOffsets(groupID, topicPartitions)
			if err != nil {
				log.Printf("获取消费者组offset失败: %v", err)
				continue
			}

			var totalLag int64
			for _, p := range partitions {
				latestOffset, err := client.GetOffset(topic, p, sarama.OffsetNewest)
				if err != nil {
					log.Printf("获取分区 %d 的最新offset失败: %v", p, err)
					continue
				}

				consumerGroupOffset := groupOffsets.Blocks[topic][p].Offset
				var lag int64
				if consumerGroupOffset == -1 {
					// No offset committed yet, so lag is the latest offset
					lag = latestOffset
				} else {
					lag = latestOffset - consumerGroupOffset
				}
				totalLag += lag
			}
			cachedLag.Store(totalLag)
			log.Printf("更新lag缓存: %d", totalLag)
		case <-ctx.Done():
			return
		}
	}
}

func main() {
	cfg, err := loadConfig("config.yml")
	if err != nil {
		log.Fatalf("读取配置失败: %v", err)
	}
	if len(cfg.Kafka.Brokers) == 0 || cfg.Consumer.Topic == "" || cfg.Consumer.Rate <= 0 {
		log.Fatalf("配置项缺失: brokers/topic/rate")
	}
	log.Printf("配置: %+v", cfg)

	if err := csv_util.InitCSV("kafka_metrics.csv", []string{"timestamp", "lag", "wait_ms"}); err != nil {
		log.Fatalf("无法初始化csv文件: %v", err)
	}
	defer csv_util.CloseCSV()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	waitForSignal(cancel)

	saramaCfg := sarama.NewConfig()
	saramaCfg.Version = sarama.V2_1_0_0
	saramaCfg.Consumer.Return.Errors = true
	saramaCfg.Consumer.Offsets.Initial = sarama.OffsetNewest

	client, err := sarama.NewClient(cfg.Kafka.Brokers, saramaCfg)
	if err != nil {
		log.Fatalf("无法初始化sarama client: %v", err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			log.Printf("关闭sarama client出错: %v", err)
		}
	}()

	clusterAdmin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		log.Fatalf("无法创建 sarama cluster admin: %v", err)
	}
	defer func() {
		if err := clusterAdmin.Close(); err != nil {
			log.Printf("关闭sarama cluster admin出错: %v", err)
		}
	}()

	saramaConsumer, err = sarama.NewConsumerGroupFromClient("my-group", client)
	if err != nil {
		log.Fatalf("无法初始化sarama consumer group: %v", err)
	}
	defer func() {
		if err := saramaConsumer.Close(); err != nil {
			log.Printf("关闭sarama consumer group出错: %v", err)
		}
	}()
	rate := cfg.Consumer.Rate

	handler := &consumerGroupHandler{
		lagCh:  make(chan lagInfo, 1),
		client: client,
		rate:   rate,
	}

	go monitorLag(ctx, client, clusterAdmin, cfg.Consumer.Topic, "my-group")

	go func() {
		for {
			if err := saramaConsumer.Consume(ctx, []string{cfg.Consumer.Topic}, handler); err != nil {
				log.Printf("[worker] 消费消息出错: %v", err)
				return
			}
		}
	}()

	go func() {
		for {
			time.Sleep(time.Second)
			count := msgCount.Swap(0)
			log.Printf("[统计] 实际每秒读取消息数量: %d", count)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			log.Println("接收到退出信号，停止消费")
			return
		default:
			consume(ctx, handler)
		}
	}
}

func waitForSignal(cancel context.CancelFunc) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Printf("收到终止信号: %v，准备退出...", sig)
		cancel()
	}()
}
