package main

import (
	"context"
	"gopkg.in/yaml.v3"
	"log"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/collector/pdata/ptrace"
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

func main() {
	cfg, err := loadConfig("config.yml")
	if err != nil {
		log.Fatalf("读取配置失败: %v", err)
	}
	if len(cfg.Kafka.Brokers) == 0 || cfg.Consumer.Topic == "" || cfg.Consumer.Rate <= 0 {
		log.Fatalf("配置项缺失: brokers/topic/rate")
	}
	log.Printf("配置: %+v", cfg)
	//r := kafka.NewReader(kafka.ReaderConfig{
	//	Brokers: cfg.Kafka.Brokers,
	//	Topic:   cfg.Consumer.Topic,
	//	GroupID: "my-group",
	//	MaxWait: 100 * time.Millisecond,
	//	Dialer: &kafka.Dialer{
	//		Timeout: 10 * time.Second,
	//	},
	//})
	//defer func() {
	//	if err := r.Close(); err != nil {
	//		log.Printf("关闭kafka reader出错: %v", err)
	//	}
	//}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		sig := <-sigCh
		log.Printf("收到终止信号: %v，准备退出...", sig)
		cancel()
	}()

	rate := cfg.Consumer.Rate
	if rate <= 0 {
		log.Fatalf("消费速率必须大于0")
	}
	interval := time.Second / time.Duration(rate)
	unmarshaler := ptrace.ProtoUnmarshaler{}

	workerNum := 4 // 可根据CPU核数或实际情况调整
	//tasks := make(chan struct{}, workerNum*2)

	consume := func(id int) {

		r := kafka.NewReader(kafka.ReaderConfig{
			Brokers: cfg.Kafka.Brokers,
			Topic:   cfg.Consumer.Topic,
			GroupID: "my-group",
			MaxWait: 100 * time.Millisecond,
			Dialer: &kafka.Dialer{
				Timeout: 10 * time.Second,
			},
		})
		defer func() {
			if err := r.Close(); err != nil {
				log.Printf("关闭kafka reader出错: %v", err)
			}
		}()
		for {
			readStart := time.Now()
			m, err := r.ReadMessage(ctx)
			readDuration := time.Since(readStart)
			if err != nil {
				if ctx.Err() != nil {
					log.Printf("[worker-%d] 消费被取消", id)
					return
				}
				log.Printf("[worker-%d] 消费消息出错: %v", id, err)
				continue
			}
			unmarshalStart := time.Now()
			traces, _ := unmarshaler.UnmarshalTraces(m.Value)
			unmarshalDuration := time.Since(unmarshalStart)
			log.Printf("[worker-%d] 消息: %v，读取耗时: %v，反序列化耗时: %v，当前goroutine数: %d", id, traces, readDuration, unmarshalDuration, runtime.NumGoroutine())
		}
	}

	//for {
	//	consume(0)
	//}

	//worker pool
	for i := 0; i < workerNum; i++ {
		go consume(i)
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Println("接收到退出信号，停止消费")

			return
		}
	}

	//for {
	//	select {
	//	case <-ctx.Done():
	//		log.Println("接收到退出信号，停止消费")
	//		close(tasks)
	//		return
	//	case <-ticker.C:
	//		select {
	//		case tasks <- struct{}{}:
	//			// 投递任务到worker
	//		default:
	//			//log.Println("任务队列已满，跳过本次消费")
	//		}
	//	}
	//}
}
