package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"gopkg.in/yaml.v3"
	"log"
	"os"
	"os/signal"
	"sync"
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

var (
	csvFile     *os.File
	csvWriter   *csv.Writer
	csvMutex    sync.Mutex
	kafkaReader *kafka.Reader
	tasks       chan struct{}
)

func writeCSV(ts string, lag int64, waitMs int64) {
	csvMutex.Lock()
	defer csvMutex.Unlock()
	if csvWriter != nil {
		_ = csvWriter.Write([]string{ts, fmt.Sprintf("%d", lag), fmt.Sprintf("%d", waitMs)})
		csvWriter.Flush()
	}
}

func initCSV() error {
	var err error
	csvFile, err = os.OpenFile("kafka_metrics.csv", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	csvWriter = csv.NewWriter(csvFile)
	_ = csvWriter.Write([]string{"timestamp", "lag", "wait_ms"})
	csvWriter.Flush()
	return nil
}

func closeCSV() {
	csvMutex.Lock()
	defer csvMutex.Unlock()
	if csvWriter != nil {
		csvWriter.Flush()
	}
	if csvFile != nil {
		_ = csvFile.Close()
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

func consume(ctx context.Context, unmarshaler *ptrace.ProtoUnmarshaler, id int) {
	for {
		select {
		case <-ctx.Done():
			log.Printf("[worker-%d] 消费被取消", id)
			return
		case <-tasks:
			readStart := time.Now()
			m, err := kafkaReader.ReadMessage(ctx)
			readDuration := time.Since(readStart)
			if err != nil {
				if ctx.Err() != nil {
					log.Printf("[worker-%d] 消费被取消", id)
					return
				}
				log.Printf("[worker-%d] 消费消息出错: %v", id, err)
				continue
			}
			//unmarshalStart := time.Now()
			//traces, _ := unmarshaler.UnmarshalTraces(m.Value)
			//unmarshalDuration := time.Since(unmarshalStart)

			lag := m.HighWaterMark - m.Offset
			waitMs := time.Since(m.Time).Milliseconds()
			ts := time.Now().Format("2006-01-02 15:04:05.000")
			writeCSV(ts, lag, waitMs)

			log.Printf("[worker-%d] 读取耗时: %v，lag: %d，waitMs: %d", id, readDuration, lag, waitMs)
		}
	}
}

func startWorkers(workerNum int, ctx context.Context) {
	unmarshaler := &ptrace.ProtoUnmarshaler{}
	for i := 0; i < workerNum; i++ {
		go consume(ctx, unmarshaler, i)
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

	if err := initCSV(); err != nil {
		log.Fatalf("无法初始化csv文件: %v", err)
	}
	defer closeCSV()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	waitForSignal(cancel)

	kafkaReader = kafka.NewReader(kafka.ReaderConfig{
		Brokers: cfg.Kafka.Brokers,
		Topic:   cfg.Consumer.Topic,
		GroupID: "my-group",
		MaxWait: 100 * time.Millisecond,
		Dialer: &kafka.Dialer{
			Timeout: 10 * time.Second,
		},
	})
	defer func() {
		if err := kafkaReader.Close(); err != nil {
			log.Printf("关闭kafka reader出错: %v", err)
		}
	}()

	rate := cfg.Consumer.Rate
	interval := time.Second / time.Duration(rate)

	workerNum := 4 // 可根据CPU核数或实际情况调整
	tasks = make(chan struct{}, workerNum*2)

	startWorkers(workerNum, ctx)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("接收到退出信号，停止消费")
			close(tasks)
			return
		case <-ticker.C:
			select {
			case tasks <- struct{}{}:
				// 投递任务到worker
			default:
			}
		}
	}
}
