package main

import (
	"bufio"
	"context"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"io"
	"log"
	"math/rand"
	"os"
	"time"
)

func main() {
	path := "traces"
	traces, err := LoadData(path)
	if err != nil {
		log.Fatalf("加载trace数据失败: %v", err)
	}
	log.Printf("成功加载 %d 条trace", len(traces))

	// 读取配置
	endpoint := os.Getenv("OTLP_ENDPOINT")
	if endpoint == "" {
		endpoint = "localhost:4317" // 默认OTLP gRPC端口
	}
	cc, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("创建gRPC连接失败: %v", err)
	}
	client := ptraceotlp.NewGRPCClient(cc)

	// 预设速率变化序列，每个阶段持续10秒
	rateSequence := []int{20, 40, 60, 90, 100, 80, 50, 30, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20}
	stageDuration := 10 // 每个速率阶段持续秒数

	log.Printf("开始按照预设速率序列发送trace到: %s", endpoint)
	for stage, rate := range rateSequence {
		log.Printf("阶段%d: 速率=%d条/秒, 持续%d秒", stage+1, rate, stageDuration)
		ticker := time.NewTicker(time.Second)
		for sec := 0; sec < stageDuration; sec++ {
			<-ticker.C
			start := time.Now()
			for i := 0; i < rate; i++ {
				idx := rand.Intn(len(traces))
				traceData := traces[idx]
				_, err = client.Export(context.Background(), ptraceotlp.NewExportRequestFromTraces(traceData))
				if err != nil {
					log.Printf("发送trace失败: %v", err)
				}
			}
			log.Printf("已发送 %d 条trace, 用时: %v", rate, time.Since(start))
		}
		ticker.Stop()
	}
	log.Printf("所有阶段发送完毕，程序退出")
}

func LoadData(path string) ([]ptrace.Traces, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	unmarshaler := &ptrace.JSONUnmarshaler{}
	var traces []ptrace.Traces

	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
		}

		trace, err := unmarshaler.UnmarshalTraces(line)
		if err != nil {
			log.Printf("反序列化trace失败: %v", err)
			continue
		}
		traces = append(traces, trace)
	}
	return traces, nil
}
