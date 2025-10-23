package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/prchen818/ot-col-custom/pkg/csv_util"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

	// 初始化CSV文件
	if err := csv_util.InitCSV("rate_log.csv", []string{"timestamp", "rate"}); err != nil {
		log.Fatalf("初始化CSV失败: %v", err)
	}
	defer csv_util.CloseCSV()

	// 预设速率变化序列，每个阶段持续10秒
	rateSequence := []int{40, 80, 180, 300, 300, 300, 240, 160, 100, 80, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 100, 200, 100, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40}
	stageDuration := 20 // 每个速率阶段持续秒数

	log.Printf("开始按照预设速率序列发送trace到: %s", endpoint)
	for stage, rate := range rateSequence {
		log.Printf("阶段%d: 速率=%d条/秒, 持续%d秒", stage+1, rate, stageDuration)
		ticker := time.NewTicker(time.Second)
		for sec := 0; sec < stageDuration; sec++ {
			<-ticker.C
			start := time.Now()
			// 记录当前时间戳和rate到csv
			ts := start.Format("2006-01-02 15:04:05.000")
			csv_util.WriteCSV([]string{ts, fmt.Sprintf("%d", rate)})
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
