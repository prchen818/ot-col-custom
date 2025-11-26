package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/prchen818/ot-col-custom/pkg/util"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	dataset := os.Getenv("DATASET")
	if dataset == "" {
		dataset = "error-mid"
	}
	path := fmt.Sprintf("data/%s/ot_trace.txt", dataset)

	traces, err := util.LoadData(path)
	if err != nil {
		log.Fatalf("加载trace数据失败: %v", err)
	}
	//sendTrace(traces)
	stressTest(traces, dataset)
}

func sendTrace(traces []ptrace.Traces) {

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
	for _, trace := range traces {
		_, err = client.Export(context.Background(), ptraceotlp.NewExportRequestFromTraces(trace))
		if err != nil {
			log.Printf("发送trace失败: %v", err)
		}
	}
	log.Printf("所有trace发送完毕")
}

func stressTest(traces []ptrace.Traces, dataset string) {
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
	if err := util.InitCSV(fmt.Sprintf("data/rate_log_%s.csv", dataset), []string{"timestamp", "rate"}); err != nil {
		log.Fatalf("初始化CSV失败: %v", err)
	}
	defer util.CloseCSV()

	// 预设速率变化序列，每个阶段持续10秒
	//rateSequence := []int{40, 80, 180, 300, 300, 300, 240, 160, 100, 80, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 100, 200, 100, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40}
	rateSequence, sum, err := readRateConfig("workload.csv")
	if err != nil {
		return
	}
	stageDuration := float64(len(traces)) / float64(sum)
	log.Printf("开始按照预设速率序列发送trace到: %s, 时间系数: %f", endpoint, stageDuration)
	//stressTestRandomOrder(traces, client, rateSequence, stageDuration)
	stressTestFixedOrder(traces, client, rateSequence, stageDuration)
}

func stressTestFixedOrder(
	traces []ptrace.Traces,
	client ptraceotlp.GRPCClient,
	rateSequence []int,
	stageDuration float64) {

	log.Printf("发送方式: 固定顺序")
	traceIndex := 0
	ticker := time.NewTicker(time.Duration(1e9 * stageDuration))
	start := time.Now()
	for stage, rate := range rateSequence {
		<-ticker.C
		now := time.Now()
		// 记录当前时间戳和rate到csv
		ts := now.Format("2006-01-02 15:04:05.000")
		log.Printf("阶段%d: 速率=%d条/秒, 持续%f秒", stage+1, rate, stageDuration)
		util.WriteCSV([]string{ts, fmt.Sprintf("%d", rate)})
		for i := 0; i < int(float64(rate)*stageDuration); i++ {
			traceData := traces[traceIndex]
			_, err := client.Export(context.Background(), ptraceotlp.NewExportRequestFromTraces(traceData))
			if err != nil {
				log.Printf("发送trace失败: %v", err)
			}
			traceIndex++
			if traceIndex >= len(traces) {
				traceIndex = 0 // 循环使用trace数据
			}
		}
		log.Printf("已发送 %d 条trace, 用时: %v", int(float64(rate)*stageDuration), time.Since(start))
		start = time.Now()
	}
	ticker.Stop() // 停止阶段ticker

	// 按整体平均速率发送最后剩余的trace，而不是一次性发送
	if traceIndex < len(traces) {
		remaining := len(traces) - traceIndex

		// 计算整体平均速率 = 所有阶段速率的算术平均
		sumRates := 0
		for _, r := range rateSequence {
			sumRates += r
		}
		avgRate := float64(sumRates) / float64(len(rateSequence))
		base := int(avgRate)
		frac := avgRate - float64(base)
		acc := 0.0

		log.Printf("发送剩余 %d 条trace，按平均速率 %.2f 条/秒", remaining, avgRate)

		perSecTicker := time.NewTicker(time.Second)
		defer perSecTicker.Stop()

		for remaining > 0 {
			<-perSecTicker.C

			// 每秒应发送的条数（处理小数部分累积，逼近平均值）
			sendCount := base
			acc += frac
			if acc >= 1.0 {
				sendCount++
				acc -= 1.0
			}
			if sendCount > remaining {
				sendCount = remaining
			}

			start := time.Now()
			ts := start.Format("2006-01-02 15:04:05.000")
			util.WriteCSV([]string{ts, fmt.Sprintf("%d", int(avgRate))})

			for i := 0; i < sendCount; i++ {
				traceData := traces[traceIndex]
				_, err := client.Export(context.Background(), ptraceotlp.NewExportRequestFromTraces(traceData))
				if err != nil {
					log.Printf("发送trace失败: %v", err)
				}
				traceIndex++
				// 保持与前面一致的索引回绕逻辑，防止越界
				if traceIndex >= len(traces) {
					traceIndex = 0
				}
				remaining--
			}
			log.Printf("已发送 %d 条trace(剩余 %d), 用时: %v", sendCount, remaining, time.Since(start))
		}
	}
	log.Printf("已发送 %d / %d 条trace，程序退出", traceIndex, len(traces))
}

func stressTestRandomOrder(
	traces []ptrace.Traces,
	client ptraceotlp.GRPCClient,
	rateSequence []int,
	stageDuration int) {

	log.Printf("发送方式: 随机顺序")

	for stage, rate := range rateSequence {
		log.Printf("阶段%d: 速率=%d条/秒, 持续%d秒", stage+1, rate, stageDuration)
		ticker := time.NewTicker(time.Second)
		for sec := 0; sec < stageDuration; sec++ {
			<-ticker.C
			start := time.Now()
			// 记录当前时间戳和rate到csv
			ts := start.Format("2006-01-02 15:04:05.000")
			util.WriteCSV([]string{ts, fmt.Sprintf("%d", rate)})
			for i := 0; i < rate; i++ {
				idx := rand.Intn(len(traces))
				traceData := traces[idx]
				_, err := client.Export(context.Background(), ptraceotlp.NewExportRequestFromTraces(traceData))
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

func readRateConfig(path string) ([]int, int, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}
	defer file.Close()
	var sum int
	var rates []int
	for {
		var rate int
		_, err := fmt.Fscanf(file, "%d\n", &rate)
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return nil, 0, err
		}
		rates = append(rates, rate)
		sum += rate
	}
	return rates, sum, nil
}
