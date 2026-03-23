package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"time"
)

// Config 描述一个轻量级压测配置。
type Config struct {
	Requests  int
	ReadRatio float64
	Keys      int
}

// Result 汇总压测结果。
type Result struct {
	TotalRequests int
	ReadRequests  int
	WriteRequests int
	Duration      time.Duration
}

// RunSynthetic 在进程内执行一个不依赖网络的合成压测。
// 该函数用于保证 benchmarks 包可编译，并提供最小可复用基准逻辑。
func RunSynthetic(ctx context.Context, cfg Config) (Result, error) {
	if cfg.Requests <= 0 {
		cfg.Requests = 1000
	}
	if cfg.Keys <= 0 {
		cfg.Keys = 128
	}
	if cfg.ReadRatio < 0 || cfg.ReadRatio > 1 {
		cfg.ReadRatio = 0.7
	}

	store := make(map[string]string, cfg.Keys)
	for i := 0; i < cfg.Keys; i++ {
		store[fmt.Sprintf("key-%d", i)] = fmt.Sprintf("value-%d", i)
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	start := time.Now()
	res := Result{TotalRequests: cfg.Requests}

	for i := 0; i < cfg.Requests; i++ {
		select {
		case <-ctx.Done():
			res.Duration = time.Since(start)
			return res, ctx.Err()
		default:
		}

		key := fmt.Sprintf("key-%d", r.Intn(cfg.Keys))
		if r.Float64() < cfg.ReadRatio {
			_ = store[key]
			res.ReadRequests++
			continue
		}

		store[key] = fmt.Sprintf("value-%d", i)
		res.WriteRequests++
	}

	res.Duration = time.Since(start)
	return res, nil
}

func main() {
	nodes := flag.String("nodes", "localhost:6000", "目标节点列表（当前用于展示）")
	workload := flag.String("workload", "write", "工作负载类型：read/write/mixed")
	clients := flag.Int("clients", 10, "并发客户端数")
	duration := flag.Duration("duration", 30*time.Second, "压测时长")
	keys := flag.Int("keys", 10000, "key 空间大小")
	readRatio := flag.Float64("read-ratio", 0.5, "读请求比例")
	flag.Parse()

	// 这里的请求总量采用“客户端数 * 每秒固定请求数 * 持续秒数”的粗粒度估算。
	requests := *clients * int(duration.Seconds()) * 1000
	if requests <= 0 {
		requests = 1000
	}

	cfg := Config{
		Requests:  requests,
		ReadRatio: *readRatio,
		Keys:      *keys,
	}

	if *workload == "read" {
		cfg.ReadRatio = 1.0
	}
	if *workload == "write" {
		cfg.ReadRatio = 0.0
	}

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	start := time.Now()
	res, err := RunSynthetic(ctx, cfg)
	elapsed := time.Since(start)
	if err != nil && err != context.DeadlineExceeded {
		fmt.Printf("benchmark failed: %v\n", err)
		return
	}

	opsPerSec := float64(res.TotalRequests)
	if elapsed.Seconds() > 0 {
		opsPerSec = float64(res.TotalRequests) / elapsed.Seconds()
	}

	fmt.Println("=== Benchmark Result ===")
	fmt.Printf("nodes: %s\n", *nodes)
	fmt.Printf("workload: %s\n", *workload)
	fmt.Printf("clients: %d\n", *clients)
	fmt.Printf("duration: %s\n", elapsed)
	fmt.Printf("total requests: %d\n", res.TotalRequests)
	fmt.Printf("read requests: %d\n", res.ReadRequests)
	fmt.Printf("write requests: %d\n", res.WriteRequests)
	fmt.Printf("throughput: %.2f ops/s\n", opsPerSec)
}
