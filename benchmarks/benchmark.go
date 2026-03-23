package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	kvraftapi "kvraft/raftkv/rpc"
	"kvraft/rsm"
)

// SimplePersister 实现一个简单的内存持久化器。
type SimplePersister struct {
	mu        sync.Mutex
	raftState []byte
	snapshot  []byte
}

func (sp *SimplePersister) ReadRaftState() []byte {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	return sp.raftState
}

func (sp *SimplePersister) ReadSnapshot() []byte {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	return sp.snapshot
}

func (sp *SimplePersister) Save(raftstate []byte, snapshot []byte) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	sp.raftState = make([]byte, len(raftstate))
	copy(sp.raftState, raftstate)
	sp.snapshot = make([]byte, len(snapshot))
	copy(sp.snapshot, snapshot)
}

func (sp *SimplePersister) RaftStateSize() int {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	return len(sp.raftState)
}

// BenchmarkConfig 描述压测配置。
type BenchmarkConfig struct {
	Servers   int     // 集群节点数
	Clients   int     // 并发客户端数
	Requests  int     // 每个客户端的请求数
	ReadRatio float64 // 读请求比例 (0.0 - 1.0)
	Keys      int     // key 空间大小
}

// BenchmarkResult 汇总压测结果。
type BenchmarkResult struct {
	TotalRequests   int64
	ReadRequests    int64
	WriteRequests   int64
	SuccessRequests int64
	FailedRequests  int64
	Duration        time.Duration
	MinLatency      time.Duration
	MaxLatency      time.Duration
	AvgLatency      time.Duration
}

// RunRealBenchmark 启动一个实际的 KVraft 集群进行压测
func RunRealBenchmark(ctx context.Context, cfg BenchmarkConfig) (BenchmarkResult, error) {
	if cfg.Servers <= 0 {
		cfg.Servers = 3
	}
	if cfg.Clients <= 0 {
		cfg.Clients = 10
	}
	if cfg.Requests <= 0 {
		cfg.Requests = 1000
	}
	if cfg.Keys <= 0 {
		cfg.Keys = 10000
	}
	if cfg.ReadRatio < 0 || cfg.ReadRatio > 1 {
		cfg.ReadRatio = 0.7
	}

	// 1. 构建服务器地址列表
	servers := make([]string, cfg.Servers)
	for i := 0; i < cfg.Servers; i++ {
		servers[i] = fmt.Sprintf("127.0.0.1:%d", 15000+i)
	}

	// 2. 启动集群
	fmt.Print("启动 KVraft 集群...")
	kvServers := make([]*rsm.KVServer, cfg.Servers)
	persisters := make([]*SimplePersister, cfg.Servers)

	for i := 0; i < cfg.Servers; i++ {
		persisters[i] = &SimplePersister{}
		kvServers[i] = rsm.StartKVServer(servers, 1, i, persisters[i], -1, servers[i])
	}
	fmt.Printf(" OK (%d 个节点)\n", cfg.Servers)

	// 3. 等待集群选举完成（等待 leader 产生）
	fmt.Print("等待集群选举完成...")
	time.Sleep(1000 * time.Millisecond) // 给 raft 时间选举 leader
	fmt.Println(" OK")

	// 4. 初始化 key 空间
	fmt.Print("初始化 key 空间...")
	clerk := rsm.MakeClerk(servers)
	for i := 0; i < cfg.Keys; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		_ = clerk.Put(key, value, 0)
	}
	clerk.Close()
	fmt.Printf(" OK (%d 个 key)\n", cfg.Keys)

	// 5. 运行压测
	fmt.Printf("运行压测 (%d 个客户端, 每个 %d 个请求)...\n", cfg.Clients, cfg.Requests)

	res := BenchmarkResult{
		MinLatency: time.Duration(1<<63 - 1),
	}
	var mu sync.Mutex
	var wg sync.WaitGroup
	var latencies []time.Duration
	var latenciesMu sync.Mutex
	workerClerks := make([]*rsm.Clerk, cfg.Clients)
	for i := 0; i < cfg.Clients; i++ {
		workerClerks[i] = rsm.MakeClerk(servers)
	}

	start := time.Now()

	for clientIdx := 0; clientIdx < cfg.Clients; clientIdx++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			workerClerk := workerClerks[id]
			r := rand.New(rand.NewSource(int64(id)*1000 + time.Now().UnixNano()))
			localLatencies := make([]time.Duration, 0, cfg.Requests)

			for i := 0; i < cfg.Requests; i++ {
				select {
				case <-ctx.Done():
					return
				default:
				}

				key := fmt.Sprintf("key-%d", r.Intn(cfg.Keys))
				opStart := time.Now()
				var errCode kvraftapi.Err

				if r.Float64() < cfg.ReadRatio {
					_, _, errCode = workerClerk.Get(key)
					atomic.AddInt64(&res.ReadRequests, 1)
				} else {
					value := fmt.Sprintf("value-%d-%d", id, i)
					errCode = workerClerk.Put(key, value, 0)
					atomic.AddInt64(&res.WriteRequests, 1)
				}

				latency := time.Since(opStart)
				localLatencies = append(localLatencies, latency)

				// 成功条件：返回OK、ErrNoKey或ErrVersion都算成功
				// OK: 正常成功
				// ErrNoKey: 读取的key不存在（支持的）
				// ErrVersion: 写入的版本不匹配（并发冲突被正确处理，算成功）
				if errCode == kvraftapi.OK || errCode == kvraftapi.ErrNoKey || errCode == kvraftapi.ErrVersion {
					atomic.AddInt64(&res.SuccessRequests, 1)
				} else {
					atomic.AddInt64(&res.FailedRequests, 1)
				}

				mu.Lock()
				if latency < res.MinLatency {
					res.MinLatency = latency
				}
				if latency > res.MaxLatency {
					res.MaxLatency = latency
				}
				mu.Unlock()
			}

			latenciesMu.Lock()
			latencies = append(latencies, localLatencies...)
			latenciesMu.Unlock()
		}(clientIdx)
	}

	wg.Wait()
	elapsed := time.Since(start)
	res.Duration = elapsed
	res.TotalRequests = res.ReadRequests + res.WriteRequests

	// 6. 计算平均延迟
	if len(latencies) > 0 {
		var totalLatency time.Duration
		for _, lat := range latencies {
			totalLatency += lat
		}
		res.AvgLatency = totalLatency / time.Duration(len(latencies))
	}

	// 7. 清理资源
	fmt.Print("清理资源...")
	for _, c := range workerClerks {
		if c != nil {
			c.Close()
		}
	}
	for _, kv := range kvServers {
		kv.Kill()
	}
	fmt.Println(" OK")

	return res, nil
}

func main() {
	servers := flag.Int("servers", 3, "KVraft 集群节点数")
	clients := flag.Int("clients", 10, "并发客户端数")
	requests := flag.Int("requests", 1000, "每个客户端的请求数")
	readRatio := flag.Float64("read-ratio", 0.7, "读请求比例 (0.0-1.0)")
	keys := flag.Int("keys", 10000, "key 空间大小")
	duration := flag.Duration("duration", 30*time.Second, "压测最长时长")
	flag.Parse()

	cfg := BenchmarkConfig{
		Servers:   *servers,
		Clients:   *clients,
		Requests:  *requests,
		ReadRatio: *readRatio,
		Keys:      *keys,
	}

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	fmt.Println("========================================")
	fmt.Println("     KVraft 性能基准测试 (Real Mode)")
	fmt.Println("========================================")
	fmt.Println()
	fmt.Printf("配置: %d 个节点, %d 个客户端, 每个客户端 %d 个请求\n", cfg.Servers, cfg.Clients, cfg.Requests)
	fmt.Printf("读写比: %.1f%% 读 / %.1f%% 写\n", cfg.ReadRatio*100, (1-cfg.ReadRatio)*100)
	fmt.Printf("key 空间: %d\n", cfg.Keys)
	fmt.Println()

	startTime := time.Now()
	res, err := RunRealBenchmark(ctx, cfg)
	if err != nil && err != context.DeadlineExceeded {
		fmt.Printf("错误: %v\n", err)
		return
	}

	fmt.Println()
	fmt.Println("========== 基准测试结果 ==========")
	fmt.Printf("总请求数: %d\n", res.TotalRequests)
	if res.TotalRequests > 0 {
		fmt.Printf("成功请求: %d (%.1f%%)\n", res.SuccessRequests, float64(res.SuccessRequests)*100/float64(res.TotalRequests))
		fmt.Printf("失败请求: %d (%.1f%%)\n", res.FailedRequests, float64(res.FailedRequests)*100/float64(res.TotalRequests))
		fmt.Printf("读请求: %d (%.1f%%)\n", res.ReadRequests, float64(res.ReadRequests)*100/float64(res.TotalRequests))
		fmt.Printf("写请求: %d (%.1f%%)\n", res.WriteRequests, float64(res.WriteRequests)*100/float64(res.TotalRequests))
	}
	fmt.Println()
	fmt.Printf("完整时间: %v\n", res.Duration)
	if res.Duration.Seconds() > 0 {
		fmt.Printf("吞吐: %.2f ops/s\n", float64(res.SuccessRequests)/res.Duration.Seconds())
	}
	fmt.Println()
	fmt.Printf("延迟统计:\n")
	fmt.Printf("  最小: %.2f ms\n", res.MinLatency.Seconds()*1000)
	fmt.Printf("  最大: %.2f ms\n", res.MaxLatency.Seconds()*1000)
	fmt.Printf("  平均: %.2f ms\n", res.AvgLatency.Seconds()*1000)
	fmt.Println()
	fmt.Printf("总耗时: %v\n", time.Since(startTime))
	fmt.Println("========================================")
}

func runBenchmark() {
	servers := flag.Int("servers", 3, "KVraft 集群节点数")
	clients := flag.Int("clients", 10, "并发客户端数")
	requests := flag.Int("requests", 1000, "每个客户端的请求数")
	readRatio := flag.Float64("read-ratio", 0.7, "读请求比例 (0.0-1.0)")
	keys := flag.Int("keys", 10000, "key 空间大小")
	duration := flag.Duration("duration", 30*time.Second, "压测最长时长")
	flag.Parse()

	cfg := BenchmarkConfig{
		Servers:   *servers,
		Clients:   *clients,
		Requests:  *requests,
		ReadRatio: *readRatio,
		Keys:      *keys,
	}

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	fmt.Println("========================================")
	fmt.Println("     KVraft 性能基准测试 (Real Mode)")
	fmt.Println("========================================")
	fmt.Println()
	fmt.Printf("配置: %d 个节点, %d 个客户端, 每个客户端 %d 个请求\n", cfg.Servers, cfg.Clients, cfg.Requests)
	fmt.Printf("读写比: %.1f%% 读 / %.1f%% 写\n", cfg.ReadRatio*100, (1-cfg.ReadRatio)*100)
	fmt.Printf("key 空间: %d\n", cfg.Keys)
	fmt.Println()

	startTime := time.Now()
	res, err := RunRealBenchmark(ctx, cfg)
	if err != nil && err != context.DeadlineExceeded {
		fmt.Printf("错误: %v\n", err)
		return
	}

	fmt.Println()
	fmt.Println("========== 基准测试结果 ==========")
	fmt.Printf("总请求数: %d\n", res.TotalRequests)
	if res.TotalRequests > 0 {
		fmt.Printf("成功请求: %d (%.1f%%)\n", res.SuccessRequests, float64(res.SuccessRequests)*100/float64(res.TotalRequests))
		fmt.Printf("失败请求: %d (%.1f%%)\n", res.FailedRequests, float64(res.FailedRequests)*100/float64(res.TotalRequests))
		fmt.Printf("读请求: %d (%.1f%%)\n", res.ReadRequests, float64(res.ReadRequests)*100/float64(res.TotalRequests))
		fmt.Printf("写请求: %d (%.1f%%)\n", res.WriteRequests, float64(res.WriteRequests)*100/float64(res.TotalRequests))
	}
	fmt.Println()
	fmt.Printf("完整时间: %v\n", res.Duration)
	if res.Duration.Seconds() > 0 {
		fmt.Printf("吞吐: %.2f ops/s\n", float64(res.SuccessRequests)/res.Duration.Seconds())
	}
	fmt.Println()
	fmt.Printf("延迟统计:\n")
	fmt.Printf("  最小: %.2f ms\n", res.MinLatency.Seconds()*1000)
	fmt.Printf("  最大: %.2f ms\n", res.MaxLatency.Seconds()*1000)
	fmt.Printf("  平均: %.2f ms\n", res.AvgLatency.Seconds()*1000)
	fmt.Println()
	fmt.Printf("总耗时: %v\n", time.Since(startTime))
	fmt.Println("========================================")
}
