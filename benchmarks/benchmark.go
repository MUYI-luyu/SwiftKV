package benchmarks

import (
	"context"
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
