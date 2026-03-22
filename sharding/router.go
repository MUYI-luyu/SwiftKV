package sharding

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	pb "kvraft/api/pb/kvraft/api/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// RaftGroupConfig 描述一个分组的副本信息。
type RaftGroupConfig struct {
	GroupID   int
	Replicas  []string
	LeaderIdx int
}

// ShardingConfig 描述路由层配置。
type ShardingConfig struct {
	Groups            []RaftGroupConfig
	VirtualNodeCount  int
	PreferredReplicas int
}

// ShardRouter 基于一致性哈希将请求路由到对应分组。
type ShardRouter struct {
	mu           sync.RWMutex
	config       ShardingConfig
	hashRing     *ConsistentHash
	groupConns   map[int]*grpc.ClientConn
	groupClients map[int]pb.KVServiceClient
	leaderCache  map[int]string
}

// NewShardRouter 创建并初始化分片路由器。
func NewShardRouter(cfg ShardingConfig) (*ShardRouter, error) {
	if cfg.VirtualNodeCount <= 0 {
		cfg.VirtualNodeCount = 150
	}

	r := &ShardRouter{
		config:       cfg,
		hashRing:     NewConsistentHash(cfg.VirtualNodeCount),
		groupConns:   make(map[int]*grpc.ClientConn),
		groupClients: make(map[int]pb.KVServiceClient),
		leaderCache:  make(map[int]string),
	}

	for _, g := range cfg.Groups {
		r.hashRing.AddNode(fmt.Sprintf("group-%d", g.GroupID))
	}

	if err := r.initConnections(); err != nil {
		r.Close()
		return nil, err
	}

	return r, nil
}

func (r *ShardRouter) initConnections() error {
	for _, g := range r.config.Groups {
		if len(g.Replicas) == 0 {
			continue
		}

		leader := g.Replicas[g.LeaderIdx%len(g.Replicas)]
		conn, err := grpc.Dial(
			leader,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			return fmt.Errorf("connect group %d at %s: %w", g.GroupID, leader, err)
		}

		r.groupConns[g.GroupID] = conn
		r.groupClients[g.GroupID] = pb.NewKVServiceClient(conn)
		r.leaderCache[g.GroupID] = leader
	}
	return nil
}

// Resolve 返回 key 对应的分组 ID。
func (r *ShardRouter) Resolve(key string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	node := r.hashRing.GetNode(key)
	if node == "" {
		return -1
	}

	var gid int
	_, _ = fmt.Sscanf(node, "group-%d", &gid)
	return gid
}

func (r *ShardRouter) clientForKey(key string) (pb.KVServiceClient, int, error) {
	gid := r.Resolve(key)
	if gid < 0 {
		return nil, -1, fmt.Errorf("no group for key %q", key)
	}

	r.mu.RLock()
	client, ok := r.groupClients[gid]
	r.mu.RUnlock()
	if !ok {
		return nil, -1, fmt.Errorf("no grpc client for group %d", gid)
	}

	return client, gid, nil
}

// GetRoute 路由 Get 请求。
func (r *ShardRouter) GetRoute(ctx context.Context, key string) (*pb.GetResponse, error) {
	client, _, err := r.clientForKey(key)
	if err != nil {
		return nil, err
	}
	return client.Get(ctx, &pb.GetRequest{Key: key})
}

// PutRoute 路由 Put 请求。
func (r *ShardRouter) PutRoute(ctx context.Context, key string, value string, version int64) (*pb.PutResponse, error) {
	client, _, err := r.clientForKey(key)
	if err != nil {
		return nil, err
	}
	return client.Put(ctx, &pb.PutRequest{Key: key, Value: value, Version: version})
}

// DeleteRoute 路由 Delete 请求。
func (r *ShardRouter) DeleteRoute(ctx context.Context, key string) (*pb.DeleteResponse, error) {
	client, _, err := r.clientForKey(key)
	if err != nil {
		return nil, err
	}
	return client.Delete(ctx, &pb.DeleteRequest{Key: key})
}

// BatchGet 按分组并发拉取多个 key。
func (r *ShardRouter) BatchGet(ctx context.Context, keys []string) map[string]*pb.GetResponse {
	grouped := make(map[int][]string)
	for _, key := range keys {
		gid := r.Resolve(key)
		if gid >= 0 {
			grouped[gid] = append(grouped[gid], key)
		}
	}

	results := make(map[string]*pb.GetResponse)
	var resultsMu sync.Mutex
	var wg sync.WaitGroup

	for gid, groupKeys := range grouped {
		wg.Add(1)
		go func(groupID int, ks []string) {
			defer wg.Done()

			r.mu.RLock()
			client := r.groupClients[groupID]
			r.mu.RUnlock()
			if client == nil {
				return
			}

			for _, key := range ks {
				resp, err := client.Get(ctx, &pb.GetRequest{Key: key})
				if err == nil && resp != nil {
					resultsMu.Lock()
					results[key] = resp
					resultsMu.Unlock()
				}
			}
		}(gid, groupKeys)
	}

	wg.Wait()
	return results
}

// Close 释放路由器持有的连接资源。
func (r *ShardRouter) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()

	for gid, conn := range r.groupConns {
		if conn != nil {
			_ = conn.Close()
		}
		delete(r.groupConns, gid)
		delete(r.groupClients, gid)
		delete(r.leaderCache, gid)
	}
}

// HealthChecker 定期检查各分组服务可用性。
type HealthChecker struct {
	router   *ShardRouter
	interval time.Duration
	ticker   *time.Ticker
	done     chan struct{}
	wg       sync.WaitGroup
}

// NewHealthChecker 创建健康检查器。
func NewHealthChecker(router *ShardRouter, interval time.Duration) *HealthChecker {
	return &HealthChecker{
		router:   router,
		interval: interval,
		done:     make(chan struct{}),
	}
}

// Start 启动后台健康检查协程。
func (hc *HealthChecker) Start() {
	hc.ticker = time.NewTicker(hc.interval)
	hc.wg.Add(1)

	go func() {
		defer hc.wg.Done()
		for {
			select {
			case <-hc.ticker.C:
				hc.checkGroupHealth()
			case <-hc.done:
				return
			}
		}
	}()
}

// Stop 停止后台健康检查。
func (hc *HealthChecker) Stop() {
	close(hc.done)
	if hc.ticker != nil {
		hc.ticker.Stop()
	}
	hc.wg.Wait()
}

func (hc *HealthChecker) checkGroupHealth() {
	hc.router.mu.RLock()
	groups := append([]RaftGroupConfig(nil), hc.router.config.Groups...)
	clients := make(map[int]pb.KVServiceClient, len(hc.router.groupClients))
	for gid, c := range hc.router.groupClients {
		clients[gid] = c
	}
	hc.router.mu.RUnlock()

	for _, group := range groups {
		client := clients[group.GroupID]
		if client == nil {
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_, err := client.GetClusterStatus(ctx, &pb.ClusterStatusRequest{})
		cancel()
		if err != nil {
			log.Printf("[sharding] group %d health check failed: %v", group.GroupID, err)
		}
	}
}
