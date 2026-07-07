package kv

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	pb "kvraft/api/pb/kvraft/api/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// shardMeta 记录单个 shard 的本地状态。
type shardMeta struct {
	state       pb.ShardState // Owned / Migrating / Importing / Absent
	targetGroup int           // MIGRATING 时双写转发的目标 group
}

// shardStateManager 管理本节点上所有 shard 的状态。
// 迁移协调器（Migrator）通过 gRPC 调用 SetShardState 修改状态。
type shardStateManager struct {
	mu       sync.RWMutex
	states   map[int]shardMeta // shardID → meta
	epoch    atomic.Int64      // 本节点已知的拓扑版本号
	groupID  int               // 本节点所属 group
	numShards int

	// 双写转发用的连接池：target group → gRPC client
	forwardClients map[int]pb.KVServiceClient
	forwardConns   map[int]*grpc.ClientConn
}

func newShardStateManager(groupID, numShards int) *shardStateManager {
	ssm := &shardStateManager{
		states:         make(map[int]shardMeta, numShards),
		groupID:        groupID,
		numShards:      numShards,
		forwardClients: make(map[int]pb.KVServiceClient),
		forwardConns:   make(map[int]*grpc.ClientConn),
	}
	ssm.epoch.Store(1)

	// 初始状态：所有 shard 属于本 group（单 group 部署）
	for i := 0; i < numShards; i++ {
		ssm.states[i] = shardMeta{state: pb.ShardState_OWNED}
	}
	return ssm
}

// SetShardState 修改 shard 状态。由迁移协调器通过 gRPC 调用。
func (ssm *shardStateManager) SetShardState(shardID int, state pb.ShardState, targetGroup int, epoch int64) error {
	if shardID < 0 || shardID >= ssm.numShards {
		return fmt.Errorf("shard %d out of range [0,%d)", shardID, ssm.numShards)
	}

	ssm.mu.Lock()
	defer ssm.mu.Unlock()

	// 更新 epoch（只增不减）
	if epoch > ssm.epoch.Load() {
		ssm.epoch.Store(epoch)
	}

	ssm.states[shardID] = shardMeta{state: state, targetGroup: targetGroup}
	log.Printf("[shard-state] group=%d shard=%d state=%v targetGroup=%d epoch=%d",
		ssm.groupID, shardID, state, targetGroup, epoch)
	return nil
}

// GetShardState 返回 shard 的本地状态。
func (ssm *shardStateManager) GetShardState(shardID int) (shardMeta, bool) {
	ssm.mu.RLock()
	defer ssm.mu.RUnlock()
	meta, ok := ssm.states[shardID]
	return meta, ok
}

// GetEpoch 返回当前拓扑版本号。
func (ssm *shardStateManager) GetEpoch() int64 {
	return ssm.epoch.Load()
}

// AllStates 返回所有 shard 状态快照（调试用）。
func (ssm *shardStateManager) AllStates() map[int]shardMeta {
	ssm.mu.RLock()
	defer ssm.mu.RUnlock()
	out := make(map[int]shardMeta, len(ssm.states))
	for k, v := range ssm.states {
		out[k] = v
	}
	return out
}

// ensureForwardClient 为目标 group 建立或复用 gRPC 连接。
func (ssm *shardStateManager) ensureForwardClient(targetGroup int, replicas []string) (pb.KVServiceClient, error) {
	ssm.mu.Lock()
	defer ssm.mu.Unlock()

	if c, ok := ssm.forwardClients[targetGroup]; ok {
		return c, nil
	}

	// 尝试每个 replica 直到有一个连通
	var lastErr error
	for _, addr := range replicas {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		conn, err := grpc.DialContext(ctx, addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
		)
		cancel()
		if err != nil {
			lastErr = err
			continue
		}
		ssm.forwardConns[targetGroup] = conn
		ssm.forwardClients[targetGroup] = pb.NewKVServiceClient(conn)
		return ssm.forwardClients[targetGroup], nil
	}
	return nil, fmt.Errorf("no reachable replica for group %d: %v", targetGroup, lastErr)
}

// ForwardWrite 将 Put 请求转发到目标 group（双写阶段）。
func (ssm *shardStateManager) ForwardWrite(ctx context.Context, targetGroup int, key, value string, version int64) error {
	ssm.mu.RLock()
	client, ok := ssm.forwardClients[targetGroup]
	ssm.mu.RUnlock()
	if !ok {
		return fmt.Errorf("no forward client for group %d", targetGroup)
	}

	resp, err := client.Put(ctx, &pb.PutRequest{
		Key: key, Value: value, Version: version,
	})
	if err != nil {
		return err
	}
	if resp.GetError() != "" && resp.GetError() != "OK" && resp.GetError() != "ErrNoKey" {
		return fmt.Errorf("forward put failed: %s", resp.GetError())
	}
	return nil
}

// ForwardDelete 将 Delete 请求转发到目标 group（双写阶段）。
func (ssm *shardStateManager) ForwardDelete(ctx context.Context, targetGroup int, key string) error {
	ssm.mu.RLock()
	client, ok := ssm.forwardClients[targetGroup]
	ssm.mu.RUnlock()
	if !ok {
		return fmt.Errorf("no forward client for group %d", targetGroup)
	}

	resp, err := client.Delete(ctx, &pb.DeleteRequest{Key: key})
	if err != nil {
		return err
	}
	if resp.GetError() != "" && resp.GetError() != "OK" && resp.GetError() != "ErrNoKey" {
		return fmt.Errorf("forward delete failed: %s", resp.GetError())
	}
	return nil
}

// Close 释放双写转发连接。
func (ssm *shardStateManager) Close() {
	ssm.mu.Lock()
	defer ssm.mu.Unlock()
	for _, conn := range ssm.forwardConns {
		_ = conn.Close()
	}
	ssm.forwardClients = make(map[int]pb.KVServiceClient)
	ssm.forwardConns = make(map[int]*grpc.ClientConn)
}
