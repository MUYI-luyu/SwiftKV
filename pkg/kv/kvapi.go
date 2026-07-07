// Package kv 定义 KVraft 分布式键值存储的业务类型。
// 该包包含所有客户端/服务器之间的请求/响应数据结构，
// 以及错误码和版本号类型。
package kv

import (
	"kvraft/pkg/raft"
	"kvraft/pkg/watch"
)

// Tversion 表示一个键的版本号。
type Tversion int

// Err 表示操作结果的错误码。
type Err string

const (
	OK             Err = "OK"
	ErrNoKey       Err = "ErrNoKey"
	ErrWrongLeader Err = "ErrWrongLeader"
	ErrVersion     Err = "ErrVersion"
	ErrMaybe       Err = "ErrMaybe"
	ErrWrongGroup  Err = "ErrWrongGroup"  // shard 不属于本 group
)

// GetArgs 是 Get 操作的参数。
type GetArgs struct {
	Key string
}

// GetReply 是 Get 操作的结果。
type GetReply struct {
	Value   string
	Version Tversion
	Expires int64
	Err     Err
}

// PutArgs 是 Put 操作的参数。
type PutArgs struct {
	Key     string
	Value   string
	Version Tversion
	TTL     int64 // seconds; <=0 means no expiry
}

// PutReply 是 Put 操作的结果。
type PutReply struct {
	Err      Err
	OldValue string // 修改前的值（用于Watch事件）
}

// DeleteArgs 是 Delete 操作的参数。
type DeleteArgs struct {
	Key string
}

// DeleteReply 是 Delete 操作的结果。
type DeleteReply struct {
	Err      Err
	OldValue string
}

// ScanArgs 是 Scan 操作的参数。
type ScanArgs struct {
	Prefix string
	Limit  int32
}

// ScanItem 是 Scan 操作返回的单个键值项。
type ScanItem struct {
	Key     string
	Value   string
	Version Tversion
	Expires int64
}

// ScanReply 是 Scan 操作的结果。
type ScanReply struct {
	Items []ScanItem
	Err   Err
}

// ExpireArgs 是过期键清理操作的参数。
type ExpireArgs struct {
	Keys   []string
	Cutoff int64
}

// ExpireReply 是过期键清理操作的结果。
type ExpireReply struct {
	ExpiredKeys      []string
	ExpiredOldValues map[string]string
	Err              Err
}

// OpCompleteListener 操作完成监听器接口
// 用于在 Raft 日志提交后回调，例如触发 Watch 事件
type OpCompleteListener interface {
	// OnOpComplete 在操作被 Raft 提交和应用后调用
	// req: 原始请求
	// result: 操作结果
	// index: Raft 日志索引
	OnOpComplete(req any, result any, index int64)
}

// ApplyLoopPerfStats 记录 applyLoop 的性能统计
type ApplyLoopPerfStats struct {
	BlockedNanos    int64
	ProcessNanos    int64
	IterationCount  int64
	BlockedAvgNanos float64
	ProcessAvgNanos float64
}

// RSMInterface 是复制状态机（RSM）的核心接口，供 KVServer 和 gRPC 层调用。
type RSMInterface interface {
	Submit(req any) (Err, any)
	SubmitLeaseReadWithMode(req any) (Err, any, bool)
	GetWatchManager() *watch.Manager
	RegisterOpCompleteListener(listener OpCompleteListener)
	GetState() (int, bool)
	GetLastApplied() int
	IsLeaderWithLease() bool
	Close()
	ApplyLoopPerfStatsSnapshot() ApplyLoopPerfStats
	RaftPerfStatsSnapshot() raft.RaftPerfStats
}
