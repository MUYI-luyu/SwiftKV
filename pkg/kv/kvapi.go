// Package kv 定义 KVraft 分布式键值存储的业务类型。
// 该包包含所有客户端/服务器之间的请求/响应数据结构，
// 以及错误码和版本号类型。
package kv

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
