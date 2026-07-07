package raft

// Node 是 Raft 共识节点必须暴露的接口。
// 应用层（如 KVServer）通过此接口与 Raft 层交互。
type Node interface {
	// Start 对一条新日志条目达成共识，返回该条目的日志索引、任期号以及本节点是否为 leader。
	Start(command interface{}) (int, int, bool)

	// GetState 返回本节点当前的任期号及是否为 leader。
	GetState() (int, bool)

	// GetLastApplied 返回已应用到状态机的最高日志索引。
	GetLastApplied() int

	// Snapshot 触发快照功能。
	Snapshot(index int, snapshot []byte)

	// GetLastIncludedIndex 返回当前快照覆盖到的最后日志索引。
	GetLastIncludedIndex() int

	// SyncAppliedIndex 在状态机自行恢复后对齐 Raft 的已提交/已应用进度。
	SyncAppliedIndex(index int)

	// PersistBytes 返回 Raft 持久化日志的字节数。
	PersistBytes() int

	// Kill 告知本节点应杀死后台 goroutine。
	Kill()
}

// ApplyMsg 是 Raft 向应用层（如 KVServer）报告已提交日志或安装快照的消息。
// 当 Raft 节点识别出连续的日志条目已提交时，应通过 applyCh 发送此消息。
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
