package rsm

import (
	"fmt"
	"net"
	"net/rpc"
	"time"

	kvraftapi "kvraft/raftkv/rpc"
	"kvraft/sharding"
)

type Clerk struct {
	servers     []string
	leader      int // 记录最近成功的leader
	hasher      *sharding.ConsistentHash
	serverToIdx map[string]int
}

func MakeClerk(servers []string) *Clerk {
	ck := &Clerk{
		servers:     servers,
		leader:      0,
		hasher:      sharding.NewConsistentHash(32),
		serverToIdx: make(map[string]int, len(servers)),
	}
	for i, s := range servers {
		ck.serverToIdx[s] = i
		ck.hasher.AddNode(s)
	}
	return ck
}

func (ck *Clerk) preferredIndex(key string) int {
	if len(ck.servers) == 0 || ck.hasher == nil {
		return 0
	}
	node := ck.hasher.GetNode(key)
	if idx, ok := ck.serverToIdx[node]; ok {
		return idx
	}
	return ck.leader
}

func (ck *Clerk) preferredCandidates(key string) []int {
	if len(ck.servers) == 0 || ck.hasher == nil {
		return []int{0}
	}

	count := 3
	if len(ck.servers) < count {
		count = len(ck.servers)
	}

	nodes := ck.hasher.GetNodes(key, count)
	seen := make(map[int]bool, len(ck.servers))
	idxs := make([]int, 0, len(ck.servers))

	for _, n := range nodes {
		if idx, ok := ck.serverToIdx[n]; ok && !seen[idx] {
			idxs = append(idxs, idx)
			seen[idx] = true
		}
	}

	if !seen[ck.leader] && ck.leader >= 0 && ck.leader < len(ck.servers) {
		idxs = append(idxs, ck.leader)
		seen[ck.leader] = true
	}

	for i := range ck.servers {
		if !seen[i] {
			idxs = append(idxs, i)
		}
	}

	if len(idxs) == 0 {
		return []int{0}
	}
	return idxs
}

func call(server string, rpcname string, args interface{}, reply interface{}) bool {
	conn, err := net.DialTimeout("tcp", server, 600*time.Millisecond)
	if err != nil {
		return false
	}
	_ = conn.SetDeadline(time.Now().Add(1200 * time.Millisecond))
	client := rpc.NewClient(conn)
	defer client.Close()

	err = client.Call(rpcname, args, reply)
	return err == nil
}

// Get 获取一个键的当前值和版本。如果键不存在，返回 ErrNoKey。
// 在面对所有其他错误时，它会不断重试。
//
// 你可以使用如下代码向服务器 i 发送 RPC：
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// args 和 reply 的类型（包括它们是否为指针）必须与 RPC 处理函数的
// 参数的声明类型相匹配。此外，reply 必须作为指针传递。
func (ck *Clerk) Get(key string) (string, kvraftapi.Tversion, kvraftapi.Err) {
	args := kvraftapi.GetArgs{Key: key}
	timeout := time.After(10 * time.Second)
	attempts := 0

	for {
		select {
		case <-timeout:
			fmt.Println("\n⚠️  获取操作超时（10秒）")
			fmt.Println("提示: 请确保 KVraft 服务器已启动:")
			fmt.Println("  bash examples/start_cluster.sh")
			return "", 0, kvraftapi.ErrWrongLeader
		default:
		}

		candidates := ck.preferredCandidates(key)
		for _, index := range candidates {
			reply := kvraftapi.GetReply{}
			ok := call(ck.servers[index], "KVServer.Get", &args, &reply)

			if ok {
				ck.leader = index
				switch reply.Err {
				case kvraftapi.OK:
					return reply.Value, reply.Version, reply.Err
				case kvraftapi.ErrNoKey:
					return "", 0, reply.Err
				}
			}

			attempts++
			if attempts > 100 {
				fmt.Printf("\n⚠️  Get 已尝试 %d 次，仍无可用服务器\n", attempts)
				fmt.Println("提示: 请确保 KVraft 服务器已启动:")
				fmt.Println("  bash examples/start_cluster.sh")
				return "", 0, kvraftapi.ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// Put 仅当请求中的版本与服务器上该键的版本匹配时，才会使用值更新键。
// 如果版本号不匹配，服务器应返回 ErrVersion。如果 Put 在其第一个 RPC
// 上收到 ErrVersion，Put 应返回 ErrVersion，因为 Put 肯定没有在服务器上
// 执行。如果服务器在重新发送 RPC 时返回 ErrVersion，那么 Put 必须向应用
// 返回 ErrMaybe，因为其较早的 RPC 可能已被服务器成功处理，但响应丢失了，
// Clerk 不知道 Put 是否被执行了。
//
// 你可以使用如下代码向服务器 i 发送 RPC：
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// args 和 reply 的类型（包括它们是否为指针）必须与 RPC 处理函数的
// 参数的声明类型相匹配。此外，reply 必须作为指针传递。
func (ck *Clerk) Put(key string, value string, version kvraftapi.Tversion) kvraftapi.Err {
	args := kvraftapi.PutArgs{Key: key, Value: value, Version: version}
	retry := false
	timeout := time.After(10 * time.Second)
	attempts := 0

	for {
		select {
		case <-timeout:
			fmt.Println("\n⚠️  写入操作超时（10秒）")
			fmt.Println("提示: 请确保 KVraft 服务器已启动:")
			fmt.Println("  bash examples/start_cluster.sh")
			return kvraftapi.ErrWrongLeader
		default:
		}

		candidates := ck.preferredCandidates(key)
		for _, index := range candidates {
			reply := kvraftapi.PutReply{}
			ok := call(ck.servers[index], "KVServer.Put", &args, &reply)
			if ok {
				switch reply.Err {
				case kvraftapi.OK, kvraftapi.ErrNoKey:
					ck.leader = index
					return reply.Err
				case kvraftapi.ErrVersion:
					ck.leader = index
					if !retry {
						return kvraftapi.ErrVersion
					}
					return kvraftapi.ErrMaybe
				default: // kvraftapi.ErrWrongLeader
				}
			}

			attempts++
			if attempts > 100 {
				fmt.Printf("\n⚠️  Put 已尝试 %d 次，仍无可用服务器\n", attempts)
				fmt.Println("提示: 请确保 KVraft 服务器已启动:")
				fmt.Println("  bash examples/start_cluster.sh")
				return kvraftapi.ErrWrongLeader
			}
		}

		retry = true
		time.Sleep(100 * time.Millisecond)
	}
}
