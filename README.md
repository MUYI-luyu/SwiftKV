# KVraft

一个基于 Go 的分布式键值存储项目，核心目标是：
- 用 Raft 保证强一致（Linearizability）
- 用 LRU + 持久化存储提升读写性能
- 用 Watch 机制实现异步事件通知
- 用一致性哈希支持分片扩展

## 一、部署与运行指南

### 1. 环境准备

建议环境：
- Linux / macOS
- Go 1.25+
- Git

待优化环境：
- Docker / Docker Compose（用于容器化演示，当前仓库内 Docker 配置是模板，需补齐入口与配置目录后再用于生产）

安装依赖：

```bash
go mod tidy
```

### 2. 获取代码

```bash
git clone git@github.com:jianger-yu/SwiftKV.git
cd KVraft
```

快速检查：

```bash
go build ./...
go test ./rsm -v
```

### 3. 启动服务

#### 方式 A：脚本启动 3 节点（推荐）

```bash
bash examples/start_cluster.sh
```

默认节点地址：
- 127.0.0.1:5001
- 127.0.0.1:5002
- 127.0.0.1:5003

#### 方式 B：手动启动（3 个终端）

```bash
go run examples/server/main.go -me 0 -servers 127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003 -port 5001
go run examples/server/main.go -me 1 -servers 127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003 -port 5002
go run examples/server/main.go -me 2 -servers 127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003 -port 5003
```

#### 客户端示例

```bash
go run examples/basic/main.go
go run examples/scenarios/main.go
```

## 二、项目架构

### 1. 系统环境与架构

KVraft 采用“共识层 + 状态机层 + 存储层 + 缓存层 + 事件层 + 接口层”的分层设计：

- 共识层（raft/）：Raft 选主、日志复制、提交应用
- 状态机层（rsm/）：将客户端请求封装为操作并提交到 Raft，提交后执行 DoOp
- 存储层（storage/）：基于 BadgerDB 的持久化读写，维护 value + version
- 缓存层（cache/）：LRU 热点缓存，加速读请求
- 事件层（watch/）：发布/订阅模型，支持异步通知与背压处理
- 分片层（sharding/）：一致性哈希将 key 路由到节点
- 接口层（api/pb, raftkv/rpc）：proto 契约 + gRPC/RPC 调用结构

### 2. 主要功能概述

#### Raft 与 KVraft 框架
- Raft 负责“复制与提交顺序”
- KVraft（RSM）负责“业务语义执行”
- 请求流：Client -> RSM.Submit -> Raft.Start -> Apply -> KVServer.DoOp

#### LRU 缓存
- Get 先查 LRU，命中直接返回
- 未命中则回源 store，并回填缓存
- Put 成功后同步更新缓存

#### Hash 分片
- 使用一致性哈希（虚拟节点）进行 key 到节点映射
- 节点增减时只迁移部分 key，降低重分布成本

#### Watch 机制
- 订阅者可按 key 或前缀订阅
- 写入成功后触发事件：Applied -> Notify -> Watcher.Channel
- 内置缓冲、死信队列和统计指标，减少慢消费者拖垮主路径的风险

#### Store 层
- 运用“可靠的本地 KV 数据库”
- 当前项目通过 BadgerDB 完成持久化，负责把数据写到磁盘并支持读取

#### API 与 Proto
- proto 文件定义服务接口和消息结构
- 代码生成后用于强类型通信，减少手写序列化错误
- 典型 RPC：Get、Put、Delete、Scan、Watch、ClusterStatus

#### Docker
- 仓库包含 Dockerfile / docker-compose.yml 作为容器化模板
- 目前模板与当前目录结构有偏差（如缺少 cmd/server/main.go、config/）
- 可作为后续部署模板，有待优化

### 3. 用户使用说明

#### 基本写入/读取
1. 启动 3 节点
2. 用 Clerk 调 Put(key, value, version)
3. 用 Clerk 调 Get(key) 获取 value 和 version

#### CAS（版本控制）
- 新建 key：version=0
- 更新 key：必须带当前 version
- 版本不匹配返回 ErrVersion，避免并发覆盖

#### Watch 使用
- 客户端订阅 key
- 其他客户端写入后接收事件
- 适合配置中心、服务发现、变更广播场景

### 4. 关键技术亮点

- 强一致：Raft 保证写入顺序和提交一致
- 读优化：LRU 热点缓存降低读延迟
- 原子更新：Version-CAS 防并发写冲突
- 事件驱动：Watch 异步通知链路完整
- 可扩展：一致性哈希为多分片扩展打基础

### 5. 维护与注意事项

- 测试优先：修改核心链路后先跑 `go test ./rsm -v`
- 注意版本语义：不要绕过 CAS 直接覆盖写
- Watch 需要关注消费速度，避免业务端长时间不读 channel
- 容器配置需与当前代码入口保持同步，避免“文档可跑、镜像不可跑”

### 6. 未来改进方向

- 完整 Read-Index 线性化读路径
- Watch 事件持久化（支持重放）
- 分片动态迁移与再均衡
- 指标与日志标准化（Prometheus + 结构化日志）
- Docker 模板与实际启动入口对齐

## 三、项目目录（核心）

```text
KVraft/
├── raft/            # Raft 共识层
├── rsm/             # 复制状态机（KVraft 核心）
├── storage/         # 持久化存储（BadgerDB）
├── cache/           # LRU 缓存
├── watch/           # Watch 订阅与事件分发
├── sharding/        # 一致性哈希分片
├── api/pb/          # proto 与生成代码
├── raftkv/rpc/      # RPC 类型与接口
├── examples/        # 启动与使用示例
└── docker-compose.yml / Dockerfile
```

## 四、常用命令速查

```bash
# 编译
go build ./...

# 核心测试
go test ./rsm -v

# 启动集群
bash examples/start_cluster.sh

# 运行示例
go run examples/basic/main.go
go run examples/scenarios/main.go
```
# SwiftKV
