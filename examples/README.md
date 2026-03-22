# examples 使用指南

本目录用于演示 KVraft 的启动方式与客户端调用示例。

## 目录说明

```text
examples/
├── server/main.go      # 启动单个 KVraft 节点
├── basic/main.go       # 基础功能示例（Put/Get/CAS/错误处理）
├── scenarios/main.go   # 场景化示例（计数器、缓存、配置管理）
├── start_cluster.sh    # 一键启动 3 节点脚本
└── README.md
```

## 是否有文件冗余？

结论：当前 examples 目录无明显冗余文件，建议全部保留。

- server/main.go：用于本地多节点启动，是所有示例前提
- basic/main.go：适合快速验证核心 API
- scenarios/main.go：体现真实业务场景和并发操作
- start_cluster.sh：降低启动成本，便于测试和演示

## 快速运行

### 1. 启动 3 节点

```bash
bash examples/start_cluster.sh
```

### 2. 运行基础示例

```bash
go run examples/basic/main.go
```

### 3. 运行场景示例

```bash
go run examples/scenarios/main.go
```

## 手动启动（不使用脚本）

在 3 个终端分别执行：

```bash
go run examples/server/main.go -me 0 -servers 127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003 -port 5001
go run examples/server/main.go -me 1 -servers 127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003 -port 5002
go run examples/server/main.go -me 2 -servers 127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003 -port 5003
```

## 常见问题

### 1) 连接失败（connection refused）

先确认 3 个节点已经启动，并监听 5001/5002/5003。

### 2) 出现 ErrVersion

这是 CAS 语义下的正常并发冲突提示，先重新 Get 最新版本再 Put。

### 3) 一个节点挂掉还能用吗

3 节点集群可容忍 1 个节点故障（取决于多数派是否仍然可达）。
