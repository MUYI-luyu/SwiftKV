# KVraft 项目简历描述版

## 一句话概述
在 Go 语言下，基于 Raft 共识协议与 BadgerDB 持久化引擎实现分布式 KV 存储系统，具备强一致性、版本化原子更新、异步 Watch 通知与分片扩展能力。

## 项目描述（可直接放简历）
设计并实现分布式 KVraft 存储系统：以 Raft 作为复制状态机共识基础，保证线性一致写入；在状态机层实现 Version-CAS 语义，避免并发覆盖；引入 LRU 热点缓存优化读路径；基于一致性哈希实现分片路由；提供 Watch 异步事件链路支持配置变更实时推送；通过 proto/gRPC 定义接口契约，形成可扩展的服务边界。

## 核心实现亮点（简历要点）

- 基于 Raft 构建 KVraft 复制状态机框架，完成请求提交、日志应用、状态机执行闭环，保障多节点下的数据一致性与容错能力。
- 在存储层封装 BadgerDB（LSM-Tree 引擎）实现 value+version 持久化读写，支持快照加载/导出与故障恢复基础能力。
- 设计 Version-CAS 原子更新语义：客户端携带版本写入，版本不匹配返回 ErrVersion，有效抑制并发写冲突。
- 实现 LRU 缓存拦截读请求（命中直返，未命中回源并回填），降低热点读取延迟并提升整体吞吐。
- 实现 Watch 发布订阅机制，形成 Applied -> Notify -> Watcher 的异步通知链路，并通过缓冲队列/死信队列缓解背压。
- 实现一致性哈希分片（虚拟节点），支持 key 路由与节点扩缩容场景下的低迁移成本。
- 定义 proto 接口并生成通信代码，覆盖 Get/Put/Delete/Scan/Watch/ClusterStatus 等能力，提升接口演进效率与跨模块协作效率。

## 可量化结果（建议按实际数据替换）

- 集成测试：通过 `go test ./rsm -v` 全链路验证（Put/Get/CAS/Watch）。
- 延迟表现：Get（缓存命中）< 1ms，Put 在毫秒级（受网络与复制延迟影响）。
- 稳定性：支持 3 节点部署，单节点故障场景下服务可继续。

## 面试讲解模板（30 秒版本）
我做的是一个 Go 实现的分布式 KV 引擎。核心是用 Raft 保证线性一致，状态机层做 Version-CAS 防并发覆盖；读路径用 LRU 做热点加速，写入后异步触发 Watch 通知；分片层用一致性哈希支撑横向扩展。接口通过 proto 定义，方便 gRPC 化和后续多语言接入。

## 面试讲解模板（2 分钟版本）
项目分为 6 层：Raft 共识、RSM 执行、Store 持久化、LRU 缓存、Watch 事件、API 契约。写请求先进入 Raft 复制并提交，再由状态机执行，保证所有副本按相同顺序应用；读请求优先走缓存，未命中回源存储并回填。为解决并发写冲突，我实现了 Version-CAS：只有版本匹配才允许更新。对于配置变更实时感知场景，我增加了 Watch 机制，写入完成后异步广播事件，并通过缓冲与死信策略处理慢消费者。分片方面使用一致性哈希，减少扩缩容时的数据迁移量。整体上这是一个强调一致性、可扩展性和工程可维护性的分布式存储项目。

## 关键词（ATS 友好）
Go, Raft, Distributed KV, Linearizability, Replicated State Machine, CAS, LRU Cache, Consistent Hashing, Watch, gRPC, Protobuf, BadgerDB, Fault Tolerance
