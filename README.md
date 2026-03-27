# KVraft

KVraft 是一个基于 Go 的分布式强一致键值存储系统，核心链路为：

Client -> RSM -> Raft -> Apply -> Storage(BadgerDB)

项目聚焦三个目标：

- 一致性：Raft 日志复制 + 版本化写入（CAS 语义）
- 可观测：metrics、watch 事件、脚本化启动
- 可扩展：一致性哈希路由 + 迁移工具

## 1. 快速开始

### 1.1 环境要求

- Linux 或 macOS
- Go 1.25+
- Git

### 1.2 安装与构建

```bash
git clone git@github.com:jianger-yu/KVraft.git
cd KVraft
go mod tidy
go build ./...
```

### 1.3 启动 3 节点集群

```bash
bash examples/start_cluster.sh
```

默认节点：

- 127.0.0.1:5001
- 127.0.0.1:5002
- 127.0.0.1:5003

清理历史数据后重启：

```bash
bash examples/start_cluster.sh --clean
```

### 1.4 运行示例

```bash
go run examples/basic/main.go
go run examples/scenarios/main.go
go run cmd/kvcli/main.go -servers 127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003
```

## 2. 功能能力

已实现：

- Get / Put / Delete / Scan
- 版本化 CAS 写入
- Watch(key/prefix)
- 快照恢复主链路
- 分片路由与迁移工具

进行中或可继续强化：

- WAL 的分段与恢复加速
- TTL 后台治理策略
- Lease 读路径稳定性和指标化

## 3. 架构分层

- raft/: 共识协议核心
- rsm/: 复制状态机与服务编排
- storage/: BadgerDB 封装
- watch/: 订阅与事件分发
- sharding/: 一致性哈希与路由迁移
- api/pb/: proto 契约与生成代码
- raftkv/rpc/: 内部 RPC 类型

## 4. 当前复杂度评估

以下统计基于当前仓库（排除生成 pb 代码）：

- Go 业务源码文件数：28
- 业务源码总行数：约 8,488 行
- Go 包数量：15

复杂度结论：中高复杂度（可作为完整工程项目展示，不只是课程实验）。

## 5. 明显不足与改进建议

### 5.1 工程一致性

- Dockerfile 版本与 go.mod 版本不一致（镜像里 Go 版本偏低）
- Docker/Compose 与本地脚本两套启动路径并存，运行手册需要更明确的“推荐路径”

建议：

- 统一 Go 版本（本地、CI、Docker）
- 在 README 明确“本地优先路径”和“容器路径的已验证范围”

### 5.2 可维护性

- 少量关键路径仍使用 panic/log.Fatal，错误恢复策略不统一
- 缺少统一配置对象（环境变量分散）

建议：

- 统一错误分层（可恢复错误 vs 致命错误）
- 引入集中配置模块并加校验

### 5.3 测试覆盖

- 目前有核心包测试，但缺少端到端压力/故障注入测试
- WAL 包暂无测试

建议：

- 增加 E2E 稳定性场景（leader 切换、网络抖动、恢复）
- 增加 WAL 单测和恢复一致性测试

### 5.4 交付体验

- 运行会产生 badger-* 数据目录，需要明确清理策略
- proto 重新生成容易受本机 PATH 或执行目录影响

建议：

- 固化 proto 编译脚本和发布流程（本仓库已做第一轮修复）
- 持续完善 .gitignore 与发布脚本，防止运行产物入库

## 6. 与“学长两个项目”复杂度对比

未拿到学长两份项目源码/仓库地址，本对比采用常见画像进行评估：

- 学长项目 A（课程实验版）：单机 KV 或简化 Raft，功能链路短，工程化较少
- 学长项目 B（工程强化版）：包含监控、容器化、路由扩展，复杂度较高

对比结论：

- 相比 A：当前 KVraft 明显更复杂（分层更完整、能力更多、脚本与监控更强）
- 相比 B：处于同一量级，但在 CI 自动化、故障演练、文档闭环上仍有提升空间

如果你补充学长项目仓库，我可以给你一版“逐模块、逐指标、逐风险”的量化对比报告。

## 7. 测试与构建状态（当前仓库）

推荐命令：

```bash
go test ./...
go build ./...
```

当前状态：通过。

## 8. 数据与运行目录说明

运行节点会在仓库根目录生成 badger-127.0.0.1:PORT/ 数据目录（用于重启恢复）。

- 开发调试：建议保留
- 演示发布：建议使用 --clean 或手动清理

## 9. 发布建议

建议使用仓库根目录下脚本：

```bash
bash push-to-github.sh
```

支持参数：

- -m "提交信息"
- --no-test （跳过测试）
- --dry-run （只看将要提交的内容）
