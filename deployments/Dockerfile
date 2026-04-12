# ============================================================
# Dockerfile for KVraft Server
# 基于 Go 1.25 构建多阶段镜像
# ============================================================

# 构建阶段
FROM golang:1.25-alpine AS builder

WORKDIR /app
RUN apk add --no-cache git

# 复制 go.mod 和 go.sum
COPY go.mod go.sum ./

# 下载依赖
RUN go mod download

# 复制源代码
COPY . .

# 编译主程序
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-s -w" \
    -o /app/kvraft-server \
    ./cmd/server

# 运行阶段
FROM alpine:3.18

WORKDIR /app

# 安装运行时依赖
RUN apk add --no-cache ca-certificates curl bash

# 从构建阶段复制二进制文件
COPY --from=builder /app/kvraft-server /app/kvraft-server

# 创建数据目录
RUN mkdir -p /data && chmod 777 /data

ENV KV_DATA_DIR=/data

# 暴露端口
EXPOSE 50000 6000 8001 9100

# 健康检查
HEALTHCHECK --interval=10s --timeout=5s --retries=3 \
    CMD curl -f http://localhost:8001/health || exit 1

# 启动命令
CMD ["/app/kvraft-server"]
