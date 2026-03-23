#!/bin/bash

# Proto 编译脚本 - 生成 gRPC Go 代码

set -e

PROTO_DIR="./api/pb"
OUTPUT_DIR="./api/pb"

echo "🔨 正在编译 Proto 文件..."

# 检查 protoc 和插件
if ! command -v protoc &> /dev/null; then
    echo "❌ protoc 未安装！请先安装："
    echo "  macOS: brew install protobuf"
    echo "  Ubuntu: apt-get install -y protobuf-compiler"
    echo "  或者从 https://github.com/protocolbuffers/protobuf/releases 下载"
    exit 1
fi

# 检查 Go 插件
if ! go list -m google.golang.org/protobuf &>/dev/null; then
    echo "📦 安装 protoc-gen-go..."
    go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
fi

if ! go list -m google.golang.org/grpc &>/dev/null; then
    echo "📦 安装 protoc-gen-go-grpc..."
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
fi

# 编译 Proto 文件
echo "📝 编译 kvraft.proto..."
protoc \
    --go_out="${OUTPUT_DIR}" \
    --go_opt=paths=source_relative \
    --go-grpc_out="${OUTPUT_DIR}" \
    --go-grpc_opt=paths=source_relative \
    -I"${PROTO_DIR}" \
    "${PROTO_DIR}/kvraft.proto"

echo "✅ Proto 编译完成！"
echo "📍 生成文件："
ls -lh "${OUTPUT_DIR}"/*.pb.go 2>/dev/null || echo "  (生成的文件还未出现)"
