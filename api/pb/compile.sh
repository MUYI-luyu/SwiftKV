#!/bin/bash

# Proto 编译脚本 - 生成 gRPC Go 代码

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
PROTO_DIR="${PROJECT_ROOT}/api/pb"
OUTPUT_DIR="${PROJECT_ROOT}"
MODULE_NAME="kvraft"

# 确保 protoc 能找到 Go 插件
export PATH="$(go env GOPATH)/bin:$PATH"

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
if ! command -v protoc-gen-go &>/dev/null; then
    echo "📦 安装 protoc-gen-go..."
    go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
fi

if ! command -v protoc-gen-go-grpc &>/dev/null; then
    echo "📦 安装 protoc-gen-go-grpc..."
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
fi

# 编译 Proto 文件
echo "📝 编译 kvraft.proto..."
protoc \
    --go_out="${OUTPUT_DIR}" \
    --go_opt="module=${MODULE_NAME}" \
    --go-grpc_out="${OUTPUT_DIR}" \
    --go-grpc_opt="module=${MODULE_NAME}" \
    -I"${PROTO_DIR}" \
    "${PROTO_DIR}/kvraft.proto"

echo "✅ Proto 编译完成！"
echo "📍 生成文件："
ls -lh "${PROJECT_ROOT}/api/pb/kvraft/api/pb"/*.pb.go 2>/dev/null || echo "  (生成的文件还未出现)"
