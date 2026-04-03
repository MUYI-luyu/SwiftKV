#!/usr/bin/env bash

# 对本地集群执行自动化性能压测，并将报告保存到 data/perf。
# 示例：
#   ./scripts/test_perf.sh
#   ./scripts/test_perf.sh --clients 20 --requests 2000 --read-ratio 0.7 --duration 45s
#   ./scripts/test_perf.sh --maxraftstate 1048576 --sharded true
#   ./scripts/test_perf.sh --restart-cluster true --clean true
#   ./scripts/test_perf.sh --fresh-run true

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_ROOT="${KV_DATA_DIR:-${ROOT_DIR}/data}"
PERF_DIR="${DATA_ROOT}/perf"
RUNTIME_ENV="${DATA_ROOT}/cluster/runtime.env"
mkdir -p "${PERF_DIR}"

SERVERS=3
CLIENTS=10
REQUESTS=1000
READ_RATIO=0.7
DURATION="30s"
KEYS=10000
MAXRAFTSTATE=1048576
INIT_KEYS=1000
SKIP_INIT_IF_SEEDED=true
BASE_PORT=15000
RESTART_CLUSTER=false
CLEAN=false
FRESH_RUN=false
SHARDED_MODE="auto"

ARCH=""
SHARD_GROUPS=""
REPLICAS=""

runtime_value() {
  local key="$1"
  if [[ ! -f "${RUNTIME_ENV}" ]]; then
    echo ""
    return
  fi
  grep -E "^${key}=" "${RUNTIME_ENV}" | head -n1 | cut -d'=' -f2-
}

csv_count() {
  local csv="$1"
  if [[ -z "${csv}" ]]; then
    echo 0
    return
  fi
  awk -F',' '{print NF}' <<<"${csv}"
}

build_addr_csv() {
  local count="$1"
  local start_port="$2"
  local result=""
  local i
  for ((i=0; i<count; i++)); do
    local addr="127.0.0.1:$((start_port + i))"
    if [[ -z "${result}" ]]; then
      result="${addr}"
    else
      result="${result},${addr}"
    fi
  done
  echo "${result}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --servers)
      SERVERS="$2"
      shift 2
      ;;
    --clients)
      CLIENTS="$2"
      shift 2
      ;;
    --requests)
      REQUESTS="$2"
      shift 2
      ;;
    --read-ratio)
      READ_RATIO="$2"
      shift 2
      ;;
    --duration)
      DURATION="$2"
      shift 2
      ;;
    --keys)
      KEYS="$2"
      shift 2
      ;;
    --maxraftstate)
      MAXRAFTSTATE="$2"
      shift 2
      ;;
    --sharded)
      SHARDED_MODE="$2"
      shift 2
      ;;
    --init-keys)
      INIT_KEYS="$2"
      shift 2
      ;;
    --skip-init-if-seeded)
      SKIP_INIT_IF_SEEDED="$2"
      shift 2
      ;;
    --base-port)
      BASE_PORT="$2"
      shift 2
      ;;
    --restart-cluster)
      RESTART_CLUSTER="$2"
      shift 2
      ;;
    --clean)
      CLEAN="$2"
      shift 2
      ;;
    --fresh-run)
      FRESH_RUN="$2"
      shift 2
      ;;
    --arch)
      ARCH="$2"
      shift 2
      ;;
    --groups)
      SHARD_GROUPS="$2"
      shift 2
      ;;
    --replicas)
      REPLICAS="$2"
      shift 2
      ;;
    --sharded-mode)
      SHARDED_MODE="$2"
      shift 2
      ;;
    *)
      echo "未知参数: $1"
      exit 1
      ;;
  esac
done

if [[ "${FRESH_RUN}" == "true" ]]; then
  RESTART_CLUSTER=true
  CLEAN=true
fi

cd "${ROOT_DIR}"

R_ARCH="$(runtime_value ARCH)"
R_BASE_PORT="$(runtime_value BASE_PORT)"
R_GROUPS="$(runtime_value GROUPS)"
R_REPLICAS="$(runtime_value REPLICAS)"
R_TOTAL_NODES="$(runtime_value TOTAL_NODES)"
R_RAFT_SERVERS="$(runtime_value RAFT_SERVERS)"
R_GRPC_SERVERS="$(runtime_value GRPC_SERVERS)"
R_SHARDING_CONFIG="$(runtime_value SHARDING_CONFIG)"

if [[ -z "${ARCH}" ]]; then
  ARCH="${R_ARCH:-node-ring}"
fi
if [[ -z "${R_BASE_PORT}" ]]; then
  R_BASE_PORT="${BASE_PORT}"
fi
if [[ -z "${SHARD_GROUPS}" ]]; then
  SHARD_GROUPS="${R_GROUPS:-1}"
fi
if [[ -z "${REPLICAS}" ]]; then
  REPLICAS="${R_REPLICAS:-3}"
fi
if [[ -n "${R_TOTAL_NODES}" && "${SERVERS}" == "3" ]]; then
  SERVERS="${R_TOTAL_NODES}"
fi

start_cluster() {
  if [[ "${ARCH}" == "group-ring" ]]; then
    if [[ "${CLEAN}" == "true" ]]; then
      ./scripts/run_cluster.sh --arch group-ring --groups "${SHARD_GROUPS}" --replicas "${REPLICAS}" --base-port "${R_BASE_PORT}" --clean >/dev/null
    else
      ./scripts/run_cluster.sh --arch group-ring --groups "${SHARD_GROUPS}" --replicas "${REPLICAS}" --base-port "${R_BASE_PORT}" >/dev/null
    fi
  else
    if [[ "${CLEAN}" == "true" ]]; then
      ./scripts/run_cluster.sh --arch node-ring --servers "${SERVERS}" --base-port "${R_BASE_PORT}" --clean >/dev/null
    else
      ./scripts/run_cluster.sh --arch node-ring --servers "${SERVERS}" --base-port "${R_BASE_PORT}" >/dev/null
    fi
  fi
}

if [[ "${RESTART_CLUSTER}" == "true" ]]; then
  ./scripts/stop_cluster.sh >/dev/null 2>&1 || true
  start_cluster
else
  CHECK_SERVERS="${R_GRPC_SERVERS}"
  if [[ -z "${CHECK_SERVERS}" ]]; then
    CHECK_SERVERS="$(build_addr_csv "${SERVERS}" "$((R_BASE_PORT + 1000))")"
  fi

  status_out="$(./scripts/check_status.sh --servers "${CHECK_SERVERS}" 2>/dev/null || true)"
  checked_nodes="$(grep -Eo 'checked nodes: [0-9]+' <<<"${status_out}" | awk '{print $3}' | tail -n1)"
  bad_nodes="$(grep -Ec 'UNREACH|ERROR' <<<"${status_out}" || true)"
  if [[ -z "${checked_nodes}" ]]; then
    checked_nodes=0
  fi

  if [[ "${checked_nodes}" -eq 0 || "${bad_nodes}" -ge "${checked_nodes}" ]]; then
    start_cluster
  fi
fi

# 重新读取运行时元数据，确保压测地址与当前集群一致。
R_ARCH="$(runtime_value ARCH)"
R_RAFT_SERVERS="$(runtime_value RAFT_SERVERS)"
R_SHARDING_CONFIG="$(runtime_value SHARDING_CONFIG)"

if [[ -n "${R_RAFT_SERVERS}" ]]; then
  RPC_SERVERS="${R_RAFT_SERVERS}"
  SERVERS="$(csv_count "${RPC_SERVERS}")"
else
  RPC_SERVERS="$(build_addr_csv "${SERVERS}" "${R_BASE_PORT}")"
fi

if [[ "${SHARDED_MODE}" == "auto" ]]; then
  if [[ "${R_ARCH}" == "group-ring" ]]; then
    SHARDED="true"
  else
    SHARDED="false"
  fi
else
  SHARDED="${SHARDED_MODE}"
fi

stamp="$(date +%Y%m%d_%H%M%S)"
report_file="${PERF_DIR}/perf_${stamp}.log"

echo "开始执行压测..."
echo "报告文件: ${report_file}"
echo "运行模式: arch=${R_ARCH:-unknown}, servers=${SERVERS}, sharded=${SHARDED}, fresh-run=${FRESH_RUN}"
echo "RPC_SERVERS=${RPC_SERVERS}"

bench_cmd=(
  go run ./cmd/benchmarks/benchmark.go
  --servers="${SERVERS}"
  --server-addrs="${RPC_SERVERS}"
  --clients="${CLIENTS}"
  --requests="${REQUESTS}"
  --read-ratio="${READ_RATIO}"
  --duration="${DURATION}"
  --keys="${KEYS}"
  --init-keys="${INIT_KEYS}"
  --skip-init-if-seeded="${SKIP_INIT_IF_SEEDED}"
  --maxraftstate="${MAXRAFTSTATE}"
  --sharded="${SHARDED}"
)

if [[ "${SHARDED}" == "true" && -n "${R_SHARDING_CONFIG}" && -f "${R_SHARDING_CONFIG}" ]]; then
  bench_cmd+=(--sharding-config="${R_SHARDING_CONFIG}")
fi

"${bench_cmd[@]}" | tee "${report_file}"

echo "压测完成，集群保持运行。"
