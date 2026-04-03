#!/usr/bin/env bash

# 查询各节点状态，并通过 GetClusterStatus 输出 Leader/Follower。
# 示例：
#   ./scripts/check_status.sh
#   ./scripts/check_status.sh --servers 127.0.0.1:16000,127.0.0.1:16001,127.0.0.1:16002

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_ROOT="${KV_DATA_DIR:-${ROOT_DIR}/data}"
RUNTIME_ENV="${DATA_ROOT}/cluster/runtime.env"
SERVERS=""
ARCH=""
GROUP_COUNT=0
REPLICA_COUNT=0

if [[ -f "${RUNTIME_ENV}" ]]; then
  while IFS='=' read -r key value; do
    [[ -n "${key}" ]] || continue
    case "${key}" in
      GRPC_SERVERS) SERVERS="${value}" ;;
      ARCH) ARCH="${value}" ;;
      GROUPS) GROUP_COUNT="${value}" ;;
      REPLICAS) REPLICA_COUNT="${value}" ;;
    esac
  done < <(grep -E '^(GRPC_SERVERS|ARCH|GROUPS|REPLICAS)=' "${RUNTIME_ENV}" || true)
fi
if [[ -z "${SERVERS}" ]]; then
  SERVERS="127.0.0.1:16000,127.0.0.1:16001,127.0.0.1:16002"
fi

while [[ $# -gt 0 ]]; do
  case "$1" in
    --servers)
      SERVERS="$2"
      shift 2
      ;;
    *)
      echo "未知参数: $1"
      exit 1
      ;;
  esac
done

TMP_GO="$(mktemp /tmp/kvraft-check-status-XXXXXX.go)"
trap 'rm -f "${TMP_GO}"' EXIT

cat > "${TMP_GO}" <<'EOF'
package main

import (
  "context"
  "fmt"
  "os"
  "strconv"
  "strings"
  "time"

  pb "kvraft/api/pb/kvraft/api/pb"
  "google.golang.org/grpc"
  "google.golang.org/grpc/credentials/insecure"
)

func main() {
  raw := strings.TrimSpace(os.Getenv("KV_STATUS_SERVERS"))
  if raw == "" {
    fmt.Println("KV_STATUS_SERVERS is empty")
    os.Exit(1)
  }
  addrs := strings.Split(raw, ",")
  arch := strings.TrimSpace(os.Getenv("KV_STATUS_ARCH"))
  groupCount := 0
  replicaCount := 0
  if v := strings.TrimSpace(os.Getenv("KV_STATUS_GROUPS")); v != "" {
    if n, err := strconv.Atoi(v); err == nil {
      groupCount = n
    }
  }
  if v := strings.TrimSpace(os.Getenv("KV_STATUS_REPLICAS")); v != "" {
    if n, err := strconv.Atoi(v); err == nil {
      replicaCount = n
    }
  }

  hasGroupView := arch == "group-ring" && groupCount > 0 && replicaCount > 0
  if hasGroupView {
    fmt.Printf("%-8s %-8s %-22s %-10s %-8s %-10s\n", "Group", "Replica", "Address", "Role", "Term", "Alive")
    fmt.Println("----------------------------------------------------------------------------")
  } else {
    fmt.Printf("%-8s %-22s %-10s %-8s %-10s\n", "Node", "Address", "Role", "Term", "Alive")
    fmt.Println("----------------------------------------------------------------")
  }

  checked := 0
  for idx, a := range addrs {
    addr := strings.TrimSpace(a)
    if addr == "" {
      continue
    }

    dialCtx, dialCancel := context.WithTimeout(context.Background(), 1200*time.Millisecond)
    conn, err := grpc.DialContext(dialCtx, addr,
      grpc.WithTransportCredentials(insecure.NewCredentials()),
      grpc.WithBlock(),
    )
    dialCancel()
    if err != nil {
      if hasGroupView {
        g := idx / replicaCount
        r := idx % replicaCount
        fmt.Printf("%-8d %-8d %-22s %-10s %-8s %-10s\n", g, r, addr, "UNREACH", "-", "false")
      } else {
        fmt.Printf("%-8d %-22s %-10s %-8s %-10s\n", idx, addr, "UNREACH", "-", "false")
      }
      checked++
      continue
    }

    cli := pb.NewKVServiceClient(conn)
    reqCtx, reqCancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
    resp, reqErr := cli.GetClusterStatus(reqCtx, &pb.ClusterStatusRequest{})
    reqCancel()
    _ = conn.Close()

    if reqErr != nil {
      if hasGroupView {
        g := idx / replicaCount
        r := idx % replicaCount
        fmt.Printf("%-8d %-8d %-22s %-10s %-8s %-10s\n", g, r, addr, "ERROR", "-", "false")
      } else {
        fmt.Printf("%-8d %-22s %-10s %-8s %-10s\n", idx, addr, "ERROR", "-", "false")
      }
      checked++
      continue
    }

    role := "Follower"
    alive := "false"
    if len(resp.GetNodes()) > 0 {
      if resp.GetNodes()[0].GetIsLeader() {
        role = "Leader"
      }
      if resp.GetNodes()[0].GetIsAlive() {
        alive = "true"
      }
    }
    if hasGroupView {
      g := idx / replicaCount
      r := idx % replicaCount
      fmt.Printf("%-8d %-8d %-22s %-10s %-8d %-10s\n", g, r, addr, role, resp.GetCurrentTerm(), alive)
    } else {
      fmt.Printf("%-8d %-22s %-10s %-8d %-10s\n", idx, addr, role, resp.GetCurrentTerm(), alive)
    }
    checked++
  }

  fmt.Printf("\nchecked nodes: %d\n", checked)
}
EOF

cd "${ROOT_DIR}"
KV_STATUS_SERVERS="${SERVERS}" \
KV_STATUS_ARCH="${ARCH}" \
KV_STATUS_GROUPS="${GROUP_COUNT}" \
KV_STATUS_REPLICAS="${REPLICA_COUNT}" \
go run "${TMP_GO}"
