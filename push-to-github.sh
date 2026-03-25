#!/bin/bash
set -e

cd /home/jianger/codes/KVraft

echo "=== 1. 初始化 Git 仓库 ==="
if [ ! -d .git ]; then
    git init
    echo "✓ Git 仓库已初始化"
else
    echo "✓ Git 仓库已存在"
fi

echo ""
echo "=== 2. 暂存所有关键文件 ==="

git add .gitignore
git add go.mod go.sum go.work go.work.sum
git add raft/ rsm/ storage/ watch/ sharding/ raftapi/
git add api/ raftkv/ examples/
git add cmd/ README.md
git add Dockerfile docker-compose.yml

echo "✓ 文件暂存完成"

echo ""
echo "=== 3. 查看待提交文件 ==="
git status

echo ""
echo "=== 4. 提交代码 ==="
git commit -m "chore: sync project updates"

echo ""
echo "=== 5. 推送至 GitHub ==="
git push -u origin main

echo ""
echo "推送完成！"