#!/bin/bash
# 重启服务脚本 - 确保正确的启动顺序

echo "正在停止所有服务..."
docker compose down

echo "正在启动服务（带健康检查）..."
docker compose up -d

echo "等待服务启动..."
sleep 5

echo "检查服务状态..."
docker compose ps

echo ""
echo "查看API服务日志（按Ctrl+C退出）..."
docker compose logs -f api
