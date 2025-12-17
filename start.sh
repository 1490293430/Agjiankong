#!/bin/bash

echo "===================================="
echo "量化交易终端 - 启动脚本"
echo "===================================="
echo ""

echo "[1/3] 检查Docker环境..."
if ! command -v docker &> /dev/null; then
    echo "错误: 未检测到Docker，请先安装Docker"
    exit 1
fi

echo "[2/3] 启动Docker Compose服务..."
docker-compose up -d

echo "[3/3] 等待服务启动..."
sleep 5

echo ""
echo "===================================="
echo "启动完成！"
echo "===================================="
echo "API服务: http://localhost:8000"
echo "前端页面: http://localhost:8000"
echo ""
echo "查看日志: docker-compose logs -f"
echo "停止服务: docker-compose down"
echo "===================================="

