#!/bin/bash
# K线表自动迁移脚本（Docker 环境）
# 使用方法: ./scripts/migrate_kline.sh 或 bash scripts/migrate_kline.sh

set -e  # 遇到错误立即退出

echo "=========================================="
echo "K线表自动迁移脚本"
echo "=========================================="

# 检查是否在 Docker 环境中
if [ -f /.dockerenv ] || [ -n "${DOCKER_CONTAINER}" ]; then
    echo "检测到 Docker 环境"
    PYTHON_CMD="python"
else
    # 如果在宿主机上，尝试进入容器执行
    if command -v docker &> /dev/null; then
        # 优先使用 stock_api 容器，如果没有则使用 stock_collector
        CONTAINER_NAME=$(docker ps --format "{{.Names}}" | grep -E "(stock_api|stock_collector)" | head -n 1)
        if [ -n "$CONTAINER_NAME" ]; then
            echo "检测到容器: $CONTAINER_NAME"
            echo "在容器中执行迁移..."
            docker exec -it "$CONTAINER_NAME" python -m scripts.migrate_kline_table
            exit $?
        else
            echo "警告: 未找到运行中的容器，尝试直接执行..."
        fi
    fi
    PYTHON_CMD="python3"
fi

# 切换到 backend 目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKEND_DIR="$(cd "$SCRIPT_DIR/../backend" && pwd)"
cd "$BACKEND_DIR"

echo "工作目录: $BACKEND_DIR"

# 执行迁移脚本
echo "开始执行迁移..."
$PYTHON_CMD -m scripts.migrate_kline_table

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo ""
    echo "=========================================="
    echo "✓ 迁移完成！"
    echo "=========================================="
else
    echo ""
    echo "=========================================="
    echo "✗ 迁移失败，退出码: $EXIT_CODE"
    echo "=========================================="
fi

exit $EXIT_CODE

