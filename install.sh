#!/bin/bash
set -e

REPO_URL="https://github.com/1490293430/Agjiankong.git"
TARGET_DIR="/opt/Agjiankong"

# 安装git和docker-compose（如未安装）
if ! command -v git &> /dev/null; then
  sudo apt update && sudo apt install -y git
fi
if ! command -v docker-compose &> /dev/null; then
  sudo apt update && sudo apt install -y docker-compose
fi

# 拉取或更新代码
if [ -d "$TARGET_DIR" ]; then
  sudo git -C "$TARGET_DIR" pull
else
  sudo git clone "$REPO_URL" "$TARGET_DIR"
fi

cd "$TARGET_DIR"
sudo docker-compose down || true
sudo docker-compose up -d --build

echo "部署完成，服务已启动。"
