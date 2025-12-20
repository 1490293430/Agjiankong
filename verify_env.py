#!/usr/bin/env python3
"""
验证环境变量配置脚本
用于确认 .env 文件中的配置是否能正确加载
"""
import os
from dotenv import load_dotenv

# 加载 .env 文件
env_file = os.path.join(os.path.dirname(__file__), ".env")
if os.path.exists(env_file):
    load_dotenv(env_file)
    print(f"✓ 找到并加载了 .env 文件: {env_file}")
else:
    print(f"⚠ .env 文件不存在: {env_file}")
    print("  如果使用 Docker Compose，环境变量会通过 env_file 自动加载")

# 检查关键环境变量
print("\n检查关键环境变量:")
print("-" * 50)

env_vars = {
    "REDIS_PASSWORD": os.getenv("REDIS_PASSWORD"),
    "CLICKHOUSE_PASSWORD": os.getenv("CLICKHOUSE_PASSWORD"),
    "API_AUTH_TOKEN": os.getenv("API_AUTH_TOKEN"),
    "ADMIN_TOKEN": os.getenv("ADMIN_TOKEN"),
    "ADMIN_USERNAME": os.getenv("ADMIN_USERNAME"),
    "ADMIN_PASSWORD": os.getenv("ADMIN_PASSWORD"),
}

for key, value in env_vars.items():
    if value:
        # 对于密码和token，只显示前3个字符和长度
        if "PASSWORD" in key or "TOKEN" in key:
            masked = value[:3] + "*" * (len(value) - 3) if len(value) > 3 else "***"
            print(f"✓ {key}: {masked} (长度: {len(value)})")
        else:
            print(f"✓ {key}: {value}")
    else:
        print(f"⚠ {key}: 未设置 (将使用默认值)")

print("\n" + "=" * 50)
print("验证完成！")
print("\n提示:")
print("1. 确保 .env 文件在项目根目录（与 docker-compose.yml 同级）")
print("2. 重启服务: docker-compose down && docker-compose up -d")
print("3. 在容器内，环境变量会由 docker-compose 自动注入")

