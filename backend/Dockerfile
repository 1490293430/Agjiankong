FROM python:3.11-slim

WORKDIR /app

# 安装系统依赖
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    make \
    && rm -rf /var/lib/apt/lists/*

# 复制依赖文件（从项目根目录的 backend/requirements.txt）
COPY backend/requirements.txt ./requirements.txt

# 安装Python依赖
RUN pip install --no-cache-dir -r requirements.txt

# 复制后端代码到 /app（保持与本地开发环境一致的包结构）
COPY backend/ .

# 复制前端代码到 /app/frontend（用于 FastAPI 静态托管）
COPY frontend/ ./frontend

# 设置环境变量
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# 默认命令
CMD ["python", "-m", "gateway.app"]

