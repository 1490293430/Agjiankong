FROM python:3.11-slim

WORKDIR /app

# 安装系统依赖（利用Docker缓存）
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    make \
    && rm -rf /var/lib/apt/lists/*

# 先复制依赖文件（利用Docker缓存，只有依赖变化时才重新安装）
COPY backend/requirements.txt ./requirements.txt

# 安装Python依赖
RUN pip install --no-cache-dir -r requirements.txt

# 复制后端代码
COPY backend/ .

# 复制前端代码
COPY frontend/ ./frontend

# 设置环境变量
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# 默认命令
CMD ["python", "-m", "gateway.app"]
