# 快速开始指南

## 方式一：Docker Compose（推荐）

### 1. 启动服务

**Windows:**
```bash
start.bat
```

**Linux/Mac:**
```bash
chmod +x start.sh
./start.sh
```

或直接运行：
```bash
docker-compose up -d
```

### 2. 访问系统

打开浏览器访问：http://localhost:8000

### 3. 查看日志

```bash
docker-compose logs -f
```

### 4. 停止服务

```bash
docker-compose down
```

## 方式二：手动部署

### 1. 安装依赖

```bash
cd backend
pip install -r requirements.txt
```

### 2. 启动Redis

```bash
docker run -d -p 6379:6379 redis:7-alpine
```

### 3. 配置环境变量（可选）

复制 `.env.example` 到 `.env` 并编辑：

```bash
cp backend/.env.example backend/.env
```

### 4. 启动行情采集服务

```bash
python -m market_collector.scheduler
```

### 5. 启动API服务

```bash
python -m gateway.app
```

访问：http://localhost:8000

## 常见问题

### 1. 无法连接Redis

确保Redis服务已启动：
```bash
docker ps | grep redis
```

### 2. 行情数据无法加载

- 检查网络连接（需要访问AKShare数据源）
- 查看采集服务日志
- 等待几分钟让数据采集完成

### 3. 前端页面空白

- 检查浏览器控制台错误
- 确认API服务正常运行
- 检查CORS配置

### 4. Docker启动失败

```bash
# 查看详细错误
docker-compose logs

# 重建容器
docker-compose up -d --build
```

## 功能测试

### 测试行情接口

```bash
curl http://localhost:8000/api/market/a/spot
```

### 测试选股功能

访问：http://localhost:8000，点击"选股"标签，点击"开始选股"

### 测试K线图

访问：http://localhost:8000，点击"K线"标签，输入代码（如：600519），点击"加载K线"

## 下一步

1. 配置通知服务（Telegram/邮箱/微信）
2. 接入AI模型（编辑 `backend/ai/analyzer.py`）
3. 自定义选股策略（编辑 `backend/strategy/scorer.py`）
4. 添加更多技术指标

详细文档请参考 README.md

