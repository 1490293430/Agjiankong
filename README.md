# 一键部署

在服务器上执行以下命令即可一键部署到 /opt/Agjiankong 并自动启动 Docker 服务：

```bash
bash <(curl -fsSL https://raw.githubusercontent.com/1490293430/Agjiankong/main/install.sh)
```

# 手动快速部署

如果你需要手动部署或更新项目，可以参考以下命令：

```bash
cd /opt/Agjiankong
git pull
docker compose build
docker compose up -d
```

上述命令会进入项目目录，拉取最新代码，构建并以后台方式启动服务。
# 量化交易终端

一个功能完整的A股/港股行情分析系统，支持实时行情、技术分析、自动选股、模拟交易和AI分析。

## ✨ 功能特性

- 📊 **实时行情**: A股和港股实时行情采集和展示
- 📈 **K线图表**: TradingView风格的专业K线图
- 🔍 **技术指标**: MA、MACD、RSI、KDJ、布林带等
- 🤖 **AI分析**: 基于技术指标的智能分析（可接入AI模型）
- 📰 **资讯采集**: 自动采集财经资讯和重要消息
- 🎯 **自动选股**: 多因子打分选股系统
- 💰 **模拟交易**: 完整的模拟交易系统（虚拟资金）
- 📱 **通知服务**: 支持Telegram、邮箱、企业微信通知
- 🚀 **WebSocket**: 实时行情推送
- 🐳 **Docker部署**: 一键部署，开箱即用

## 🏗️ 系统架构

```
stock-platform/
├── backend/              # 后端服务
│   ├── common/          # 公共模块（配置、日志、Redis、数据库）
│   ├── market_collector/# 行情采集（A股、港股）
│   ├── market/          # 行情服务（API、WebSocket、指标）
│   ├── news/            # 资讯采集和分析
│   ├── ai/              # AI分析服务
│   ├── strategy/        # 策略选股
│   ├── trading/         # 模拟交易
│   ├── notify/          # 通知服务
│   └── gateway/         # API网关
└── frontend/            # 前端页面
```

## 📦 技术栈

### 后端
- Python 3.11+
- FastAPI - Web框架
- AKShare - 免费行情数据源
- Redis - 缓存和实时数据
- ClickHouse - 历史数据存储（可选）
- WebSocket - 实时推送

### 前端
- Vanilla JavaScript (ES6+)
- TradingView Lightweight Charts - K线图
- 响应式设计

## 🚀 快速开始

### 1. 环境要求

- Python 3.11+
- Docker & Docker Compose（推荐）
- 或：Redis服务器

### 2. 配置

复制环境变量配置文件（如有 `.env.example`）：

```bash
cp backend/.env.example backend/.env
```

编辑 `backend/.env`，可以配置以下选项（均为可选）：

```env
# Redis配置（如果使用Docker Compose，保持默认即可）
REDIS_HOST=localhost
REDIS_PORT=6379

# ClickHouse 配置（生产环境务必设置安全密码）
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=9000
CLICKHOUSE_DB=stock
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=your-strong-password

# 通知服务配置（可选）
TELEGRAM_BOT_TOKEN=your_token
TELEGRAM_CHAT_ID=your_chat_id

EMAIL_SMTP_HOST=smtp.qq.com
EMAIL_USER=your_email@qq.com
EMAIL_PASSWORD=your_password

WECHAT_WEBHOOK_URL=your_webhook_url

# API 安全配置（生产环境强烈建议设置）
# 允许的前端地址，多个用逗号分隔，例如：http://localhost:8000,https://your-domain.com
API_ALLOWED_ORIGINS=http://localhost:8000
# 简单 API Token，用于保护接口访问；前端需通过请求头 X-API-Token 携带
API_AUTH_TOKEN=please-change-me
# 超级管理员 Token，用于修改配置、重置账户等敏感操作；不设置时默认使用 API_AUTH_TOKEN
ADMIN_TOKEN=please-change-me-too
```

### 3. 使用Docker Compose部署（推荐）

```bash
# 启动所有服务（本地开发环境）
docker-compose up -d

# 查看日志
docker-compose logs -f

# 停止服务
docker-compose down

# 生产环境示例（自定义密码与安全配置）
CLICKHOUSE_PASSWORD='your-strong-password' \
API_ALLOWED_ORIGINS='https://your-domain.com' \
API_AUTH_TOKEN='please-change-me' \
ADMIN_TOKEN='please-change-me-too' \
docker-compose up -d
```

服务启动后：
- API服务：http://localhost:8000
- 前端页面：http://localhost:8000
- Redis：localhost:6379
- ClickHouse：localhost:8123（可选）

### 4. 手动部署

#### 安装依赖

```bash
cd backend
pip install -r requirements.txt
```

#### 启动Redis

```bash
# 使用Docker
docker run -d -p 6379:6379 redis:7-alpine

# 或使用系统服务
redis-server
```

#### 启动行情采集服务

```bash
python -m market_collector.scheduler
```

#### 启动API服务

```bash
python -m gateway.app
```

访问：http://localhost:8000

## 📖 使用指南

### 行情查看

1. 打开前端页面
2. 点击"行情"标签
3. 选择A股或港股
4. 可以搜索股票代码或名称
5. 点击"查看K线"按钮查看详细图表

### K线图

1. 点击"K线"标签
2. 输入股票代码（如：600519）
3. 选择周期（日线/周线/月线）
4. 点击"加载K线"
5. 查看技术指标数据

### 自动选股

1. 点击"选股"标签
2. 设置选股阈值（默认65分）
3. 设置最大数量（默认30只）
4. 点击"开始选股"
5. 查看选中的股票及评分

### 模拟交易

1. 点击"交易"标签
2. 查看账户信息
3. 填写订单信息（代码、价格、数量）
4. 选择买入或卖出
5. 提交订单
6. 查看持仓和盈亏

### 资讯查看

1. 点击"资讯"标签
2. 查看最新财经资讯
3. 点击"刷新资讯"更新

## 🔧 API接口

### 行情接口

- `GET /api/market/a/spot` - 获取A股实时行情
- `GET /api/market/hk/spot` - 获取港股实时行情
- `GET /api/market/a/kline?code=600519` - 获取A股K线
- `GET /api/market/a/indicators?code=600519` - 获取技术指标
- `GET /api/market/search?keyword=茅台` - 搜索股票

### WebSocket

- `ws://localhost:8000/api/ws/market` - 实时行情推送
- `ws://localhost:8000/api/ws/stock/{code}` - 单只股票实时推送

### 策略接口

- `GET /api/strategy/select?threshold=65&max_count=30` - 自动选股
- `GET /api/ai/analyze/{code}?use_ai=false` - 分析股票

### 交易接口

- `GET /api/trading/account` - 获取账户信息
- `GET /api/trading/positions` - 获取持仓
- `POST /api/trading/order` - 提交订单
- `POST /api/trading/reset` - 重置账户

### 资讯接口

- `GET /api/news/latest` - 获取最新资讯

## 🔌 扩展开发

### 接入AI模型

编辑 `backend/ai/analyzer.py` 中的 `analyze_stock_with_ai` 函数：

```python
def analyze_stock_with_ai(stock, indicators, news):
    # 构建提示词
    prompt = build_stock_analysis_prompt(stock, indicators, news)
    
    # 调用AI模型（示例：OpenAI）
    # response = openai.ChatCompletion.create(...)
    # return parse_ai_response(response)
    
    pass
```

### 添加自定义策略

在 `backend/strategy/scorer.py` 中修改打分逻辑，或创建新的策略文件。

### 添加新的数据源

在 `backend/market_collector/` 中创建新的适配器，实现标准接口。

## ⚠️ 注意事项

1. **数据源**: 本项目使用AKShare作为数据源，完全免费但可能有频率限制
2. **模拟交易**: 仅用于学习和测试，不涉及真实资金
3. **AI分析**: 当前使用规则分析，需要接入实际AI模型才能获得更好的分析效果
4. **性能**: 首次选股可能较慢（需要计算大量指标），建议限制计算数量
5. **部署**: 生产环境建议使用Nginx反向代理，配置HTTPS

## 📝 许可证

MIT License

## 🤝 贡献

欢迎提交Issue和Pull Request！

## 📧 联系方式

如有问题或建议，请提交Issue。

