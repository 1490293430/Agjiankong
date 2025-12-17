# 项目结构说明

## 目录结构

```
Agjiankong/
├── backend/                    # 后端代码
│   ├── common/                # 公共模块
│   │   ├── config.py         # 配置管理
│   │   ├── logger.py         # 日志配置
│   │   ├── redis.py          # Redis连接
│   │   └── db.py             # ClickHouse连接
│   │
│   ├── market_collector/      # 行情采集模块
│   │   ├── cn.py             # A股行情采集
│   │   ├── hk.py             # 港股行情采集
│   │   └── scheduler.py      # 定时调度器
│   │
│   ├── market/               # 行情服务模块
│   │   ├── indicator/        # 技术指标
│   │   │   └── ta.py        # 指标计算（MA/MACD/RSI等）
│   │   └── service/          # 行情服务
│   │       ├── api.py        # REST API
│   │       └── ws.py         # WebSocket推送
│   │
│   ├── news/                 # 资讯模块
│   │   ├── collector.py      # 资讯采集
│   │   └── analyzer.py       # 资讯分析
│   │
│   ├── ai/                   # AI分析模块
│   │   ├── prompt.py         # 提示词构建
│   │   └── analyzer.py       # AI分析（可接入模型）
│   │
│   ├── strategy/             # 策略模块
│   │   ├── scorer.py         # 股票打分器
│   │   └── selector.py       # 选股引擎
│   │
│   ├── trading/              # 模拟交易模块
│   │   ├── account.py        # 账户管理
│   │   └── engine.py         # 交易引擎
│   │
│   ├── notify/               # 通知模块
│   │   ├── telegram.py       # Telegram通知
│   │   ├── email.py          # 邮箱通知
│   │   ├── wechat.py         # 企业微信通知
│   │   └── dispatcher.py     # 统一通知调度
│   │
│   ├── gateway/              # API网关
│   │   └── app.py            # 主应用入口
│   │
│   ├── requirements.txt      # Python依赖
│   └── Dockerfile           # Docker镜像配置
│
├── frontend/                  # 前端代码
│   ├── index.html           # 主页面
│   ├── style.css            # 样式文件
│   └── app.js               # JavaScript逻辑
│
├── docker-compose.yml        # Docker Compose配置
├── README.md                 # 项目说明
├── QUICKSTART.md            # 快速开始
└── start.bat / start.sh     # 启动脚本
```

## 模块说明

### 1. common（公共模块）

**config.py**: 配置管理
- 从环境变量读取配置
- 支持Redis、ClickHouse、通知服务等配置

**logger.py**: 日志配置
- 统一的日志格式
- 支持不同日志级别

**redis.py**: Redis连接
- 连接池管理
- JSON数据存储/读取

**db.py**: ClickHouse连接
- 数据库连接管理
- 表结构初始化

### 2. market_collector（行情采集）

**cn.py**: A股行情采集
- 实时行情获取
- K线数据获取

**hk.py**: 港股行情采集
- 港股实时行情
- 港股K线数据

**scheduler.py**: 定时调度
- 每5秒采集一次行情
- 可配置采集频率

### 3. market（行情服务）

**indicator/ta.py**: 技术指标计算
- MA均线
- MACD
- RSI
- KDJ
- 布林带

**service/api.py**: REST API
- 行情查询接口
- K线数据接口
- 技术指标接口
- 股票搜索接口

**service/ws.py**: WebSocket推送
- 实时行情推送
- 单只股票推送
- 连接管理

### 4. news（资讯模块）

**collector.py**: 资讯采集
- 从AKShare获取财经资讯
- 重要资讯筛选

**analyzer.py**: 资讯分析
- 提取股票代码
- 情感分析（利好/利空）
- 影响级别判断

### 5. ai（AI分析）

**prompt.py**: 提示词构建
- 股票分析提示词
- 市场总结提示词

**analyzer.py**: AI分析服务
- 规则分析（默认）
- AI模型分析（可扩展）
- 统一分析接口

### 6. strategy（策略模块）

**scorer.py**: 股票打分器
- 多因子打分（0-100分）
- 趋势、MACD、RSI、成交量、涨跌幅

**selector.py**: 选股引擎
- 根据阈值筛选股票
- 过滤ST股票
- 按分数排序

### 7. trading（模拟交易）

**account.py**: 账户管理
- 账户信息
- 持仓管理
- 交易记录
- 数据持久化（Redis）

**engine.py**: 交易引擎
- 订单执行
- 账户查询
- 持仓查询

### 8. notify（通知服务）

**telegram.py**: Telegram通知
- 消息推送
- 股票提醒
- 选股提醒

**email.py**: 邮箱通知
- SMTP邮件发送
- HTML格式邮件

**wechat.py**: 企业微信通知
- Webhook消息推送

**dispatcher.py**: 统一调度
- 多渠道通知
- 统一接口

### 9. gateway（API网关）

**app.py**: 主应用
- FastAPI应用
- 路由注册
- 静态文件服务
- CORS配置

## 数据流

### 行情数据流

```
AKShare数据源
    ↓
market_collector（采集）
    ↓
Redis（缓存）
    ↓
market/service（API服务）
    ↓
前端 / WebSocket
```

### 选股流程

```
行情数据（Redis）
    ↓
计算技术指标
    ↓
股票打分
    ↓
筛选排序
    ↓
返回结果
```

### 交易流程

```
订单请求
    ↓
trading/engine（验证）
    ↓
trading/account（执行）
    ↓
更新持仓/资金
    ↓
保存到Redis
    ↓
返回结果
```

## 扩展点

### 1. 接入新的数据源

在 `market_collector/` 中创建新的适配器，实现标准接口。

### 2. 添加新的技术指标

在 `market/indicator/ta.py` 中添加新的指标函数。

### 3. 自定义选股策略

修改 `strategy/scorer.py` 中的打分逻辑。

### 4. 接入AI模型

修改 `ai/analyzer.py` 中的 `analyze_stock_with_ai` 函数。

### 5. 添加新的通知渠道

在 `notify/` 中创建新的通知模块，并在 `dispatcher.py` 中注册。

## 配置说明

### 环境变量

所有配置都在 `backend/.env` 文件中，包括：

- Redis配置
- ClickHouse配置
- 通知服务配置（Telegram/邮箱/微信）
- 日志级别
- API配置

### Docker Compose

- `redis`: Redis服务
- `clickhouse`: ClickHouse服务（可选）
- `collector`: 行情采集服务
- `api`: API服务

## 注意事项

1. **数据源**: 使用AKShare免费数据源，可能有频率限制
2. **性能**: 首次选股需要计算大量指标，可能较慢
3. **存储**: 当前使用Redis存储，数据会在重启后保留（如果配置了持久化）
4. **扩展**: 所有模块都是独立的，可以单独替换或扩展

