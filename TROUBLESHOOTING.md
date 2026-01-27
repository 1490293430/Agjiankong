# 故障排查指南

## ClickHouse 连接失败问题

### 症状
API服务日志显示：
```
ConnectionRefusedError: [Errno 111] Connection refused
Code: 210. Connection refused (stock_clickhouse:9000)
```

### 原因
ClickHouse 启动较慢，API服务在ClickHouse完全就绪前尝试连接。

### 解决方案

#### 方案1：使用修复后的配置（推荐）
已添加以下改进：
1. **健康检查**：docker-compose.yml 中为 ClickHouse 添加了健康检查
2. **启动依赖**：API服务等待 ClickHouse 健康后才启动
3. **重试机制**：API启动时自动重试连接（最多10次，指数退避）

重启服务：
```bash
docker compose down
docker compose up -d
```

或使用便捷脚本：
```bash
bash restart_services.sh
```

#### 方案2：手动重启API服务
如果ClickHouse已经启动但API还在报错：
```bash
docker compose restart api
```

#### 方案3：查看详细日志
```bash
# 查看所有服务日志
docker compose logs

# 只看ClickHouse日志
docker compose logs clickhouse

# 只看API日志
docker compose logs api

# 实时跟踪日志
docker compose logs -f api
```

### 验证修复
成功启动后，API日志应显示：
```
✓ 数据库表初始化完成
交易日历初始化完成
API服务启动完成
```

## 其他常见问题

### Redis 连接失败
检查Redis是否运行：
```bash
docker compose ps redis
```

重启Redis：
```bash
docker compose restart redis
```

### 端口占用
如果8000或9000端口被占用：
```bash
# 查看端口占用
netstat -tulpn | grep 8000
netstat -tulpn | grep 9000

# 修改 .env 文件中的端口配置
```

### 磁盘空间不足
ClickHouse数据量大，定期清理：
```bash
# 查看磁盘使用
df -h

# 查看Docker卷使用
docker system df -v

# 清理未使用的数据
docker system prune -a --volumes
```
