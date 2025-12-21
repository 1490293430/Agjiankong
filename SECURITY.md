# 🔒 安全配置指南

## Redis 安全配置

### ✅ 已实施的安全措施

1. **Redis 仅允许 Docker 内部访问**
   - Redis 端口不再映射到公网
   - 仅通过 Docker 内部网络访问
   - Docker 网络内的服务（stock_api、stock_collector）可以通过服务名 `redis` 访问

2. **Redis 密码保护（可选但推荐）**
   - 通过环境变量 `REDIS_PASSWORD` 设置密码
   - 如果设置了密码，Redis 会启用 `requirepass` 认证

### 📝 配置说明

#### Docker Compose 环境（推荐）

在 `docker-compose.yml` 中，Redis 配置如下：

```yaml
redis:
  expose:
    - "6379"  # 仅暴露给 Docker 网络内的其他容器
  # 不设置 ports，避免暴露到公网
```

**优点：**
- ✅ Redis 完全隔离在 Docker 网络内
- ✅ 无法从公网访问
- ✅ 防止端口扫描和暴力破解攻击

#### 如果需要从宿主机访问 Redis（仅用于调试）

如果需要临时从宿主机访问 Redis（例如使用 `redis-cli`），可以修改 `docker-compose.yml`：

```yaml
redis:
  ports:
    - "127.0.0.1:6379:6379"  # 只绑定到本地回环地址，不暴露到公网
```

**注意：**
- ⚠️ 这仍然允许从宿主机访问，但不会暴露到公网
- ⚠️ 调试完成后，建议移除 ports 配置

### 🔐 设置 Redis 密码

在 `.env` 文件中设置：

```env
REDIS_PASSWORD=your-strong-password-here
```

设置密码后：
- Redis 会启用 `requirepass` 认证
- Docker 网络内的服务会自动使用密码连接（通过环境变量传递）

### 🛡️ 其他安全建议

1. **定期更新 Redis 镜像**
   ```bash
   docker-compose pull redis
   docker-compose up -d redis
   ```

2. **监控 Redis 连接**
   - 检查日志：`docker-compose logs redis`
   - 监控异常连接尝试

3. **使用防火墙**
   - 即使 Redis 不暴露到公网，也建议配置防火墙规则
   - 只允许必要的端口对外开放

4. **定期备份**
   - Redis 数据已配置 AOF 和 RDB 双重持久化
   - 定期备份 `redis_data` 卷的数据

## ClickHouse 安全配置

### ⚠️ 当前状态

ClickHouse 仍然暴露在公网（端口 8123、9000），建议：

1. **生产环境设置强密码**
   ```env
   CLICKHOUSE_PASSWORD=your-very-strong-password
   ```

2. **考虑限制访问**
   - 使用防火墙规则限制访问来源
   - 或修改 `docker-compose.yml`，只绑定到本地：
     ```yaml
     ports:
       - "127.0.0.1:8123:8123"
       - "127.0.0.1:9000:9000"
     ```

## API 安全配置

### 推荐配置

在 `.env` 文件中设置：

```env
# API Token（用于保护接口访问）
API_AUTH_TOKEN=your-strong-api-token

# 管理员 Token（用于敏感操作）
ADMIN_TOKEN=your-strong-admin-token

# 允许的前端地址（防止 CSRF）
API_ALLOWED_ORIGINS=https://your-domain.com

# 管理员账号密码
ADMIN_USERNAME=admin
ADMIN_PASSWORD=your-strong-password
```

## 总结

✅ **已实施：**
- Redis 仅 Docker 内部访问
- Redis 密码保护（可选）
- AOF/RDB 双重持久化

⚠️ **建议改进：**
- ClickHouse 访问限制
- API Token 和密码强度
- 定期更新镜像
- 监控和日志审计

