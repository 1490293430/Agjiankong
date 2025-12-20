# Volume 挂载说明

## 已配置的持久化挂载

### 1. 数据持久化（已有）
- **Redis数据**: `redis_data` volume -> `/data`
- **ClickHouse数据**: `clickhouse_data` volume -> `/var/lib/clickhouse`
- **ClickHouse配置**: `./clickhouse-config` -> `/etc/clickhouse-server/config.d`

### 2. 代码目录挂载（新增）
- **API服务**: `./backend` -> `/app` 
  - 代码修改后无需重建镜像
  - uvicorn 的 `--reload` 参数会自动检测代码变化并重载
- **Collector服务**: `./backend` -> `/app`
  - 代码修改后需要重启容器才能生效

## 使用方法

### 修改代码后
1. **API服务**：保存文件后自动重载（约1-2秒）
2. **Collector服务**：修改代码后需要重启容器
   ```bash
   docker restart stock_collector
   ```

### 验证挂载
```bash
# 检查挂载点
docker exec stock_api ls -la /app

# 检查代码是否同步
docker exec stock_api cat /app/market_collector/hk.py | head -20
```

## 注意事项

1. **代码同步**：本地 `./backend` 目录的修改会立即反映到容器中
2. **依赖安装**：如果需要安装新的Python包，需要：
   - 修改 `backend/requirements.txt`
   - 重新构建镜像：`docker-compose build api collector`
3. **生产环境**：生产环境建议移除 `--reload` 参数以提高性能

## 好处

- ✅ 代码修改后无需重建镜像，提高开发效率
- ✅ 数据持久化，容器重建后数据不丢失
- ✅ 配置持久化，ClickHouse配置修改后无需重建
