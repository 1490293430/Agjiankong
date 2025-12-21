# 部署和更新指南

## 版本更新说明

当前版本：`20251221_3`

## 强制刷新浏览器缓存

### 方法1：硬刷新（推荐）
- **Windows/Linux**: `Ctrl + F5` 或 `Ctrl + Shift + R`
- **Mac**: `Cmd + Shift + R`

### 方法2：清除浏览器缓存
1. 打开浏览器开发者工具（F12）
2. 右键点击刷新按钮
3. 选择"清空缓存并硬性重新加载"

### 方法3：无痕模式测试
- 使用浏览器的无痕/隐私模式打开页面，确保看到最新版本

## 部署步骤

### 1. 更新代码后重启服务（如果需要）

由于使用了volume挂载，前端代码修改后会自动生效，但建议重启API容器以确保更新：

```bash
# 重启API容器
docker compose restart api

# 或者重启所有服务
docker compose restart
```

### 2. 验证更新

1. 打开浏览器开发者工具（F12）
2. 查看Network标签
3. 刷新页面（Ctrl+F5）
4. 检查 `app.js` 和 `style.css` 的请求URL，应该包含版本号 `?v=20251221_3`

### 3. 检查版本号

在浏览器控制台运行：
```javascript
// 检查当前加载的脚本版本
document.querySelector('script[src*="app.js"]').src
// 应该看到: .../app.js?v=20251221_3
```

## 版本号更新规则

每次发布新版本时，更新 `frontend/index.html` 中的版本号：
- CSS: `<link rel="stylesheet" href="style.css?v=YYYYMMDD_N">`
- JS: `<script src="app.js?v=YYYYMMDD_N"></script>`

格式：`YYYYMMDD_N`（日期_序号）

## 故障排查

如果浏览器仍然显示旧版本：

1. **检查版本号是否正确更新**
   ```bash
   grep "v=" frontend/index.html
   ```

2. **检查Docker容器中的文件**
   ```bash
   docker exec stock_api cat /app/frontend/index.html | grep "v="
   ```

3. **清除浏览器所有缓存**
   - Chrome: 设置 > 隐私和安全 > 清除浏览数据 > 选择"缓存的图片和文件"
   - Firefox: 设置 > 隐私与安全 > Cookie和网站数据 > 清除数据

4. **检查CDN或代理缓存**
   - 如果使用了CDN或反向代理，可能需要清除其缓存
