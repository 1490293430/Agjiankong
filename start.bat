@echo off
chcp 65001 >nul
echo ====================================
echo 量化交易终端 - 启动脚本
echo ====================================
echo.

echo [1/3] 检查Docker环境...
docker --version >nul 2>&1
if %errorlevel% neq 0 (
    echo 错误: 未检测到Docker，请先安装Docker Desktop
    pause
    exit /b 1
)

echo [2/3] 启动Docker Compose服务...
docker-compose up -d

echo [3/3] 等待服务启动...
timeout /t 5 /nobreak >nul

echo.
echo ====================================
echo 启动完成！
echo ====================================
echo API服务: http://localhost:8000
echo 前端页面: http://localhost:8000
echo.
echo 查看日志: docker-compose logs -f
echo 停止服务: docker-compose down
echo ====================================
pause

