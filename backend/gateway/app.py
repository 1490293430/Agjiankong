"""
API网关 - 主应用入口
"""
from fastapi import FastAPI, Depends, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import os

# 导入各模块路由
from market.service.api import router as market_router
from market.service.ws import router as ws_router
from news.collector import fetch_news
from ai.analyzer import analyze_stock
from strategy.selector import select_stocks
from trading.engine import execute_order, get_account_info, get_positions
from trading.account import get_account
from market.indicator.ta import calculate_all_indicators
from market_collector.cn import fetch_a_stock_kline
from common.redis import get_json
from common.logger import get_logger
from common.db import init_tables
from common.config import settings
from fastapi import APIRouter, Query, Body
from typing import List, Dict, Any, Optional
import pandas as pd
from pydantic import BaseModel

from common.runtime_config import (
    RuntimeConfig,
    RuntimeConfigUpdate,
    get_runtime_config,
    update_runtime_config,
)

logger = get_logger(__name__)

app = FastAPI(
    title="量化交易终端API",
    description="A股/港股行情分析系统",
    version="1.0.0"
)

# CORS配置（支持通过环境变量限制来源）
if settings.api_allowed_origins and settings.api_allowed_origins != "*":
    origins = [
        o.strip()
        for o in settings.api_allowed_origins.split(",")
        if o.strip()
    ]
else:
    origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


async def verify_api_token(
    x_api_token: Optional[str] = Header(default=None, alias="X-API-Token"),
) -> None:
    """简单可选的 API Token 校验。

    - 未配置 `API_AUTH_TOKEN` 时不做任何校验（便于开发体验）
    - 配置后，所有带此依赖的接口必须在请求头中携带 `X-API-Token`
    """
    if not settings.api_auth_token:
        return

    if not x_api_token or x_api_token != settings.api_auth_token:
        raise HTTPException(status_code=401, detail="Unauthorized")


async def verify_admin_token(
    x_admin_token: Optional[str] = Header(default=None, alias="X-Admin-Token"),
) -> None:
    """超级管理员校验

    - 优先使用 `ADMIN_TOKEN`
    - 若未配置 `ADMIN_TOKEN`，则回退使用 `API_AUTH_TOKEN`
    """
    # 决定当前有效的 admin token
    admin_token = settings.admin_token or settings.api_auth_token
    if not admin_token:
        # 未启用任何 Token，视为未开启管理员校验
        return

    if not x_admin_token or x_admin_token != admin_token:
        raise HTTPException(status_code=401, detail="Admin Unauthorized")


# 带鉴权依赖的路由配置
secured_dependencies = [Depends(verify_api_token)]
admin_dependencies = [Depends(verify_api_token), Depends(verify_admin_token)]

# 认证路由（登录不需要前置 Token）
auth_router = APIRouter(prefix="/api/auth", tags=["Auth"])


@auth_router.post("/login")
async def login(data: Dict[str, Any] = Body(...)):
    """管理员登录

    默认账号密码为 admin / admin，可通过环境变量覆盖：
    - ADMIN_USERNAME
    - ADMIN_PASSWORD
    """
    username = str(data.get("username", "")).strip()
    password = str(data.get("password", "")).strip()

    if (
        username == settings.admin_username
        and password == settings.admin_password
    ):
        # 返回当前生效的 API Token 与 Admin Token（可能为空，用于开发环境）
        return {
            "success": True,
            "role": "admin",
            "token": settings.api_auth_token,
            "admin_token": settings.admin_token or settings.api_auth_token,
        }

    raise HTTPException(status_code=401, detail="用户名或密码错误")


# 注册路由
app.include_router(auth_router)

app.include_router(
    market_router,
    prefix="/api",
    dependencies=secured_dependencies,
)
# WebSocket路由直接注册（不使用prefix，因为ws_router中已定义完整路径）
app.include_router(ws_router)

# 创建其他路由
api_router = APIRouter(
    prefix="/api",
    tags=["API"],
    dependencies=secured_dependencies,
)


@api_router.get("/news/latest")
async def get_latest_news():
    """获取最新资讯"""
    try:
        news = get_json("news:latest")
        if not news:
            news = fetch_news()
        return {"code": 0, "data": news or [], "message": "success"}
    except Exception as e:
        logger.error(f"获取资讯失败: {e}", exc_info=True)
        return {"code": 1, "data": [], "message": str(e)}


@api_router.get("/strategy/select")
async def select_stocks_api(
    threshold: int | None = Query(None, description="选股阈值，留空则使用系统配置"),
    max_count: int | None = Query(None, description="最大数量，留空则使用系统配置"),
):
    """自动选股"""
    try:
        # 读取系统运行时配置
        cfg = get_runtime_config()
        if threshold is None:
            threshold = cfg.selection_threshold
        if max_count is None:
            max_count = cfg.selection_max_count

        # 获取A股行情
        a_stocks = get_json("market:a:spot") or []
        
        # 计算每只股票的技术指标
        indicators_map = {}
        for stock in a_stocks[:200]:  # 限制计算数量
            code = str(stock.get("code", ""))
            try:
                kline_data = fetch_a_stock_kline(code, period="daily")
                if kline_data and len(kline_data) >= 20:
                    df = pd.DataFrame(kline_data)
                    indicators = calculate_all_indicators(df)
                    if indicators:
                        indicators_map[code] = indicators
            except Exception as e:
                logger.debug(f"计算指标失败 {code}: {e}")
                continue
        
        # 执行选股
        selected = select_stocks(a_stocks, indicators_map, threshold, max_count)
        
        return {"code": 0, "data": selected, "message": "success"}
    except Exception as e:
        logger.error(f"选股失败: {e}", exc_info=True)
        return {"code": 1, "data": [], "message": str(e)}


@api_router.get("/config", response_model=RuntimeConfig, dependencies=admin_dependencies)
async def get_config_api():
    """获取系统运行时配置"""
    return get_runtime_config()


@api_router.put("/config", response_model=RuntimeConfig, dependencies=admin_dependencies)
async def update_config_api(data: RuntimeConfigUpdate = Body(...)):
    """更新系统运行时配置"""
    try:
        cfg = update_runtime_config(data)
        return cfg
    except Exception as e:
        logger.error(f"更新配置失败: {e}", exc_info=True)
        from fastapi import HTTPException

        raise HTTPException(status_code=400, detail=str(e))


@api_router.get("/ai/analyze/{code}")
async def analyze_stock_api(code: str, use_ai: bool = Query(False, description="是否使用AI模型")):
    """分析股票"""
    try:
        # 获取股票行情
        a_stocks = get_json("market:a:spot") or []
        stock = next((s for s in a_stocks if str(s.get("code", "")) == code), None)
        
        if not stock:
            return {"code": 1, "data": {}, "message": "股票不存在"}
        
        # 获取技术指标
        kline_data = fetch_a_stock_kline(code, period="daily")
        if not kline_data or len(kline_data) < 20:
            return {"code": 1, "data": {}, "message": "K线数据不足"}
        
        df = pd.DataFrame(kline_data)
        indicators = calculate_all_indicators(df)
        
        # AI分析
        analysis = analyze_stock(stock, indicators, None, use_ai)
        
        return {"code": 0, "data": analysis, "message": "success"}
    except Exception as e:
        logger.error(f"分析股票失败 {code}: {e}", exc_info=True)
        return {"code": 1, "data": {}, "message": str(e)}


@api_router.get("/trading/account")
async def get_account_api():
    """获取账户信息"""
    return get_account_info()


@api_router.get("/trading/positions")
async def get_positions_api():
    """获取持仓信息"""
    # 获取市场价格
    a_stocks = get_json("market:a:spot") or []
    market_prices = {str(s.get("code", "")): float(s.get("price", 0)) for s in a_stocks}
    
    return get_positions("default", market_prices)


@api_router.post("/trading/order")
async def submit_order_api(data: Dict[str, Any] = Body(...)):
    """提交订单"""
    account_id = data.get("account_id", "default")
    action = data.get("action")
    code = data.get("code")
    price = data.get("price")
    qty = data.get("qty")
    
    return execute_order(account_id, action, code, price, qty)


@api_router.post("/trading/reset", dependencies=admin_dependencies)
async def reset_account_api():
    """重置账户"""
    try:
        account = get_account("default")
        account.reset()
        return {"success": True, "message": "账户已重置"}
    except Exception as e:
        logger.error(f"重置账户失败: {e}", exc_info=True)
        return {"success": False, "message": str(e)}


# 注册路由
app.include_router(api_router)

# 静态文件服务（前端）
frontend_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "frontend")
if os.path.exists(frontend_path):
    # 静态资源
    static_path = os.path.join(frontend_path, "static")
    if os.path.exists(static_path):
        app.mount("/static", StaticFiles(directory=static_path), name="static")
    
    # 前端文件直接服务
    @app.get("/")
    async def index():
        """前端首页"""
        index_file = os.path.join(frontend_path, "index.html")
        if os.path.exists(index_file):
            return FileResponse(index_file)
        return {"message": "前端文件未找到"}
    
    # 前端资源文件（仅允许特定文件）
    @app.get("/{filename:path}")
    async def frontend_files(filename: str):
        """前端资源文件"""
        # 只允许访问前端目录下的文件
        allowed_extensions = {'.html', '.js', '.css', '.json'}
        if '.' in filename:
            ext = os.path.splitext(filename)[1]
            if ext in allowed_extensions:
                file_path = os.path.join(frontend_path, filename)
                if os.path.exists(file_path) and os.path.isfile(file_path):
                    # 确保路径在frontend目录内（防止路径遍历）
                    real_path = os.path.realpath(file_path)
                    real_frontend = os.path.realpath(frontend_path)
                    if real_path.startswith(real_frontend):
                        return FileResponse(file_path)
        # 不是前端文件，返回404或转发到API
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Not found")


@app.on_event("startup")
async def startup_event():
    """启动事件"""
    logger.info("API服务启动中...")
    try:
        # 初始化数据库表
        init_tables()
        logger.info("数据库表初始化完成")
    except Exception as e:
        logger.warning(f"数据库初始化失败（可能是ClickHouse未启动）: {e}")
    logger.info("API服务启动完成")


@app.get("/health")
async def health_check():
    """健康检查"""
    return {"status": "ok", "message": "服务运行正常"}


if __name__ == "__main__":
    import uvicorn
    from common.config import settings
    
    uvicorn.run(
        "gateway.app:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=True
    )

