"""
API网关 - 主应用入口
"""
from fastapi import FastAPI, Depends, Header, HTTPException, BackgroundTasks, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import os
import asyncio
import time

# 导入各模块路由
from market.service.api import router as market_router
from market.service.ws import router as ws_router
from market.service.sse import router as sse_router
from news.collector import fetch_news
from ai.analyzer import analyze_stock
from trading.plan import (
    create_trade_plan,
    get_pending_plans,
    get_active_plans,
    close_trade_plan,
    get_trade_statistics,
    get_stock_statistics,
    check_trade_plans_by_spot_price
)
from ai.analyzer import get_system_metrics
from strategy.selector import select_stocks
from trading.engine import execute_order, get_account_info, get_positions
from trading.account import get_account
from market.indicator.ta import calculate_all_indicators
from market_collector.cn import fetch_a_stock_kline
from common.redis import get_json, set_json, delete
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
from notify.dispatcher import notify as notify_message

logger = get_logger(__name__)

AI_ANALYSIS_KEY = "ai:analysis"

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


# 添加响应头中间件，确保移动端正确渲染
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request as StarletteRequest

class MobileCompatMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: StarletteRequest, call_next):
        response = await call_next(request)
        # 添加 Vary 头，告诉浏览器根据 User-Agent 缓存不同版本
        response.headers["Vary"] = "User-Agent, Accept-Encoding"
        return response

app.add_middleware(MobileCompatMiddleware)


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
    
    密码优先级：Redis配置 > 环境变量
    """
    from common.runtime_config import get_runtime_config
    
    username = str(data.get("username", "")).strip()
    password = str(data.get("password", "")).strip()

    # 优先使用Redis中存储的密码，如果没有则使用环境变量
    runtime_config = get_runtime_config()
    effective_password = runtime_config.admin_password or settings.admin_password

    if (
        username == settings.admin_username
        and password == effective_password
    ):
        # 返回当前生效的 API Token 与 Admin Token（可能为空，用于开发环境）
        return {
            "success": True,
            "role": "admin",
            "token": settings.api_auth_token,
            "admin_token": settings.admin_token or settings.api_auth_token,
        }

    raise HTTPException(status_code=401, detail="用户名或密码错误")


@auth_router.post("/change-password", dependencies=admin_dependencies)
async def change_password(data: Dict[str, Any] = Body(...)):
    """修改管理员密码
    
    需要提供旧密码和新密码，密码会保存到运行时配置（Redis）中
    """
    from common.runtime_config import get_runtime_config, update_runtime_config
    
    old_password = str(data.get("old_password", "")).strip()
    new_password = str(data.get("new_password", "")).strip()
    
    if not old_password:
        raise HTTPException(status_code=400, detail="请输入旧密码")
    
    if not new_password:
        raise HTTPException(status_code=400, detail="请输入新密码")
    
    if len(new_password) < 6:
        raise HTTPException(status_code=400, detail="新密码长度至少6位")
    
    # 验证旧密码
    runtime_config = get_runtime_config()
    effective_password = runtime_config.admin_password or settings.admin_password
    
    if old_password != effective_password:
        raise HTTPException(status_code=401, detail="旧密码错误")
    
    # 更新密码到运行时配置
    try:
        from common.runtime_config import RuntimeConfigUpdate
        update_data = RuntimeConfigUpdate(admin_password=new_password)
        update_runtime_config(update_data)
        
        logger.info("管理员密码已更新")
        return {
            "success": True,
            "message": "密码修改成功"
        }
    except Exception as e:
        logger.error(f"修改密码失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"修改密码失败: {str(e)}")


# 注册路由
app.include_router(auth_router)

app.include_router(
    market_router,
    prefix="/api",
    dependencies=secured_dependencies,
)
# WebSocket路由注册（不使用prefix，保持原有路径）
# 注意：WebSocket不支持dependencies，所以不使用认证依赖
# 前端连接路径：/ws/watchlist, /ws/selection/progress 等（不需要/api前缀）
# 保留WebSocket用于向后兼容，但推荐使用SSE
app.include_router(ws_router)

# SSE路由注册（统一推送服务）
app.include_router(sse_router, prefix="/api")

# 创建其他路由
api_router = APIRouter(
    prefix="/api",
    tags=["API"],
    dependencies=secured_dependencies,
)


@api_router.get("/news/latest", dependencies=[])  # 不需要认证，公开接口
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


@api_router.get("/watchlist")
async def get_watchlist():
    """获取自选股列表"""
    try:
        logger.info("[自选] 收到获取自选股列表请求")
        watchlist = get_json("watchlist:default") or []
        logger.info(f"[自选] 返回自选股列表，共{len(watchlist)}只股票: {[s.get('code') for s in watchlist]}")
        return {"code": 0, "data": watchlist, "message": "success"}
    except Exception as e:
        logger.error(f"[自选] 获取自选股列表失败: {e}", exc_info=True)
        return {"code": 1, "data": [], "message": str(e)}


@api_router.post("/watchlist")
async def save_watchlist(data: Dict[str, Any] = Body(...), background_tasks: BackgroundTasks = None):
    """保存自选股列表
    
    Body参数:
        stocks: 自选股列表，格式: [{"code": "000001", "name": "平安银行", "addTime": 1234567890}]
    """
    try:
        logger.info(f"[自选] 收到保存请求，原始数据: {data}")
        stocks = data.get("stocks", [])
        logger.info(f"[自选] 提取的stocks: {stocks}, 类型: {type(stocks)}, 长度: {len(stocks) if isinstance(stocks, list) else 'N/A'}")
        
        if not isinstance(stocks, list):
            logger.warning(f"[自选] stocks参数不是数组: {type(stocks)}")
            return {"code": 1, "data": [], "message": "stocks参数必须是数组"}
        
        # 验证数据格式
        validated_stocks = []
        for stock in stocks:
            if isinstance(stock, dict) and "code" in stock:
                validated_stocks.append({
                    "code": str(stock.get("code", "")).strip(),
                    "name": str(stock.get("name", stock.get("code", ""))).strip(),
                    "addTime": stock.get("addTime", int(time.time() * 1000))
                })
        
        logger.info(f"[自选] 验证后的股票列表: {validated_stocks}, 共{len(validated_stocks)}只")
        
        # 保存到Redis
        logger.info(f"[自选] 开始保存到Redis: watchlist:default")
        success = set_json("watchlist:default", validated_stocks)
        logger.info(f"[自选] Redis保存结果: {success}")
        
        if success:
            logger.info(f"[自选] 自选股列表保存成功，共{len(validated_stocks)}只股票: {[s['code'] for s in validated_stocks]}")
            
            # 通过SSE广播给所有连接的客户端（后台任务）
            async def do_sse_broadcast():
                try:
                    from market.service.sse import broadcast_watchlist_update
                    logger.info(f"[自选] 开始SSE广播")
                    broadcast_watchlist_update(validated_stocks)
                    logger.info(f"[自选] 自选股变化已通过SSE广播")
                except Exception as e:
                    logger.error(f"[自选] SSE广播自选股变化失败: {e}", exc_info=True)
            
            if background_tasks:
                logger.info(f"[自选] 使用background_tasks异步广播")
                background_tasks.add_task(do_sse_broadcast)
            else:
                # 如果没有background_tasks，直接执行（可能阻塞）
                logger.info(f"[自选] 直接执行广播（无background_tasks）")
                try:
                    await do_sse_broadcast()
                except Exception as e:
                    logger.error(f"[自选] 广播自选股变化失败: {e}", exc_info=True)
            
            logger.info(f"[自选] 返回成功响应，数据: {validated_stocks}")
            return {"code": 0, "data": validated_stocks, "message": "success"}
        else:
            logger.error(f"[自选] Redis保存失败")
            return {"code": 1, "data": [], "message": "保存失败"}
    except Exception as e:
        logger.error(f"[自选] 保存自选股列表异常: {e}", exc_info=True)
        return {"code": 1, "data": [], "message": str(e)}


@api_router.get("/market/status", dependencies=[])  # 不需要认证，公开接口
async def get_market_status():
    """获取A股和港股的交易状态"""
    logger.info("[市场状态] 收到市场状态查询请求")
    try:
        from common.trading_hours import is_a_stock_trading_time, is_hk_stock_trading_time
        
        is_a_trading = is_a_stock_trading_time()
        is_hk_trading = is_hk_stock_trading_time()
        
        logger.info(f"[市场状态] 市场状态检查完成: A股={is_a_trading}, 港股={is_hk_trading}")
        
        return {
            "code": 0,
            "data": {
                "a": {
                    "is_trading": is_a_trading,
                    "status": "交易中" if is_a_trading else "已收盘"
                },
                "hk": {
                    "is_trading": is_hk_trading,
                    "status": "交易中" if is_hk_trading else "已收盘"
                }
            },
            "message": "success"
        }
    except ImportError as e:
        logger.error(f"导入交易时间模块失败: {e}", exc_info=True)
        return {
            "code": 1,
            "data": {
                "a": {"is_trading": False, "status": "模块错误"},
                "hk": {"is_trading": False, "status": "模块错误"}
            },
            "message": f"模块导入失败: {str(e)}"
        }
    except Exception as e:
        logger.error(f"获取市场状态失败: {e}", exc_info=True)
        return {
            "code": 1,
            "data": {
                "a": {"is_trading": False, "status": "未知"},
                "hk": {"is_trading": False, "status": "未知"}
            },
            "message": str(e)
        }


@api_router.get("/tushare/status")
async def get_tushare_status():
    """检查 Tushare 数据源连接状态"""
    try:
        from market_collector.tushare_source import check_tushare_connection
        result = check_tushare_connection()
        return {"code": 0, "data": result, "message": "success"}
    except ImportError as e:
        return {
            "code": 1,
            "data": {"connected": False, "message": f"模块未安装: {e}", "token_configured": False},
            "message": str(e)
        }
    except Exception as e:
        logger.error(f"检查 Tushare 状态失败: {e}", exc_info=True)
        return {
            "code": 1,
            "data": {"connected": False, "message": str(e), "token_configured": False},
            "message": str(e)
        }


@api_router.api_route("/strategy/select", methods=["GET", "POST"])
async def select_stocks_api(
    request: Request,
    background_tasks: BackgroundTasks,
    max_count: int | None = Query(None, description="最大数量，留空则使用系统配置"),
    market: str | None = Query(None, description="市场类型：A（A股）或HK（港股），留空则使用系统配置"),
    task_id: str | None = Query(None, description="任务ID，用于进度追踪"),
):
    """自动选股（异步执行，通过SSE推送进度）"""
    import uuid
    from market.service.ws import selection_progress
    from market.service.sse import broadcast_message
    
    # 如果没有提供task_id，生成一个
    if not task_id:
        task_id = str(uuid.uuid4())
    
    # 解析筛选配置（从POST body获取）
    filter_config = {}
    try:
        body = await request.body()
        if body:
            import json
            filter_config = json.loads(body)
            logger.info(f"收到筛选配置: {filter_config}")
    except Exception as e:
        logger.warning(f"解析筛选配置失败: {e}")
    
    # 读取系统运行时配置
    cfg = get_runtime_config()
    if max_count is None:
        max_count = cfg.selection_max_count
    if market is None:
        market = cfg.selection_market
    
    # 初始化进度
    selection_progress[task_id] = {
        "status": "running",
        "stage": "init",
        "message": "正在启动选股任务...",
        "progress": 0,
        "total": 0,
        "processed": 0,
        "passed": 0
    }
    
    # 通过SSE推送初始进度
    broadcast_message({
        "type": "selection_progress",
        "task_id": task_id,
        "data": selection_progress[task_id]
    })
    
    # 在后台线程中执行选股
    def run_selection_sync():
        return _run_selection_task(task_id, market, max_count, filter_config)
    
    try:
        # 使用 asyncio.to_thread 在线程池中执行，避免阻塞事件循环
        result = await asyncio.to_thread(run_selection_sync)
        return result
    except Exception as e:
        logger.error(f"选股失败: {e}", exc_info=True)
        selection_progress[task_id] = {
            "status": "failed",
            "stage": "error",
            "message": f"选股失败: {str(e)[:100]}",
            "progress": 0,
            "elapsed_time": 0
        }
        broadcast_message({
            "type": "selection_progress",
            "task_id": task_id,
            "data": selection_progress[task_id]
        })
        return {"code": 1, "data": [], "message": str(e), "task_id": task_id}


def _broadcast_selection_progress(task_id: str, progress_data: dict):
    """广播选股进度到SSE（同时更新 selection_progress 字典）"""
    from market.service.ws import selection_progress
    from market.service.sse import broadcast_message
    
    # 更新进度字典
    selection_progress[task_id] = progress_data
    
    # 广播到所有SSE连接
    try:
        broadcast_message({
            "type": "selection_progress",
            "task_id": task_id,
            "data": progress_data
        })
    except Exception as e:
        logger.warning(f"广播选股进度失败: {e}")


def _run_selection_task(task_id: str, market: str, max_count: int, filter_config: dict):
    """执行选股任务（简化版：只用勾选的指标筛选全部股票）"""
    from market.service.ws import selection_progress
    
    try:
        from strategy.selector import save_selected_stocks
        from market_collector.hk import fetch_hk_stock_kline
        from common.db import batch_get_indicators, save_indicator, get_stock_list_from_db, get_stock_name_map
        from market.indicator.ta import calculate_all_indicators
        import time
        import math
        
        start_time = time.time()
        
        # 读取系统运行时配置
        cfg = get_runtime_config()
        if max_count is None:
            max_count = cfg.selection_max_count
        if market is None:
            market = cfg.selection_market
        
        # 从ClickHouse获取股票列表
        all_stocks = get_stock_list_from_db(market.upper())
        
        if market.upper() == "HK":
            fetch_kline_func = fetch_hk_stock_kline
        else:
            fetch_kline_func = fetch_a_stock_kline
        
        if not all_stocks:
            return {
                "code": 1, 
                "data": [], 
                "message": f"未获取到{market}股行情数据，请先运行数据采集程序。",
                "task_id": task_id
            }
        
        # 获取股票名称映射
        stock_name_map = get_stock_name_map(market.upper())
        if stock_name_map:
            for stock in all_stocks:
                code = str(stock.get("code", ""))
                if code in stock_name_map:
                    stock["name"] = stock_name_map[code]
        
        logger.info(f"开始选股：市场={market}，总股票数={len(all_stocks)}")
        
        # 过滤无效股票
        valid_stocks = []
        for stock in all_stocks:
            price = stock.get("price", 0)
            if price is None or (isinstance(price, float) and math.isnan(price)):
                continue
            try:
                if float(price) > 0:
                    valid_stocks.append(stock)
            except (ValueError, TypeError):
                continue
        
        if not valid_stocks:
            return {"code": 0, "data": [], "message": "没有有效的股票数据", "task_id": task_id}
        
        total_stocks = len(valid_stocks)
        logger.info(f"有效股票数：{total_stocks}")
        
        # 解析启用的筛选指标（只用勾选的，不使用默认指标）
        enabled_filters = []
        filter_names = []
        
        # 按计算复杂度排序：简单的先筛选
        if filter_config.get("volume_ratio_enable"):
            enabled_filters.append(("volume_ratio", filter_config))
            filter_names.append("量比")
        if filter_config.get("rsi_enable"):
            enabled_filters.append(("rsi", filter_config))
            filter_names.append("RSI")
        if filter_config.get("ma_enable"):
            enabled_filters.append(("ma", filter_config))
            filter_names.append(f"MA{filter_config.get('ma_period', 20)}")
        if filter_config.get("ema_enable"):
            enabled_filters.append(("ema", filter_config))
            filter_names.append(f"EMA{filter_config.get('ema_period', 12)}")
        if filter_config.get("macd_enable"):
            enabled_filters.append(("macd", filter_config))
            filter_names.append("MACD")
        if filter_config.get("kdj_enable"):
            enabled_filters.append(("kdj", filter_config))
            filter_names.append("KDJ")
        if filter_config.get("bias_enable"):
            enabled_filters.append(("bias", filter_config))
            filter_names.append("BIAS")
        if filter_config.get("williams_r_enable"):
            enabled_filters.append(("williams_r", filter_config))
            filter_names.append("威廉指标")
        if filter_config.get("break_high_enable"):
            enabled_filters.append(("break_high", filter_config))
            filter_names.append("突破高点")
        if filter_config.get("boll_enable"):
            enabled_filters.append(("boll", filter_config))
            filter_names.append("布林带")
        if filter_config.get("adx_enable"):
            enabled_filters.append(("adx", filter_config))
            filter_names.append("ADX")
        if filter_config.get("ichimoku_enable"):
            enabled_filters.append(("ichimoku", filter_config))
            filter_names.append("一目均衡")
        
        if not enabled_filters:
            # 没有启用任何筛选，返回全部股票（按成交额排序）
            sorted_stocks = sorted(valid_stocks, key=lambda x: x.get("amount", 0) or 0, reverse=True)
            selected = sorted_stocks[:max_count]
            save_selected_stocks(selected, market.upper())
            _broadcast_selection_progress(task_id, {
                "status": "completed",
                "stage": "completed", 
                "message": f"未启用筛选条件，返回成交额前{len(selected)}只股票",
                "progress": 100,
                "total": total_stocks,
                "selected": len(selected),
                "elapsed_time": round(time.time() - start_time, 1)
            })
            return {"code": 0, "data": selected, "message": "success", "task_id": task_id}
        
        total_filters = len(enabled_filters)
        logger.info(f"启用的筛选指标：{filter_names}，共{total_filters}个")
        
        # 批量读取缓存
        all_codes = [str(s.get("code", "")) for s in valid_stocks]
        cached_indicators = {}
        try:
            cached_indicators = batch_get_indicators(all_codes, market.upper(), None)
            logger.info(f"缓存命中：{len(cached_indicators)}只股票")
        except Exception as e:
            logger.warning(f"读取缓存失败: {e}")
        
        # 找出缺失指标的股票，预先批量计算
        missing_codes = [code for code in all_codes if code not in cached_indicators]
        failed_codes = []  # 记录计算失败的股票
        
        if missing_codes:
            logger.info(f"缺失指标的股票：{len(missing_codes)}只，开始预计算...")
            _broadcast_selection_progress(task_id, {
                "status": "running",
                "stage": "computing",
                "message": f"预计算指标中，共{len(missing_codes)}只股票缺失指标...",
                "progress": 3,
                "total": total_stocks,
                "missing_count": len(missing_codes)
            })
            
            # 使用线程池并发计算指标
            import concurrent.futures
            computed_count = 0
            saved_count = 0
            
            def compute_and_save_indicator(code):
                """计算并保存单只股票的指标"""
                try:
                    kline_data = fetch_kline_func(code, "daily", "", None, None, False, False)
                    if not kline_data:
                        logger.debug(f"股票 {code} 无K线数据")
                        return (code, None, "无K线数据")
                    if len(kline_data) < 20:
                        logger.debug(f"股票 {code} K线数据不足: {len(kline_data)}条")
                        return (code, None, f"K线不足({len(kline_data)}条)")
                    
                    df = pd.DataFrame(kline_data)
                    indicators = calculate_all_indicators(df)
                    if not indicators:
                        return (code, None, "指标计算失败")
                    
                    # 保存到缓存
                    latest_date = kline_data[-1].get("date", "") if kline_data else ""
                    if latest_date:
                        try:
                            save_indicator(code, market.upper(), latest_date, indicators)
                        except Exception as e:
                            logger.debug(f"保存指标失败 {code}: {e}")
                    return (code, indicators, None)
                except Exception as e:
                    logger.debug(f"计算指标失败 {code}: {e}")
                    return (code, None, str(e)[:50])
            
            # 并发计算（限制并发数避免过载）
            max_workers = min(30, len(missing_codes))
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(compute_and_save_indicator, code): code for code in missing_codes}
                
                for future in concurrent.futures.as_completed(futures):
                    try:
                        code, indicators, error = future.result(timeout=30)
                        if indicators:
                            cached_indicators[code] = indicators
                            computed_count += 1
                            saved_count += 1
                        else:
                            failed_codes.append((code, error))
                    except concurrent.futures.TimeoutError:
                        code = futures[future]
                        failed_codes.append((code, "超时"))
                        logger.debug(f"计算指标超时: {code}")
                    except Exception as e:
                        code = futures[future]
                        failed_codes.append((code, str(e)[:30]))
                        logger.debug(f"计算指标失败: {code}, {e}")
                    
                    # 每计算50只更新一次进度
                    processed = computed_count + len(failed_codes)
                    if processed % 50 == 0:
                        _broadcast_selection_progress(task_id, {
                            "status": "running",
                            "stage": "computing",
                            "message": f"预计算指标中... 成功{computed_count}，失败{len(failed_codes)}，共{len(missing_codes)}",
                            "progress": 3 + int((processed / len(missing_codes)) * 2),
                            "total": total_stocks,
                            "computed": computed_count,
                            "failed": len(failed_codes),
                            "missing_count": len(missing_codes)
                        })
            
            logger.info(f"预计算完成：成功{computed_count}只，失败{len(failed_codes)}只")
            if failed_codes and len(failed_codes) <= 20:
                # 只显示前20个失败的股票
                logger.info(f"失败的股票: {failed_codes[:20]}")
        
        # 筛选结果
        passed_stocks = valid_stocks.copy()
        indicators_map = dict(cached_indicators)  # 复制缓存的指标
        
        # 初始化进度
        _broadcast_selection_progress(task_id, {
            "status": "running",
            "stage": "filtering",
            "message": f"开始筛选，共{total_filters}个指标：{', '.join(filter_names)}",
            "progress": 5,
            "total": total_stocks,
            "processed": 0,
            "filters_total": total_filters,
            "filters_done": 0,
            "current_filter": filter_names[0] if filter_names else "",
            "cached_count": len(cached_indicators)
        })
        
        # 逐个指标筛选全部股票
        for filter_idx, (filter_type, cfg_item) in enumerate(enabled_filters):
            current_filter_name = filter_names[filter_idx]
            remaining_filters = total_filters - filter_idx - 1
            
            _broadcast_selection_progress(task_id, {
                "status": "running",
                "stage": "filtering",
                "message": f"正在筛选【{current_filter_name}】，还剩{remaining_filters}个指标",
                "progress": 5 + int((filter_idx / total_filters) * 85),
                "total": total_stocks,
                "candidates": len(passed_stocks),
                "filters_total": total_filters,
                "filters_done": filter_idx,
                "filters_remaining": remaining_filters,
                "current_filter": current_filter_name
            })
            
            new_passed = []
            processed = 0
            
            for stock in passed_stocks:
                code = str(stock.get("code", ""))
                current_price = stock.get("price", 0)
                
                # 获取指标（已在预计算阶段准备好）
                indicators = indicators_map.get(code, {})
                
                # 根据指标类型筛选
                passed = _check_single_filter(filter_type, filter_config, stock, indicators, current_price)
                
                if passed:
                    new_passed.append(stock)
                
                processed += 1
                
                # 每处理500只更新一次进度（筛选很快，减少更新频率）
                if processed % 500 == 0:
                    _broadcast_selection_progress(task_id, {
                        "status": "running",
                        "stage": "filtering",
                        "message": f"【{current_filter_name}】筛选中 {processed}/{len(passed_stocks)}，还剩{remaining_filters}个指标",
                        "progress": 5 + int((filter_idx / total_filters) * 85) + int((processed / len(passed_stocks)) * (85 / total_filters)),
                        "total": total_stocks,
                        "candidates": len(passed_stocks),
                        "current_passed": len(new_passed),
                        "filters_total": total_filters,
                        "filters_done": filter_idx,
                        "filters_remaining": remaining_filters,
                        "current_filter": current_filter_name
                    })
            
            passed_stocks = new_passed
            logger.info(f"【{current_filter_name}】筛选完成，剩余{len(passed_stocks)}只")
            
            # 如果没有股票通过，提前结束
            if not passed_stocks:
                _broadcast_selection_progress(task_id, {
                    "status": "completed",
                    "stage": "completed",
                    "message": f"【{current_filter_name}】筛选后无股票通过",
                    "progress": 100,
                    "total": total_stocks,
                    "selected": 0,
                    "filters_total": total_filters,
                    "filters_done": filter_idx + 1,
                    "elapsed_time": round(time.time() - start_time, 1)
                })
                return {"code": 0, "data": [], "message": f"【{current_filter_name}】筛选后无股票通过", "task_id": task_id}
        
        # 按成交额排序，取前N只
        passed_stocks = sorted(passed_stocks, key=lambda x: x.get("amount", 0) or 0, reverse=True)
        selected = passed_stocks[:max_count]
        
        # 将指标数据添加到选中的股票中（包括量比、RSI等）
        for stock in selected:
            code = str(stock.get("code", ""))
            indicators = indicators_map.get(code, {})
            if indicators:
                stock["vol_ratio"] = indicators.get("vol_ratio")
                stock["rsi"] = indicators.get("rsi")
                stock["ma5"] = indicators.get("ma5")
                stock["ma10"] = indicators.get("ma10")
                stock["ma20"] = indicators.get("ma20")
                stock["ma60"] = indicators.get("ma60")
                stock["macd_dif"] = indicators.get("macd_dif")
                stock["macd_dea"] = indicators.get("macd_dea")
                stock["kdj_k"] = indicators.get("kdj_k")
                stock["kdj_d"] = indicators.get("kdj_d")
        
        # 保存结果
        save_selected_stocks(selected, market.upper())
        
        total_time = time.time() - start_time
        logger.info(f"选股完成：从{total_stocks}只中筛选出{len(selected)}只，耗时{total_time:.1f}秒")
        
        _broadcast_selection_progress(task_id, {
            "status": "completed",
            "stage": "completed",
            "message": f"选股完成：筛选出{len(selected)}只股票",
            "progress": 100,
            "total": total_stocks,
            "selected": len(selected),
            "filters_total": total_filters,
            "filters_done": total_filters,
            "elapsed_time": round(total_time, 1)
        })
        
        return {"code": 0, "data": selected, "message": "success", "task_id": task_id}
        
    except Exception as e:
        logger.error(f"选股失败: {e}", exc_info=True)
        _broadcast_selection_progress(task_id, {
            "status": "failed",
            "stage": "error",
            "message": f"选股失败: {str(e)[:100]}",
            "progress": 0
        })
        return {"code": 1, "data": [], "message": str(e), "task_id": task_id}


def _check_single_filter(filter_type: str, config: dict, stock: dict, indicators: dict, current_price: float) -> bool:
    """检查单个筛选条件"""
    import math
    
    if not indicators:
        return False
    
    if filter_type == "volume_ratio":
        vol_ratio = indicators.get("vol_ratio", 0)
        min_val = config.get("volume_ratio_min", 0.8)
        max_val = config.get("volume_ratio_max", 8.0)
        return min_val <= vol_ratio <= max_val
    
    elif filter_type == "rsi":
        rsi = indicators.get("rsi")
        if rsi is None:
            return False
        min_val = config.get("rsi_min", 30)
        max_val = config.get("rsi_max", 75)
        return min_val <= rsi <= max_val
    
    elif filter_type == "ma":
        period = config.get("ma_period", "20")
        condition = config.get("ma_condition", "above")
        ma_key = f"ma{period}"
        ma_value = indicators.get(ma_key)
        if not ma_value:
            return False
        if condition == "above":
            return current_price > ma_value
        elif condition == "below":
            return current_price < ma_value
        elif condition == "up":
            return indicators.get(f"{ma_key}_trend") in ["向上", "up"]
        return True
    
    elif filter_type == "ema":
        period = config.get("ema_period", "12")
        condition = config.get("ema_condition", "above")
        ema_key = f"ema{period}"
        ema_value = indicators.get(ema_key)
        if not ema_value:
            return False
        if condition == "above":
            return current_price > ema_value
        elif condition == "golden":
            ema12 = indicators.get("ema12")
            ema26 = indicators.get("ema26")
            return ema12 and ema26 and ema12 > ema26
        return True
    
    elif filter_type == "macd":
        condition = config.get("macd_condition", "golden")
        macd_dif = indicators.get("macd_dif")
        macd_dea = indicators.get("macd_dea")
        if macd_dif is None or macd_dea is None:
            return False
        if condition == "golden":
            return macd_dif > macd_dea
        elif condition == "dead":
            return macd_dif < macd_dea
        elif condition == "above_zero":
            return macd_dif > 0
        elif condition == "below_zero":
            return macd_dif < 0
        return True
    
    elif filter_type == "kdj":
        condition = config.get("kdj_condition", "golden")
        kdj_k = indicators.get("kdj_k")
        kdj_d = indicators.get("kdj_d")
        if kdj_k is None or kdj_d is None:
            return False
        if condition == "golden":
            return kdj_k > kdj_d
        elif condition == "dead":
            return kdj_k < kdj_d
        elif condition == "oversold":
            return kdj_k < 20
        elif condition == "overbought":
            return kdj_k > 80
        return True
    
    elif filter_type == "bias":
        bias = indicators.get("bias")
        if bias is None:
            return False
        min_val = config.get("bias_min", -6)
        max_val = config.get("bias_max", 6)
        return min_val <= bias <= max_val
    
    elif filter_type == "williams_r":
        wr = indicators.get("williams_r")
        if wr is None:
            return False
        return wr > -80
    
    elif filter_type == "break_high":
        return indicators.get("break_high_20d", False)
    
    elif filter_type == "boll":
        condition = config.get("boll_condition", "expanding")
        if condition == "expanding":
            return indicators.get("boll_expanding", False)
        elif condition == "above_mid":
            boll_middle = indicators.get("boll_middle")
            return boll_middle and current_price > boll_middle
        return True
    
    elif filter_type == "adx":
        adx = indicators.get("adx")
        if adx is None:
            return False
        min_val = config.get("adx_min", 25)
        return adx >= min_val
    
    elif filter_type == "ichimoku":
        condition = config.get("ichimoku_condition", "above_cloud")
        senkou_a = indicators.get("ichimoku_senkou_a")
        senkou_b = indicators.get("ichimoku_senkou_b")
        if senkou_a is None or senkou_b is None:
            return False
        cloud_top = max(senkou_a, senkou_b)
        if condition == "above_cloud":
            return current_price > cloud_top
        return True
    
    return True


@api_router.get("/strategy/selected")
async def get_selected_stocks_api():
    """获取上次选股结果"""
    try:
        from strategy.selector import get_selected_stocks
        result = get_selected_stocks()
        return {"code": 0, "data": result, "message": "success"}
    except Exception as e:
        logger.error(f"获取选股结果失败: {e}", exc_info=True)
        return {"code": 1, "data": {}, "message": str(e)}


@api_router.get("/strategy/batch-compute-indicators")
async def batch_compute_indicators_api(
    market: str = Query("A", description="市场类型：A（A股）或HK（港股）"),
    max_count: int = Query(1000, description="最多计算的股票数量"),
    incremental: bool = Query(True, description="是否增量更新（只计算当日数据有变化的股票）")
):
    """批量计算并缓存技术指标（用于收盘后预计算，减少盘中计算压力）
    
    支持增量更新：只计算当日数据有变化的股票，大幅减少计算量
    """
    try:
        from strategy.indicator_batch import batch_compute_indicators
        result = batch_compute_indicators(market, max_count, incremental)
        return {"code": 0, "data": result, "message": "success"}
    except Exception as e:
        logger.error(f"批量计算指标失败: {e}", exc_info=True)
        return {"code": 1, "data": {}, "message": str(e)}


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
async def analyze_stock_api(
    code: str,
    use_ai: bool = Query(
        True,
        description="是否使用AI模型（已固定为True，仅为兼容旧前端保留）",
    ),
):
    """分析股票"""
    try:
        # 获取配置
        from common.runtime_config import get_runtime_config
        config = get_runtime_config()
        ai_period = config.ai_data_period or "daily"
        ai_count = config.ai_data_count or 500
        
        # 获取股票行情
        a_stocks = get_json("market:a:spot") or []
        stock = next((s for s in a_stocks if str(s.get("code", "")) == code), None)
        
        if not stock:
            return {"code": 1, "data": {}, "message": "股票不存在"}
        
        # 根据配置获取K线数据
        kline_data = fetch_a_stock_kline(code, period=ai_period)
        if not kline_data:
            return {"code": 1, "data": {}, "message": "K线数据不足"}
        
        # 限制数据数量为配置的根数（取最新的N根）
        if len(kline_data) > ai_count:
            kline_data = kline_data[-ai_count:]
        
        if len(kline_data) < 20:
            return {"code": 1, "data": {}, "message": f"K线数据不足（当前{len(kline_data)}根，需要至少20根）"}
        
        df = pd.DataFrame(kline_data)
        indicators = calculate_all_indicators(df)
        
        # 更新动态参数优化器的市场状态
        try:
            from ai.parameter_optimizer import get_parameter_optimizer
            optimizer = get_parameter_optimizer()
            optimizer.update_market_status(indicators, df)
        except Exception as e:
            logger.debug(f"更新市场状态失败: {e}")
        
        # AI分析（统一使用AI模型，包含交易点位）
        analysis = analyze_stock(stock, indicators, None, True, include_trading_points=True)
        
        # 如果AI返回买入信号且有交易点位，自动创建交易计划
        plan_id = None
        if analysis.get("signal") == "买入" and analysis.get("buy_price"):
            try:
                plan = create_trade_plan(
                    code=code,
                    name=stock.get("name", ""),
                    buy_price=analysis["buy_price"],
                    sell_price=analysis["sell_price"],
                    stop_loss=analysis["stop_loss"],
                    confidence=analysis.get("confidence", 0) / 100.0,
                    reason=analysis.get("reason", "AI分析")
                )
                plan_id = plan["id"]
                analysis["plan_id"] = plan_id
            except Exception as e:
                logger.warning(f"创建交易计划失败 {code}: {e}")

        # 持久化单只AI分析结果
        from datetime import datetime

        now = datetime.now().isoformat()
        try:
            existing = get_json(AI_ANALYSIS_KEY)
            if not isinstance(existing, dict):
                existing = {}
        except Exception:
            existing = {}

        existing[code] = {
            "code": code,
            "name": stock.get("name"),
            "analysis": analysis,
            "updated_at": now,
        }
        set_json(AI_ANALYSIS_KEY, existing)

        return {"code": 0, "data": analysis, "message": "success"}
    except Exception as e:
        logger.error(f"分析股票失败 {code}: {e}", exc_info=True)
        return {"code": 1, "data": {}, "message": str(e)}


class AiAnalyzeBatchRequest(BaseModel):
    """批量AI分析请求"""

    codes: List[str]
    use_ai: bool = False  # 保留字段，向后兼容（实际忽略）


class NotifyTestRequest(BaseModel):
    """通知测试请求"""

    channels: Optional[List[str]] = None


async def retry_batch_analysis_with_backoff(
    batch_data: list,
    batch_index: int,
    retry_count: int = 0,
    max_retries: int = 30,
    retry_interval: int = 120  # 2分钟 = 120秒
):
    """异步重试批量分析，每2分钟重试一次，最多30次
    
    Args:
        batch_data: 股票数据列表，每个元素为 (stock, indicators, news) 的元组
        batch_index: 批次索引
        retry_count: 当前重试次数
        max_retries: 最大重试次数
        retry_interval: 重试间隔（秒）
    """
    if retry_count >= max_retries:
        logger.error(f"批量分析第 {batch_index + 1} 批重试{max_retries}次后仍然失败，放弃分析")
        return
    
    try:
        from ai.analyzer import analyze_stocks_batch_with_ai
        from trading.plan import create_trade_plan
        
        logger.info(f"批量分析第 {batch_index + 1} 批，第 {retry_count + 1} 次尝试，共 {len(batch_data)} 支股票")
        
        # 执行批量分析
        batch_results = analyze_stocks_batch_with_ai(
            batch_data,
            include_trading_points=True
        )
        
        # 处理每支股票的分析结果
        for i, analysis in enumerate(batch_results):
            stock, indicators, news = batch_data[i]
            code = stock.get("code", "")
            
            # 如果AI返回买入信号且有交易点位，自动创建交易计划
            if analysis.get("signal") == "买入" and analysis.get("buy_price"):
                try:
                    plan = create_trade_plan(
                        code=code,
                        name=stock.get("name", ""),
                        buy_price=analysis["buy_price"],
                        sell_price=analysis["sell_price"],
                        stop_loss=analysis["stop_loss"],
                        confidence=analysis.get("confidence", 0) / 100.0,
                        reason=analysis.get("reason", f"AI批量分析（第{retry_count + 1}次尝试）")
                    )
                    plan_id = plan["id"]
                    analysis["plan_id"] = plan_id
                    logger.info(f"批量分析成功，已为 {code} 创建交易计划")
                except Exception as e:
                    logger.warning(f"创建交易计划失败 {code}: {e}")
        
        logger.info(f"批量分析第 {batch_index + 1} 批第 {retry_count + 1} 次尝试成功")
        
    except Exception as e:
        logger.warning(f"批量分析第 {batch_index + 1} 批第 {retry_count + 1} 次尝试失败: {e}，将在{retry_interval}秒后重试")
        # 等待后重试
        await asyncio.sleep(retry_interval)
        # 递归调用重试
        await retry_batch_analysis_with_backoff(
            batch_data, batch_index, retry_count + 1, max_retries, retry_interval
        )


@api_router.post("/ai/analyze/batch")
async def analyze_stock_batch_api(
    payload: AiAnalyzeBatchRequest,
    background_tasks: BackgroundTasks,
    notify: bool = Query(
        False, description="是否根据 AI 配置发送通知（Telegram/邮箱/企业微信）"
    ),
):
    """批量分析多只股票（用于自选股自动/手动批量分析）

    - 始终返回逐只股票的分析结果；
    - 当 notify=True 时，会根据 AI 配置中的通知开关发送一条汇总通知。
    """
    try:
        raw_codes = payload.codes or []
        codes = [str(c).strip() for c in raw_codes if str(c).strip()]
        if not codes:
            return {"code": 1, "data": [], "message": "股票代码列表不能为空"}

        # 获取A股行情快照并构建索引
        a_stocks = get_json("market:a:spot") or []
        stock_map = {
            str(s.get("code", "")): s for s in a_stocks if s.get("code") is not None
        }

        results: List[Dict[str, Any]] = []

        # 获取配置
        from common.runtime_config import get_runtime_config
        config = get_runtime_config()
        ai_period = config.ai_data_period or "daily"
        ai_count = config.ai_data_count or 500
        ai_batch_size = config.ai_batch_size or 5
        
        # 第一步：收集所有股票的数据
        stocks_data_list: List[Tuple[dict, dict, list]] = []  # (stock, indicators, news)
        stocks_info_map: Dict[str, Dict[str, Any]] = {}  # code -> {stock, kline_data}
        
        for code in codes:
            stock = stock_map.get(code)
            if not stock:
                results.append({
                    "code": code,
                    "name": None,
                    "success": False,
                    "message": "股票不存在",
                    "analysis": None,
                })
                continue

            try:
                # 获取K线数据
                kline_data = fetch_a_stock_kline(code, period=ai_period)
                if not kline_data:
                    results.append({
                        "code": code,
                        "name": stock.get("name"),
                        "success": False,
                        "message": "K线数据不足",
                        "analysis": None,
                    })
                    continue
                
                # 限制数据数量
                if len(kline_data) > ai_count:
                    kline_data = kline_data[-ai_count:]
                
                if len(kline_data) < 20:
                    results.append({
                        "code": code,
                        "name": stock.get("name"),
                        "success": False,
                        "message": f"K线数据不足（当前{len(kline_data)}根，需要至少20根）",
                        "analysis": None,
                    })
                    continue

                df = pd.DataFrame(kline_data)
                indicators = calculate_all_indicators(df)
                
                # 更新动态参数优化器的市场状态（使用第一只股票的数据）
                if len(stocks_data_list) == 0:
                    try:
                        from ai.parameter_optimizer import get_parameter_optimizer
                        optimizer = get_parameter_optimizer()
                        optimizer.update_market_status(indicators, df)
                    except Exception as e:
                        logger.debug(f"更新市场状态失败: {e}")
                
                # 保存数据，准备批量分析
                stocks_data_list.append((stock, indicators, None))  # news暂时为None
                stocks_info_map[code] = {
                    "stock": stock,
                    "kline_data": kline_data,
                }
                
            except Exception as e:
                logger.error(f"准备股票数据失败 {code}: {e}", exc_info=True)
                results.append({
                    "code": code,
                    "name": stock.get("name") if stock else None,
                    "success": False,
                    "message": str(e),
                    "analysis": None,
                })
        
        # 第二步：按批次进行批量分析（使用线程池异步执行，不阻塞事件循环）
        from ai.analyzer import analyze_stocks_batch_with_ai
        import asyncio
        
        for batch_start in range(0, len(stocks_data_list), ai_batch_size):
            batch_end = min(batch_start + ai_batch_size, len(stocks_data_list))
            batch_data = stocks_data_list[batch_start:batch_end]
            batch_index = batch_start // ai_batch_size
            
            try:
                logger.info(f"批量分析第 {batch_index + 1} 批，共 {len(batch_data)} 支股票（线程池异步执行）")
                
                # 使用 asyncio.to_thread 在线程池中执行同步的 AI 分析，避免阻塞事件循环
                batch_results = await asyncio.to_thread(
                    analyze_stocks_batch_with_ai,
                    batch_data,
                    True  # include_trading_points
                )
                
                # 处理每支股票的分析结果
                for i, analysis in enumerate(batch_results):
                    stock, indicators, news = batch_data[i]
                    code = stock.get("code", "")
                    
                    # 如果AI返回买入信号且有交易点位，自动创建交易计划
                    plan_id = None
                    if analysis.get("signal") == "买入" and analysis.get("buy_price"):
                        try:
                            plan = create_trade_plan(
                                code=code,
                                name=stock.get("name", ""),
                                buy_price=analysis["buy_price"],
                                sell_price=analysis["sell_price"],
                                stop_loss=analysis["stop_loss"],
                                confidence=analysis.get("confidence", 0) / 100.0,
                                reason=analysis.get("reason", "AI批量分析")
                            )
                            plan_id = plan["id"]
                            analysis["plan_id"] = plan_id
                        except Exception as e:
                            logger.warning(f"创建交易计划失败 {code}: {e}")
                    
                    results.append({
                        "code": code,
                        "name": stock.get("name"),
                        "success": True,
                        "message": "success",
                        "analysis": analysis,
                        "plan_id": plan_id,
                    })
                    
            except Exception as e:
                logger.error(f"批量分析第 {batch_index + 1} 批失败: {e}，将启动后台重试任务", exc_info=True)
                # 失败后启动后台异步重试任务，不阻塞主流程
                background_tasks.add_task(
                    retry_batch_analysis_with_backoff,
                    batch_data,
                    batch_index,
                    retry_count=0,
                    max_retries=30,
                    retry_interval=120  # 2分钟
                )
                # 记录失败结果（但不进行降级分析）
                for stock, indicators, news in batch_data:
                    code = stock.get("code", "")
                    results.append({
                        "code": code,
                        "name": stock.get("name"),
                        "success": False,
                        "message": f"批量分析失败，已启动后台重试任务（最多重试30次，每2分钟一次）",
                        "analysis": None,
                    })

        # 持久化成功的AI分析结果
        try:
            from datetime import datetime

            now = datetime.now().isoformat()
            existing = get_json(AI_ANALYSIS_KEY)
            if not isinstance(existing, dict):
                existing = {}

            for item in results:
                if not item.get("success") or not item.get("analysis"):
                    continue
                code = str(item.get("code") or "").strip()
                if not code:
                    continue
                existing[code] = {
                    "code": code,
                    "name": item.get("name"),
                    "analysis": item.get("analysis"),
                    "updated_at": now,
                }

            set_json(AI_ANALYSIS_KEY, existing)
        except Exception as e:
            logger.error(f"保存AI分析结果到Redis失败: {e}", exc_info=True)

        # 根据 AI 通知配置发送汇总通知
        if notify:
            try:
                cfg = get_runtime_config()
                channels: List[str] = []
                if getattr(cfg, "ai_notify_telegram", False):
                    channels.append("telegram")
                if getattr(cfg, "ai_notify_email", False):
                    channels.append("email")
                if getattr(cfg, "ai_notify_wechat", False):
                    channels.append("wechat")

                if channels:
                    # 构造简要汇总消息
                    success_items = [
                        item
                        for item in results
                        if item.get("success") and item.get("analysis")
                    ]
                    total = len(results)
                    success_count = len(success_items)
                    failed_count = total - success_count

                    lines = []
                    lines.append("🤖 AI分析结果通知")
                    lines.append("")
                    lines.append(f"本次共分析自选股票 {total} 只：")
                    lines.append(f"- 成功：{success_count} 只")
                    lines.append(f"- 失败/数据不足：{failed_count} 只")
                    lines.append("")

                    # 选取前若干只重点股票（按score降序）
                    top_items = sorted(
                        success_items,
                        key=lambda x: x.get("analysis", {}).get("score", 0),
                        reverse=True,
                    )[:10]

                    if top_items:
                        lines.append("重点关注（按评分从高到低）：")
                        for idx, item in enumerate(top_items, 1):
                            a = item.get("analysis", {}) or {}
                            code = item.get("code", "")
                            name = item.get("name", "") or ""
                            trend = a.get("trend", "未知")
                            risk = a.get("risk", "未知")
                            score = a.get("score", 0)
                            advice = a.get("advice", "")
                            lines.append(
                                f"{idx}. {name} ({code}) - 评分:{score} 趋势:{trend} 风险:{risk}"
                            )
                            if advice:
                                lines.append(f"   建议：{advice}")
                    else:
                        lines.append("暂无成功分析的股票。")

                    message = "\n".join(lines)
                    notify_message(message, channels=channels)
            except Exception as e:
                logger.error(f"发送 AI 分析通知失败: {e}", exc_info=True)

        return {"code": 0, "data": results, "message": "success"}
    except Exception as e:
        logger.error(f"批量分析股票失败: {e}", exc_info=True)
        return {"code": 1, "data": [], "message": str(e)}


@api_router.get("/ai/analysis")
async def get_ai_analysis_history():
    """获取已持久化的AI分析结果列表"""
    try:
        data = get_json(AI_ANALYSIS_KEY) or {}
        if not isinstance(data, dict):
            data = {}

        items: List[Dict[str, Any]] = []
        for code, item in data.items():
            if not isinstance(item, dict):
                continue
            items.append(
                {
                    "code": item.get("code") or code,
                    "name": item.get("name"),
                    "success": True,
                    "message": "from_cache",
                    "analysis": item.get("analysis") or {},
                    "updated_at": item.get("updated_at"),
                }
            )

        # 按更新时间降序
        items.sort(key=lambda x: x.get("updated_at") or "", reverse=True)

        return {"code": 0, "data": items, "message": "success"}
    except Exception as e:
        logger.error(f"获取AI分析历史失败: {e}", exc_info=True)
        return {"code": 1, "data": [], "message": str(e)}


@api_router.post("/ai/analysis/clear")
async def clear_ai_analysis_history():
    """清除所有已持久化的AI分析结果"""
    try:
        delete(AI_ANALYSIS_KEY)
        return {"code": 0, "data": {}, "message": "AI分析结果已清除"}
    except Exception as e:
        logger.error(f"清除AI分析历史失败: {e}", exc_info=True)
        return {"code": 1, "data": {}, "message": str(e)}


@api_router.post("/notify/test", dependencies=admin_dependencies)
async def notify_test_api(payload: NotifyTestRequest):
    """测试通知渠道是否配置正确

    - 前端会根据勾选的通知渠道传入 channels；
    - 若 channels 为空，则使用运行时配置中的 notify_channels。
    """
    try:
        channels = payload.channels or []
        if not channels:
            cfg = get_runtime_config()
            channels = cfg.notify_channels or []

        if not channels:
            return {
                "code": 1,
                "data": {},
                "message": "没有可用的通知渠道，请先在通知配置中勾选至少一个渠道",
            }

        message_lines = [
            "📢 通知渠道配置测试",
            "",
            "如果你收到这条消息，说明该渠道配置成功可用。",
        ]
        message = "\n".join(message_lines)

        results = notify_message(message, channels=channels)
        return {
            "code": 0,
            "data": results,
            "message": "success",
        }
    except Exception as e:
        logger.error(f"测试通知失败: {e}", exc_info=True)
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


class TradePlanRequest(BaseModel):
    """交易计划请求"""
    code: str
    name: str
    buy_price: float
    sell_price: float
    stop_loss: float
    confidence: float
    reason: str


class CloseTradeRequest(BaseModel):
    """关闭交易请求"""
    plan_id: int
    outcome: str  # win / loss
    entry_date: str
    exit_date: str
    entry_price: float
    exit_price: float
    profit_pct: float


@api_router.post("/trading/plan")
async def create_trade_plan_api(payload: TradePlanRequest):
    """创建交易计划"""
    try:
        plan = create_trade_plan(
            code=payload.code,
            name=payload.name,
            buy_price=payload.buy_price,
            sell_price=payload.sell_price,
            stop_loss=payload.stop_loss,
            confidence=payload.confidence,
            reason=payload.reason
        )
        return {"code": 0, "data": plan, "message": "success"}
    except Exception as e:
        logger.error(f"创建交易计划失败: {e}", exc_info=True)
        return {"code": 1, "data": {}, "message": str(e)}


@api_router.get("/trading/plan/pending")
async def get_pending_plans_api(code: Optional[str] = Query(None)):
    """获取待执行的交易计划（兼容旧接口）"""
    try:
        plans = get_pending_plans(code=code)
        return {"code": 0, "data": plans, "message": "success"}
    except Exception as e:
        logger.error(f"获取交易计划失败: {e}", exc_info=True)
        return {"code": 1, "data": [], "message": str(e)}


@api_router.get("/trading/plan/active")
async def get_active_plans_api(code: Optional[str] = Query(None)):
    """获取活跃的交易计划（等待买入或已买入）"""
    try:
        plans = get_active_plans(code=code)
        return {"code": 0, "data": plans, "message": "success"}
    except Exception as e:
        logger.error(f"获取交易计划失败: {e}", exc_info=True)
        return {"code": 1, "data": [], "message": str(e)}


@api_router.post("/trading/plan/check/spot", dependencies=admin_dependencies)
async def check_trade_plans_spot_api():
    """根据实时行情价格检查交易计划（手动触发）"""
    try:
        result = check_trade_plans_by_spot_price()
        return {"code": 0, "data": result, "message": "success"}
    except Exception as e:
        logger.error(f"检查交易计划失败: {e}", exc_info=True)
        return {"code": 1, "data": {}, "message": str(e)}


@api_router.post("/market/spot/collect")
async def collect_spot_data_api(
    background_tasks: BackgroundTasks,
):
    """手动触发行情数据采集（实时行情）
    
    说明：
    - 采集A股和港股的实时行情数据到Redis
    - 后台异步执行，避免阻塞
    - 主要用于新部署环境初始化或手动刷新数据
    """
    try:
        from market_collector.scheduler import collect_job
        
        def run_collect():
            try:
                collect_job()
            except Exception as e:
                logger.error(f"行情数据采集失败: {e}", exc_info=True)
        
        # 后台异步执行
        background_tasks.add_task(run_collect)
        
        return {
            "code": 0,
            "data": {"status": "started"},
            "message": "行情数据采集任务已启动，请稍后查看结果"
        }
    except Exception as e:
        logger.error(f"触发行情数据采集失败: {e}", exc_info=True)
        return {"code": 1, "data": {}, "message": str(e)}


def _collect_market_kline_internal(market: str, all_stocks: List[Dict], fetch_kline_func, max_count: int):
    """内部函数：采集单个市场的K线数据
    
    Args:
        market: 市场类型 "A" 或 "HK"
        all_stocks: 股票列表
        fetch_kline_func: 采集K线数据的函数
        max_count: 最多采集的股票数量
    """
    from common.db import get_stock_list_from_db
    from market.service.ws import kline_collect_progress
    from datetime import datetime
    import uuid
    from concurrent.futures import ThreadPoolExecutor
    
    # 按成交额排序，优先采集活跃股票
    sorted_stocks = sorted(all_stocks, key=lambda x: x.get("amount", 0) or 0, reverse=True)
    
    # 检查哪些股票在数据库中还没有数据，优先采集这些股票
    try:
        db_stocks = get_stock_list_from_db(market.upper())
        db_codes = {s.get("code") for s in db_stocks} if db_stocks else set()
        
        # 分离：有数据的股票和没有数据的股票
        stocks_with_data = [s for s in sorted_stocks if s.get("code") in db_codes]
        stocks_without_data = [s for s in sorted_stocks if s.get("code") not in db_codes]
        
        # 优先采集没有数据的股票，然后是有数据的股票（用于增量更新）
        target_stocks = stocks_without_data[:max_count]
        remaining_slots = max_count - len(target_stocks)
        if remaining_slots > 0:
            target_stocks.extend(stocks_with_data[:remaining_slots])
        
        logger.info(f"[{market}]采集策略：无数据股票={len(stocks_without_data)}只，已有数据股票={len(stocks_with_data)}只，目标采集={len(target_stocks)}只")
    except Exception as e:
        logger.warning(f"[{market}]检查数据库股票列表失败，使用默认策略: {e}")
        target_stocks = sorted_stocks[:max_count]
    
    if not target_stocks:
        logger.warning(f"[{market}]没有需要采集的股票")
        return
    
    logger.info(f"[{market}]开始批量采集K线数据，目标股票数={len(target_stocks)}")
    
    # 生成任务ID
    task_id = f"{market}_{str(uuid.uuid4())}"
    start_time = datetime.now().isoformat()
    success_count = 0
    failed_count = 0
    
    # 获取数据源信息
    if market.upper() == "A":
        data_source = "AKShare(主) + Tushare(备用)"
    else:
        data_source = "AKShare"
    
    # 初始化进度
    kline_collect_progress[task_id] = {
        "status": "running",
        "total": len(target_stocks),
        "success": 0,
        "failed": 0,
        "current": 0,
        "message": f"[{market}]开始采集K线数据...",
        "start_time": start_time,
        "progress": 0,
        "market": market,
        "data_source": data_source
    }
    
    def collect_kline_for_stock(stock):
        nonlocal success_count, failed_count
        from market.service.ws import kline_collect_stop_flags
        import threading
        
        # 检查停止标志
        if kline_collect_stop_flags.get(task_id, False):
            return
        
        code = str(stock.get("code", ""))
        if not code:
            return
        
        try:
            kline_data = fetch_kline_func(code, "daily", "", None, None, False, False)
            if kline_data and len(kline_data) > 0:
                success_count += 1
            else:
                failed_count += 1
        except Exception as e:
            failed_count += 1
            # 只记录关键错误，减少日志输出
            if "timeout" not in str(e).lower() and "连接" not in str(e):
                logger.debug(f"[{market}]采集K线数据失败 {code}: {e}")
    
    def batch_collect():
        """同步批量采集函数"""
        from market.service.ws import kline_collect_stop_flags
        import concurrent.futures
        import time
        
        # 动态调整并发数：根据股票数量，但不超过50
        max_workers = min(50, max(10, len(target_stocks) // 20))
        executor = ThreadPoolExecutor(max_workers=max_workers)
        
        # 进度更新计数器（每完成10只股票或每2秒更新一次）
        last_update_time = time.time()
        update_interval = 2  # 2秒更新一次进度
        
        try:
            # 使用线程池并发执行采集任务
            futures = {executor.submit(collect_kline_for_stock, stock): stock for stock in target_stocks}
            
            # 使用 as_completed 更快响应完成的任务
            for future in concurrent.futures.as_completed(futures):
                # 检查停止标志
                if kline_collect_stop_flags.get(task_id, False):
                    logger.info(f"收到停止信号，中断批量采集")
                    # 取消未完成的任务
                    for f in futures:
                        f.cancel()
                    break
                
                try:
                    future.result()
                except Exception as e:
                    logger.debug(f"[{market}]采集任务异常: {e}")
                
                # 批量更新进度（减少更新频率）
                current_time = time.time()
                current = success_count + failed_count
                if (current_time - last_update_time >= update_interval) or (current % 10 == 0):
                    progress_pct = int((current / len(target_stocks)) * 100) if target_stocks else 0
                    if task_id in kline_collect_progress:
                        kline_collect_progress[task_id].update({
                            "success": success_count,
                            "failed": failed_count,
                            "current": current,
                            "progress": progress_pct,
                            "message": f"[{market}]采集中({data_source})... 成功={success_count}，失败={failed_count}，进度={current}/{len(target_stocks)}"
                        })
                    last_update_time = current_time
                    
                    # 每50只股票输出一次日志
                    if current % 50 == 0:
                        logger.info(f"[{market}]K线数据采集进度：成功={success_count}，失败={failed_count}，进度={current}/{len(target_stocks)}")
        except Exception as e:
            end_time = datetime.now().isoformat()
            kline_collect_progress[task_id] = {
                "status": "failed",
                "total": len(target_stocks),
                "success": success_count,
                "failed": failed_count,
                "current": success_count + failed_count,
                "message": f"[{market}]采集异常终止({data_source}): {e}",
                "start_time": start_time,
                "end_time": end_time,
                "progress": int(((success_count + failed_count) / len(target_stocks)) * 100) if target_stocks else 0,
                "market": market,
                "data_source": data_source
            }
            logger.error(f"[{market}]K线采集异常终止: {e}", exc_info=True)
            raise
        finally:
            executor.shutdown(wait=True, cancel_futures=True)
        
        # 更新最终进度（检查是否被停止）
        from market.service.ws import kline_collect_stop_flags
        end_time = datetime.now().isoformat()
        is_cancelled = kline_collect_stop_flags.get(task_id, False)
        current_processed = success_count + failed_count
        
        if is_cancelled:
            kline_collect_progress[task_id] = {
                "status": "cancelled",
                "total": len(target_stocks),
                "success": success_count,
                "failed": failed_count,
                "current": current_processed,
                "message": f"[{market}]K线数据采集已停止({data_source})：成功={success_count}，失败={failed_count}，已处理={current_processed}/{len(target_stocks)}",
                "start_time": start_time,
                "end_time": end_time,
                "progress": int((current_processed / len(target_stocks)) * 100) if target_stocks else 0,
                "market": market,
                "data_source": data_source
            }
            logger.info(f"[{market}]K线数据采集已停止：成功={success_count}，失败={failed_count}，已处理={current_processed}/{len(target_stocks)}")
            # 清理停止标志
            kline_collect_stop_flags.pop(task_id, None)
        else:
            kline_collect_progress[task_id] = {
                "status": "completed",
                "total": len(target_stocks),
                "success": success_count,
                "failed": failed_count,
                "current": len(target_stocks),
                "message": f"[{market}]K线数据采集完成({data_source})：成功={success_count}，失败={failed_count}，总计={len(target_stocks)}",
                "start_time": start_time,
                "end_time": end_time,
                "progress": 100,
                "market": market,
                "data_source": data_source
            }
            logger.info(f"[{market}]K线数据采集完成：成功={success_count}，失败={failed_count}，总计={len(target_stocks)}")
    
    # 运行批量采集
    batch_collect()


@api_router.post("/market/kline/collect/single")
async def collect_single_stock_kline_api(
    code: str = Query(..., description="股票代码（如：600519）"),
    market: str = Query("A", description="市场类型：A（A股）或HK（港股）"),
    period: str = Query("daily", description="周期：daily, weekly, monthly"),
):
    """采集单个股票的K线数据到ClickHouse
    
    说明：
    - 只采集指定股票代码的K线数据
    - 同步执行，立即返回结果
    - 用于测试或单独更新某只股票的数据
    """
    try:
        from market_collector.cn import fetch_a_stock_kline
        from market_collector.hk import fetch_hk_stock_kline
        
        if market.upper() == "HK":
            fetch_kline_func = fetch_hk_stock_kline
        else:
            fetch_kline_func = fetch_a_stock_kline
        
        logger.info(f"开始采集单个股票K线数据：{code}，市场={market}，周期={period}")
        
        # 同步采集（不跳过数据库，保存数据）
        kline_data = fetch_kline_func(code, period, "", None, None, False, False)
        
        if kline_data and len(kline_data) > 0:
            return {
                "code": 0,
                "data": {
                    "stock_code": code,
                    "market": market.upper(),
                    "period": period,
                    "count": len(kline_data),
                    "latest_date": kline_data[-1].get("date") if kline_data else None
                },
                "message": f"成功采集{code}的K线数据，共{len(kline_data)}条"
            }
        else:
            return {
                "code": 1,
                "data": {},
                "message": f"采集{code}的K线数据失败或数据为空"
            }
    except Exception as e:
        logger.error(f"采集单个股票K线数据失败 {code}: {e}", exc_info=True)
        return {"code": 1, "data": {}, "message": f"采集失败: {str(e)}"}


@api_router.post("/market/kline/collect/batch-single")
async def collect_batch_single_stock_kline_api(
    background_tasks: BackgroundTasks,
    batch_size: int = Query(10, ge=1, le=100, description="每次采集的股票数量"),
    market: str = Query("A", description="市场类型：A（A股）、HK（港股）或ALL（同时采集A股和港股）"),
    period: str = Query("daily", description="周期：daily, weekly, monthly"),
):
    """单个批量采集K线数据（使用Redis快照数据列表，循环采集）
    
    说明：
    - 直接从Redis快照（market:a:spot, market:hk:spot）获取股票代码列表
    - 每次采集指定数量的股票
    - 先采集A股，再采集港股
    - 后台异步执行，避免阻塞
    - 如果Redis中没有数据，返回错误
    """
    try:
        from market_collector.cn import fetch_a_stock_kline
        from market_collector.hk import fetch_hk_stock_kline
        from common.redis import set_json
        import uuid
        from datetime import datetime
        from market.service.ws import kline_collect_progress
        
        logger.info(f"开始单个批量采集，每次{batch_size}只，市场={market}")
        
        def collect_batch_internal():
            from market.service.ws import kline_collect_stop_flags
            from common.redis import get_json
            import time
            import concurrent.futures
            from concurrent.futures import ThreadPoolExecutor
            
            task_id = f"batch_single_{str(uuid.uuid4())}"
            start_time = datetime.now().isoformat()
            total_success = 0
            total_failed = 0
            total_processed = 0
            
            # 从Redis快照获取股票代码列表
            a_codes = []
            hk_codes = []
            
            # 获取A股代码列表
            if market.upper() in ["A", "ALL"]:
                try:
                    a_stocks_redis = get_json("market:a:spot") or []
                    if a_stocks_redis:
                        a_codes = [s.get("code") for s in a_stocks_redis if s.get("code")]
                        logger.info(f"从Redis获取A股代码列表：{len(a_codes)}只")
                    else:
                        logger.warning("Redis中A股快照数据为空")
                except Exception as e:
                    logger.error(f"从Redis获取A股代码列表失败: {e}")
            
            # 获取港股代码列表
            if market.upper() in ["HK", "ALL"]:
                try:
                    hk_stocks_redis = get_json("market:hk:spot") or []
                    if hk_stocks_redis:
                        hk_codes = [s.get("code") for s in hk_stocks_redis if s.get("code")]
                        logger.info(f"从Redis获取港股代码列表：{len(hk_codes)}只")
                    else:
                        logger.warning("Redis中港股快照数据为空")
                except Exception as e:
                    logger.error(f"从Redis获取港股代码列表失败: {e}")
            
            total_stocks = len(a_codes) + len(hk_codes)
            
            if total_stocks == 0:
                logger.error("未获取到任何股票代码列表")
                kline_collect_progress[task_id] = {
                    "status": "failed",
                    "total": 0,
                    "success": 0,
                    "failed": 0,
                    "current": 0,
                    "message": "未获取到股票代码列表",
                    "start_time": start_time,
                    "end_time": datetime.now().isoformat(),
                    "progress": 0
                }
                return
            
            # 确定数据源信息
            data_source_a = "AKShare(主) + Tushare(备用)"
            data_source_hk = "AKShare"
            if market.upper() == "A":
                data_source = data_source_a
            elif market.upper() == "HK":
                data_source = data_source_hk
            else:
                data_source = f"A股:{data_source_a}, 港股:{data_source_hk}"
            
            # 初始化进度
            kline_collect_progress[task_id] = {
                "status": "running",
                "total": total_stocks,
                "success": 0,
                "failed": 0,
                "current": 0,
                "message": f"开始采集({data_source})，A股{len(a_codes)}只，港股{len(hk_codes)}只",
                "start_time": start_time,
                "progress": 0,
                "data_source": data_source
            }
            
            # 采集A股（使用线程池并发处理，避免单只股票阻塞）
            if market.upper() in ["A", "ALL"] and a_codes:
                logger.info(f"开始采集A股，共{len(a_codes)}只，每次{batch_size}只")
                
                # 使用线程池并发处理，避免单只股票阻塞整个流程
                # batch_size=1时使用较小的并发数（2-3个），避免ClickHouse连接过多
                max_workers = max(2, min(batch_size, 5)) if batch_size == 1 else min(batch_size, 10)
                executor = ThreadPoolExecutor(max_workers=max_workers)
                
                def collect_a_stock(code):
                    """采集单只A股（带超时控制）"""
                    nonlocal total_success, total_failed, total_processed
                    try:
                        # 使用线程池包装，添加超时控制（每只股票最多120秒）
                        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as single_executor:
                            future = single_executor.submit(
                                fetch_a_stock_kline, code, period, "", None, None, False, False
                            )
                            try:
                                result = future.result(timeout=120)  # 120秒超时
                                if result and len(result) > 0:
                                    total_success += 1
                                else:
                                    total_failed += 1
                            except concurrent.futures.TimeoutError:
                                logger.warning(f"A股采集超时 {code}（120秒），跳过")
                                total_failed += 1
                            except Exception as e:
                                total_failed += 1
                                logger.debug(f"A股采集失败 {code}: {e}")
                    except Exception as e:
                        total_failed += 1
                        logger.debug(f"A股采集异常 {code}: {e}")
                    finally:
                        total_processed += 1
                
                try:
                    for i in range(0, len(a_codes), batch_size):
                        # 检查停止标志
                        if kline_collect_stop_flags.get(task_id, False):
                            logger.info(f"收到停止信号，中断A股采集")
                            break
                        
                        batch = a_codes[i:i+batch_size]
                        logger.info(f"采集A股批次 {i//batch_size + 1}/{(len(a_codes)-1)//batch_size + 1}，本批次{len(batch)}只")
                        
                        # 提交批次任务到线程池
                        futures = [executor.submit(collect_a_stock, code) for code in batch]
                        
                        # 等待批次完成，并更新进度
                        for future in concurrent.futures.as_completed(futures):
                            try:
                                future.result()  # 获取结果（异常已在函数内处理）
                            except Exception as e:
                                logger.debug(f"A股批次任务异常: {e}")
                            
                            # 检查停止标志
                            if kline_collect_stop_flags.get(task_id, False):
                                logger.info(f"收到停止信号，中断A股采集")
                                # 取消未完成的任务
                                for f in futures:
                                    f.cancel()
                                break
                            
                            # 更新进度（每完成一只股票更新一次）
                            progress_pct = int((total_processed / total_stocks) * 100) if total_stocks > 0 else 0
                            kline_collect_progress[task_id].update({
                                "success": total_success,
                                "failed": total_failed,
                                "current": total_processed,
                                "progress": progress_pct,
                                "message": f"A股采集中({data_source_a})... 已处理{total_processed}/{total_stocks}，成功{total_success}，失败{total_failed}"
                            })
                        
                        # 批次间延迟，避免请求过快
                        if not kline_collect_stop_flags.get(task_id, False):
                            time.sleep(1)
                finally:
                    executor.shutdown(wait=True, cancel_futures=True)
            
            # 采集港股（只在A股采集完成且未停止时进行，使用线程池并发处理）
            if not kline_collect_stop_flags.get(task_id, False) and market.upper() in ["HK", "ALL"] and hk_codes:
                logger.info(f"开始采集港股，共{len(hk_codes)}只，每次{batch_size}只")
                
                # 使用线程池并发处理，避免单只股票阻塞整个流程
                # batch_size=1时使用较小的并发数（2-3个），避免ClickHouse连接过多
                max_workers = max(2, min(batch_size, 5)) if batch_size == 1 else min(batch_size, 10)
                executor = ThreadPoolExecutor(max_workers=max_workers)
                
                def collect_hk_stock(code):
                    """采集单只港股（带超时控制）"""
                    nonlocal total_success, total_failed, total_processed
                    try:
                        # 使用线程池包装，添加超时控制（每只股票最多120秒）
                        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as single_executor:
                            future = single_executor.submit(
                                fetch_hk_stock_kline, code, period, "", None, None, False, False
                            )
                            try:
                                result = future.result(timeout=120)  # 120秒超时
                                if result and len(result) > 0:
                                    total_success += 1
                                else:
                                    total_failed += 1
                            except concurrent.futures.TimeoutError:
                                logger.warning(f"港股采集超时 {code}（120秒），跳过")
                                total_failed += 1
                            except Exception as e:
                                total_failed += 1
                                logger.debug(f"港股采集失败 {code}: {e}")
                    except Exception as e:
                        total_failed += 1
                        logger.debug(f"港股采集异常 {code}: {e}")
                    finally:
                        total_processed += 1
                
                try:
                    for i in range(0, len(hk_codes), batch_size):
                        # 检查停止标志
                        if kline_collect_stop_flags.get(task_id, False):
                            logger.info(f"收到停止信号，中断港股采集")
                            break
                        
                        batch = hk_codes[i:i+batch_size]
                        logger.info(f"采集港股批次 {i//batch_size + 1}/{(len(hk_codes)-1)//batch_size + 1}，本批次{len(batch)}只")
                        
                        # 提交批次任务到线程池
                        futures = [executor.submit(collect_hk_stock, code) for code in batch]
                        
                        # 等待批次完成，并更新进度
                        for future in concurrent.futures.as_completed(futures):
                            try:
                                future.result()  # 获取结果（异常已在函数内处理）
                            except Exception as e:
                                logger.debug(f"港股批次任务异常: {e}")
                            
                            # 检查停止标志
                            if kline_collect_stop_flags.get(task_id, False):
                                logger.info(f"收到停止信号，中断港股采集")
                                # 取消未完成的任务
                                for f in futures:
                                    f.cancel()
                                break
                            
                            # 更新进度（每完成一只股票更新一次）
                            progress_pct = int((total_processed / total_stocks) * 100) if total_stocks > 0 else 0
                            kline_collect_progress[task_id].update({
                                "success": total_success,
                                "failed": total_failed,
                                "current": total_processed,
                                "progress": progress_pct,
                                "message": f"港股采集中({data_source_hk})... 已处理{total_processed}/{total_stocks}，成功{total_success}，失败{total_failed}"
                            })
                        
                        # 批次间延迟
                        if not kline_collect_stop_flags.get(task_id, False):
                            time.sleep(1)
                finally:
                    executor.shutdown(wait=True, cancel_futures=True)
            
            # 完成（检查是否被停止）
            end_time = datetime.now().isoformat()
            is_cancelled = kline_collect_stop_flags.get(task_id, False)
            
            if is_cancelled:
                kline_collect_progress[task_id] = {
                    "status": "cancelled",
                    "total": total_stocks,
                    "success": total_success,
                    "failed": total_failed,
                    "current": total_processed,
                    "message": f"采集已停止({data_source})！成功{total_success}，失败{total_failed}，已处理{total_processed}/{total_stocks}",
                    "start_time": start_time,
                    "end_time": end_time,
                    "progress": int((total_processed / total_stocks) * 100) if total_stocks > 0 else 0,
                    "data_source": data_source
                }
                logger.info(f"单个批量采集已停止：成功{total_success}，失败{total_failed}，已处理{total_processed}/{total_stocks}")
                # 清理停止标志
                kline_collect_stop_flags.pop(task_id, None)
            else:
                kline_collect_progress[task_id] = {
                    "status": "completed",
                    "total": total_stocks,
                    "success": total_success,
                    "failed": total_failed,
                    "current": total_processed,
                    "message": f"采集完成({data_source})！成功{total_success}，失败{total_failed}，总计{total_processed}",
                    "start_time": start_time,
                    "end_time": end_time,
                    "progress": 100,
                    "data_source": data_source
                }
                logger.info(f"单个批量采集完成：成功{total_success}，失败{total_failed}，总计{total_processed}")
        
        # 后台执行
        background_tasks.add_task(collect_batch_internal)
        
        return {
            "code": 0,
            "data": {
                "status": "started",
                "market": market.upper(),
                "batch_size": batch_size
            },
            "message": f"已开始单个批量采集任务，每次采集{batch_size}只股票"
        }
        
    except Exception as e:
        logger.error(f"单个批量采集失败: {e}", exc_info=True)
        return {"code": 1, "data": {}, "message": f"启动失败: {str(e)}"}


@api_router.post("/market/kline/collect/stop")
async def stop_kline_collect_api():
    """停止正在进行的K线采集任务
    
    说明：
    - 停止当前最新的采集任务（单个批量采集或批量采集）
    - 通过设置停止标志来中断采集循环
    """
    try:
        from market.service.ws import kline_collect_progress, kline_collect_stop_flags
        from datetime import datetime
        
        # 找到最新的运行中的任务
        running_tasks = [
            (task_id, progress) 
            for task_id, progress in kline_collect_progress.items()
            if progress.get("status") == "running"
        ]
        
        if not running_tasks:
            return {
                "code": 1,
                "data": {},
                "message": "没有正在运行的采集任务"
            }
        
        # 获取最新的任务（按start_time排序）
        latest_task = max(running_tasks, key=lambda x: x[1].get("start_time", ""))
        task_id, progress = latest_task
        
        # 设置停止标志
        kline_collect_stop_flags[task_id] = True
        
        # 更新任务状态为取消
        kline_collect_progress[task_id].update({
            "status": "cancelled",
            "message": "用户手动停止采集任务",
            "end_time": datetime.now().isoformat()
        })
        
        logger.info(f"用户停止K线采集任务: {task_id}")
        
        return {
            "code": 0,
            "data": {
                "task_id": task_id,
                "status": "stopped"
            },
            "message": "已发送停止信号，采集任务将在下一个循环停止"
        }
        
    except Exception as e:
        logger.error(f"停止K线采集失败: {e}", exc_info=True)
        return {"code": 1, "data": {}, "message": f"停止失败: {str(e)}"}


@api_router.post("/market/kline/collect")
async def collect_kline_data_api(
    background_tasks: BackgroundTasks,
    market: str = Query("A", description="市场类型：A（A股）、HK（港股）或ALL（同时采集A股和港股）"),
    max_count: int = Query(6000, ge=1, le=10000, description="最多采集的股票数量，默认6000（可覆盖全部A股）"),
):
    """批量采集K线数据到ClickHouse（手动触发）
    
    说明：
    - 从Redis获取股票列表
    - 批量采集每只股票的K线数据并保存到ClickHouse
    - 后台异步执行，避免阻塞
    - market参数支持"A"、"HK"或"ALL"（同时采集A股和港股）
    """
    try:
        from common.redis import get_json
        from market_collector.cn import fetch_a_stock_kline, fetch_a_stock_spot
        from market_collector.hk import fetch_hk_stock_kline
        
        # 支持同时采集A股和港股
        if market.upper() == "ALL":
            # 同时采集A股和港股，创建两个后台任务
            def collect_all_markets():
                from common.db import save_stock_info_batch
                # 先采集A股
                try:
                    a_stocks = get_json("market:a:spot") or []
                    if not a_stocks:
                        logger.info("检测到A股行情数据为空，先执行一次行情采集...")
                        fetch_a_stock_spot()
                        a_stocks = get_json("market:a:spot") or []
                    
                    if a_stocks:
                        # 保存股票基本信息
                        try:
                            save_stock_info_batch(a_stocks, "A")
                        except Exception as e:
                            logger.warning(f"保存A股基本信息失败: {e}")
                        
                        logger.info(f"开始采集A股K线数据，共{len(a_stocks)}只股票")
                        _collect_market_kline_internal("A", a_stocks, fetch_a_stock_kline, max_count)
                except Exception as e:
                    logger.error(f"A股K线采集失败: {e}", exc_info=True)
                
                # 再采集港股
                try:
                    hk_stocks = get_json("market:hk:spot") or []
                    if hk_stocks:
                        # 保存股票基本信息
                        try:
                            save_stock_info_batch(hk_stocks, "HK")
                        except Exception as e:
                            logger.warning(f"保存港股基本信息失败: {e}")
                        
                        logger.info(f"开始采集港股K线数据，共{len(hk_stocks)}只股票")
                        _collect_market_kline_internal("HK", hk_stocks, fetch_hk_stock_kline, max_count)
                except Exception as e:
                    logger.error(f"港股K线采集失败: {e}", exc_info=True)
            
            background_tasks.add_task(collect_all_markets)
            return {
                "code": 0,
                "data": {"status": "started", "market": "ALL"},
                "message": "已开始后台同时采集A股和港股的K线数据，请稍后查看结果"
            }
        
        # 获取股票列表（单个市场）
        if market.upper() == "HK":
            all_stocks = get_json("market:hk:spot") or []
            fetch_kline_func = fetch_hk_stock_kline
            fetch_spot_func = None  # 港股暂时不自动采集
        else:
            all_stocks = get_json("market:a:spot") or []
            fetch_kline_func = fetch_a_stock_kline
            fetch_spot_func = fetch_a_stock_spot
        
        # 如果没有数据，尝试先采集一次行情数据（仅A股）
        if not all_stocks and fetch_spot_func:
            logger.info(f"检测到{market}股行情数据为空，先执行一次行情采集...")
            try:
                fetch_spot_func()
                # 重新获取数据
                all_stocks = get_json("market:a:spot") or []
            except Exception as e:
                logger.warning(f"自动采集行情数据失败: {e}")
        
        if not all_stocks:
            return {
                "code": 1,
                "data": {},
                "message": f"未获取到{market}股行情数据。请先调用 /api/market/spot/collect 接口采集行情数据，或等待行情采集程序自动采集"
            }
        
        # 保存股票基本信息到数据库
        try:
            from common.db import save_stock_info_batch
            save_stock_info_batch(all_stocks, market.upper())
        except Exception as e:
            logger.warning(f"保存股票基本信息失败: {e}")
        
        # 按成交额排序，优先采集活跃股票
        sorted_stocks = sorted(all_stocks, key=lambda x: x.get("amount", 0) or 0, reverse=True)
        
        # 检查哪些股票在数据库中还没有数据，优先采集这些股票
        # 优化：只检查目标股票范围内的代码，减少数据库查询
        try:
            # 先按成交额排序取前max_count只，减少需要检查的数量
            candidate_stocks = sorted_stocks[:max_count * 2]  # 多取一些候选，避免数据筛选后不够
            candidate_codes = {str(s.get("code", "")) for s in candidate_stocks if s.get("code")}
            
            if candidate_codes:
                # 批量查询这些股票在数据库中的状态（优化：一次查询）
                from common.db import _create_clickhouse_client
                client = None
                try:
                    client = _create_clickhouse_client()
                    # 批量查询：检查这些代码中哪些在数据库中有数据
                    codes_str = ','.join([f"'{c}'" for c in candidate_codes])
                    query = f"SELECT DISTINCT code FROM kline WHERE code IN ({codes_str}) AND period = 'daily'"
                    db_result = client.execute(query)
                    db_codes = {str(row[0]) for row in db_result} if db_result else set()
                except Exception as e:
                    logger.warning(f"批量查询数据库股票状态失败: {e}，使用默认策略")
                    db_codes = set()
                finally:
                    if client:
                        try:
                            client.disconnect()
                        except Exception:
                            pass
            else:
                db_codes = set()
            
            # 分离：有数据的股票和没有数据的股票
            stocks_with_data = [s for s in candidate_stocks if str(s.get("code", "")) in db_codes]
            stocks_without_data = [s for s in candidate_stocks if str(s.get("code", "")) not in db_codes]
            
            # 优先采集没有数据的股票，然后是有数据的股票（用于增量更新）
            target_stocks = stocks_without_data[:max_count]
            remaining_slots = max_count - len(target_stocks)
            if remaining_slots > 0:
                target_stocks.extend(stocks_with_data[:remaining_slots])
            
            logger.info(f"采集策略：无数据股票={len(stocks_without_data)}只，已有数据股票={len(stocks_with_data)}只，目标采集={len(target_stocks)}只（优先无数据股票）")
        except Exception as e:
            logger.warning(f"检查数据库股票列表失败，使用默认策略: {e}")
            target_stocks = sorted_stocks[:max_count]
        
        logger.info(f"开始批量采集K线数据：市场={market}，目标股票数={len(target_stocks)}")
        
        # 后台异步采集
        success_count = 0
        failed_count = 0
        
        def collect_kline_for_stock(stock):
            nonlocal success_count, failed_count
            from market.service.ws import kline_collect_stop_flags
            
            # 检查停止标志
            if kline_collect_stop_flags.get(task_id, False):
                return
            
            code = str(stock.get("code", ""))
            if not code:
                return
            
            try:
                # 优化：直接调用fetch_kline_func，它内部会检查数据库并跳过已是最新的数据
                # 减少重复的数据库查询
                kline_data = fetch_kline_func(code, "daily", "", None, None, False, False)
                
                # 如果获取到数据，说明采集成功（fetch_kline_func内部已处理增量逻辑）
                if kline_data and len(kline_data) > 0:
                    success_count += 1
                else:
                    failed_count += 1
            except Exception as e:
                failed_count += 1
                # 只记录关键错误，减少日志输出
                if "timeout" not in str(e).lower() and "连接" not in str(e):
                    logger.debug(f"采集K线数据失败 {code}: {e}")
        
        
        # 使用后台任务异步执行（减少并发数，避免ClickHouse连接冲突）
        import asyncio
        import uuid
        from concurrent.futures import ThreadPoolExecutor
        from market.service.ws import kline_collect_progress
        from datetime import datetime
        
        # 生成任务ID
        task_id = str(uuid.uuid4())
        start_time = datetime.now().isoformat()
        
        # 确定数据源信息
        if market.upper() == "HK":
            data_source = "AKShare"
        else:
            data_source = "AKShare(主) + Tushare(备用)"
        
        # 初始化进度
        kline_collect_progress[task_id] = {
            "status": "running",
            "total": len(target_stocks),
            "success": 0,
            "failed": 0,
            "current": 0,
            "message": f"开始采集K线数据({data_source})...",
            "start_time": start_time,
            "progress": 0,
            "data_source": data_source
        }
        
        async def batch_collect():
            from market.service.ws import kline_collect_stop_flags
            import time
            
            # 动态调整并发数：根据股票数量，但不超过50
            max_workers = min(50, max(10, len(target_stocks) // 20))
            executor = ThreadPoolExecutor(max_workers=max_workers)
            
            # 进度更新计数器（每完成10只股票或每2秒更新一次）
            last_update_time = time.time()
            update_interval = 2  # 2秒更新一次进度
            
            try:
                loop = asyncio.get_event_loop()
                futures = {loop.run_in_executor(executor, collect_kline_for_stock, stock): stock for stock in target_stocks}
                
                # 使用 as_completed 更快响应完成的任务
                completed = 0
                for future in asyncio.as_completed(futures):
                    # 检查停止标志
                    if kline_collect_stop_flags.get(task_id, False):
                        logger.info(f"收到停止信号，中断批量采集")
                        # 取消未完成的任务
                        for f in futures:
                            f.cancel()
                        break
                    
                    try:
                        await future
                    except Exception as e:
                        logger.debug(f"采集任务异常: {e}")
                    
                    completed += 1
                    
                    # 批量更新进度（减少更新频率）
                    current_time = time.time()
                    current = success_count + failed_count
                    if (current_time - last_update_time >= update_interval) or (completed % 10 == 0):
                        progress_pct = int((current / len(target_stocks)) * 100) if target_stocks else 0
                        if task_id in kline_collect_progress:
                            kline_collect_progress[task_id].update({
                                "success": success_count,
                                "failed": failed_count,
                                "current": current,
                                "progress": progress_pct,
                                "message": f"采集中({data_source})... 成功={success_count}，失败={failed_count}，进度={current}/{len(target_stocks)}"
                            })
                        last_update_time = current_time
                        
                        # 每50只股票输出一次日志
                        if completed % 50 == 0:
                            logger.info(f"K线数据采集进度：成功={success_count}，失败={failed_count}，进度={current}/{len(target_stocks)}")
            except Exception as e:
                end_time = datetime.now().isoformat()
                kline_collect_progress[task_id] = {
                    "status": "failed",
                    "total": len(target_stocks),
                    "success": success_count,
                    "failed": failed_count,
                    "current": success_count + failed_count,
                    "message": f"采集异常终止({data_source}): {e}",
                    "start_time": start_time,
                    "end_time": end_time,
                    "progress": int(((success_count + failed_count) / len(target_stocks)) * 100) if target_stocks else 0,
                    "data_source": data_source
                }
                logger.error(f"K线采集异常终止: {e}", exc_info=True)
                raise
            finally:
                # 确保线程池资源释放，即便出现异常
                executor.shutdown(wait=True, cancel_futures=True)
            
            # 更新最终进度
            end_time = datetime.now().isoformat()
            kline_collect_progress[task_id] = {
                "status": "completed",
                "total": len(target_stocks),
                "success": success_count,
                "failed": failed_count,
                "current": len(target_stocks),
                "message": f"K线数据采集完成({data_source})：成功={success_count}，失败={failed_count}，总计={len(target_stocks)}",
                "start_time": start_time,
                "end_time": end_time,
                "progress": 100,
                "data_source": data_source
            }
            
            logger.info(f"K线数据采集完成：成功={success_count}，失败={failed_count}，总计={len(target_stocks)}")
        
        # 启动后台任务
        background_tasks.add_task(batch_collect)
        
        return {
            "code": 0,
            "data": {
                "total": len(target_stocks),
                "market": market.upper(),
                "status": "started",
                "task_id": task_id
            },
            "message": f"已开始后台采集{len(target_stocks)}只股票的K线数据，请稍后查看结果"
        }
        
    except Exception as e:
        logger.error(f"批量采集K线数据失败: {e}", exc_info=True)
        return {"code": 1, "data": {}, "message": str(e)}


@api_router.post("/trading/plan/close")
async def close_trade_plan_api(payload: CloseTradeRequest):
    """关闭交易计划并记录结果"""
    try:
        from datetime import datetime
        entry_date = datetime.fromisoformat(payload.entry_date).date()
        exit_date = datetime.fromisoformat(payload.exit_date).date()
        
        result = close_trade_plan(
            plan_id=payload.plan_id,
            outcome=payload.outcome,
            entry_date=entry_date,
            exit_date=exit_date,
            entry_price=payload.entry_price,
            exit_price=payload.exit_price,
            profit_pct=payload.profit_pct
        )
        return {"code": 0, "data": result, "message": "success"}
    except Exception as e:
        logger.error(f"关闭交易计划失败: {e}", exc_info=True)
        return {"code": 1, "data": {}, "message": str(e)}


@api_router.get("/trading/statistics")
async def get_trade_statistics_api(code: Optional[str] = Query(None)):
    """获取交易统计（胜率、平均收益等）"""
    try:
        stats = get_trade_statistics(code=code)
        return {"code": 0, "data": stats, "message": "success"}
    except Exception as e:
        logger.error(f"获取交易统计失败: {e}", exc_info=True)
        return {"code": 1, "data": {}, "message": str(e)}


@api_router.get("/trading/statistics/stocks")
async def get_stock_statistics_api():
    """获取每只股票的统计信息"""
    try:
        stats = get_stock_statistics()
        return {"code": 0, "data": stats, "message": "success"}
    except Exception as e:
        logger.error(f"获取股票统计失败: {e}", exc_info=True)
        return {"code": 1, "data": [], "message": str(e)}


@api_router.get("/ai/metrics")
async def get_ai_system_metrics_api():
    """获取AI交易系统性能指标"""
    try:
        metrics = get_system_metrics()
        return {"code": 0, "data": metrics, "message": "success"}
    except Exception as e:
        logger.error(f"获取AI系统指标失败: {e}", exc_info=True)
        return {"code": 1, "data": {}, "message": str(e)}


@api_router.get("/ai/parameters")
async def get_ai_parameters_api():
    """获取当前动态参数设置"""
    try:
        from ai.parameter_optimizer import get_parameter_optimizer
        optimizer = get_parameter_optimizer()
        params = optimizer.get_parameters()
        status_history = optimizer.get_status_history(limit=20)
        return {
            "code": 0,
            "data": {
                "current_status": optimizer.market_status,
                "parameters": params,
                "status_history": status_history
            },
            "message": "success"
        }
    except Exception as e:
        logger.error(f"获取AI参数失败: {e}", exc_info=True)
        return {"code": 1, "data": {}, "message": str(e)}


# 注册路由
app.include_router(api_router)

# 静态文件服务（前端）
# 尝试多种路径构建方式，以适应不同的部署环境
# 1. 尝试容器内的路径：/app/frontend（Docker容器）
_app_dir = os.path.dirname(os.path.dirname(__file__))  # /app 目录（在Docker中）或 backend 目录
frontend_path_in_app = os.path.join(_app_dir, "frontend")  # /app/frontend

# 2. 尝试项目根目录：项目根目录/frontend（本地开发）
_base_dir = os.path.dirname(_app_dir)  # backend的父目录（项目根目录）
frontend_path_in_root = os.path.join(_base_dir, "frontend")

# 3. 尝试当前工作目录
frontend_path_in_cwd = os.path.join(os.getcwd(), "frontend")

# 按优先级选择存在的路径
frontend_path = None
for path in [frontend_path_in_app, frontend_path_in_root, frontend_path_in_cwd]:
    if path and os.path.exists(path):
        frontend_path = path
        break

if frontend_path and os.path.exists(frontend_path):
    logger.info(f"前端路径: {frontend_path} (绝对路径: {os.path.abspath(frontend_path)})")
    
    # 静态资源
    static_path = os.path.join(frontend_path, "static")
    if os.path.exists(static_path):
        app.mount("/static", StaticFiles(directory=static_path), name="static")
    
    # 根路径路由（必须在通配符路由之前）
    @app.get("/", include_in_schema=False)
    async def index():
        """前端首页"""
        from fastapi.responses import HTMLResponse
        index_file = os.path.join(frontend_path, "index.html")
        logger.info(f"访问根路径，尝试加载首页: {index_file}")
        logger.info(f"文件存在: {os.path.exists(index_file)}")
        logger.info(f"前端路径: {frontend_path}")
        
        if os.path.exists(index_file):
            logger.info(f"成功加载首页: {index_file}")
            with open(index_file, 'r', encoding='utf-8') as f:
                html_content = f.read()
            return HTMLResponse(
                content=html_content,
                headers={
                    "Cache-Control": "no-cache",
                    "X-Content-Type-Options": "nosniff",
                    "Content-Type": "text/html; charset=utf-8",
                }
            )
        logger.error(f"前端文件未找到: {index_file}")
        return {"message": "前端文件未找到", "path": index_file, "frontend_path": frontend_path}
    
    # 前端资源文件路由
    @app.get("/{filename:path}", include_in_schema=False)
    async def frontend_files(filename: str):
        """前端资源文件"""
        # 排除API和WebSocket路径
        if filename.startswith("api") or filename.startswith("ws"):
            from fastapi import HTTPException
            raise HTTPException(status_code=404, detail="Not found")
        
        # 处理根路径（虽然已经有专门的根路由，但这里作为后备）
        if not filename or filename == "/":
            index_file = os.path.join(frontend_path, "index.html")
            if os.path.exists(index_file):
                return FileResponse(
                    index_file,
                    media_type="text/html; charset=utf-8",
                    headers={
                        "Cache-Control": "no-cache",
                        "X-Content-Type-Options": "nosniff",
                    }
                )
        
        # 处理前端资源文件
        allowed_extensions = {'.html', '.js', '.css', '.json', '.ico', '.png', '.jpg', '.svg', '.woff', '.woff2', '.ttf', '.eot'}
        if '.' in filename:
            ext = os.path.splitext(filename)[1].lower()
            if ext in allowed_extensions:
                file_path = os.path.join(frontend_path, filename)
                if os.path.exists(file_path) and os.path.isfile(file_path):
                    # 确保路径在frontend目录内（防止路径遍历）
                    real_path = os.path.realpath(file_path)
                    real_frontend = os.path.realpath(frontend_path)
                    if real_path.startswith(real_frontend):
                        # 设置正确的Content-Type
                        media_type = None
                        if ext == '.js':
                            media_type = 'application/javascript'
                        elif ext == '.css':
                            media_type = 'text/css'
                        elif ext == '.html':
                            media_type = 'text/html'
                        elif ext == '.json':
                            media_type = 'application/json'
                        elif ext in ['.png', '.jpg', '.jpeg', '.gif', '.svg']:
                            media_type = f'image/{ext[1:]}'
                        
                        return FileResponse(
                            file_path,
                            media_type=media_type,
                            headers={"Cache-Control": "public, max-age=3600"}
                        )
        
        # 对于其他路径，返回index.html（支持前端路由，如 SPA）
        index_file = os.path.join(frontend_path, "index.html")
        if os.path.exists(index_file):
            return FileResponse(
                index_file,
                media_type="text/html",
                headers={"Cache-Control": "no-cache"}
            )
        
        # 文件不存在，返回404
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Not found")
else:
    logger.warning(f"前端目录不存在: {frontend_path}")


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


@app.get("/health", include_in_schema=False)
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

