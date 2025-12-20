"""
API网关 - 主应用入口
"""
from fastapi import FastAPI, Depends, Header, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import os
import asyncio

# 导入各模块路由
from market.service.api import router as market_router
from market.service.ws import router as ws_router
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


@api_router.get("/market/status")
async def get_market_status():
    """获取A股和港股的交易状态"""
    try:
        from common.trading_hours import is_a_stock_trading_time, is_hk_stock_trading_time
        
        is_a_trading = is_a_stock_trading_time()
        is_hk_trading = is_hk_stock_trading_time()
        
        logger.debug(f"市场状态检查: A股={is_a_trading}, 港股={is_hk_trading}")
        
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


@api_router.get("/strategy/select")
async def select_stocks_api(
    background_tasks: BackgroundTasks,
    max_count: int | None = Query(None, description="最大数量，留空则使用系统配置"),
    market: str | None = Query(None, description="市场类型：A（A股）或HK（港股），留空则使用系统配置"),
    task_id: str | None = Query(None, description="任务ID，用于进度追踪"),
):
    """自动选股（使用新的多维度筛选策略，支持异步计算）"""
    import uuid
    from market.service.ws import selection_progress
    
    # 如果没有提供task_id，生成一个
    if not task_id:
        task_id = str(uuid.uuid4())
    
    try:
        from strategy.selector import (
            check_market_environment,
            filter_stocks_by_criteria,
            save_selected_stocks
        )
        from market_collector.hk import fetch_hk_stock_kline
        
        # 读取系统运行时配置
        cfg = get_runtime_config()
        if max_count is None:
            max_count = cfg.selection_max_count
        if market is None:
            market = cfg.selection_market
        
        # 从ClickHouse获取股票列表
        from common.db import get_stock_list_from_db
        all_stocks = get_stock_list_from_db(market.upper())
        
        if market.upper() == "HK":
            fetch_kline_func = fetch_hk_stock_kline
        else:
            fetch_kline_func = fetch_a_stock_kline
        
        if not all_stocks:
            return {
                "code": 1, 
                "data": [], 
                "message": f"未获取到{market}股行情数据。ClickHouse的kline表中没有数据，请先运行数据采集程序采集K线数据。",
                "task_id": task_id
            }
        
        # 1. 检查市场环境（上涨家数比率 > 50%）
        logger.info(f"开始选股：市场={market}，总股票数={len(all_stocks)}")
        selection_progress[task_id] = {
            "status": "running",
            "stage": "market_check",
            "message": "检查市场环境...",
            "progress": 5,
            "total": len(all_stocks),
            "processed": 0,
            "passed": 0
        }
        market_ok = check_market_environment(all_stocks)
        if not market_ok:
            logger.warning(f"市场环境不佳，上涨家数比率 <= 50%，暂停选股")
            selection_progress[task_id] = {
                "status": "failed",
                "stage": "market_check",
                "message": "市场环境不佳，上涨家数比率 <= 50%，暂停选股",
                "progress": 0
            }
            return {"code": 0, "data": [], "message": "市场环境不佳，暂停选股", "task_id": task_id}
        logger.info(f"市场环境检查通过，继续选股")
        selection_progress[task_id] = {
            "status": "running",
            "stage": "layer1",
            "message": "市场环境检查通过，开始第一层筛选...",
            "progress": 10,
            "total": len(all_stocks),
            "processed": 0,
            "passed": 0
        }
        
        # 2. 分层计算，逐步淘汰（CPU优化）
        from common.db import batch_get_indicators, get_indicator, save_indicator
        from market.indicator.ta import calculate_ma60_only
        from datetime import datetime
        
        today = datetime.now().strftime("%Y-%m-%d")
        
        # 检查ClickHouse连接，如果连接失败，使用简化模式（不依赖数据库缓存）
        clickhouse_available = True
        try:
            from common.db import get_clickhouse
            get_clickhouse()
        except Exception as e:
            logger.warning(f"ClickHouse连接失败，将使用简化模式（不依赖数据库缓存）: {e}")
            clickhouse_available = False
        
        # 先过滤掉无效股票（price为NaN或无效的）
        import math
        valid_stocks = []
        for stock in all_stocks:
            price = stock.get("price", 0)
            if price is None or (isinstance(price, float) and math.isnan(price)):
                continue
            try:
                price_val = float(price)
                if price_val > 0 and not math.isnan(price_val):
                    valid_stocks.append(stock)
            except (ValueError, TypeError):
                continue
        
        if not valid_stocks:
            logger.warning(f"没有有效的股票数据（所有股票的price都是NaN或无效）")
            selection_progress[task_id] = {
                "status": "failed",
                "stage": "data_check",
                "message": "没有有效的股票数据，请检查数据采集是否正常",
                "progress": 0
            }
            return {"code": 0, "data": [], "message": "没有有效的股票数据，请检查数据采集是否正常", "task_id": task_id}
        
        logger.info(f"过滤后有效股票数：{len(valid_stocks)}/{len(all_stocks)}")
        
        # 先按成交额排序，优先处理活跃股票
        sorted_stocks = sorted(valid_stocks, key=lambda x: x.get("amount", 0) or 0, reverse=True)
        
        logger.info(f"开始分层筛选：市场={market}，总股票数={len(sorted_stocks)}（处理全部股票）")
        
        # 第一层：尝试从缓存读取MA60（或快速计算），筛选价格>MA60且MA60向上的股票
        # 如果缓存中没有MA60，暂时跳过MA60检查，只做基本筛选（价格>0，涨幅>0等）
        layer1_passed = []
        cached_count = 0
        calc_ma60_count = 0
        skip_ma60_count = 0  # 跳过MA60检查的股票数
        
        # 先批量读取缓存（如果有很多股票，可以分批读取）
        all_codes = [str(s.get("code", "")) for s in sorted_stocks]
        logger.info(f"准备批量读取缓存，股票数量={len(all_codes)}")
        cached_indicators = {}
        cache_coverage = 0.0
        
        if clickhouse_available:
            try:
                cached_indicators = batch_get_indicators(all_codes, market.upper(), today)
                cache_coverage = len(cached_indicators) / len(all_codes) if all_codes else 0
                logger.info(f"批量读取缓存完成，缓存命中={len(cached_indicators)}只股票，覆盖率={cache_coverage:.1%}")
            except Exception as e:
                logger.warning(f"批量读取缓存失败，将跳过缓存: {e}")
                cached_indicators = {}
                cache_coverage = 0.0
        else:
            logger.info(f"ClickHouse不可用，缓存覆盖率视为0%")
            cache_coverage = 0.0
        
        # 只有在缓存覆盖率低于30%时才批量计算指标（首次运行或缓存不足）
        if cache_coverage < 0.3 and len(all_codes) > 0:
            logger.warning(f"缓存覆盖率过低（{cache_coverage:.1%}），开始批量计算指标...")
            selection_progress[task_id] = {
                "status": "running",
                "stage": "precompute",
                "message": f"缓存不足，正在批量计算指标（已缓存{len(cached_indicators)}/{len(all_codes)}）...",
                "progress": 5,
                "total": len(sorted_stocks),
                "processed": 0,
                "passed": 0,
                "elapsed_time": 0
            }
            
            # 批量计算指标（全量计算模式，计算前2000只活跃股票）
            from strategy.indicator_batch import batch_compute_indicators
            batch_result = batch_compute_indicators(market.upper(), max_count=2000, incremental=False)
            logger.info(f"批量计算指标完成：成功={batch_result.get('success', 0)}，失败={batch_result.get('failed', 0)}，跳过={batch_result.get('skipped', 0)}")
            
            # 重新读取缓存（如果ClickHouse可用）
            if clickhouse_available:
                try:
                    cached_indicators = batch_get_indicators(all_codes, market.upper(), today)
                    cache_coverage = len(cached_indicators) / len(all_codes) if all_codes else 0
                    logger.info(f"重新读取缓存完成，缓存命中={len(cached_indicators)}只股票，覆盖率={cache_coverage:.1%}")
                except Exception as e:
                    logger.warning(f"重新读取缓存失败: {e}")
        else:
            # 缓存覆盖率足够，直接使用缓存
            logger.info(f"缓存覆盖率足够（{cache_coverage:.1%}），直接使用缓存，跳过批量计算")
        
        # 添加超时控制：如果处理时间超过30秒，提前退出（更激进）
        import time
        start_time = time.time()
        max_process_time = 30  # 30秒超时，留30秒给后续处理
        
        for idx, stock in enumerate(sorted_stocks):
            # 每处理50只股票更新一次进度（更频繁的更新）
            if idx > 0 and idx % 50 == 0:
                elapsed_time = time.time() - start_time
                progress_pct = min(90, 10 + int((idx / len(sorted_stocks)) * 60))  # 10-70%进度
                selection_progress[task_id] = {
                    "status": "running",
                    "stage": "layer1",
                    "message": f"第一层筛选中：已处理 {idx}/{len(sorted_stocks)} 只，通过 {len(layer1_passed)} 只",
                    "progress": progress_pct,
                    "total": len(sorted_stocks),
                    "processed": idx,
                    "passed": len(layer1_passed),
                    "elapsed_time": round(elapsed_time, 1)
                }
                logger.info(f"第一层筛选进度：已处理{idx}/{len(sorted_stocks)}只，耗时{elapsed_time:.1f}秒，通过{len(layer1_passed)}只")
            
            # 检查是否超时
            elapsed_time = time.time() - start_time
            if elapsed_time > max_process_time:
                logger.warning(f"第一层筛选超时（{elapsed_time:.1f}秒），已处理{idx}/{len(sorted_stocks)}只股票，提前退出")
                selection_progress[task_id] = {
                    "status": "running",
                    "stage": "layer1",
                    "message": f"第一层筛选超时，已处理 {idx}/{len(sorted_stocks)} 只",
                    "progress": 70,
                    "total": len(sorted_stocks),
                    "processed": idx,
                    "passed": len(layer1_passed),
                    "elapsed_time": round(elapsed_time, 1)
                }
                break
            code = str(stock.get("code", ""))
            current_price = stock.get("price", 0)
            
            # 过滤掉价格无效的股票（NaN、None、0或负数）
            import math
            if current_price is None or (isinstance(current_price, float) and math.isnan(current_price)):
                continue
            try:
                current_price = float(current_price)
                if current_price <= 0 or math.isnan(current_price):
                    continue
            except (ValueError, TypeError):
                continue
            
            # 优先从缓存读取
            cached = cached_indicators.get(code)
            if cached and cached.get("ma60") and cached.get("ma60_trend"):
                cached_count += 1
                ma60 = cached["ma60"]
                ma60_trend = cached["ma60_trend"]
                
                # 检查价格 > MA60 且 MA60 向上（放宽条件：允许MA60趋势为"向上"或"up"或空）
                if current_price > ma60 and (ma60_trend in ["向上", "up"] or not ma60_trend):
                    layer1_passed.append(stock)
                continue
            
            # 缓存未命中，尝试快速计算MA60
            # 如果获取K线数据失败或超时，暂时跳过MA60检查，只做基本筛选
            ma60_available = False
            try:
                import concurrent.futures
                
                # 使用线程池执行，带超时控制
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                    # 不跳过数据库，让数据保存到ClickHouse（skip_db=False）
                    future = executor.submit(fetch_kline_func, code, "daily", "", None, None, False, False)
                    try:
                        kline_data = future.result(timeout=5)  # 增加到5秒超时
                    except concurrent.futures.TimeoutError:
                        if idx < 10:  # 只记录前10只的详细日志
                            logger.debug(f"获取K线数据超时 {code}，跳过MA60检查")
                        ma60_available = False
                    except Exception as e:
                        if idx < 10:  # 只记录前10只的详细日志
                            logger.debug(f"获取K线数据失败 {code}: {e}，跳过MA60检查")
                        ma60_available = False
                    else:
                        # 成功获取K线数据
                        if kline_data and len(kline_data) >= 60:
                            df = pd.DataFrame(kline_data)
                            ma60_data = calculate_ma60_only(df)
                            
                            if ma60_data:
                                calc_ma60_count += 1
                                ma60 = ma60_data["ma60"]
                                ma60_trend = ma60_data.get("ma60_trend", "")
                                
                                # 检查价格 > MA60 且 MA60 向上（放宽条件：允许MA60趋势为"向上"或"up"或空）
                                if current_price > ma60 and (ma60_trend in ["向上", "up"] or not ma60_trend):
                                    ma60_available = True  # MA60可用且满足条件
                                    layer1_passed.append(stock)
                                    
                                    # 异步计算并保存完整指标（后台任务，不阻塞）
                                    from strategy.indicator_batch import compute_indicator_async
                                    background_tasks.add_task(
                                        compute_indicator_async,
                                        code,
                                        market.upper(),
                                        kline_data
                                    )
                                else:
                                    # MA60计算成功但不满足条件，也跳过MA60检查，使用基本筛选
                                    ma60_available = False
                            elif idx < 10:  # 只记录前10只的详细日志
                                logger.debug(f"计算MA60失败 {code}：K线数据不足或计算失败，跳过MA60检查")
                        elif idx < 10:  # 只记录前10只的详细日志
                            logger.debug(f"K线数据不足 {code}：{len(kline_data) if kline_data else 0}条，需要60条，跳过MA60检查")
            except Exception as e:
                if idx < 10:  # 只记录前10只的详细日志
                    logger.debug(f"计算MA60异常 {code}: {e}，跳过MA60检查")
                ma60_available = False
            
            # 如果MA60不可用，暂时跳过MA60检查，只做基本筛选（价格>0，涨幅>0等）
            if not ma60_available:
                skip_ma60_count += 1
                # 基本筛选：价格>0，涨幅>=0（允许平盘），成交额>=0（允许无成交）
                pct = stock.get("pct", 0)
                amount = stock.get("amount", 0)
                # 放宽条件：只要价格有效，涨幅>=0（允许平盘），就通过
                if isinstance(pct, (int, float)) and not math.isnan(pct) and pct >= 0:
                    # 暂时通过，后续层会进一步筛选
                    layer1_passed.append(stock)
                    if skip_ma60_count <= 5:  # 只记录前5只的详细日志
                        logger.debug(f"跳过MA60检查，通过基本筛选 {code}：价格={current_price}, 涨幅={pct}, 成交额={amount}")
        
        elapsed_time = time.time() - start_time
        logger.info(f"第一层筛选完成：总耗时{elapsed_time:.1f}秒，缓存命中={cached_count}，计算MA60={calc_ma60_count}，跳过MA60={skip_ma60_count}，通过={len(layer1_passed)}只")
        if calc_ma60_count == 0 and cached_count == 0:
            logger.warning(f"第一层筛选：没有获取到任何MA60数据（缓存命中=0，计算=0），已跳过MA60检查{skip_ma60_count}只股票，使用基本筛选条件。已处理{min(len(sorted_stocks), idx+1)}只股票")
        if len(layer1_passed) == 0 and skip_ma60_count > 0:
            # 如果跳过了MA60检查但没有股票通过，说明基本筛选条件可能太严格
            logger.warning(f"第一层筛选：跳过了{skip_ma60_count}只股票的MA60检查，但没有股票通过基本筛选。可能原因：1) 所有股票的涨幅<0；2) 涨幅数据为NaN")
            # 尝试放宽条件：只检查前10只股票的数据
            sample_count = min(10, len(sorted_stocks))
            for i in range(sample_count):
                sample_stock = sorted_stocks[i]
                sample_pct = sample_stock.get("pct", 0)
                sample_price = sample_stock.get("price", 0)
                logger.debug(f"样本股票 {sample_stock.get('code')}：价格={sample_price}, 涨幅={sample_pct}, 涨幅类型={type(sample_pct)}")
        
        selection_progress[task_id] = {
            "status": "running",
            "stage": "layer1_complete",
            "message": f"第一层筛选完成：通过 {len(layer1_passed)} 只股票",
            "progress": 70,
            "total": len(sorted_stocks),
            "processed": min(len(sorted_stocks), idx+1),
            "passed": len(layer1_passed),
            "elapsed_time": round(elapsed_time, 1)
        }
        
        if not layer1_passed:
            logger.warning(f"第一层筛选未通过任何股票，可能原因：1) 市场环境不佳；2) 筛选条件过严；3) 处理时间不足（只处理了部分股票）")
            selection_progress[task_id] = {
                "status": "failed",
                "stage": "layer1_complete",
                "message": f"未找到符合条件的股票（已检查{min(len(sorted_stocks), idx+1)}只，耗时{elapsed_time:.1f}秒）",
                "progress": 100,
                "total": len(sorted_stocks),
                "processed": min(len(sorted_stocks), idx+1),
                "passed": 0,
                "elapsed_time": round(elapsed_time, 1)
            }
            return {"code": 0, "data": [], "message": f"未找到符合条件的股票（已检查{min(len(sorted_stocks), idx+1)}只，耗时{elapsed_time:.1f}秒）", "task_id": task_id}
        
        # 第二层：对通过第一层的股票，获取完整指标（优先从缓存读取，缺失的再计算）
        selection_progress[task_id] = {
            "status": "running",
            "stage": "layer2",
            "message": f"开始第二层筛选：获取 {len(layer1_passed)} 只股票的完整指标...",
            "progress": 75,
            "total": len(sorted_stocks),
            "processed": min(len(sorted_stocks), idx+1),
            "passed": len(layer1_passed),
            "elapsed_time": round(time.time() - start_time, 1)
        }
        
        indicators_map = {}
        from_cache_count = 0
        calc_full_count = 0
        
        layer1_codes = [str(s.get("code", "")) for s in layer1_passed]
        full_cached = {}
        if clickhouse_available:
            try:
                full_cached = batch_get_indicators(layer1_codes, market.upper(), today)
            except Exception as e:
                logger.warning(f"批量读取完整指标缓存失败，将跳过缓存: {e}")
                full_cached = {}
        
        for idx2, stock in enumerate(layer1_passed):
            # 每处理10只股票更新一次进度
            if idx2 > 0 and idx2 % 10 == 0:
                elapsed_time = time.time() - start_time
                progress_pct = min(90, 75 + int((idx2 / len(layer1_passed)) * 15))  # 75-90%进度
                selection_progress[task_id] = {
                    "status": "running",
                    "stage": "layer2",
                    "message": f"第二层筛选中：已处理 {idx2}/{len(layer1_passed)} 只，获取指标 {len(indicators_map)} 个",
                    "progress": progress_pct,
                    "total": len(sorted_stocks),
                    "processed": min(len(sorted_stocks), idx+1),
                    "passed": len(layer1_passed),
                    "layer2_processed": idx2,
                    "indicators_count": len(indicators_map),
                    "elapsed_time": round(elapsed_time, 1)
                }
            
            code = str(stock.get("code", ""))
            
            # 优先从缓存读取完整指标
            cached_full = full_cached.get(code)
            if cached_full and cached_full.get("rsi") is not None:
                # 将缓存的指标转换为筛选函数期望的格式
                indicators_map[code] = {
                    "ma60": cached_full.get("ma60"),
                    "ma60_trend": cached_full.get("ma60_trend"),
                    "ma5": cached_full.get("ma5"),
                    "ma10": cached_full.get("ma10"),
                    "ma20": cached_full.get("ma20"),
                    "ma5_trend": cached_full.get("ma5_trend"),
                    "ma10_trend": cached_full.get("ma10_trend"),
                    "vol_ratio": cached_full.get("vol_ratio"),
                    "rsi": cached_full.get("rsi"),
                    "williams_r": cached_full.get("williams_r"),
                    "williams_r_prev": cached_full.get("williams_r_prev"),
                    "break_high_20d": cached_full.get("break_high_20d"),
                    "boll_middle": cached_full.get("boll_middle"),
                    "boll_expanding": cached_full.get("boll_expanding"),
                }
                from_cache_count += 1
                continue
            
            # 缓存未命中，同步计算完整指标（已通过第一层筛选，数量少，计算成本可接受）
            # 但保存到缓存使用异步任务，不阻塞响应
            # 添加超时控制：如果单只股票处理时间超过3秒，跳过（更激进的超时）
            try:
                import concurrent.futures
                
                # 使用线程池执行，带超时控制
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                    # 不跳过数据库，让数据保存到ClickHouse（skip_db=False）
                    future = executor.submit(fetch_kline_func, code, "daily", "", None, None, False, False)
                    try:
                        kline_data = future.result(timeout=3)  # 3秒超时（更激进）
                    except concurrent.futures.TimeoutError:
                        logger.debug(f"获取K线数据超时 {code}，跳过")
                        continue
                    except Exception as e:
                        logger.debug(f"获取K线数据失败 {code}: {e}")
                        continue
                
                if kline_data and len(kline_data) >= 60:
                    df = pd.DataFrame(kline_data)
                    indicators = calculate_all_indicators(df)
                    if indicators:
                        indicators_map[code] = indicators
                        calc_full_count += 1
                        
                        # 异步保存到缓存（后台任务，不阻塞选股流程）
                        from strategy.indicator_batch import compute_indicator_async
                        background_tasks.add_task(
                            compute_indicator_async,
                            code,
                            market.upper(),
                            kline_data
                        )
            except Exception as e:
                logger.debug(f"计算完整指标失败 {code}: {e}")
                continue
        
        elapsed_time = time.time() - start_time
        logger.info(f"第二层指标获取完成：耗时{elapsed_time:.1f}秒，缓存={from_cache_count}，计算={calc_full_count}，有效指标={len(indicators_map)}个")
        
        # 4. 构建筛选配置
        filter_config = {
            "volume_ratio_min": cfg.filter_volume_ratio_min,
            "volume_ratio_max": cfg.filter_volume_ratio_max,
            "rsi_min": cfg.filter_rsi_min,
            "rsi_max": cfg.filter_rsi_max,
            "williams_r_enable": cfg.filter_williams_r_enable,
            "break_high_enable": cfg.filter_break_high_enable,
            "boll_enable": cfg.filter_boll_enable,
        }
        
        # 5. 执行筛选（使用通过第一层的股票和完整指标）
        selection_progress[task_id] = {
            "status": "running",
            "stage": "filtering",
            "message": f"执行筛选条件：通过第一层 {len(layer1_passed)} 只，有指标数据 {len(indicators_map)} 只",
            "progress": 90,
            "total": len(sorted_stocks),
            "processed": min(len(sorted_stocks), idx+1),
            "passed": len(layer1_passed),
            "indicators_count": len(indicators_map),
            "elapsed_time": round(time.time() - start_time, 1)
        }
        
        logger.info(f"开始第二层筛选：通过第一层={len(layer1_passed)}只，有指标数据={len(indicators_map)}只")
        filtered = filter_stocks_by_criteria(layer1_passed, indicators_map, filter_config)
        logger.info(f"第二层筛选完成：筛选出{len(filtered)}只股票")
        
        if not filtered:
            logger.warning(f"第二层筛选未通过任何股票")
            selection_progress[task_id] = {
                "status": "failed",
                "stage": "filtering",
                "message": f"未找到符合条件的股票（第二层筛选未通过任何股票）",
                "progress": 100,
                "total": len(sorted_stocks),
                "processed": min(len(sorted_stocks), idx+1),
                "passed": len(layer1_passed),
                "selected": 0,
                "elapsed_time": round(time.time() - start_time, 1)
            }
            return {"code": 0, "data": [], "message": "未找到符合条件的股票（第二层筛选未通过任何股票）", "task_id": task_id}
        
        # 6. 限制数量
        selected = filtered[:max_count]
        
        # 7. 保存选股结果
        save_selected_stocks(selected, market.upper())
        
        total_time = time.time() - start_time
        logger.info(f"选股完成：{market}股市场，从{len(all_stocks)}只中筛选出{len(selected)}只，总耗时{total_time:.1f}秒")
        
        # 更新最终进度
        selection_progress[task_id] = {
            "status": "completed",
            "stage": "completed",
            "message": f"选股完成：筛选出 {len(selected)} 只股票",
            "progress": 100,
            "total": len(sorted_stocks),
            "processed": min(len(sorted_stocks), idx+1),
            "passed": len(layer1_passed),
            "selected": len(selected),
            "elapsed_time": round(total_time, 1)
        }
        
        return {"code": 0, "data": selected, "message": "success", "task_id": task_id}
    except Exception as e:
        logger.error(f"选股失败: {e}", exc_info=True)
        # 更新失败进度
        if task_id:
            from market.service.ws import selection_progress
            selection_progress[task_id] = {
                "status": "failed",
                "stage": "error",
                "message": f"选股失败: {str(e)[:100]}",
                "progress": 0,
                "elapsed_time": 0
            }
        return {"code": 1, "data": [], "message": str(e), "task_id": task_id}


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
        
        # 第二步：按批次进行批量分析（异步执行，不阻塞）
        from ai.analyzer import analyze_stocks_batch_with_ai
        
        for batch_start in range(0, len(stocks_data_list), ai_batch_size):
            batch_end = min(batch_start + ai_batch_size, len(stocks_data_list))
            batch_data = stocks_data_list[batch_start:batch_end]
            batch_index = batch_start // ai_batch_size
            
            try:
                logger.info(f"批量分析第 {batch_index + 1} 批，共 {len(batch_data)} 支股票（异步执行）")
                
                # 一次性分析整批股票
                batch_results = analyze_stocks_batch_with_ai(
                    batch_data,
                    include_trading_points=True
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
        "market": market
    }
    
    def collect_kline_for_stock(stock):
        nonlocal success_count, failed_count
        code = str(stock.get("code", ""))
        if not code:
            return
        
        try:
            kline_data = fetch_kline_func(code, "daily", "", None, None, False, False)
            if kline_data and len(kline_data) > 0:
                success_count += 1
                if success_count % 50 == 0:
                    logger.info(f"[{market}]K线数据采集进度：成功={success_count}，失败={failed_count}，当前={code}")
            else:
                failed_count += 1
        except Exception as e:
            failed_count += 1
            logger.debug(f"[{market}]采集K线数据失败 {code}: {e}")
        finally:
            current = success_count + failed_count
            progress_pct = int((current / len(target_stocks)) * 100) if target_stocks else 0
            if task_id in kline_collect_progress:
                kline_collect_progress[task_id].update({
                    "success": success_count,
                    "failed": failed_count,
                    "current": current,
                    "progress": progress_pct,
                    "message": f"[{market}]采集中... 成功={success_count}，失败={failed_count}，进度={current}/{len(target_stocks)}"
                })
    
    def batch_collect():
        """同步批量采集函数"""
        executor = ThreadPoolExecutor(max_workers=50)
        try:
            # 使用线程池并发执行采集任务
            futures = [executor.submit(collect_kline_for_stock, stock) for stock in target_stocks]
            # 等待所有任务完成
            for future in futures:
                try:
                    future.result()
                except Exception as e:
                    logger.debug(f"[{market}]采集任务异常: {e}")
        except Exception as e:
            end_time = datetime.now().isoformat()
            kline_collect_progress[task_id] = {
                "status": "failed",
                "total": len(target_stocks),
                "success": success_count,
                "failed": failed_count,
                "current": success_count + failed_count,
                "message": f"[{market}]采集异常终止: {e}",
                "start_time": start_time,
                "end_time": end_time,
                "progress": int(((success_count + failed_count) / len(target_stocks)) * 100) if target_stocks else 0,
                "market": market
            }
            logger.error(f"[{market}]K线采集异常终止: {e}", exc_info=True)
            raise
        finally:
            executor.shutdown(wait=True)
        
        # 更新最终进度
        end_time = datetime.now().isoformat()
        kline_collect_progress[task_id] = {
            "status": "completed",
            "total": len(target_stocks),
            "success": success_count,
            "failed": failed_count,
            "current": len(target_stocks),
            "message": f"[{market}]K线数据采集完成：成功={success_count}，失败={failed_count}，总计={len(target_stocks)}",
            "start_time": start_time,
            "end_time": end_time,
            "progress": 100,
            "market": market
        }
        logger.info(f"[{market}]K线数据采集完成：成功={success_count}，失败={failed_count}，总计={len(target_stocks)}")
    
    # 运行批量采集
    batch_collect()


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
                # 先采集A股
                try:
                    a_stocks = get_json("market:a:spot") or []
                    if not a_stocks:
                        logger.info("检测到A股行情数据为空，先执行一次行情采集...")
                        fetch_a_stock_spot()
                        a_stocks = get_json("market:a:spot") or []
                    
                    if a_stocks:
                        logger.info(f"开始采集A股K线数据，共{len(a_stocks)}只股票")
                        _collect_market_kline_internal("A", a_stocks, fetch_a_stock_kline, max_count)
                except Exception as e:
                    logger.error(f"A股K线采集失败: {e}", exc_info=True)
                
                # 再采集港股
                try:
                    hk_stocks = get_json("market:hk:spot") or []
                    if hk_stocks:
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
        
        # 按成交额排序，优先采集活跃股票
        sorted_stocks = sorted(all_stocks, key=lambda x: x.get("amount", 0) or 0, reverse=True)
        
        # 检查哪些股票在数据库中还没有数据，优先采集这些股票
        from common.db import get_stock_list_from_db
        try:
            db_stocks = get_stock_list_from_db(market.upper())
            db_codes = {s.get("code") for s in db_stocks} if db_stocks else set()
            
            # 分离：有数据的股票和没有数据的股票
            stocks_with_data = [s for s in sorted_stocks if s.get("code") in db_codes]
            stocks_without_data = [s for s in sorted_stocks if s.get("code") not in db_codes]
            
            # 优先采集没有数据的股票，然后是有数据的股票（用于增量更新）
            # 确保没有数据的股票优先，但总数不超过max_count
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
            from market.service.ws import kline_collect_progress
            from datetime import datetime
            
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
                    if success_count % 50 == 0:  # 每50只更新一次日志，减少日志输出
                        logger.info(f"K线数据采集进度：成功={success_count}，失败={failed_count}，当前={code}")
                else:
                    failed_count += 1
            except Exception as e:
                failed_count += 1
                logger.debug(f"采集K线数据失败 {code}: {e}")
            finally:
                # 更新进度
                current = success_count + failed_count
                progress_pct = int((current / len(target_stocks)) * 100) if target_stocks else 0
                if task_id in kline_collect_progress:
                    kline_collect_progress[task_id].update({
                        "success": success_count,
                        "failed": failed_count,
                        "current": current,
                        "progress": progress_pct,
                        "message": f"采集中... 成功={success_count}，失败={failed_count}，进度={current}/{len(target_stocks)}"
                    })
        
        # 使用后台任务异步执行（减少并发数，避免ClickHouse连接冲突）
        import asyncio
        import uuid
        from concurrent.futures import ThreadPoolExecutor
        from market.service.ws import kline_collect_progress
        from datetime import datetime
        
        # 生成任务ID
        task_id = str(uuid.uuid4())
        start_time = datetime.now().isoformat()
        
        # 初始化进度
        kline_collect_progress[task_id] = {
            "status": "running",
            "total": len(target_stocks),
            "success": 0,
            "failed": 0,
            "current": 0,
            "message": "开始采集K线数据...",
            "start_time": start_time,
            "progress": 0
        }
        
        async def batch_collect():
            # 并发数调整为50，仅在采集时创建线程池；异常也保证销毁
            executor = ThreadPoolExecutor(max_workers=50)
            try:
                loop = asyncio.get_event_loop()
                tasks = [
                    loop.run_in_executor(executor, collect_kline_for_stock, stock)
                    for stock in target_stocks
                ]
                await asyncio.gather(*tasks)
            except Exception as e:
                end_time = datetime.now().isoformat()
                kline_collect_progress[task_id] = {
                    "status": "failed",
                    "total": len(target_stocks),
                    "success": success_count,
                    "failed": failed_count,
                    "current": success_count + failed_count,
                    "message": f"采集异常终止: {e}",
                    "start_time": start_time,
                    "end_time": end_time,
                    "progress": int(((success_count + failed_count) / len(target_stocks)) * 100) if target_stocks else 0
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
                "message": f"K线数据采集完成：成功={success_count}，失败={failed_count}，总计={len(target_stocks)}",
                "start_time": start_time,
                "end_time": end_time,
                "progress": 100
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
        index_file = os.path.join(frontend_path, "index.html")
        logger.info(f"访问根路径，尝试加载首页: {index_file}")
        logger.info(f"文件存在: {os.path.exists(index_file)}")
        logger.info(f"前端路径: {frontend_path}")
        
        if os.path.exists(index_file):
            logger.info(f"成功加载首页: {index_file}")
            return FileResponse(
                index_file,
                media_type="text/html",
                headers={"Cache-Control": "no-cache"}
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
                    media_type="text/html",
                    headers={"Cache-Control": "no-cache"}
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

