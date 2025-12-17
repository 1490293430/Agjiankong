"""
行情API服务
"""
from fastapi import APIRouter, Query
from typing import Optional, List, Dict, Any
import math

from market_collector.cn import fetch_a_stock_spot, fetch_a_stock_kline
from market_collector.hk import fetch_hk_stock_spot, fetch_hk_stock_kline
from common.redis import get_json
from common.logger import get_logger
import pandas as pd
from market.indicator.ta import calculate_all_indicators

logger = get_logger(__name__)
router = APIRouter(prefix="/market", tags=["行情"])


def _sanitize_spot_data(data: Optional[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    """
    处理行情数据中的 NaN/Inf，避免 JSON 序列化报错：
    "Out of range float values are not JSON compliant"
    """
    if not data:
        return []

    sanitized: List[Dict[str, Any]] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        new_item: Dict[str, Any] = {}
        for k, v in item.items():
            if isinstance(v, float):
                if not math.isfinite(v):
                    new_item[k] = None
                else:
                    new_item[k] = v
            else:
                new_item[k] = v
        sanitized.append(new_item)
    return sanitized


@router.get("/a/spot")
async def get_a_stock_spot():
    """获取A股实时行情"""
    try:
        data = get_json("market:a:spot")
        if not data:
            # 如果Redis没有，直接采集
            data = fetch_a_stock_spot()
        data = _sanitize_spot_data(data)
        return {"code": 0, "data": data, "message": "success"}
    except Exception as e:
        logger.error(f"获取A股行情失败: {e}", exc_info=True)
        return {"code": 1, "data": [], "message": str(e)}


@router.get("/hk/spot")
async def get_hk_stock_spot():
    """获取港股实时行情"""
    try:
        data = get_json("market:hk:spot")
        if not data:
            data = fetch_hk_stock_spot()
        data = _sanitize_spot_data(data)
        return {"code": 0, "data": data, "message": "success"}
    except Exception as e:
        logger.error(f"获取港股行情失败: {e}", exc_info=True)
        return {"code": 1, "data": [], "message": str(e)}


@router.get("/a/kline")
async def get_a_stock_kline(
    code: str = Query(..., description="股票代码"),
    period: str = Query("daily", description="周期: daily, weekly, monthly"),
    adjust: str = Query("", description="复权: '', qfq, hfq")
):
    """获取A股K线数据"""
    try:
        kline_data = fetch_a_stock_kline(code, period, adjust)
        return {"code": 0, "data": kline_data, "message": "success"}
    except Exception as e:
        logger.error(f"获取A股K线失败 {code}: {e}", exc_info=True)
        return {"code": 1, "data": [], "message": str(e)}


@router.get("/hk/kline")
async def get_hk_stock_kline(
    code: str = Query(..., description="股票代码"),
    period: str = Query("daily", description="周期")
):
    """获取港股K线数据"""
    try:
        kline_data = fetch_hk_stock_kline(code, period)
        return {"code": 0, "data": kline_data, "message": "success"}
    except Exception as e:
        logger.error(f"获取港股K线失败 {code}: {e}", exc_info=True)
        return {"code": 1, "data": [], "message": str(e)}


@router.get("/a/indicators")
async def get_a_stock_indicators(
    code: str = Query(..., description="股票代码")
):
    """获取A股技术指标"""
    try:
        # 获取K线数据
        kline_data = fetch_a_stock_kline(code, period="daily")
        if not kline_data:
            return {"code": 1, "data": {}, "message": "无法获取K线数据"}
        
        # 转换为DataFrame
        df = pd.DataFrame(kline_data)
        
        # 计算指标
        indicators = calculate_all_indicators(df)
        
        return {"code": 0, "data": indicators, "message": "success"}
    except Exception as e:
        logger.error(f"获取技术指标失败 {code}: {e}", exc_info=True)
        return {"code": 1, "data": {}, "message": str(e)}


@router.get("/search")
async def search_stock(
    keyword: str = Query(..., description="搜索关键词（代码或名称）")
):
    """搜索股票"""
    try:
        # 搜索A股
        a_stocks = get_json("market:a:spot") or []
        hk_stocks = get_json("market:hk:spot") or []
        
        all_stocks = a_stocks + hk_stocks
        
        # 过滤匹配的股票
        keyword = keyword.upper()
        results = [
            stock for stock in all_stocks
            if keyword in str(stock.get("code", "")).upper() or
               keyword in str(stock.get("name", "")).upper()
        ]
        
        return {"code": 0, "data": results[:50], "message": "success"}
    except Exception as e:
        logger.error(f"搜索股票失败: {e}", exc_info=True)
        return {"code": 1, "data": [], "message": str(e)}

