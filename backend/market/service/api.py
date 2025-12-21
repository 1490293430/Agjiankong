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


def _calculate_stock_heat(stock: Dict[str, Any]) -> float:
    """计算股票热度值
    
    热度计算方式：
    - 成交量（volume）
    - 成交额（amount）
    - 换手率（turnover）
    - 综合热度 = 成交量 * 成交额 / 1000000 + 换手率 * 100
    
    Args:
        stock: 股票数据字典
    
    Returns:
        热度值（数值越大越热）
    """
    volume = float(stock.get('volume', 0) or 0)
    amount = float(stock.get('amount', 0) or 0)
    turnover = float(stock.get('turnover', 0) or 0)  # 换手率（百分比）
    
    # 综合热度计算：
    # 1. 成交额权重（成交额越大说明资金关注度高）
    # 2. 成交量权重（成交量越大说明交易活跃）
    # 3. 换手率权重（换手率高说明交易活跃）
    heat = (amount / 1000000) * 0.5 + (volume / 10000) * 0.3 + (turnover * 10) * 0.2
    
    return heat


@router.get("/a/spot")
async def get_a_stock_spot(
    page: int = Query(1, ge=1, description="页码，从1开始"),
    page_size: int = Query(100, ge=1, le=500, description="每页数量，最大500")
):
    """获取A股实时行情（支持分页）"""
    try:
        data = get_json("market:a:spot")
        if not data:
            # 如果Redis没有数据，返回提示而不是阻塞等待采集
            return {
                "code": 0,
                "data": [],
                "pagination": {"page": 1, "page_size": page_size, "total": 0, "total_pages": 0},
                "message": "数据正在采集中，请稍后刷新..."
            }
        if not isinstance(data, list):
            logger.warning(f"A股行情数据格式错误: {type(data)}")
            return {
                "code": 1,
                "data": [],
                "pagination": {"page": 1, "page_size": page_size, "total": 0, "total_pages": 0},
                "message": "数据格式错误，请稍后刷新"
            }
        data = _sanitize_spot_data(data)
        
        # 按热度排序（最热的排最前）
        # 为每只股票计算热度值
        for stock in data:
            stock['_heat'] = _calculate_stock_heat(stock)
        
        # 按热度值降序排序
        data.sort(key=lambda x: x.get('_heat', 0), reverse=True)
        
        # 移除临时热度字段（不返回给前端）
        for stock in data:
            stock.pop('_heat', None)
        
        # 分页处理
        total = len(data)
        start = (page - 1) * page_size
        end = start + page_size
        paginated_data = data[start:end]
        
        return {
            "code": 0,
            "data": paginated_data,
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total": total,
                "total_pages": (total + page_size - 1) // page_size
            },
            "message": "success"
        }
    except Exception as e:
        logger.error(f"获取A股行情失败: {e}", exc_info=True)
        return {"code": 1, "data": [], "message": str(e)}


@router.get("/hk/spot")
async def get_hk_stock_spot(
    page: int = Query(1, ge=1, description="页码，从1开始"),
    page_size: int = Query(100, ge=1, le=500, description="每页数量，最大500")
):
    """获取港股实时行情（支持分页）"""
    try:
        data = get_json("market:hk:spot")
        if not data:
            # 如果Redis没有数据，返回提示而不是阻塞等待采集
            return {
                "code": 0,
                "data": [],
                "pagination": {"page": 1, "page_size": page_size, "total": 0, "total_pages": 0},
                "message": "数据正在采集中，请稍后刷新..."
            }
        if not isinstance(data, list):
            logger.warning(f"港股行情数据格式错误: {type(data)}")
            return {
                "code": 1,
                "data": [],
                "pagination": {"page": 1, "page_size": page_size, "total": 0, "total_pages": 0},
                "message": "数据格式错误，请稍后刷新"
            }
        data = _sanitize_spot_data(data)
        
        # 按热度排序（最热的排最前）
        # 为每只股票计算热度值
        for stock in data:
            stock['_heat'] = _calculate_stock_heat(stock)
        
        # 按热度值降序排序
        data.sort(key=lambda x: x.get('_heat', 0), reverse=True)
        
        # 移除临时热度字段（不返回给前端）
        for stock in data:
            stock.pop('_heat', None)
        
        # 分页处理
        total = len(data)
        start = (page - 1) * page_size
        end = start + page_size
        paginated_data = data[start:end]
        
        return {
            "code": 0,
            "data": paginated_data,
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total": total,
                "total_pages": (total + page_size - 1) // page_size
            },
            "message": "success"
        }
    except Exception as e:
        logger.error(f"获取港股行情失败: {e}", exc_info=True)
        return {"code": 1, "data": [], "message": str(e)}


@router.get("/a/kline")
async def get_a_stock_kline(
    code: str = Query(..., description="股票代码"),
    period: str = Query("daily", description="周期: daily, weekly, monthly, 1h/hourly(1小时K线)"),
    adjust: str = Query("", description="复权: '', qfq, hfq"),
    start_date: str | None = Query(None, description="开始日期 YYYYMMDD，可选"),
    end_date: str | None = Query(None, description="结束日期 YYYYMMDD，可选")
):
    """获取A股K线数据"""
    try:
        import numpy as np
        kline_data = fetch_a_stock_kline(code, period, adjust, start_date, end_date)
        # 将numpy类型转换为Python原生类型，避免序列化错误
        def convert_numpy_types(obj):
            if isinstance(obj, np.integer):
                return int(obj)
            elif isinstance(obj, np.floating):
                return float(obj)
            elif isinstance(obj, np.bool_):
                return bool(obj)
            elif isinstance(obj, dict):
                return {k: convert_numpy_types(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_numpy_types(item) for item in obj]
            return obj
        
        kline_data = convert_numpy_types(kline_data)
        return {"code": 0, "data": kline_data, "message": "success"}
    except Exception as e:
        logger.error(f"获取A股K线失败 {code}: {e}", exc_info=True)
        return {"code": 1, "data": [], "message": str(e)}


@router.get("/hk/kline")
async def get_hk_stock_kline(
    code: str = Query(..., description="股票代码"),
    period: str = Query("daily", description="周期"),
    start_date: str | None = Query(None, description="开始日期 YYYYMMDD，可选"),
    end_date: str | None = Query(None, description="结束日期 YYYYMMDD，可选")
):
    """获取港股K线数据（支持增量更新）"""
    try:
        import numpy as np
        kline_data = fetch_hk_stock_kline(code, period, "", start_date, end_date)
        # 将numpy类型转换为Python原生类型，避免序列化错误
        def convert_numpy_types(obj):
            if isinstance(obj, np.integer):
                return int(obj)
            elif isinstance(obj, np.floating):
                return float(obj)
            elif isinstance(obj, np.bool_):
                return bool(obj)
            elif isinstance(obj, dict):
                return {k: convert_numpy_types(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_numpy_types(item) for item in obj]
            return obj
        
        kline_data = convert_numpy_types(kline_data)
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
        import numpy as np
        # 获取K线数据
        kline_data = fetch_a_stock_kline(code, period="daily")
        if not kline_data:
            return {"code": 1, "data": {}, "message": "无法获取K线数据"}
        
        # 转换为DataFrame
        df = pd.DataFrame(kline_data)
        
        # 计算指标
        indicators = calculate_all_indicators(df)
        
        # 双重保险：确保所有numpy类型都转换为Python原生类型
        def convert_numpy_types(obj):
            if isinstance(obj, np.integer):
                return int(obj)
            elif isinstance(obj, np.floating):
                return float(obj)
            elif isinstance(obj, np.bool_):
                return bool(obj)
            elif isinstance(obj, dict):
                return {k: convert_numpy_types(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_numpy_types(item) for item in obj]
            elif pd.isna(obj):
                return None
            return obj
        
        indicators = convert_numpy_types(indicators)
        
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

