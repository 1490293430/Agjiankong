"""
行情API服务
"""
from fastapi import APIRouter, Query, Body
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


def _apply_sort(data: List[Dict[str, Any]], sort: str) -> List[Dict[str, Any]]:
    """对行情数据应用排序
    
    Args:
        data: 行情数据列表
        sort: 排序方式
            - default: 按热度排序（默认）
            - pct_desc: 涨跌幅降序
            - pct_asc: 涨跌幅升序
            - volume_desc: 成交量降序
            - amount_desc: 成交额降序
            - turnover_desc: 换手率降序
    
    Returns:
        排序后的数据列表
    """
    if sort == 'pct_desc':
        data.sort(key=lambda x: float(x.get('pct', 0) or 0), reverse=True)
    elif sort == 'pct_asc':
        data.sort(key=lambda x: float(x.get('pct', 0) or 0), reverse=False)
    elif sort == 'volume_desc':
        data.sort(key=lambda x: float(x.get('volume', 0) or 0), reverse=True)
    elif sort == 'amount_desc':
        data.sort(key=lambda x: float(x.get('amount', 0) or 0), reverse=True)
    elif sort == 'turnover_desc':
        data.sort(key=lambda x: float(x.get('turnover', 0) or 0), reverse=True)
    else:
        # 默认按热度排序
        for stock in data:
            stock['_heat'] = _calculate_stock_heat(stock)
        data.sort(key=lambda x: x.get('_heat', 0), reverse=True)
        for stock in data:
            stock.pop('_heat', None)
    
    return data


@router.get("/search")
async def search_stocks(
    q: str = Query(..., min_length=1, description="搜索关键词（股票代码或名称）"),
    market: str = Query("all", description="市场：all/A/HK")
):
    """搜索股票（根据代码或名称模糊匹配）
    
    用于智能导入自选股等场景，支持：
    - 股票代码精确匹配
    - 股票名称模糊匹配（包含即可）
    - 去掉ST前缀后匹配
    """
    try:
        results = []
        
        # 清理搜索词
        q = q.strip()
        # 去掉特殊分隔符
        q_clean = ''.join(c for c in q if c.isalnum() or '\u4e00' <= c <= '\u9fa5')
        
        # 获取股票数据
        markets_to_search = []
        if market in ["all", "A"]:
            a_data = get_json("market:a:spot") or []
            markets_to_search.append(("A", a_data))
        if market in ["all", "HK"]:
            hk_data = get_json("market:hk:spot") or []
            markets_to_search.append(("HK", hk_data))
        
        for mkt, data in markets_to_search:
            if not isinstance(data, list):
                continue
            for item in data:
                code = str(item.get('code', '')).strip()
                name = str(item.get('name', '')).strip()
                
                # 去掉ST前缀
                pure_name = name
                for prefix in ['*ST', 'ST', 'N', 'C', 'XD', 'XR', 'DR']:
                    if pure_name.startswith(prefix):
                        pure_name = pure_name[len(prefix):].strip()
                        break
                
                matched = False
                
                # 代码匹配
                if code and code in q_clean:
                    matched = True
                
                # 名称匹配（完整名称或去ST后的名称）
                if not matched and name and name in q_clean:
                    matched = True
                if not matched and pure_name and len(pure_name) >= 2 and pure_name in q_clean:
                    matched = True
                
                # 名称前缀匹配（2-4个字）
                if not matched and pure_name:
                    for length in range(min(4, len(pure_name)), 1, -1):
                        prefix = pure_name[:length]
                        if prefix in q_clean:
                            # 排除常见前缀
                            common = ['中国', '中信', '中金', '华夏', '国泰', '招商', '平安', '工商', '建设', '农业', '上海', '北京', '深圳', '广州']
                            if prefix not in common:
                                matched = True
                                break
                
                if matched:
                    results.append({
                        "code": code,
                        "name": name,
                        "market": mkt
                    })
        
        return {
            "code": 0,
            "data": results,
            "message": f"找到 {len(results)} 只股票"
        }
    except Exception as e:
        logger.error(f"搜索股票失败: {e}", exc_info=True)
        return {"code": 1, "data": [], "message": str(e)}


@router.get("/a/spot")
async def get_a_stock_spot(
    page: int = Query(1, ge=1, description="页码，从1开始"),
    page_size: int = Query(100, ge=1, le=500, description="每页数量，最大500"),
    stock_only: bool = Query(False, description="是否仅显示股票（排除ETF/指数）"),
    sort: str = Query("default", description="排序方式: default/pct_desc/pct_asc/volume_desc/amount_desc/turnover_desc")
):
    """获取A股实时行情（支持分页和排序）"""
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
        
        # 如果启用了"仅显示股票"，先过滤数据
        if stock_only:
            data = [item for item in data if item.get('sec_type') == 'stock']
        
        # 应用排序（对全量数据排序后再分页）
        data = _apply_sort(data, sort)
        
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
    page_size: int = Query(100, ge=1, le=500, description="每页数量，最大500"),
    stock_only: bool = Query(False, description="是否仅显示股票（排除ETF/指数）"),
    sort: str = Query("default", description="排序方式: default/pct_desc/pct_asc/volume_desc/amount_desc/turnover_desc")
):
    """获取港股实时行情（支持分页和排序）"""
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
        
        # 如果启用了"仅显示股票"，先过滤数据
        if stock_only:
            data = [item for item in data if item.get('sec_type') == 'stock']
        
        # 应用排序（对全量数据排序后再分页）
        data = _apply_sort(data, sort)
        
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


@router.post("/spot/batch")
async def get_stocks_by_codes(
    codes: List[str] = Body(..., description="股票代码列表")
):
    """根据股票代码列表批量查询行情数据（用于自选股等场景）
    
    Args:
        codes: 股票代码列表，如 ["000001", "600519", "00700"]
    
    Returns:
        匹配的股票行情数据列表
    """
    try:
        if not codes or len(codes) == 0:
            return {"code": 0, "data": [], "message": "success"}
        
        # 标准化代码（去除空格，转为字符串）
        codes_set = {str(c).strip() for c in codes if str(c).strip()}
        if not codes_set:
            return {"code": 0, "data": [], "message": "success"}
        
        # 获取所有行情数据
        a_stocks = get_json("market:a:spot") or []
        hk_stocks = get_json("market:hk:spot") or []
        
        all_stocks = a_stocks + hk_stocks
        
        # 根据代码筛选（保持codes列表的顺序）
        # 使用字典快速查找
        stock_map = {}
        for stock in all_stocks:
            stock_code = str(stock.get("code", "")).strip()
            if stock_code:
                stock_map[stock_code] = stock
        
        # 按照codes的顺序返回结果
        results = []
        for code in codes:
            code_str = str(code).strip()
            if code_str in stock_map:
                results.append(stock_map[code_str])
        
        # 清理数据
        results = _sanitize_spot_data(results)
        
        return {"code": 0, "data": results, "message": "success"}
    except Exception as e:
        logger.error(f"批量查询股票行情失败: {e}", exc_info=True)
        return {"code": 1, "data": [], "message": str(e)}


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

