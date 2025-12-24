"""
快照转日K线模块

收盘后将实时快照数据转换为当日K线并入库
- A股：15:30 后执行（收盘时间15:00，延后30分钟确保数据完整）
- 港股：16:30 后执行（收盘时间16:00，延后30分钟确保数据完整）
"""
from datetime import datetime, date, time
from typing import List, Dict, Any, Optional
import pytz

from common.logger import get_logger
from common.redis import get_json, set_json, get_redis
from common.db import save_kline_data
from common.trading_hours import is_trading_day, TZ_SHANGHAI, TZ_HONGKONG

logger = get_logger(__name__)

# Redis key 用于记录已转换的日期
KLINE_CONVERTED_KEY_A = "kline:converted:a"
KLINE_CONVERTED_KEY_HK = "kline:converted:hk"

# 收盘后延迟时间（分钟）- 实际执行时间
# A股15:00收盘，15:12后执行
# 港股16:00收盘，16:22后执行
A_STOCK_CONVERT_TIME = time(15, 12)
HK_STOCK_CONVERT_TIME = time(16, 22)


def _get_converted_date(market: str) -> Optional[str]:
    """获取已转换的日期"""
    key = KLINE_CONVERTED_KEY_A if market == "A" else KLINE_CONVERTED_KEY_HK
    try:
        return get_redis().get(key)
    except Exception:
        return None


def _set_converted_date(market: str, date_str: str):
    """设置已转换的日期"""
    key = KLINE_CONVERTED_KEY_A if market == "A" else KLINE_CONVERTED_KEY_HK
    try:
        # 保存7天，避免重复转换
        get_redis().set(key, date_str, ex=7 * 24 * 3600)
    except Exception as e:
        logger.warning(f"设置已转换日期失败: {e}")


def should_convert_snapshot(market: str) -> bool:
    """判断是否应该执行快照转K线
    
    条件：
    1. 今天是交易日
    2. 当前时间已过收盘延迟时间（A股15:30，港股16:30）
    3. 今天还没有转换过
    """
    tz = TZ_SHANGHAI if market == "A" else TZ_HONGKONG
    now = datetime.now(tz)
    today = now.date()
    today_str = today.strftime("%Y%m%d")
    
    # 检查是否是交易日
    if not is_trading_day(market, today):
        return False
    
    # 检查是否已过收盘延迟时间
    if market == "A":
        convert_time = A_STOCK_CONVERT_TIME  # 15:02
    else:
        convert_time = HK_STOCK_CONVERT_TIME  # 16:12
    
    if now.time() < convert_time:
        return False
    
    # 检查今天是否已转换
    converted_date = _get_converted_date(market)
    if converted_date and converted_date.decode() if isinstance(converted_date, bytes) else converted_date == today_str:
        return False
    
    return True


def convert_snapshot_to_kline(market: str) -> Dict[str, Any]:
    """将快照数据转换为日K线并入库
    
    Args:
        market: "A" 或 "HK"
    
    Returns:
        {
            "success": bool,
            "count": int,  # 转换的股票数量
            "message": str
        }
    """
    tz = TZ_SHANGHAI if market == "A" else TZ_HONGKONG
    now = datetime.now(tz)
    today = now.date()
    today_str = today.strftime("%Y%m%d")
    today_formatted = today.strftime("%Y-%m-%d")
    
    logger.info(f"[{market}] 开始将快照转换为日K线: {today_formatted}")
    
    # 获取快照数据
    redis_key = f"market:{market.lower()}:spot"
    snapshot_data = get_json(redis_key)
    
    if not snapshot_data:
        logger.warning(f"[{market}] 快照数据为空，无法转换")
        return {"success": False, "count": 0, "message": "快照数据为空"}
    
    # 转换为K线格式
    kline_data = []
    skipped = 0
    
    for item in snapshot_data:
        try:
            code = str(item.get("code", "")).strip()
            if not code:
                skipped += 1
                continue
            
            # 获取价格数据
            open_price = float(item.get("open", 0) or 0)
            high_price = float(item.get("high", 0) or 0)
            low_price = float(item.get("low", 0) or 0)
            close_price = float(item.get("price", 0) or item.get("close", 0) or 0)
            volume = float(item.get("volume", 0) or 0)
            amount = float(item.get("amount", 0) or 0)
            
            # 跳过无效数据（价格为0或成交量为0的可能是停牌股）
            if close_price <= 0:
                skipped += 1
                continue
            
            # 如果开盘价为0，使用收盘价
            if open_price <= 0:
                open_price = close_price
            if high_price <= 0:
                high_price = close_price
            if low_price <= 0:
                low_price = close_price
            
            kline_item = {
                "code": code,
                "date": today_formatted,
                "open": open_price,
                "high": high_price,
                "low": low_price,
                "close": close_price,
                "volume": volume,
                "amount": amount,
                "market": market
            }
            kline_data.append(kline_item)
            
        except Exception as e:
            logger.debug(f"[{market}] 转换股票数据失败: {e}")
            skipped += 1
            continue
    
    if not kline_data:
        logger.warning(f"[{market}] 没有有效的K线数据可转换")
        return {"success": False, "count": 0, "message": "没有有效数据"}
    
    # 保存到数据库
    try:
        success = save_kline_data(kline_data, "daily")
        if success:
            # 标记今天已转换
            _set_converted_date(market, today_str)
            logger.info(f"[{market}] 快照转K线完成: 成功={len(kline_data)}只, 跳过={skipped}只")
            return {
                "success": True,
                "count": len(kline_data),
                "message": f"成功转换{len(kline_data)}只股票的日K线"
            }
        else:
            logger.error(f"[{market}] 保存K线数据失败")
            return {"success": False, "count": 0, "message": "保存数据库失败"}
    except Exception as e:
        logger.error(f"[{market}] 保存K线数据异常: {e}")
        return {"success": False, "count": 0, "message": str(e)}


def auto_convert_snapshot_to_kline():
    """自动检查并执行快照转K线（由定时任务调用）"""
    results = {}
    
    # 检查A股
    if should_convert_snapshot("A"):
        logger.info("[A股] 满足转换条件，开始执行快照转K线")
        results["A"] = convert_snapshot_to_kline("A")
    else:
        results["A"] = {"success": True, "count": 0, "message": "不需要转换"}
    
    # 检查港股
    if should_convert_snapshot("HK"):
        logger.info("[港股] 满足转换条件，开始执行快照转K线")
        results["HK"] = convert_snapshot_to_kline("HK")
    else:
        results["HK"] = {"success": True, "count": 0, "message": "不需要转换"}
    
    return results
