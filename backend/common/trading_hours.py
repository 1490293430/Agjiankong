"""
交易时间判断工具（实时获取）
"""
from datetime import datetime, time
from typing import Tuple, Optional
import pytz
from common.logger import get_logger

logger = get_logger(__name__)

# 时区定义
TZ_SHANGHAI = pytz.timezone("Asia/Shanghai")  # A股时区
TZ_HONGKONG = pytz.timezone("Asia/Hong_Kong")  # 港股时区

# 缓存交易状态，避免频繁请求（缓存5秒）
_trading_status_cache = {
    "a": {"status": None, "timestamp": None},
    "hk": {"status": None, "timestamp": None}
}
_cache_ttl = 5  # 缓存5秒


def _check_market_status(market: str) -> Optional[bool]:
    """通过检查Redis缓存中的行情数据来判断市场是否在交易
    
    使用缓存数据判断，避免频繁调用akshare（可能很慢）
    
    Args:
        market: 市场类型，"A" 或 "HK"
    
    Returns:
        True: 在交易时间内
        False: 不在交易时间内
        None: 无法判断（缓存数据不可用）
    """
    try:
        from common.redis import get_json, get_redis
        from datetime import datetime
        
        # 优先检查独立的时间戳key（更可靠）
        time_key = "market:a:time" if market == "A" else "market:hk:time"
        redis_client = get_redis()
        update_time_str = redis_client.get(time_key)
        
        if update_time_str:
            try:
                # 解析更新时间
                if isinstance(update_time_str, str):
                    if 'T' in update_time_str:
                        update_time = datetime.fromisoformat(update_time_str.replace('Z', '+00:00'))
                    else:
                        update_time = datetime.fromisoformat(update_time_str)
                else:
                    update_time = update_time_str
                
                # 处理时区
                if update_time.tzinfo is None:
                    tz = TZ_SHANGHAI if market == "A" else TZ_HONGKONG
                    update_time = tz.localize(update_time)
                
                now = datetime.now(update_time.tzinfo)
                time_diff = (now - update_time).total_seconds()
                
                logger.debug(f"市场{market}缓存时间差: {time_diff:.0f}秒")
                
                # 如果缓存数据在最近5分钟内更新，认为在交易时间
                if time_diff < 300:
                    return True
                # 如果缓存数据超过30分钟未更新，认为不在交易时间
                elif time_diff > 1800:
                    return False
                # 介于5-30分钟之间，返回None（不确定）
                else:
                    return None
            except Exception as e:
                logger.debug(f"解析时间戳失败 {market}: {e}")
        
        # 如果没有独立时间戳，尝试从数据中获取
        cache_key = "market:a:spot" if market == "A" else "market:hk:spot"
        cached_data = get_json(cache_key)
        
        # 如果缓存中有数据，检查更新时间
        if cached_data and isinstance(cached_data, list) and len(cached_data) > 0:
            # 获取第一条数据的时间戳
            first_item = cached_data[0]
            if isinstance(first_item, dict):
                update_time_str = first_item.get("update_time")
                
                if update_time_str:
                    try:
                        # 解析更新时间
                        if isinstance(update_time_str, str):
                            if 'T' in update_time_str:
                                update_time = datetime.fromisoformat(update_time_str.replace('Z', '+00:00'))
                            else:
                                update_time = datetime.fromisoformat(update_time_str)
                        else:
                            update_time = update_time_str
                        
                        # 处理时区
                        if update_time.tzinfo is None:
                            tz = TZ_SHANGHAI if market == "A" else TZ_HONGKONG
                            update_time = tz.localize(update_time)
                        
                        now = datetime.now(update_time.tzinfo)
                        time_diff = (now - update_time).total_seconds()
                        
                        logger.debug(f"市场{market}数据时间差: {time_diff:.0f}秒")
                        
                        # 如果缓存数据在最近5分钟内更新，认为在交易时间
                        if time_diff < 300:
                            return True
                        # 如果缓存数据超过30分钟未更新，认为不在交易时间
                        elif time_diff > 1800:
                            return False
                        # 介于5-30分钟之间，返回None（不确定）
                        else:
                            return None
                    except Exception as e:
                        logger.debug(f"解析缓存时间失败 {market}: {e}")
        
        # 如果没有缓存数据，返回None（不确定）
        logger.debug(f"市场{market}无缓存数据")
        return None
        
    except Exception as e:
        logger.warning(f"检查{market}市场状态失败: {e}", exc_info=True)
        return None


def _fallback_time_check(market: str) -> bool:
    """使用常规交易时间窗口兜底判断
    
    场景：Redis无数据或数据过期时，避免永远判定为收盘。
    """
    tz = TZ_SHANGHAI if market == "A" else TZ_HONGKONG
    now = datetime.now(tz)
    
    # 周末休市
    if now.weekday() >= 5:
        return False
    
    if market == "A":
        windows = [
            (time(9, 30), time(11, 30)),
            (time(13, 0), time(15, 0)),
        ]
    else:  # HK
        # 港股收盘竞价通常到 16:10，取 16:10 作为兜底
        windows = [
            (time(9, 30), time(12, 0)),
            (time(13, 0), time(16, 10)),
        ]
    
    current = now.time()
    return any(start <= current <= end for start, end in windows)


def is_a_stock_trading_time(dt: datetime = None) -> bool:
    """实时判断A股是否在交易时间内
    
    通过尝试获取实时行情数据来判断：
    - 如果能获取到有效的实时数据，说明在交易时间内
    - 如果获取不到或数据无效，说明不在交易时间内
    
    Args:
        dt: 日期时间，None则使用当前时间（此参数保留兼容性，实际使用当前时间）
    
    Returns:
        是否在交易时间内
    """
    global _trading_status_cache
    
    # 检查缓存
    now = datetime.now()
    cache_key = "a"
    cache_entry = _trading_status_cache[cache_key]
    
    if cache_entry["timestamp"] and (now - cache_entry["timestamp"]).total_seconds() < _cache_ttl:
        return cache_entry["status"] if cache_entry["status"] is not None else False
    
    # 实时检查（优先使用实时行情更新时间）
    status = _check_market_status("A")
    if status is None:
        status = _fallback_time_check("A")
    
    # 更新缓存
    _trading_status_cache[cache_key] = {
        "status": status,
        "timestamp": now
    }
    
    return status if status is not None else False


def is_hk_stock_trading_time(dt: datetime = None) -> bool:
    """实时判断港股是否在交易时间内
    
    通过尝试获取实时行情数据来判断：
    - 如果能获取到有效的实时数据，说明在交易时间内
    - 如果获取不到或数据无效，说明不在交易时间内
    
    Args:
        dt: 日期时间，None则使用当前时间（此参数保留兼容性，实际使用当前时间）
    
    Returns:
        是否在交易时间内
    """
    global _trading_status_cache
    
    # 检查缓存
    now = datetime.now()
    cache_key = "hk"
    cache_entry = _trading_status_cache[cache_key]
    
    if cache_entry["timestamp"] and (now - cache_entry["timestamp"]).total_seconds() < _cache_ttl:
        return cache_entry["status"] if cache_entry["status"] is not None else False
    
    # 实时检查（优先使用实时行情更新时间）
    status = _check_market_status("HK")
    if status is None:
        status = _fallback_time_check("HK")
    
    # 更新缓存
    _trading_status_cache[cache_key] = {
        "status": status,
        "timestamp": now
    }
    
    return status if status is not None else False


def is_any_market_trading() -> Tuple[bool, bool]:
    """实时判断A股和港股是否在交易时间内
    
    Returns:
        (is_a_trading, is_hk_trading)
    """
    return is_a_stock_trading_time(), is_hk_stock_trading_time()


def get_next_trading_start_time(market: str = "A") -> datetime:
    """获取下一个可能的交易开始时间（估算）
    
    由于采用实时判断，此函数仅提供估算值
    实际交易时间判断应使用 is_a_stock_trading_time 或 is_hk_stock_trading_time
    
    Args:
        market: 市场类型，"A" 或 "HK"
    
    Returns:
        估算的下一个交易开始时间
    """
    from datetime import timedelta
    now = datetime.now(TZ_SHANGHAI if market == "A" else TZ_HONGKONG)
    
    # 如果当前在交易时间内，返回下一个可能的交易开始时间（下午或明天）
    if market == "A":
        morning_start = now.replace(hour=9, minute=30, second=0, microsecond=0)
        afternoon_start = now.replace(hour=13, minute=0, second=0, microsecond=0)
        afternoon_end = now.replace(hour=15, minute=0, second=0, microsecond=0)
    else:  # HK
        morning_start = now.replace(hour=9, minute=30, second=0, microsecond=0)
        afternoon_start = now.replace(hour=13, minute=0, second=0, microsecond=0)
        afternoon_end = now.replace(hour=16, minute=0, second=0, microsecond=0)
    
    # 如果还没到上午开始时间
    if now < morning_start:
        return morning_start
    # 如果还在上午，返回下午开始时间
    elif now < afternoon_start:
        return afternoon_start
    # 如果已经过了收盘时间，返回明天上午
    elif now >= afternoon_end:
        next_day = (now + timedelta(days=1)).replace(hour=9, minute=30, second=0, microsecond=0)
        return next_day
    
    # 默认返回明天上午
    return (now + timedelta(days=1)).replace(hour=9, minute=30, second=0, microsecond=0)
