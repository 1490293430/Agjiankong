"""
交易时间判断工具
"""
from datetime import datetime, time
from typing import Tuple
import pytz


# 时区定义
TZ_SHANGHAI = pytz.timezone("Asia/Shanghai")  # A股时区
TZ_HONGKONG = pytz.timezone("Asia/Hong_Kong")  # 港股时区


def is_trading_day(dt: datetime, market: str = "A") -> bool:
    """判断是否为交易日（简单版本，不考虑节假日）
    
    Args:
        dt: 日期时间
        market: 市场类型，"A" 表示A股，"HK" 表示港股
    
    Returns:
        是否为交易日（周一到周五）
    """
    weekday = dt.weekday()  # 0=Monday, 6=Sunday
    return weekday < 5  # 周一到周五


def is_a_stock_trading_time(dt: datetime = None) -> bool:
    """判断A股是否在交易时间内
    
    A股交易时间：
    - 上午：9:30 - 11:30
    - 下午：13:00 - 15:00
    
    Args:
        dt: 日期时间，None则使用当前时间
    
    Returns:
        是否在交易时间内
    """
    if dt is None:
        dt = datetime.now(TZ_SHANGHAI)
    else:
        # 确保时区正确
        if dt.tzinfo is None:
            dt = TZ_SHANGHAI.localize(dt)
        else:
            dt = dt.astimezone(TZ_SHANGHAI)
    
    # 判断是否为交易日
    if not is_trading_day(dt, "A"):
        return False
    
    current_time = dt.time()
    
    # 上午交易时间：9:30 - 11:30
    morning_start = time(9, 30)
    morning_end = time(11, 30)
    
    # 下午交易时间：13:00 - 15:00
    afternoon_start = time(13, 0)
    afternoon_end = time(15, 0)
    
    return (morning_start <= current_time <= morning_end) or \
           (afternoon_start <= current_time <= afternoon_end)


def is_hk_stock_trading_time(dt: datetime = None) -> bool:
    """判断港股是否在交易时间内
    
    港股交易时间：
    - 上午：9:30 - 12:00
    - 下午：13:00 - 16:00
    
    Args:
        dt: 日期时间，None则使用当前时间
    
    Returns:
        是否在交易时间内
    """
    if dt is None:
        dt = datetime.now(TZ_HONGKONG)
    else:
        # 确保时区正确
        if dt.tzinfo is None:
            dt = TZ_HONGKONG.localize(dt)
        else:
            dt = dt.astimezone(TZ_HONGKONG)
    
    # 判断是否为交易日
    if not is_trading_day(dt, "HK"):
        return False
    
    current_time = dt.time()
    
    # 上午交易时间：9:30 - 12:00
    morning_start = time(9, 30)
    morning_end = time(12, 0)
    
    # 下午交易时间：13:00 - 16:00
    afternoon_start = time(13, 0)
    afternoon_end = time(16, 0)
    
    return (morning_start <= current_time <= morning_end) or \
           (afternoon_start <= current_time <= afternoon_end)


def is_any_market_trading() -> Tuple[bool, bool]:
    """判断A股和港股是否在交易时间内
    
    Returns:
        (is_a_trading, is_hk_trading)
    """
    return is_a_stock_trading_time(), is_hk_stock_trading_time()


def get_next_trading_start_time(market: str = "A") -> datetime:
    """获取下一个交易开始时间
    
    Args:
        market: 市场类型，"A" 或 "HK"
    
    Returns:
        下一个交易开始时间
    """
    now = datetime.now(TZ_SHANGHAI if market == "A" else TZ_HONGKONG)
    
    # 如果今天是交易日且在交易时间内，返回今天上午开始时间
    if is_trading_day(now, market):
        if market == "A":
            morning_start = now.replace(hour=9, minute=30, second=0, microsecond=0)
            afternoon_start = now.replace(hour=13, minute=0, second=0, microsecond=0)
        else:  # HK
            morning_start = now.replace(hour=9, minute=30, second=0, microsecond=0)
            afternoon_start = now.replace(hour=13, minute=0, second=0, microsecond=0)
        
        if now < morning_start:
            return morning_start
        elif now < afternoon_start:
            return afternoon_start
    
    # 找到下一个交易日
    days_ahead = 1
    while True:
        next_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
        from datetime import timedelta
        next_day += timedelta(days=days_ahead)
        
        if is_trading_day(next_day, market):
            if market == "A":
                return next_day.replace(hour=9, minute=30, second=0, microsecond=0)
            else:  # HK
                return next_day.replace(hour=9, minute=30, second=0, microsecond=0)
        
        days_ahead += 1
        if days_ahead > 7:  # 防止无限循环
            break
    
    # 默认返回明天9:30
    from datetime import timedelta
    return (now + timedelta(days=1)).replace(hour=9, minute=30, second=0, microsecond=0)






