"""
交易时间判断工具（基于交易日历 + 固定交易时间）

使用 akshare 获取A股交易日历，港股使用固定节假日列表
"""
from datetime import datetime, date, time, timedelta
from typing import Tuple, Optional, Dict, Any
import pytz
from common.logger import get_logger
from common.redis import get_json, set_json

logger = get_logger(__name__)

# 时区定义
TZ_SHANGHAI = pytz.timezone("Asia/Shanghai")  # A股时区
TZ_HONGKONG = pytz.timezone("Asia/Hong_Kong")  # 港股时区

# A股交易时间段（延长到15:12，确保收盘数据完整）
A_STOCK_TRADING_WINDOWS = [
    (time(9, 30), time(11, 30)),   # 上午
    (time(13, 0), time(15, 12)),   # 下午（延长12分钟）
]

# 港股交易时间段（延长到16:22，确保收盘数据完整）
HK_STOCK_TRADING_WINDOWS = [
    (time(9, 30), time(12, 0)),    # 上午
    (time(13, 0), time(16, 22)),   # 下午（延长22分钟）
]

# 交易日历缓存（Redis key）
A_STOCK_CALENDAR_KEY = "trading:calendar:a"
HK_STOCK_CALENDAR_KEY = "trading:calendar:hk"

# 内存缓存（避免频繁读取Redis）
_calendar_cache: Dict[str, Any] = {
    "a": {"data": None, "timestamp": None},
    "hk": {"data": None, "timestamp": None}
}
_cache_ttl = 3600  # 内存缓存1小时


def _fetch_a_stock_calendar() -> list:
    """从 akshare 获取A股交易日历
    
    Returns:
        交易日期列表，格式：['20240101', '20240102', ...]
    """
    try:
        import akshare as ak
        logger.info("正在从 akshare 获取A股交易日历...")
        
        # 获取新浪的交易日历
        df = ak.tool_trade_date_hist_sina()
        
        if df is not None and not df.empty:
            # 转换为日期字符串列表
            trade_dates = []
            for _, row in df.iterrows():
                trade_date = row.get('trade_date')
                if trade_date:
                    # 转换为字符串格式 YYYYMMDD
                    if hasattr(trade_date, 'strftime'):
                        trade_dates.append(trade_date.strftime('%Y%m%d'))
                    else:
                        date_str = str(trade_date).replace('-', '')[:8]
                        if len(date_str) == 8:
                            trade_dates.append(date_str)
            
            logger.info(f"获取A股交易日历成功，共 {len(trade_dates)} 个交易日")
            return sorted(trade_dates)
        
        logger.warning("akshare 返回空数据")
        return []
        
    except Exception as e:
        logger.error(f"获取A股交易日历失败: {e}")
        return []


def _fetch_hk_stock_calendar() -> list:
    """获取港股交易日历
    
    港股交易日历较难直接获取，使用以下策略：
    1. 港股通常与A股交易日相近，但有些差异
    2. 港股特有的休市日：圣诞节、复活节、佛诞等
    
    Returns:
        交易日期列表
    """
    try:
        # 港股交易日历可以通过 yfinance 间接获取，但较慢
        # 这里使用简化策略：基于A股日历，排除港股特有休市日
        
        # 先获取A股日历作为基础
        a_calendar = _get_a_stock_calendar_from_cache()
        if not a_calendar:
            a_calendar = _fetch_a_stock_calendar()
        
        if not a_calendar:
            return []
        
        # 港股特有休市日（每年需要更新）
        # 2024-2025年港股特有休市日
        hk_only_holidays = {
            # 2024年
            '20240101',  # 元旦
            '20240212',  # 农历年初三
            '20240329',  # 耶稣受难节
            '20240330',  # 耶稣受难节翌日
            '20240401',  # 复活节星期一
            '20240404',  # 清明节
            '20240501',  # 劳动节
            '20240515',  # 佛诞
            '20240610',  # 端午节
            '20240701',  # 香港特别行政区成立纪念日
            '20240918',  # 中秋节翌日
            '20241001',  # 国庆日
            '20241011',  # 重阳节
            '20241225',  # 圣诞节
            '20241226',  # 圣诞节后第一个周日
            # 2025年
            '20250101',  # 元旦
            '20250129',  # 农历年初一
            '20250130',  # 农历年初二
            '20250131',  # 农历年初三
            '20250404',  # 清明节
            '20250418',  # 耶稣受难节
            '20250419',  # 耶稣受难节翌日
            '20250421',  # 复活节星期一
            '20250501',  # 劳动节
            '20250505',  # 佛诞
            '20250531',  # 端午节
            '20250701',  # 香港特别行政区成立纪念日
            '20251001',  # 国庆日
            '20251007',  # 中秋节翌日
            '20251029',  # 重阳节
            '20251225',  # 圣诞节
            '20251226',  # 圣诞节后第一个周日
        }
        
        # 从A股日历中排除港股特有休市日
        hk_calendar = [d for d in a_calendar if d not in hk_only_holidays]
        
        logger.info(f"生成港股交易日历，共 {len(hk_calendar)} 个交易日")
        return hk_calendar
        
    except Exception as e:
        logger.error(f"获取港股交易日历失败: {e}")
        return []


def _get_a_stock_calendar_from_cache() -> list:
    """从缓存获取A股交易日历"""
    global _calendar_cache
    
    now = datetime.now()
    cache = _calendar_cache["a"]
    
    # 检查内存缓存
    if cache["data"] and cache["timestamp"]:
        if (now - cache["timestamp"]).total_seconds() < _cache_ttl:
            logger.debug(f"A股日历命中内存缓存，共{len(cache['data'])}天")
            return cache["data"]
    
    # 从Redis获取
    try:
        redis_data = get_json(A_STOCK_CALENDAR_KEY)
        if redis_data and isinstance(redis_data, list) and len(redis_data) > 0:
            _calendar_cache["a"] = {"data": redis_data, "timestamp": now}
            logger.info(f"A股日历从Redis加载成功，共{len(redis_data)}天")
            return redis_data
        else:
            logger.debug(f"Redis中无A股日历数据: {type(redis_data)}")
    except Exception as e:
        logger.warning(f"从Redis获取A股日历失败: {e}")
    
    return []


def _get_hk_stock_calendar_from_cache() -> list:
    """从缓存获取港股交易日历"""
    global _calendar_cache
    
    now = datetime.now()
    cache = _calendar_cache["hk"]
    
    # 检查内存缓存
    if cache["data"] and cache["timestamp"]:
        if (now - cache["timestamp"]).total_seconds() < _cache_ttl:
            logger.debug(f"港股日历命中内存缓存，共{len(cache['data'])}天")
            return cache["data"]
    
    # 从Redis获取
    try:
        redis_data = get_json(HK_STOCK_CALENDAR_KEY)
        if redis_data and isinstance(redis_data, list) and len(redis_data) > 0:
            _calendar_cache["hk"] = {"data": redis_data, "timestamp": now}
            logger.info(f"港股日历从Redis加载成功，共{len(redis_data)}天")
            return redis_data
        else:
            logger.debug(f"Redis中无港股日历数据: {type(redis_data)}")
    except Exception as e:
        logger.warning(f"从Redis获取港股日历失败: {e}")
    
    return []


def refresh_trading_calendar(market: str = "ALL") -> bool:
    """刷新交易日历缓存
    
    Args:
        market: "A", "HK", 或 "ALL"
    
    Returns:
        是否成功
    """
    global _calendar_cache
    success = True
    
    if market in ["A", "ALL"]:
        try:
            logger.info("开始从akshare获取A股交易日历...")
            a_calendar = _fetch_a_stock_calendar()
            if a_calendar:
                # 保存到Redis
                save_result = set_json(A_STOCK_CALENDAR_KEY, a_calendar, ex=86400 * 7)  # 缓存7天
                if save_result:
                    _calendar_cache["a"] = {"data": a_calendar, "timestamp": datetime.now()}
                    logger.info(f"A股交易日历已保存到Redis，共 {len(a_calendar)} 个交易日")
                    
                    # 验证保存是否成功
                    verify = get_json(A_STOCK_CALENDAR_KEY)
                    if verify and len(verify) == len(a_calendar):
                        logger.info(f"A股交易日历验证成功")
                    else:
                        logger.warning(f"A股交易日历验证失败: 保存{len(a_calendar)}，读取{len(verify) if verify else 0}")
                else:
                    logger.error("A股交易日历保存到Redis失败")
                    success = False
            else:
                logger.warning("从akshare获取A股交易日历返回空数据")
                success = False
        except Exception as e:
            logger.error(f"刷新A股交易日历失败: {e}", exc_info=True)
            success = False
    
    if market in ["HK", "ALL"]:
        try:
            logger.info("开始生成港股交易日历...")
            hk_calendar = _fetch_hk_stock_calendar()
            if hk_calendar:
                save_result = set_json(HK_STOCK_CALENDAR_KEY, hk_calendar, ex=86400 * 7)  # 缓存7天
                if save_result:
                    _calendar_cache["hk"] = {"data": hk_calendar, "timestamp": datetime.now()}
                    logger.info(f"港股交易日历已保存到Redis，共 {len(hk_calendar)} 个交易日")
                else:
                    logger.error("港股交易日历保存到Redis失败")
                    success = False
            else:
                logger.warning("生成港股交易日历返回空数据")
                success = False
        except Exception as e:
            logger.error(f"刷新港股交易日历失败: {e}", exc_info=True)
            success = False
    
    return success


def is_trading_day(market: str, check_date: date = None) -> bool:
    """判断指定日期是否为交易日
    
    Args:
        market: "A" 或 "HK"
        check_date: 要检查的日期，None则使用今天
    
    Returns:
        是否为交易日
    """
    if check_date is None:
        tz = TZ_SHANGHAI if market == "A" else TZ_HONGKONG
        check_date = datetime.now(tz).date()
    
    date_str = check_date.strftime('%Y%m%d')
    
    # 获取交易日历（只从缓存获取，不触发网络请求）
    if market == "A":
        calendar = _get_a_stock_calendar_from_cache()
        if not calendar:
            # 后台异步刷新，不阻塞当前请求
            _trigger_async_refresh("A")
    else:
        calendar = _get_hk_stock_calendar_from_cache()
        if not calendar:
            _trigger_async_refresh("HK")
    
    # 如果没有日历数据，使用简单的周末判断
    if not calendar:
        return check_date.weekday() < 5  # 周一到周五
    
    return date_str in calendar


# 异步刷新标志，避免重复触发
_async_refresh_triggered = {"A": False, "HK": False}

def _trigger_async_refresh(market: str):
    """触发后台异步刷新交易日历"""
    global _async_refresh_triggered
    
    if _async_refresh_triggered.get(market):
        return  # 已经触发过，不重复
    
    _async_refresh_triggered[market] = True
    
    import threading
    def do_refresh():
        try:
            refresh_trading_calendar(market)
            logger.info(f"{market}交易日历后台刷新完成")
        except Exception as e:
            logger.error(f"{market}交易日历后台刷新失败: {e}")
        finally:
            _async_refresh_triggered[market] = False
    
    thread = threading.Thread(target=do_refresh, daemon=True)
    thread.start()
    logger.info(f"已触发{market}交易日历后台刷新")


def is_in_trading_hours(market: str, check_time: time = None) -> bool:
    """判断指定时间是否在交易时间段内
    
    Args:
        market: "A" 或 "HK"
        check_time: 要检查的时间，None则使用当前时间
    
    Returns:
        是否在交易时间段内
    """
    if check_time is None:
        tz = TZ_SHANGHAI if market == "A" else TZ_HONGKONG
        check_time = datetime.now(tz).time()
    
    windows = A_STOCK_TRADING_WINDOWS if market == "A" else HK_STOCK_TRADING_WINDOWS
    
    for start, end in windows:
        if start <= check_time <= end:
            return True
    
    return False


def is_a_stock_trading_time(dt: datetime = None) -> bool:
    """判断A股是否在交易时间内
    
    Args:
        dt: 日期时间，None则使用当前时间
    
    Returns:
        是否在交易时间内
    """
    if dt is None:
        dt = datetime.now(TZ_SHANGHAI)
    elif dt.tzinfo is None:
        dt = TZ_SHANGHAI.localize(dt)
    
    # 1. 检查是否为交易日
    if not is_trading_day("A", dt.date()):
        return False
    
    # 2. 检查是否在交易时间段
    return is_in_trading_hours("A", dt.time())


def is_hk_stock_trading_time(dt: datetime = None) -> bool:
    """判断港股是否在交易时间内
    
    Args:
        dt: 日期时间，None则使用当前时间
    
    Returns:
        是否在交易时间内
    """
    if dt is None:
        dt = datetime.now(TZ_HONGKONG)
    elif dt.tzinfo is None:
        dt = TZ_HONGKONG.localize(dt)
    
    # 1. 检查是否为交易日
    if not is_trading_day("HK", dt.date()):
        return False
    
    # 2. 检查是否在交易时间段
    return is_in_trading_hours("HK", dt.time())


def is_any_market_trading() -> Tuple[bool, bool]:
    """判断A股和港股是否在交易时间内
    
    Returns:
        (is_a_trading, is_hk_trading)
    """
    return is_a_stock_trading_time(), is_hk_stock_trading_time()


def _get_next_trading_datetime_fast(market: str) -> Optional[datetime]:
    """快速获取下一个交易开始时间（不触发网络请求）
    
    使用已有缓存，如果没有缓存则使用简单的周末判断
    
    Args:
        market: "A" 或 "HK"
    
    Returns:
        下一个交易开始时间（带时区），如果无法确定返回None
    """
    tz = TZ_SHANGHAI if market == "A" else TZ_HONGKONG
    now = datetime.now(tz)
    windows = A_STOCK_TRADING_WINDOWS if market == "A" else HK_STOCK_TRADING_WINDOWS
    
    morning_start = windows[0][0]  # 上午开盘时间
    afternoon_start = windows[1][0]  # 下午开盘时间
    afternoon_end = windows[1][1]  # 下午收盘时间
    
    current_time = now.time()
    current_date = now.date()
    
    # 只从缓存获取日历，不触发网络请求
    if market == "A":
        calendar = _get_a_stock_calendar_from_cache()
    else:
        calendar = _get_hk_stock_calendar_from_cache()
    
    # 如果今天是交易日（或没有日历时假设工作日是交易日）
    today_str = current_date.strftime('%Y%m%d')
    is_today_trading = today_str in calendar if calendar else current_date.weekday() < 5
    
    if is_today_trading:
        # 还没到上午开盘
        if current_time < morning_start:
            return tz.localize(datetime.combine(current_date, morning_start))
        # 上午休市期间
        elif windows[0][1] < current_time < afternoon_start:
            return tz.localize(datetime.combine(current_date, afternoon_start))
        # 还在交易时间内（上午或下午）
        elif current_time <= afternoon_end:
            return None  # 正在交易中
        # 已收盘（当前时间超过下午收盘时间），查找下一个交易日
    
    # 查找下一个交易日（最多查找10天，快速返回）
    check_date = current_date + timedelta(days=1)
    max_days = 10
    
    for _ in range(max_days):
        check_str = check_date.strftime('%Y%m%d')
        # 如果有日历就用日历，没有就用周末判断
        is_trading = check_str in calendar if calendar else check_date.weekday() < 5
        
        if is_trading:
            return tz.localize(datetime.combine(check_date, morning_start))
        
        check_date += timedelta(days=1)
    
    # 找不到就返回下一个工作日
    check_date = current_date + timedelta(days=1)
    while check_date.weekday() >= 5:  # 跳过周末
        check_date += timedelta(days=1)
    return tz.localize(datetime.combine(check_date, morning_start))


def get_next_trading_datetime(market: str) -> Optional[datetime]:
    """获取下一个交易开始时间
    
    Args:
        market: "A" 或 "HK"
    
    Returns:
        下一个交易开始时间（带时区），如果无法确定返回None
    """
    tz = TZ_SHANGHAI if market == "A" else TZ_HONGKONG
    now = datetime.now(tz)
    windows = A_STOCK_TRADING_WINDOWS if market == "A" else HK_STOCK_TRADING_WINDOWS
    
    morning_start = windows[0][0]  # 上午开盘时间
    afternoon_start = windows[1][0]  # 下午开盘时间
    afternoon_end = windows[1][1]  # 下午收盘时间
    
    current_time = now.time()
    current_date = now.date()
    
    # 获取交易日历
    if market == "A":
        calendar = _get_a_stock_calendar_from_cache()
        if not calendar:
            refresh_trading_calendar("A")
            calendar = _get_a_stock_calendar_from_cache()
    else:
        calendar = _get_hk_stock_calendar_from_cache()
        if not calendar:
            refresh_trading_calendar("HK")
            calendar = _get_hk_stock_calendar_from_cache()
    
    # 如果今天是交易日
    today_str = current_date.strftime('%Y%m%d')
    is_today_trading = today_str in calendar if calendar else current_date.weekday() < 5
    
    if is_today_trading:
        # 还没到上午开盘
        if current_time < morning_start:
            return tz.localize(datetime.combine(current_date, morning_start))
        # 上午休市期间
        elif windows[0][1] < current_time < afternoon_start:
            return tz.localize(datetime.combine(current_date, afternoon_start))
        # 还在交易时间内
        elif current_time <= afternoon_end:
            return None  # 正在交易中
    
    # 查找下一个交易日
    check_date = current_date + timedelta(days=1)
    max_days = 30  # 最多查找30天
    
    for _ in range(max_days):
        check_str = check_date.strftime('%Y%m%d')
        is_trading = check_str in calendar if calendar else check_date.weekday() < 5
        
        if is_trading:
            return tz.localize(datetime.combine(check_date, morning_start))
        
        check_date += timedelta(days=1)
    
    # 找不到下一个交易日
    return None


def get_market_status_with_next(market: str) -> Dict[str, Any]:
    """获取市场状态，包含下一个开盘时间
    
    Args:
        market: "A" 或 "HK"
    
    Returns:
        {
            "is_trading": bool,
            "status": str,  # "交易中" / "已收盘" / "休市"
            "next_open": str | None,  # 下一个开盘时间，格式："12-25 09:30"
            "next_open_full": str | None  # 完整格式："2024-12-25 09:30"
        }
    """
    tz = TZ_SHANGHAI if market == "A" else TZ_HONGKONG
    now = datetime.now(tz)
    
    is_trading = is_a_stock_trading_time() if market == "A" else is_hk_stock_trading_time()
    is_today_trading_day = is_trading_day(market, now.date())
    
    result = {
        "is_trading": is_trading,
        "status": "交易中" if is_trading else ("已收盘" if is_today_trading_day else "休市"),
        "next_open": None,
        "next_open_full": None
    }
    
    if not is_trading:
        # 快速计算下一个开盘时间（不等待网络请求）
        next_dt = _get_next_trading_datetime_fast(market)
        if next_dt:
            result["next_open"] = next_dt.strftime("%m-%d %H:%M")
            result["next_open_full"] = next_dt.strftime("%Y-%m-%d %H:%M")
    
    return result


def get_next_trading_start_time(market: str) -> datetime:
    """获取下一个交易开始时间（兼容旧接口）
    
    Args:
        market: "A" 或 "HK"
    
    Returns:
        下一个交易开始时间（带时区）
    """
    result = _get_next_trading_datetime_fast(market)
    if result is None:
        # 如果正在交易中，返回下一个交易时段的开始时间
        tz = TZ_SHANGHAI if market == "A" else TZ_HONGKONG
        now = datetime.now(tz)
        windows = A_STOCK_TRADING_WINDOWS if market == "A" else HK_STOCK_TRADING_WINDOWS
        
        current_time = now.time()
        # 如果在上午交易时段，返回下午开盘时间
        if current_time <= windows[0][1]:
            return tz.localize(datetime.combine(now.date(), windows[1][0]))
        # 如果在下午交易时段，返回明天上午开盘时间
        else:
            tomorrow = now.date() + timedelta(days=1)
            # 跳过周末
            while tomorrow.weekday() >= 5:
                tomorrow += timedelta(days=1)
            return tz.localize(datetime.combine(tomorrow, windows[0][0]))
    return result


# 启动时自动刷新交易日历
def init_trading_calendar():
    """初始化交易日历（启动时调用）"""
    try:
        # 检查是否需要刷新
        a_calendar = _get_a_stock_calendar_from_cache()
        hk_calendar = _get_hk_stock_calendar_from_cache()
        
        if not a_calendar or not hk_calendar:
            logger.info("交易日历缓存为空，开始刷新...")
            refresh_trading_calendar("ALL")
        else:
            logger.info(f"交易日历已加载: A股 {len(a_calendar)} 天, 港股 {len(hk_calendar)} 天")
    except Exception as e:
        logger.error(f"初始化交易日历失败: {e}")
