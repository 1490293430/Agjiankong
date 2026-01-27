"""
收盘后K线采集模块

收盘后从东方财富获取最近5天的日K线数据并入库
- A股：15:12 后执行
- 港股：16:22 后执行
"""
from datetime import datetime, time
from typing import Dict, Any, List
import concurrent.futures

from common.logger import get_logger
from common.redis import get_redis
from common.db import save_kline_data
from common.trading_hours import is_trading_day, TZ_SHANGHAI, TZ_HONGKONG

logger = get_logger(__name__)

# Redis key 用于记录已采集的日期
KLINE_COLLECTED_KEY_A = "kline:collected:a"
KLINE_COLLECTED_KEY_HK = "kline:collected:hk"

# 收盘后执行时间
A_STOCK_COLLECT_TIME = time(15, 12)
HK_STOCK_COLLECT_TIME = time(16, 30)


def _get_collected_date(market: str) -> str | None:
    """获取已采集的日期"""
    key = KLINE_COLLECTED_KEY_A if market == "A" else KLINE_COLLECTED_KEY_HK
    try:
        result = get_redis().get(key)
        if result and isinstance(result, bytes):
            return result.decode()
        return result
    except Exception:
        return None


def _set_collected_date(market: str, date_str: str):
    """设置已采集的日期"""
    key = KLINE_COLLECTED_KEY_A if market == "A" else KLINE_COLLECTED_KEY_HK
    try:
        get_redis().set(key, date_str, ex=7 * 24 * 3600)
    except Exception as e:
        logger.warning(f"设置已采集日期失败: {e}")


def should_convert_snapshot(market: str) -> bool:
    """判断是否应该执行K线采集
    
    条件：
    1. 今天是交易日
    2. 当前时间已过收盘延迟时间（带10分钟窗口期）
    3. 今天还没有采集过
    """
    tz = TZ_SHANGHAI if market == "A" else TZ_HONGKONG
    now = datetime.now(tz)
    today = now.date()
    today_str = today.strftime("%Y%m%d")
    
    if not is_trading_day(market, today):
        return False
    
    collect_time = A_STOCK_COLLECT_TIME if market == "A" else HK_STOCK_COLLECT_TIME
    # 使用时间范围判断，避免因调度器延迟错过（10分钟窗口期）
    current_minutes = now.hour * 60 + now.minute
    collect_minutes = collect_time.hour * 60 + collect_time.minute
    
    # 必须在采集时间之后，且在当天23:59之前
    if current_minutes < collect_minutes:
        return False
    
    collected_date = _get_collected_date(market)
    if collected_date == today_str:
        return False
    
    return True


def convert_snapshot_to_kline(market: str) -> Dict[str, Any]:
    """从东方财富获取最近5天K线数据并入库
    
    Args:
        market: "A" 或 "HK"
    
    Returns:
        {"success": bool, "count": int, "message": str}
    """
    tz = TZ_SHANGHAI if market == "A" else TZ_HONGKONG
    now = datetime.now(tz)
    today_str = now.strftime("%Y%m%d")
    
    logger.info(f"[{market}] 开始采集最近5天K线数据")
    
    # 从ClickHouse数据库获取股票列表
    try:
        from common.db import get_clickhouse
        client = get_clickhouse()
        # 查询数据库中有日K线数据的股票代码
        query = """
            SELECT DISTINCT code 
            FROM kline 
            WHERE market = %(market)s AND period = 'daily'
        """
        result = client.execute(query, {"market": market})
        codes = [row[0] for row in result if row[0]]
    except Exception as e:
        logger.error(f"[{market}] 从数据库获取股票列表失败: {e}")
        return {"success": False, "count": 0, "message": f"获取股票列表失败: {e}"}
    
    if not codes:
        logger.warning(f"[{market}] 数据库中无股票数据")
        return {"success": False, "count": 0, "message": "数据库中无股票数据"}
    
    logger.info(f"[{market}] 从数据库获取到 {len(codes)} 只股票")
    
    logger.info(f"[{market}] 准备采集 {len(codes)} 只股票的K线")
    
    # 导入K线获取函数
    if market == "A":
        from market_collector.eastmoney_source import fetch_eastmoney_a_kline as fetch_kline
    else:
        from market_collector.eastmoney_source import fetch_eastmoney_hk_kline as fetch_kline
    
    # 并发获取K线数据（优化版：降低并发+添加延迟）
    all_kline_data = []
    success_count = 0
    failed_count = 0
    
    def fetch_single(code: str) -> List[Dict]:
        """获取单只股票的K线（添加延迟控制）"""
        try:
            import random
            import time
            # 添加随机延迟 0.3-0.8秒，避免请求过于密集
            time.sleep(random.uniform(0.3, 0.8))
            
            klines = fetch_kline(code, period="daily", limit=5)
            if klines:
                # 添加market字段
                for k in klines:
                    k["market"] = market
                return klines
        except Exception as e:
            logger.debug(f"[{market}] 获取 {code} K线失败: {e}")
        return []
    
    # 降低并发数到3个线程，避免触发反爬虫
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(fetch_single, code): code for code in codes}
        
        for future in concurrent.futures.as_completed(futures):
            try:
                klines = future.result()
                if klines:
                    all_kline_data.extend(klines)
                    success_count += 1
                else:
                    failed_count += 1
            except Exception:
                failed_count += 1
    
    if not all_kline_data:
        logger.warning(f"[{market}] 没有获取到K线数据")
        return {"success": False, "count": 0, "message": "没有获取到K线数据"}
    
    # A股市场额外采集上证指数K线
    if market == "A":
        try:
            from market_collector.eastmoney_source import fetch_eastmoney_index_kline
            sh_index_klines = fetch_eastmoney_index_kline("1.000001", period="daily", limit=5)
            if sh_index_klines:
                for k in sh_index_klines:
                    k["code"] = "1A0001"  # 统一代码格式
                    k["market"] = "A"
                all_kline_data.extend(sh_index_klines)
                logger.info(f"[A股] 额外采集上证指数K线: {len(sh_index_klines)}条")
        except Exception as e:
            logger.warning(f"[A股] 采集上证指数K线失败（不影响股票数据）: {e}")
    
    # 保存到数据库（重复数据会被自动丢弃，因为有主键约束）
    try:
        success = save_kline_data(all_kline_data, "daily")
        if success:
            _set_collected_date(market, today_str)
            logger.info(f"[{market}] K线采集完成: 成功={success_count}只, 失败={failed_count}只, 总K线={len(all_kline_data)}条")
            return {
                "success": True,
                "count": success_count,
                "message": f"成功采集{success_count}只股票的K线"
            }
        else:
            return {"success": False, "count": 0, "message": "保存数据库失败"}
    except Exception as e:
        logger.error(f"[{market}] 保存K线数据异常: {e}")
        return {"success": False, "count": 0, "message": str(e)}


def auto_convert_snapshot_to_kline():
    """自动检查并执行K线采集（由定时任务调用）"""
    results = {}
    
    if should_convert_snapshot("A"):
        logger.info("[A股] 满足采集条件，开始采集K线")
        results["A"] = convert_snapshot_to_kline("A")
    else:
        results["A"] = {"success": True, "count": 0, "message": "不需要采集"}
    
    if should_convert_snapshot("HK"):
        logger.info("[港股] 满足采集条件，开始采集K线")
        results["HK"] = convert_snapshot_to_kline("HK")
    else:
        results["HK"] = {"success": True, "count": 0, "message": "不需要采集"}
    
    return results
