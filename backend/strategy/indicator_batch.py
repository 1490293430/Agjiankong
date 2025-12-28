"""
批量计算和缓存技术指标
用于收盘后批量计算，减少盘中计算压力

设计原则：
- 自动计算选股配置中所有指标（不管是否启用）
- 后续添加新指标时，只需在 ta.py 的 calculate_all_indicators 中添加计算逻辑
- 并发查询K线数据和计算指标，提高效率
"""
from typing import List, Dict, Any
from datetime import datetime, timedelta
from common.logger import get_logger
from common.db import save_indicator, get_kline_from_db
from common.redis import get_json
from market.indicator.ta import calculate_all_indicators
import pandas as pd
import concurrent.futures
import time

logger = get_logger(__name__)

# ============ 选股指标所需的最小K线数量 ============
MIN_KLINE_REQUIRED = 180  # 日线
MIN_KLINE_REQUIRED_HOURLY = 30  # 小时线


def batch_compute_indicators(market: str = "A", max_count: int = None, incremental: bool = True, period: str = "daily") -> Dict[str, Any]:
    """批量计算并缓存技术指标（并发查询和计算）
    
    Args:
        market: 市场类型（A或HK）
        max_count: 最多计算的股票数量（None表示计算所有股票）
        incremental: 是否增量更新（只计算当日数据有变化的股票，默认True）
        period: K线周期，daily（日线）或 1h（小时线），默认 daily
    
    Returns:
        统计信息字典
    """
    try:
        from common.db import get_indicator_date
        
        # 获取股票列表
        if market.upper() == "HK":
            all_stocks = get_json("market:hk:spot") or []
        else:
            all_stocks = get_json("market:a:spot") or []
        
        if not all_stocks:
            logger.warning(f"未获取到{market}股行情数据")
            return {"success": 0, "failed": 0, "total": 0, "skipped": 0}
        
        # 按成交额排序，优先计算活跃股票
        sorted_stocks = sorted(all_stocks, key=lambda x: x.get("amount", 0), reverse=True)
        
        # 如果指定了max_count，则只取前N只；否则计算所有
        if max_count is not None:
            target_stocks = sorted_stocks[:max_count]
        else:
            target_stocks = sorted_stocks
        
        today = datetime.now().strftime("%Y-%m-%d")
        
        period_name = "日线" if period == "daily" else "小时线"
        min_kline = MIN_KLINE_REQUIRED if period == "daily" else MIN_KLINE_REQUIRED_HOURLY
        
        # 计算查询的起始日期（往前推足够多的天数，考虑节假日）
        if period == "daily":
            # 日线需要180根，考虑周末和节假日，往前推300天
            start_date = (datetime.now() - timedelta(days=300)).strftime("%Y%m%d")
        else:
            # 小时线，往前推更多天（每天4根小时线）
            start_date = (datetime.now() - timedelta(days=60)).strftime("%Y%m%d")
        
        # 过滤需要计算的股票
        codes_to_compute = []
        skipped_count = 0
        
        for stock in target_stocks:
            code = str(stock.get("code", ""))
            if not code:
                continue
            
            if incremental:
                indicator_date = get_indicator_date(code, market.upper(), period)
                if indicator_date == today:
                    skipped_count += 1
                    continue
            
            codes_to_compute.append(code)
        
        logger.info(f"开始计算{period_name}指标：市场={market}，需计算={len(codes_to_compute)}，已跳过={skipped_count}，最小K线数={min_kline}")
        
        if not codes_to_compute:
            return {"success": 0, "failed": 0, "skipped": skipped_count, "total": skipped_count, "period": period}
        
        start_time = time.time()
        success_count = 0
        failed_count = 0
        
        def compute_single(code: str) -> tuple:
            """单只股票的查询和计算"""
            try:
                # 查询K线数据（使用日期范围）
                kline_data = get_kline_from_db(code, start_date=start_date, period=period)
                
                if not kline_data or len(kline_data) < min_kline:
                    return ("failed", code, f"K线不足: {len(kline_data) if kline_data else 0}/{min_kline}")
                
                # 计算指标
                df = pd.DataFrame(kline_data)
                indicators = calculate_all_indicators(df)
                
                if indicators and indicators.get("ma60"):
                    if save_indicator(code, market.upper(), today, indicators, period):
                        return ("success", code)
                    else:
                        return ("failed", code, "保存失败")
                else:
                    return ("failed", code, "计算失败")
                    
            except Exception as e:
                return ("failed", code, str(e))
        
        # 并发执行（10个线程）
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(compute_single, code): code for code in codes_to_compute}
            
            completed = 0
            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result()
                    if result[0] == "success":
                        success_count += 1
                    else:
                        failed_count += 1
                except Exception:
                    failed_count += 1
                
                completed += 1
                if completed % 500 == 0:
                    logger.info(f"计算{period_name}进度：{completed}/{len(codes_to_compute)}，成功={success_count}，失败={failed_count}")
        
        elapsed = time.time() - start_time
        logger.info(f"计算{period_name}完成：成功={success_count}，跳过={skipped_count}，失败={failed_count}，耗时={elapsed:.1f}秒")
        
        return {
            "success": success_count,
            "failed": failed_count,
            "skipped": skipped_count,
            "total": success_count + skipped_count + failed_count,
            "period": period,
            "elapsed": elapsed
        }
        
    except Exception as e:
        logger.error(f"计算{period_name}指标失败: {e}", exc_info=True)
        return {"success": 0, "failed": 0, "total": 0, "skipped": 0, "error": str(e), "period": period}


def compute_indicator_async(code: str, market: str, kline_data: List[Dict[str, Any]], period: str = "daily") -> None:
    """异步计算并保存指标（不阻塞主流程）"""
    try:
        if not kline_data or len(kline_data) < 60:
            return
        
        today = datetime.now().strftime("%Y-%m-%d")
        df = pd.DataFrame(kline_data)
        indicators = calculate_all_indicators(df)
        
        if indicators and indicators.get("ma60"):
            save_indicator(code, market.upper(), today, indicators, period)
            logger.debug(f"异步计算{period}指标完成: {code}")
    except Exception as e:
        logger.debug(f"异步计算{period}指标失败 {code}: {e}")
