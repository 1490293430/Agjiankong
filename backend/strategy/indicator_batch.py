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
        from common.db import get_clickhouse
        
        today = datetime.now().strftime("%Y-%m-%d")
        period_name = "日线" if period == "daily" else "小时线"
        min_kline = MIN_KLINE_REQUIRED if period == "daily" else MIN_KLINE_REQUIRED_HOURLY
        
        # 从数据库获取今天有K线更新的股票列表
        client = get_clickhouse()
        try:
            query = """
                SELECT DISTINCT code 
                FROM kline 
                WHERE market = %(market)s AND period = %(period)s AND date = %(date)s
            """
            result = client.execute(query, {"market": market.upper(), "period": period, "date": today})
            codes_with_today_kline = [row[0] for row in result if row[0]]
        finally:
            try:
                client.disconnect()
            except Exception:
                pass
        
        if not codes_with_today_kline:
            logger.info(f"[{market}] {period_name}K线今天没有更新，跳过指标计算")
            return {"success": 0, "failed": 0, "total": 0, "skipped": 0, "period": period, "message": "K线未更新"}
        
        logger.info(f"[{market}] 从数据库获取到 {len(codes_with_today_kline)} 只股票今天有{period_name}K线更新")
        
        # 如果指定了max_count，则只取前N只；否则计算所有
        if max_count is not None:
            target_codes = codes_with_today_kline[:max_count]
        else:
            target_codes = codes_with_today_kline
        
        # 计算查询的起始日期（往前推足够多的天数，考虑节假日）
        if period == "daily":
            # 日线需要180根，考虑周末和节假日，往前推300天
            start_date = (datetime.now() - timedelta(days=300)).strftime("%Y%m%d")
        else:
            # 小时线，往前推更多天（每天4根小时线）
            start_date = (datetime.now() - timedelta(days=60)).strftime("%Y%m%d")
        
        # 过滤需要计算的股票（增量模式下跳过已计算的）
        codes_to_compute = []
        skipped_count = 0
        
        # 导入收盘后更新检查函数
        from common.db import is_indicator_updated_after_close
        
        for code in target_codes:
            if incremental:
                # 检查是否在收盘后已更新（而不是只检查日期）
                if is_indicator_updated_after_close(code, market.upper(), period):
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
                # 查询K线数据（使用日期范围，低优先级查询）
                kline_data = get_kline_from_db(code, start_date=start_date, period=period, low_priority=True)
                
                if not kline_data or len(kline_data) < min_kline:
                    return ("failed", code, f"K线不足: {len(kline_data) if kline_data else 0}/{min_kline}")
                
                # 计算指标（先去重，避免ReplacingMergeTree未合并导致的重复数据）
                df = pd.DataFrame(kline_data)
                if 'time' in df.columns:
                    df = df.drop_duplicates(subset=['date', 'time'], keep='last')
                else:
                    df = df.drop_duplicates(subset=['date'], keep='last')
                df = df.sort_values(by='date').reset_index(drop=True)
                
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
        
        # 并发执行（5个线程）
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
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
                
                # 每处理100只股票，短暂休眠，让ClickHouse释放内存
                if completed % 100 == 0:
                    time.sleep(0.5)
        
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
