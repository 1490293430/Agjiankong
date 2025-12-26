"""
批量计算和缓存技术指标
用于收盘后批量计算，减少盘中计算压力

设计原则：
- 自动计算选股配置中所有指标（不管是否启用）
- 后续添加新指标时，只需在 ta.py 的 calculate_all_indicators 中添加计算逻辑
- 这里的 SELECTION_INDICATORS 用于记录所需的最小K线数量
"""
from typing import List, Dict, Any
from datetime import datetime
from common.logger import get_logger
from common.db import save_indicator, get_kline_from_db
from common.redis import get_json
from market.indicator.ta import calculate_all_indicators
import pandas as pd

logger = get_logger(__name__)

# ============ 选股指标所需的最小K线数量 ============
# 后续添加新指标时，如果需要更多K线，在这里更新
# 当前最大需求：斐波那契回撤需要180根K线
MIN_KLINE_REQUIRED = 180  # 保守值，确保所有指标都能计算


def batch_compute_indicators(market: str = "A", max_count: int = None, incremental: bool = True, period: str = "daily") -> Dict[str, Any]:
    """批量计算并缓存技术指标（支持增量更新）
    
    Args:
        market: 市场类型（A或HK）
        max_count: 最多计算的股票数量（None表示计算所有股票）
        incremental: 是否增量更新（只计算当日数据有变化的股票，默认True）
        period: K线周期，daily（日线）或 1h（小时线），默认 daily
    
    Returns:
        统计信息字典
    """
    try:
        from common.db import get_indicator_date, get_kline_latest_date
        
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
        today_ymd = datetime.now().strftime("%Y%m%d")
        success_count = 0
        failed_count = 0
        skipped_count = 0
        
        period_name = "日线" if period == "daily" else "小时线"
        logger.info(f"开始批量计算{period_name}指标：市场={market}，目标股票数={len(target_stocks)}，增量更新={incremental}")
        
        for i, stock in enumerate(target_stocks):
            code = str(stock.get("code", ""))
            
            if (i + 1) % 100 == 0:
                logger.info(f"批量计算{period_name}进度：{i+1}/{len(target_stocks)}，成功={success_count}，跳过={skipped_count}，失败={failed_count}")
            
            try:
                # 增量更新：检查是否需要计算
                if incremental:
                    # 检查指标表的最新日期（带周期参数）
                    indicator_date = get_indicator_date(code, market.upper(), period)
                    # 检查K线数据的最新日期
                    kline_latest_date = get_kline_latest_date(code, period)
                    
                    # 如果指标日期是今天，且K线最新日期也是今天（或更早），说明已经是最新的，跳过
                    if indicator_date == today and kline_latest_date:
                        # 比较日期（转换为YYYYMMDD格式比较）
                        indicator_date_ymd = indicator_date.replace("-", "") if indicator_date else None
                        if indicator_date_ymd == today_ymd and kline_latest_date <= today_ymd:
                            skipped_count += 1
                            continue
                
                # 获取K线数据（只从数据库获取）
                kline_data = get_kline_from_db(code, None, None, period)
                
                if not kline_data or len(kline_data) < MIN_KLINE_REQUIRED:
                    failed_count += 1
                    continue
                
                # 转换为DataFrame并计算指标
                df = pd.DataFrame(kline_data)
                indicators = calculate_all_indicators(df)
                
                if indicators and indicators.get("ma60"):
                    # 保存到数据库（带周期参数）
                    if save_indicator(code, market.upper(), today, indicators, period):
                        success_count += 1
                    else:
                        failed_count += 1
                else:
                    failed_count += 1
                    
            except Exception as e:
                logger.debug(f"计算指标失败 {code}: {e}")
                failed_count += 1
                continue
        
        logger.info(f"批量计算{period_name}完成：成功={success_count}，跳过={skipped_count}，失败={failed_count}，总计={success_count + skipped_count + failed_count}")
        
        return {
            "success": success_count,
            "failed": failed_count,
            "skipped": skipped_count,
            "total": success_count + skipped_count + failed_count,
            "period": period
        }
        
    except Exception as e:
        logger.error(f"批量计算{period_name}指标失败: {e}", exc_info=True)
        return {"success": 0, "failed": 0, "total": 0, "skipped": 0, "error": str(e), "period": period}


def compute_indicator_async(code: str, market: str, kline_data: List[Dict[str, Any]], period: str = "daily") -> None:
    """异步计算并保存指标（不阻塞主流程）
    
    Args:
        code: 股票代码
        market: 市场类型（A或HK）
        kline_data: K线数据列表
        period: K线周期，daily（日线）或 1h（小时线），默认 daily
    """
    try:
        if not kline_data or len(kline_data) < 60:
            return
        
        today = datetime.now().strftime("%Y-%m-%d")
        df = pd.DataFrame(kline_data)
        indicators = calculate_all_indicators(df)
        
        if indicators and indicators.get("ma60"):
            save_indicator(code, market.upper(), today, indicators, period)
            logger.debug(f"异步计算{period}指标完成并保存: {code}")
    except Exception as e:
        logger.debug(f"异步计算{period}指标失败 {code}: {e}")

