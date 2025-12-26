"""
行情采集调度器（优化版：交易时间内才采集，非交易时间延长等待）
"""
import time
from datetime import datetime, timedelta

from market_collector.cn import fetch_a_stock_spot
from market_collector.hk import fetch_hk_stock_spot
from common.logger import get_logger
from common.runtime_config import get_runtime_config
from common.trading_hours import is_a_stock_trading_time, is_hk_stock_trading_time, get_next_trading_start_time

logger = get_logger(__name__)

# 记录上次批量计算指标的日期，避免同一天重复计算
_last_batch_compute_date = None
_last_batch_compute_market = set()  # 存储格式："{market}_{date}"，例如 "A_2025-12-20"

# 记录上次资讯采集时间
_last_news_collect_time = None


def _broadcast_spot_result(a_count: int, hk_count: int, a_time: str, hk_time: str, a_source: str, hk_source: str, a_success: bool, hk_success: bool):
    """广播采集结果到前端顶部状态栏
    
    每次推送时，从Redis读取A股和港股各自的最新采集时间，
    这样即使一个市场采集失败，也能显示另一个市场的最新状态。
    
    Args:
        a_count: A股采集数量
        hk_count: 港股采集数量
        a_time: A股采集时间（格式：MM-DD HH:MM），如果为空则从Redis读取
        hk_time: 港股采集时间（格式：MM-DD HH:MM），如果为空则从Redis读取
        a_source: A股数据源
        hk_source: 港股数据源
        a_success: A股采集是否成功
        hk_success: 港股采集是否成功
    """
    try:
        from market.service.sse import broadcast_message
        from common.redis import set_json, get_json
        
        # 从Redis读取上次保存的采集结果
        last_result = get_json("spot:collect:result") or {}
        
        # 如果本次有新的采集时间，使用新的；否则使用上次保存的
        final_a_time = a_time if a_time else last_result.get("a_time", "")
        final_hk_time = hk_time if hk_time else last_result.get("hk_time", "")
        final_a_count = a_count if a_success else last_result.get("a_count", 0)
        final_hk_count = hk_count if hk_success else last_result.get("hk_count", 0)
        final_a_source = a_source if a_source else last_result.get("a_source", "")
        final_hk_source = hk_source if hk_source else last_result.get("hk_source", "")
        final_a_success = a_success if a_time else last_result.get("a_success", False)
        final_hk_success = hk_success if hk_time else last_result.get("hk_success", False)
        
        spot_result_data = {
            "success": final_a_success or final_hk_success,
            "time": datetime.now().strftime("%m-%d %H:%M"),
            "a_count": final_a_count,
            "hk_count": final_hk_count,
            "a_time": final_a_time,
            "hk_time": final_hk_time,
            "a_source": final_a_source,
            "hk_source": final_hk_source,
            "a_success": final_a_success,
            "hk_success": final_hk_success,
            "source": final_a_source  # 兼容旧版本
        }
        # 保存到Redis持久化
        set_json("spot:collect:result", spot_result_data)
        broadcast_message({
            "type": "spot_collect_result",
            "data": spot_result_data
        })
    except Exception as e:
        logger.debug(f"广播采集结果失败: {e}")


def collect_job():
    """采集任务（单次采集，不做交易时间判断）

    说明：
    - 是否在交易时间内的判断由调度循环控制；
    - 这里始终尝试采集一次 A 股和港股，方便在启动时或手动触发时获取最新快照，
      避免非交易时间页面完全没有数据。
    - A股和港股分开采集，各自完成后立即推送采集结果（包含独立的时间戳）
    - 每次推送会从Redis读取另一个市场的最新状态，确保即使一个市场采集失败也不影响另一个
    - 采集完成后自动检查交易计划
    - 采集结果会通过SSE广播到前端顶部状态栏
    - 只在交易时间内采集对应市场的数据
    """
    try:
        # 检查各市场是否在交易时间
        is_a_trading = is_a_stock_trading_time()
        is_hk_trading = is_hk_stock_trading_time()
        
        if not is_a_trading and not is_hk_trading:
            logger.debug("A股和港股都不在交易时间，跳过采集")
            return
        
        logger.info(f"开始采集行情数据... (A股交易中={is_a_trading}, 港股交易中={is_hk_trading})")
        
        # ========== 采集A股（仅在A股交易时间）==========
        a_count = 0
        a_source = ""
        a_success = False
        a_time = ""
        
        if is_a_trading:
            try:
                from market_collector.cn import fetch_a_stock_spot_with_source
                from common.runtime_config import get_runtime_config
                
                config = get_runtime_config()
                a_spot_source = config.spot_data_source or "auto"
                
                a_result, a_source = fetch_a_stock_spot_with_source(a_spot_source, 2)
                a_count = len(a_result) if a_result else 0
                a_success = a_count > 0
                if a_success:
                    a_time = datetime.now().strftime("%m-%d %H:%M")
                logger.info(f"A股采集完成: {a_count}只，数据源: {a_source}（配置: {a_spot_source}），时间: {a_time}")
                
            except Exception as e:
                logger.error(f"A股采集失败: {e}", exc_info=True)
                # 回退到原始方法
                try:
                    a_result = fetch_a_stock_spot()
                    a_count = len(a_result) if a_result else 0
                    a_source = "AKShare"
                    a_success = a_count > 0
                    if a_success:
                        a_time = datetime.now().strftime("%m-%d %H:%M")
                except Exception as e2:
                    logger.error(f"A股采集回退也失败: {e2}")
            
            # A股采集完成后立即推送结果（港股数据从Redis读取上次的）
            _broadcast_spot_result(a_count, 0, a_time, "", a_source, "", a_success, False)
        else:
            logger.debug("A股不在交易时间，跳过采集")
        
        # ========== 采集港股（仅在港股交易时间）==========
        hk_count = 0
        hk_source = ""
        hk_success = False
        hk_time = ""
        
        if is_hk_trading:
            try:
                # 港股使用独立的数据源配置
                from common.runtime_config import get_runtime_config
                config = get_runtime_config()
                hk_spot_source = config.hk_spot_data_source or "auto"
                
                result_tuple = fetch_hk_stock_spot(source=hk_spot_source)
                # 新的返回格式：(result, source_name)
                if isinstance(result_tuple, tuple) and len(result_tuple) == 2:
                    hk_result, hk_source = result_tuple
                else:
                    hk_result = result_tuple
                    hk_source = "AKShare(东方财富)"
                hk_count = len(hk_result) if hk_result else 0
                hk_success = hk_count > 0
                if hk_success:
                    hk_time = datetime.now().strftime("%m-%d %H:%M")
                logger.info(f"港股采集完成: {hk_count}只 [{hk_source}]（配置: {hk_spot_source}），时间: {hk_time}")
                
            except Exception as e:
                logger.error(f"港股采集失败: {e}", exc_info=True)
            
            # 港股采集完成后推送结果（A股数据从Redis读取上次的）
            _broadcast_spot_result(0, hk_count, "", hk_time, "", hk_source, False, hk_success)
        else:
            logger.debug("港股不在交易时间，跳过采集")
        
        if is_a_trading or is_hk_trading:
            logger.info("行情数据采集完成")
        
        # 采集完成后，自动检查交易计划（只在有采集时检查）
        if a_success or hk_success:
            try:
                from trading.plan import check_trade_plans_by_spot_price
                result = check_trade_plans_by_spot_price()
                if result.get("checked", 0) > 0:
                    logger.info(f"交易计划检查完成：检查{result.get('checked')}个，买入{result.get('bought')}个，盈利{result.get('win')}个，亏损{result.get('loss')}个")
            except Exception as e:
                logger.warning(f"自动检查交易计划失败: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"采集任务执行失败: {e}", exc_info=True)


def news_collect_job():
    """资讯采集任务（每30分钟执行一次）"""
    global _last_news_collect_time
    
    try:
        now = datetime.now()
        
        # 检查是否需要采集（每30分钟一次）
        if _last_news_collect_time:
            elapsed = (now - _last_news_collect_time).total_seconds()
            if elapsed < 1800:  # 30分钟 = 1800秒
                return
        
        logger.info("开始采集财经资讯...")
        from news.collector import fetch_news
        
        result = fetch_news()
        news_count = len(result) if result else 0
        
        if news_count > 0:
            _last_news_collect_time = now
            logger.info(f"资讯采集完成: {news_count}条")
        else:
            logger.warning("资讯采集返回空数据")
            
    except Exception as e:
        logger.error(f"资讯采集失败: {e}", exc_info=True)


def batch_compute_indicators_job(market: str = "A"):
    """批量计算指标任务（收盘后自动执行）
    
    Args:
        market: 市场类型（A或HK）
    """
    global _last_batch_compute_date, _last_batch_compute_market
    
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        market_key = f"{market}_{today}"
        
        # 清理过期的记录（只保留最近3天的）
        if _last_batch_compute_date and _last_batch_compute_date != today:
            # 清理旧记录
            _last_batch_compute_market = {
                key for key in _last_batch_compute_market
                if key.endswith(f"_{today}") or key.endswith(f"_{_last_batch_compute_date}")
            }
        
        # 检查今天是否已经计算过
        if market_key in _last_batch_compute_market:
            logger.debug(f"{market}股指标今天已批量计算过，跳过")
            return
        
        logger.info(f"开始收盘后批量计算{market}股技术指标（增量更新模式）...")
        from strategy.indicator_batch import batch_compute_indicators
        
        result = batch_compute_indicators(market, max_count=1000, incremental=True)
        
        if result.get("success", 0) > 0 or result.get("skipped", 0) > 0:
            _last_batch_compute_market.add(market_key)
            _last_batch_compute_date = today
            logger.info(f"{market}股指标批量计算完成：成功={result.get('success')}，跳过={result.get('skipped')}（已是最新），失败={result.get('failed')}")
        else:
            logger.warning(f"{market}股指标批量计算失败或无数据")
            
    except Exception as e:
        logger.error(f"批量计算{market}股指标失败: {e}", exc_info=True)


def snapshot_to_kline_job():
    """快照转K线任务（收盘后自动执行）
    
    将实时快照数据转换为当日K线并入库
    - A股：15:30 后执行
    - 港股：16:30 后执行
    """
    try:
        from market_collector.snapshot_to_kline import auto_convert_snapshot_to_kline
        results = auto_convert_snapshot_to_kline()
        
        for market, result in results.items():
            if result.get("count", 0) > 0:
                logger.info(f"[{market}] 快照转K线完成: {result.get('message')}")
    except Exception as e:
        logger.error(f"快照转K线任务失败: {e}", exc_info=True)


def main():
    """主函数"""
    logger.info("行情采集调度器启动（实时判断交易时间）...")

    # 简单循环调度，根据运行时配置动态调整采集间隔
    while True:
        try:
            cfg = get_runtime_config()
            interval = max(30, int(cfg.collector_interval_seconds))  # 最小30秒，降低CPU占用
        except Exception as e:
            logger.warning(f"获取采集间隔配置失败，使用默认60秒: {e}")
            interval = 60

        # 实时检查是否在交易时间内
        is_a_trading, is_hk_trading = is_a_stock_trading_time(), is_hk_stock_trading_time()
        now = datetime.now()
        current_hour = now.hour
        current_minute = now.minute
        
        if is_a_trading or is_hk_trading:
            # 在交易时间内，正常采集
            start_ts = time.time()
            collect_job()
            elapsed = time.time() - start_ts
            if elapsed > 10:  # 只有采集时间超过10秒才记录
                logger.info(f"本次采集耗时: {elapsed:.2f}秒")
            
            # 采集资讯（每30分钟一次）
            news_collect_job()
            
            # 采集完成后，广播市场状态更新（通过SSE）
            try:
                from market.service.sse import broadcast_market_status_update
                broadcast_market_status_update()
            except Exception as e:
                logger.debug(f"SSE广播市场状态失败（不影响采集）: {e}")
            
            # 交易时间内使用正常间隔
            time.sleep(interval)
        else:
            # 不在交易时间内，也要采集资讯（每30分钟一次）
            news_collect_job()
            
            # 不在交易时间内，检查是否需要执行收盘后批量计算指标
            # 策略：
            # 1. 港股收盘时（16:30-17:00）：如果港股今天有交易，一起计算A股和港股
            # 2. A股收盘时（15:30-16:00）：如果港股今天休市，只计算A股
            
            # 判断港股今天是否有交易（检查今天是否有数据更新）
            hk_has_traded_today = False
            try:
                from common.redis import get_redis
                from common.trading_hours import TZ_HONGKONG
                
                redis_client = get_redis()
                hk_time_key = "market:hk:time"
                update_time_str = redis_client.get(hk_time_key)
                
                if update_time_str:
                    try:
                        if isinstance(update_time_str, bytes):
                            update_time_str = update_time_str.decode('utf-8')
                        
                        if isinstance(update_time_str, str):
                            if 'T' in update_time_str:
                                update_time = datetime.fromisoformat(update_time_str.replace('Z', '+00:00'))
                            else:
                                update_time = datetime.fromisoformat(update_time_str)
                        else:
                            update_time = update_time_str
                        
                        if update_time.tzinfo is None:
                            update_time = TZ_HONGKONG.localize(update_time)
                        else:
                            update_time = update_time.astimezone(TZ_HONGKONG)
                        
                        now_hk = datetime.now(TZ_HONGKONG)
                        today_start = now_hk.replace(hour=0, minute=0, second=0, microsecond=0)
                        
                        # 如果今天有数据更新，说明港股今天有交易
                        if update_time >= today_start:
                            hk_has_traded_today = True
                            logger.debug(f"检测到港股今日有交易（最后更新时间: {update_time.strftime('%Y-%m-%d %H:%M:%S')}）")
                    except Exception as e:
                        logger.debug(f"解析港股更新时间失败: {e}")
            except Exception as e:
                logger.debug(f"检查港股今日交易状态失败: {e}")
            
            # A股收盘后（15:12-16:22）：如果港股今天休市，只计算A股
            if not is_a_trading:
                if (current_hour == 15 and current_minute >= 12) or (current_hour == 16 and current_minute < 22):
                    # 先执行快照转K线（A股）
                    snapshot_to_kline_job()
                    
                    # 如果港股今天休市，只计算A股
                    if not hk_has_traded_today:
                        logger.info("A股收盘，港股今日休市，开始批量计算A股指标...")
                        batch_compute_indicators_job("A")
            
            # 港股收盘后（16:22-17:30）：如果港股今天有交易，一起计算A股和港股
            if not is_hk_trading:
                if (current_hour == 16 and current_minute >= 22) or (current_hour == 17 and current_minute < 30):
                    # 先执行快照转K线（港股）
                    snapshot_to_kline_job()
                    
                    # 如果港股今天有交易，一起计算A股和港股
                    if hk_has_traded_today:
                        logger.info("港股收盘，开始批量计算A股和港股指标...")
                        batch_compute_indicators_job("A")
                        batch_compute_indicators_job("HK")
            
            # 不在交易时间内，不采集数据，延长等待时间
            # 估算下一个可能的交易开始时间
            next_a_start = get_next_trading_start_time("A")
            next_hk_start = get_next_trading_start_time("HK")
            next_start = min(next_a_start, next_hk_start)
            
            if next_start.tzinfo:
                from common.trading_hours import TZ_SHANGHAI
                if now.tzinfo is None:
                    now_tz = TZ_SHANGHAI.localize(now)
                else:
                    now_tz = now.astimezone(TZ_SHANGHAI)
            else:
                now_tz = now
            
            wait_seconds = max(300, int((next_start - now_tz).total_seconds()))  # 至少等待5分钟
            
            logger.info(f"当前不在交易时间内，等待 {wait_seconds // 60} 分钟后重新检查（估算下次交易时间: {next_start.strftime('%Y-%m-%d %H:%M:%S')}）")
            time.sleep(min(wait_seconds, 3600))  # 最多等待1小时，然后重新检查


if __name__ == "__main__":
    main()

