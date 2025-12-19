"""
行情采集调度器（优化版：交易时间内才采集，非交易时间延长等待）
"""
import time
from datetime import datetime

from market_collector.cn import fetch_a_stock_spot
from market_collector.hk import fetch_hk_stock_spot
from common.logger import get_logger
from common.runtime_config import get_runtime_config
from common.trading_hours import is_a_stock_trading_time, is_hk_stock_trading_time, get_next_trading_start_time

logger = get_logger(__name__)


def collect_job():
    """采集任务（只在交易时间内采集）"""
    try:
        is_a_trading, is_hk_trading = is_a_stock_trading_time(), is_hk_stock_trading_time()
        
        if not is_a_trading and not is_hk_trading:
            logger.debug("当前不在任何市场交易时间内，跳过采集")
            return
        
        logger.info("开始采集行情数据...")
        
        if is_a_trading:
            fetch_a_stock_spot()
        else:
            logger.debug("A股不在交易时间内，跳过A股采集")
        
        if is_hk_trading:
            fetch_hk_stock_spot()
        else:
            logger.debug("港股不在交易时间内，跳过港股采集")
        
        logger.info("行情数据采集完成")
    except Exception as e:
        logger.error(f"采集任务执行失败: {e}", exc_info=True)


def main():
    """主函数"""
    logger.info("行情采集调度器启动...")
    
    # 启动时立即采集一次（如果在交易时间内）
    collect_job()

    # 简单循环调度，根据运行时配置动态调整采集间隔
    while True:
        try:
            cfg = get_runtime_config()
            interval = max(30, int(cfg.collector_interval_seconds))  # 最小30秒，降低CPU占用
        except Exception as e:
            logger.warning(f"获取采集间隔配置失败，使用默认60秒: {e}")
            interval = 60

        # 检查是否在交易时间内
        is_a_trading, is_hk_trading = is_a_stock_trading_time(), is_hk_stock_trading_time()
        
        if is_a_trading or is_hk_trading:
            # 在交易时间内，正常采集
            start_ts = time.time()
            collect_job()
            elapsed = time.time() - start_ts
            if elapsed > 10:  # 只有采集时间超过10秒才记录
                logger.info(f"本次采集耗时: {elapsed:.2f}秒")
            
            # 交易时间内使用正常间隔
            time.sleep(interval)
        else:
            # 不在交易时间内，延长等待时间，降低CPU占用
            # 计算到下一个交易开始时间还有多久
            next_a_start = get_next_trading_start_time("A")
            next_hk_start = get_next_trading_start_time("HK")
            next_start = min(next_a_start, next_hk_start)
            
            now = datetime.now()
            if next_start.tzinfo:
                from common.trading_hours import TZ_SHANGHAI
                if now.tzinfo is None:
                    now = TZ_SHANGHAI.localize(now)
                else:
                    now = now.astimezone(TZ_SHANGHAI)
            
            wait_seconds = max(300, int((next_start - now).total_seconds()))  # 至少等待5分钟
            
            logger.info(f"当前不在交易时间内，等待 {wait_seconds // 60} 分钟后重试（下次交易开始时间: {next_start.strftime('%Y-%m-%d %H:%M:%S')}）")
            time.sleep(min(wait_seconds, 3600))  # 最多等待1小时，然后重新检查


if __name__ == "__main__":
    main()

