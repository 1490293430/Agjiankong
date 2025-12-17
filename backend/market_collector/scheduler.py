"""
行情采集调度器
"""
import time

from market_collector.cn import fetch_a_stock_spot
from market_collector.hk import fetch_hk_stock_spot
from common.logger import get_logger
from common.runtime_config import get_runtime_config

logger = get_logger(__name__)


def collect_job():
    """采集任务"""
    try:
        logger.info("开始采集行情数据...")
        fetch_a_stock_spot()
        fetch_hk_stock_spot()
        logger.info("行情数据采集完成")
    except Exception as e:
        logger.error(f"采集任务执行失败: {e}", exc_info=True)


def main():
    """主函数"""
    logger.info("行情采集调度器启动...")

    # 简单循环调度，根据运行时配置动态调整采集间隔
    while True:
        start_ts = time.time()

        collect_job()

        try:
            cfg = get_runtime_config()
            interval = max(1, int(cfg.collector_interval_seconds))
        except Exception as e:
            logger.warning(f"获取采集间隔配置失败，使用默认5秒: {e}")
            interval = 5

        elapsed = time.time() - start_ts
        sleep_seconds = max(1, interval - int(elapsed))
        logger.info(f"下次采集将在 {sleep_seconds} 秒后执行（间隔配置: {interval}s）")
        time.sleep(sleep_seconds)


if __name__ == "__main__":
    main()

