"""修复港股小时K线数据的market字段（从A改为HK）"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'backend'))

from common.db import _create_clickhouse_client
from common.logger import get_logger

logger = get_logger(__name__)

def fix_hk_hourly_market():
    """将5位代码的小时K线数据的market字段从A改为HK"""
    client = None
    try:
        client = _create_clickhouse_client()
        
        # 1. 统计需要修复的数据量
        count_query = """
            SELECT count() 
            FROM kline 
            WHERE period = '1h' 
            AND market = 'A' 
            AND length(code) = 5
        """
        result = client.execute(count_query)
        count = result[0][0] if result else 0
        
        if count == 0:
            logger.info("没有需要修复的数据")
            return
        
        logger.info(f"发现 {count:,} 条需要修复的港股小时K线数据（5位代码，market='A'）")
        
        # 2. 执行修复（使用ALTER TABLE UPDATE）
        logger.info("开始修复数据...")
        update_query = """
            ALTER TABLE kline 
            UPDATE market = 'HK' 
            WHERE period = '1h' 
            AND market = 'A' 
            AND length(code) = 5
        """
        client.execute(update_query)
        
        logger.info("✓ 修复命令已提交（异步执行）")
        logger.info("提示：ClickHouse的UPDATE是异步操作，可能需要几秒到几分钟完成")
        logger.info("可以通过以下命令查看修复进度：")
        logger.info("  docker exec -it stock_clickhouse clickhouse-client --database=stock --query \"SELECT count() FROM kline WHERE period='1h' AND market='A' AND length(code)=5\"")
        
        # 3. 验证修复结果（等待几秒后查询）
        import time
        time.sleep(3)
        
        verify_query = """
            SELECT count() 
            FROM kline 
            WHERE period = '1h' 
            AND market = 'A' 
            AND length(code) = 5
        """
        result = client.execute(verify_query)
        remaining = result[0][0] if result else 0
        
        if remaining == 0:
            logger.info("✓ 修复完成！所有港股小时K线数据已正确标记为HK市场")
        else:
            logger.info(f"修复进行中...还有 {remaining:,} 条数据待处理（异步执行中）")
        
        # 4. 显示修复后的统计
        stats_query = """
            SELECT market, period, COUNT(*) as count, COUNT(DISTINCT code) as stocks 
            FROM kline 
            WHERE period = '1h'
            GROUP BY market, period 
            ORDER BY market, period
        """
        result = client.execute(stats_query)
        logger.info("\n修复后的小时K线统计：")
        for row in result:
            logger.info(f"  {row[0]:4s} {row[1]:6s}: {row[2]:,} 条数据, {row[3]:,} 只股票")
        
    except Exception as e:
        logger.error(f"修复失败: {e}", exc_info=True)
    finally:
        if client:
            try:
                client.disconnect()
            except Exception:
                pass

if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("港股小时K线market字段修复工具")
    logger.info("=" * 60)
    fix_hk_hourly_market()
