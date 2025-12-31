"""
检查磁盘占用情况
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.db import get_clickhouse
from common.logger import get_logger

logger = get_logger(__name__)


def check_disk_usage():
    """检查ClickHouse数据库占用情况"""
    client = None
    try:
        client = get_clickhouse()
        
        print("=" * 60)
        print("ClickHouse 数据库占用情况")
        print("=" * 60)
        
        # 检查表大小
        print("\n【表大小统计】")
        size_query = """
        SELECT 
            table,
            formatReadableSize(sum(bytes)) as size,
            sum(rows) as rows,
            count() as parts
        FROM system.parts
        WHERE database = 'stock' AND active = 1
        GROUP BY table
        ORDER BY sum(bytes) DESC
        """
        result = client.execute(size_query)
        for table, size, rows, parts in result:
            print(f"  {table:20s} | {size:15s} | {rows:>15,} 行 | {parts:>5} 个分区")
        
        # 检查未完成的mutations
        print("\n【未完成的删除操作（Mutations）】")
        mutation_query = """
        SELECT count() as count
        FROM system.mutations
        WHERE is_done = 0
        """
        result = client.execute(mutation_query)
        pending_mutations = result[0][0] if result else 0
        print(f"  未完成的删除操作: {pending_mutations} 个")
        
        if pending_mutations > 0:
            print("\n  ⚠️  有未完成的删除操作，需要执行 OPTIMIZE TABLE 才能释放空间")
        
        # 检查K线数据量
        print("\n【K线数据统计】")
        kline_query = """
        SELECT 
            period,
            count() as count,
            min(date) as min_date,
            max(date) as max_date
        FROM kline
        GROUP BY period
        ORDER BY count DESC
        """
        result = client.execute(kline_query)
        for period, count, min_date, max_date in result:
            print(f"  {period:10s} | {count:>15,} 条 | {min_date} ~ {max_date}")
        
        # 检查快照数据量
        print("\n【快照数据统计】")
        try:
            snapshot_query = """
            SELECT 
                count() as count,
                min(date) as min_date,
                max(date) as max_date
            FROM snapshot
            """
            result = client.execute(snapshot_query)
            if result:
                count, min_date, max_date = result[0]
                print(f"  快照数据: {count:>15,} 条 | {min_date} ~ {max_date}")
        except Exception as e:
            print(f"  快照表不存在或查询失败: {e}")
        
        # 检查指标数据量
        print("\n【指标数据统计】")
        try:
            indicator_query = """
            SELECT 
                period,
                count() as count,
                min(date) as min_date,
                max(date) as max_date
            FROM indicators
            GROUP BY period
            ORDER BY count DESC
            """
            result = client.execute(indicator_query)
            for period, count, min_date, max_date in result:
                print(f"  {period:10s} | {count:>15,} 条 | {min_date} ~ {max_date}")
        except Exception as e:
            print(f"  指标表查询失败: {e}")
        
        print("\n" + "=" * 60)
        print("提示：")
        print("1. 如果表很大，可以运行清理脚本删除旧数据")
        print("2. 删除后需要执行 OPTIMIZE TABLE 才能释放空间")
        print("3. 检查Docker卷占用: docker system df")
        print("=" * 60)
        
    except Exception as e:
        logger.error(f"检查磁盘占用失败: {e}", exc_info=True)
        print(f"\n错误: {e}")
    finally:
        if client:
            try:
                client.disconnect()
            except Exception:
                pass


if __name__ == "__main__":
    check_disk_usage()

