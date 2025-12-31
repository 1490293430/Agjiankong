"""
清理旧数据并优化表，释放磁盘空间
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.db import get_clickhouse
from common.logger import get_logger
from datetime import datetime, timedelta
from common.runtime_config import get_runtime_config
import time

logger = get_logger(__name__)


def cleanup_old_data(dry_run=True, retention_years=None):
    """清理超过保留期限的数据"""
    client = None
    try:
        client = get_clickhouse()
        
        # 获取保留期限配置
        if retention_years is None:
            config = get_runtime_config()
            retention_years = config.kline_years
        
        cutoff_date = datetime.now() - timedelta(days=int(retention_years * 365))
        cutoff_date_str = cutoff_date.strftime("%Y-%m-%d")
        
        print("=" * 60)
        print(f"清理旧数据（保留 {retention_years} 年，删除 {cutoff_date_str} 之前的数据）")
        print("=" * 60)
        
        # 清理K线数据
        print("\n【清理K线数据】")
        try:
            # 先统计要删除的数据量
            count_query = """
            SELECT count() 
            FROM kline 
            WHERE date < %(date)s
            """
            result = client.execute(count_query, {'date': cutoff_date_str})
            count = result[0][0] if result else 0
            
            print(f"  待删除数据: {count:,} 条")
            
            if count == 0:
                print("  ✓ 无需清理")
            elif dry_run:
                print(f"  [DRY RUN] 将删除 {count:,} 条数据")
            else:
                print(f"  正在删除 {count:,} 条数据...")
                delete_query = "ALTER TABLE kline DELETE WHERE date < %(date)s"
                client.execute(delete_query, {'date': cutoff_date_str})
                print(f"  ✓ 已提交删除请求（异步执行）")
        except Exception as e:
            print(f"  ✗ 清理K线数据失败: {e}")
        
        # 清理小时线数据（保留1年）
        print("\n【清理小时线数据（保留1年）】")
        try:
            hourly_cutoff = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
            count_query = """
            SELECT count() 
            FROM kline 
            WHERE period = '1h' AND date < %(date)s
            """
            result = client.execute(count_query, {'date': hourly_cutoff})
            count = result[0][0] if result else 0
            
            print(f"  待删除数据: {count:,} 条")
            
            if count == 0:
                print("  ✓ 无需清理")
            elif dry_run:
                print(f"  [DRY RUN] 将删除 {count:,} 条小时线数据")
            else:
                print(f"  正在删除 {count:,} 条小时线数据...")
                delete_query = "ALTER TABLE kline DELETE WHERE period = '1h' AND date < %(date)s"
                client.execute(delete_query, {'date': hourly_cutoff})
                print(f"  ✓ 已提交删除请求（异步执行）")
        except Exception as e:
            print(f"  ✗ 清理小时线数据失败: {e}")
        
        # 清理指标数据（只保留最近2天）
        print("\n【清理指标数据（只保留最近2天）】")
        try:
            indicator_cutoff = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
            count_query = """
            SELECT count() 
            FROM indicators 
            WHERE date < %(date)s
            """
            result = client.execute(count_query, {'date': indicator_cutoff})
            count = result[0][0] if result else 0
            
            print(f"  待删除数据: {count:,} 条")
            
            if count == 0:
                print("  ✓ 无需清理")
            elif dry_run:
                print(f"  [DRY RUN] 将删除 {count:,} 条指标数据")
            else:
                print(f"  正在删除 {count:,} 条指标数据...")
                delete_query = "ALTER TABLE indicators DELETE WHERE date < %(date)s"
                client.execute(delete_query, {'date': indicator_cutoff})
                print(f"  ✓ 已提交删除请求（异步执行）")
        except Exception as e:
            print(f"  ✗ 清理指标数据失败: {e}")
        
        print("\n" + "=" * 60)
        if dry_run:
            print("这是预览模式（dry-run），实际未删除数据")
            print("运行时不加 --dry-run 参数才会真正删除")
        else:
            print("删除请求已提交，ClickHouse将在后台异步执行")
            print("需要等待删除完成后再执行 OPTIMIZE TABLE 才能释放空间")
        print("=" * 60)
        
    except Exception as e:
        logger.error(f"清理旧数据失败: {e}", exc_info=True)
        print(f"\n错误: {e}")
    finally:
        if client:
            try:
                client.disconnect()
            except Exception:
                pass


def wait_mutations_complete(max_wait=300):
    """等待所有mutations完成"""
    client = None
    try:
        client = get_clickhouse()
        
        print("\n等待删除操作完成...")
        waited = 0
        while waited < max_wait:
            result = client.execute("SELECT count() FROM system.mutations WHERE is_done = 0")
            pending = result[0][0] if result else 0
            
            if pending == 0:
                print("✓ 所有删除操作已完成")
                return True
            
            print(f"  还有 {pending} 个删除操作进行中，等待... ({waited}/{max_wait}秒)")
            time.sleep(5)
            waited += 5
        
        print(f"⚠️  等待超时（{max_wait}秒），部分删除操作可能仍在进行中")
        return False
        
    except Exception as e:
        logger.error(f"等待mutations完成失败: {e}", exc_info=True)
        return False
    finally:
        if client:
            try:
                client.disconnect()
            except Exception:
                pass


def optimize_tables():
    """优化表，释放磁盘空间"""
    client = None
    try:
        client = get_clickhouse()
        
        print("\n" + "=" * 60)
        print("优化表结构，释放磁盘空间")
        print("=" * 60)
        
        tables = ['kline', 'indicators', 'snapshot']
        
        for table in tables:
            try:
                print(f"\n优化表: {table}...")
                client.execute(f"OPTIMIZE TABLE {table} FINAL")
                print(f"✓ {table} 表优化完成")
            except Exception as e:
                print(f"⚠️  {table} 表优化失败（可能表不存在）: {e}")
        
        print("\n" + "=" * 60)
        print("表优化完成，磁盘空间已释放")
        print("=" * 60)
        
    except Exception as e:
        logger.error(f"优化表失败: {e}", exc_info=True)
        print(f"\n错误: {e}")
    finally:
        if client:
            try:
                client.disconnect()
            except Exception:
                pass


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='清理旧数据并优化表，释放磁盘空间')
    parser.add_argument('--dry-run', action='store_true', help='预览模式，不实际删除数据')
    parser.add_argument('--retention-years', type=float, help='保留年限（默认使用配置值）')
    parser.add_argument('--wait', action='store_true', help='等待删除操作完成后再优化')
    parser.add_argument('--optimize-only', action='store_true', help='只优化表，不清理数据')
    parser.add_argument('--check-only', action='store_true', help='只检查，不执行任何操作')
    
    args = parser.parse_args()
    
    if args.check_only:
        from scripts.check_disk_usage import check_disk_usage
        check_disk_usage()
        return
    
    if args.optimize_only:
        optimize_tables()
        return
    
    # 清理数据
    cleanup_old_data(dry_run=args.dry_run, retention_years=args.retention_years)
    
    if not args.dry_run:
        if args.wait:
            # 等待删除完成
            wait_mutations_complete()
            # 优化表
            optimize_tables()
        else:
            print("\n提示：")
            print("1. 删除操作是异步的，需要等待一段时间")
            print("2. 运行 'python -m scripts.cleanup_and_optimize --optimize-only' 来优化表释放空间")
            print("3. 或运行 'python -m scripts.cleanup_and_optimize --wait' 等待删除完成后自动优化")


if __name__ == "__main__":
    main()

