"""
K线表迁移脚本：添加time字段并重建ORDER BY

问题背景：
- 原kline表的date字段是Date类型，只能存储日期
- 对于小时线数据，同一天的多条数据会因为ORDER BY (code, period, date)被去重成1条
- 东方财富接口可以获取128条小时线数据，但存入数据库后只剩32条左右

解决方案：
- 添加time字段（DateTime类型）存储完整时间戳
- 修改ORDER BY为(code, period, date, time)，确保小时线数据不被去重

使用方法：
1. 停止数据采集服务
2. 运行此脚本: python -m scripts.migrate_kline_add_time
3. 重启数据采集服务

注意：
- 此脚本会重建kline表，需要一定时间
- 建议在低峰期执行
- 执行前请确保有数据备份
"""
import sys
import os

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from clickhouse_driver import Client
from common.config import settings
from common.logger import get_logger

logger = get_logger(__name__)


def migrate_kline_table():
    """迁移kline表，添加time字段并重建ORDER BY"""
    
    print("=" * 60)
    print("K线表迁移脚本：添加time字段支持小时线数据")
    print("=" * 60)
    
    client = None
    try:
        # 连接数据库
        client = Client(
            host=settings.clickhouse_host,
            port=settings.clickhouse_port,
            database=settings.clickhouse_db,
            user=settings.clickhouse_user,
            password=settings.clickhouse_password,
            connect_timeout=30,
            send_receive_timeout=300  # 迁移可能需要较长时间
        )
        
        print(f"\n✓ 已连接到ClickHouse: {settings.clickhouse_host}:{settings.clickhouse_port}")
        
        # 检查kline表是否存在
        tables = client.execute("SHOW TABLES")
        table_names = [t[0] for t in tables]
        
        if 'kline' not in table_names:
            print("\n⚠️ kline表不存在，无需迁移")
            print("新表将在服务启动时自动创建")
            return True
        
        # 检查当前表结构
        print("\n检查当前表结构...")
        columns = client.execute("DESCRIBE kline")
        column_names = [col[0] for col in columns]
        print(f"当前字段: {column_names}")
        
        # 检查是否已有time字段
        if 'time' in column_names:
            print("\n✓ kline表已有time字段")
            
            # 检查ORDER BY是否包含time
            engine_info = client.execute("""
                SELECT engine_full 
                FROM system.tables 
                WHERE database = %(db)s AND name = 'kline'
            """, {'db': settings.clickhouse_db})
            
            if engine_info:
                engine_full = engine_info[0][0]
                print(f"当前引擎配置: {engine_full}")
                
                # 检查ORDER BY是否包含time字段
                # 需要精确匹配，避免误判（比如update_time）
                import re
                # 提取ORDER BY子句
                order_by_match = re.search(r'ORDER BY\s*\(([^)]+)\)', engine_full, re.IGNORECASE)
                if order_by_match:
                    order_by_fields = order_by_match.group(1).lower()
                    # 检查是否有独立的time字段（不是update_time）
                    fields = [f.strip() for f in order_by_fields.split(',')]
                    if 'time' in fields:
                        print("\n✓ ORDER BY已包含time字段，无需迁移")
                        return True
                    else:
                        print(f"\n⚠️ ORDER BY字段: {fields}")
                        print("⚠️ ORDER BY未包含time字段，需要重建表")
                else:
                    print("\n⚠️ 无法解析ORDER BY，需要重建表")
        else:
            print("\n⚠️ kline表缺少time字段，需要迁移")
        
        # 获取当前数据量
        count_result = client.execute("SELECT count() FROM kline")
        total_rows = count_result[0][0] if count_result else 0
        print(f"\n当前数据量: {total_rows:,} 条")
        
        if total_rows > 0:
            # 确认是否继续
            print("\n" + "=" * 60)
            print("⚠️ 警告：此操作将重建kline表")
            print("⚠️ 请确保已停止数据采集服务")
            print("⚠️ 建议在执行前备份数据")
            print("=" * 60)
            
            confirm = input("\n是否继续迁移？(yes/no): ").strip().lower()
            if confirm != 'yes':
                print("\n已取消迁移")
                return False
        
        print("\n开始迁移...")
        
        # 步骤1：创建新表
        print("\n[1/4] 创建新表 kline_new...")
        client.execute("DROP TABLE IF EXISTS kline_new")
        client.execute("""
        CREATE TABLE kline_new
        (
            code String,
            period String DEFAULT 'daily',
            date Date,
            time DateTime DEFAULT toDateTime(date),
            open Float64,
            high Float64,
            low Float64,
            close Float64,
            volume Float64,
            amount Float64,
            update_time DateTime DEFAULT now()
        )
        ENGINE = ReplacingMergeTree(update_time)
        ORDER BY (code, period, date, time)
        """)
        print("✓ 新表创建成功")
        
        # 步骤2：迁移数据
        if total_rows > 0:
            print(f"\n[2/4] 迁移数据 ({total_rows:,} 条)...")
            
            # 检查旧表是否有time字段
            if 'time' in column_names:
                # 旧表有time字段，直接复制
                client.execute("""
                INSERT INTO kline_new 
                SELECT code, period, date, time, open, high, low, close, volume, amount, update_time
                FROM kline
                """)
            else:
                # 旧表没有time字段，使用date转换
                # 检查是否有period字段
                if 'period' in column_names:
                    client.execute("""
                    INSERT INTO kline_new (code, period, date, time, open, high, low, close, volume, amount, update_time)
                    SELECT 
                        code, 
                        period, 
                        date, 
                        toDateTime(date) as time,
                        open, high, low, close, volume, amount, 
                        update_time
                    FROM kline
                    """)
                else:
                    # 旧表没有period字段
                    client.execute("""
                    INSERT INTO kline_new (code, period, date, time, open, high, low, close, volume, amount)
                    SELECT 
                        code, 
                        'daily' as period, 
                        date, 
                        toDateTime(date) as time,
                        open, high, low, close, volume, amount
                    FROM kline
                    """)
            
            # 验证数据量
            new_count = client.execute("SELECT count() FROM kline_new")[0][0]
            print(f"✓ 数据迁移完成: {new_count:,} 条")
        else:
            print("\n[2/4] 跳过数据迁移（原表为空）")
        
        # 步骤3：重命名表
        print("\n[3/4] 重命名表...")
        client.execute("DROP TABLE IF EXISTS kline_backup")
        client.execute("RENAME TABLE kline TO kline_backup")
        client.execute("RENAME TABLE kline_new TO kline")
        print("✓ 表重命名完成")
        
        # 步骤4：验证
        print("\n[4/4] 验证迁移结果...")
        
        # 检查新表结构
        new_columns = client.execute("DESCRIBE kline")
        new_column_names = [col[0] for col in new_columns]
        print(f"新表字段: {new_column_names}")
        
        if 'time' not in new_column_names:
            print("❌ 迁移失败：time字段未添加")
            return False
        
        # 检查数据量
        final_count = client.execute("SELECT count() FROM kline")[0][0]
        print(f"最终数据量: {final_count:,} 条")
        
        # 检查ORDER BY
        engine_info = client.execute("""
            SELECT engine_full 
            FROM system.tables 
            WHERE database = %(db)s AND name = 'kline'
        """, {'db': settings.clickhouse_db})
        
        if engine_info:
            print(f"新引擎配置: {engine_info[0][0]}")
        
        print("\n" + "=" * 60)
        print("✓ 迁移完成！")
        print("=" * 60)
        print("\n后续步骤：")
        print("1. 重启数据采集服务")
        print("2. 验证小时线数据是否正常存储")
        print("3. 如果一切正常，可以删除备份表: DROP TABLE kline_backup")
        print("\n注意：旧的小时线数据因为没有时间信息，仍然是每天1条")
        print("新采集的小时线数据将正确保存每小时1条")
        
        return True
        
    except Exception as e:
        print(f"\n❌ 迁移失败: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        if client:
            try:
                client.disconnect()
            except Exception:
                pass


def check_hourly_data():
    """检查小时线数据存储情况"""
    client = None
    try:
        client = Client(
            host=settings.clickhouse_host,
            port=settings.clickhouse_port,
            database=settings.clickhouse_db,
            user=settings.clickhouse_user,
            password=settings.clickhouse_password,
        )
        
        print("\n检查小时线数据存储情况...")
        
        # 查询小时线数据统计
        result = client.execute("""
            SELECT 
                code,
                count() as cnt,
                min(date) as min_date,
                max(date) as max_date,
                countDistinct(date) as distinct_dates
            FROM kline FINAL
            WHERE period = '1h'
            GROUP BY code
            ORDER BY cnt DESC
            LIMIT 10
        """)
        
        if result:
            print("\n小时线数据统计（前10只股票）：")
            print("-" * 70)
            print(f"{'代码':<10} {'总条数':<10} {'最早日期':<12} {'最新日期':<12} {'不同日期数':<10}")
            print("-" * 70)
            for row in result:
                code, cnt, min_date, max_date, distinct_dates = row
                print(f"{code:<10} {cnt:<10} {min_date} {max_date} {distinct_dates:<10}")
            
            # 检查是否有同一天多条数据
            print("\n检查同一天是否有多条小时线数据...")
            multi_result = client.execute("""
                SELECT 
                    code,
                    date,
                    count() as cnt
                FROM kline FINAL
                WHERE period = '1h'
                GROUP BY code, date
                HAVING cnt > 1
                ORDER BY cnt DESC
                LIMIT 5
            """)
            
            if multi_result:
                print("✓ 发现同一天有多条小时线数据（迁移成功）：")
                for row in multi_result:
                    print(f"  {row[0]} {row[1]}: {row[2]}条")
            else:
                print("⚠️ 未发现同一天有多条小时线数据")
                print("  可能原因：")
                print("  1. 还没有采集新的小时线数据")
                print("  2. 迁移后的ORDER BY未生效")
        else:
            print("暂无小时线数据")
            
    except Exception as e:
        print(f"检查失败: {e}")
    finally:
        if client:
            try:
                client.disconnect()
            except Exception:
                pass


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='K线表迁移脚本')
    parser.add_argument('--check', action='store_true', help='仅检查小时线数据情况，不执行迁移')
    args = parser.parse_args()
    
    if args.check:
        check_hourly_data()
    else:
        success = migrate_kline_table()
        if success:
            check_hourly_data()
        sys.exit(0 if success else 1)
