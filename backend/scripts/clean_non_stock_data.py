"""
清理数据库中的非股票数据

删除 ClickHouse 中 kline 和 snapshot 表中的非股票数据（ETF/指数/基金/债券等）
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from clickhouse_driver import Client
from common.config import settings
from common.logger import get_logger

logger = get_logger(__name__)


# A股股票代码前缀（只有这些开头的才是股票）
A_STOCK_PREFIXES = (
    # 沪市主板
    "600", "601", "603", "605",
    # 科创板
    "688", "689",
    # 深市主板
    "000", "001",
    # 中小板
    "002", "003",
    # 创业板
    "300", "301",
    # 北交所
    "43", "83", "87", "88",
)

# 非股票代码前缀（需要删除的）
NON_STOCK_PREFIXES = (
    # 沪市ETF
    "510", "511", "512", "513", "515", "516", "517", "518",
    "560", "561", "562", "563",
    # 深市ETF
    "159",
    # 深市指数
    "399",
    # 沪市基金
    "501", "502", "505", "506",
    # 深市基金
    "160", "161", "162", "163", "164", "165", "166", "167", "168", "169",
    # 可转债
    "11", "12",
    # 新三板/北交所
    "43", "83", "87", "88", "400", "420", "430", "830", "831", "832", "833", 
    "834", "835", "836", "837", "838", "839", "870", "871", "872", "873", 
    "874", "899", "920",
)


def get_client():
    """创建 ClickHouse 连接"""
    return Client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        database=settings.clickhouse_db,
        user=settings.clickhouse_user,
        password=settings.clickhouse_password,
        connect_timeout=10,
        send_receive_timeout=300  # 删除操作可能需要较长时间
    )


def clean_kline_table(client, dry_run=True):
    """清理 kline 表中的非股票数据"""
    print("\n=== 清理 kline 表 ===")
    
    # 构建删除条件
    conditions = []
    for prefix in NON_STOCK_PREFIXES:
        conditions.append(f"code LIKE '{prefix}%'")
    
    where_clause = " OR ".join(conditions)
    
    # 先统计要删除的数据量
    count_sql = f"SELECT count() FROM kline WHERE {where_clause}"
    result = client.execute(count_sql)
    count = result[0][0] if result else 0
    
    print(f"kline 表中非股票数据: {count} 条")
    
    if count == 0:
        print("无需清理")
        return 0
    
    if dry_run:
        print(f"[DRY RUN] 将删除 {count} 条数据")
        # 显示一些示例
        sample_sql = f"SELECT DISTINCT code FROM kline WHERE {where_clause} LIMIT 20"
        samples = client.execute(sample_sql)
        if samples:
            codes = [row[0] for row in samples]
            print(f"示例代码: {', '.join(codes)}")
        return count
    
    # 执行删除
    print(f"正在删除 {count} 条数据...")
    delete_sql = f"ALTER TABLE kline DELETE WHERE {where_clause}"
    client.execute(delete_sql)
    print(f"已提交删除请求（ClickHouse 异步执行）")
    
    return count


def clean_snapshot_table(client, dry_run=True):
    """清理 snapshot 表中的非股票数据"""
    print("\n=== 清理 snapshot 表 ===")
    
    # 构建删除条件（基于 sec_type 字段）
    where_clause = "sec_type != 'stock' AND sec_type != ''"
    
    # 先统计要删除的数据量
    count_sql = f"SELECT count() FROM snapshot WHERE {where_clause}"
    try:
        result = client.execute(count_sql)
        count = result[0][0] if result else 0
    except Exception as e:
        print(f"查询失败（可能表不存在）: {e}")
        return 0
    
    print(f"snapshot 表中非股票数据: {count} 条")
    
    if count == 0:
        print("无需清理")
        return 0
    
    if dry_run:
        print(f"[DRY RUN] 将删除 {count} 条数据")
        # 显示各类型数量
        type_sql = "SELECT sec_type, count() FROM snapshot WHERE sec_type != 'stock' GROUP BY sec_type"
        types = client.execute(type_sql)
        if types:
            for sec_type, cnt in types:
                print(f"  - {sec_type or '空'}: {cnt} 条")
        return count
    
    # 执行删除
    print(f"正在删除 {count} 条数据...")
    delete_sql = f"ALTER TABLE snapshot DELETE WHERE {where_clause}"
    client.execute(delete_sql)
    print(f"已提交删除请求（ClickHouse 异步执行）")
    
    return count


def check_mutations(client):
    """检查正在执行的 mutations"""
    sql = """
    SELECT table, mutation_id, command, is_done, parts_to_do 
    FROM system.mutations 
    WHERE database = currentDatabase() AND is_done = 0
    """
    result = client.execute(sql)
    if result:
        print("\n=== 正在执行的 mutations ===")
        for row in result:
            print(f"表: {row[0]}, ID: {row[1]}, 剩余: {row[4]} 个分区")
    else:
        print("\n没有正在执行的 mutations")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="清理数据库中的非股票数据")
    parser.add_argument("--execute", action="store_true", help="实际执行删除（默认只预览）")
    parser.add_argument("--check", action="store_true", help="只检查 mutations 状态")
    args = parser.parse_args()
    
    dry_run = not args.execute
    
    print("=" * 50)
    print("清理数据库非股票数据")
    print("=" * 50)
    
    if dry_run:
        print("\n[预览模式] 不会实际删除数据，添加 --execute 参数执行删除")
    
    client = get_client()
    
    if args.check:
        check_mutations(client)
        return
    
    total = 0
    total += clean_kline_table(client, dry_run)
    total += clean_snapshot_table(client, dry_run)
    
    print("\n" + "=" * 50)
    if dry_run:
        print(f"[预览] 共 {total} 条非股票数据待删除")
        print("运行 'python clean_non_stock_data.py --execute' 执行删除")
    else:
        print(f"已提交删除 {total} 条数据")
        print("ClickHouse 将在后台异步执行删除")
        print("运行 'python clean_non_stock_data.py --check' 查看执行状态")
    
    check_mutations(client)
    client.disconnect()


if __name__ == "__main__":
    main()
