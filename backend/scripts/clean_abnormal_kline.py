#!/usr/bin/env python3
"""
清理K线异常数据脚本

检测并清理以下异常情况：
1. 价格突变（单日涨跌超过阈值，如50%）
2. 价格为0或负数
3. 成交量异常（突然放大100倍以上）
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.db import get_clickhouse
from common.logger import get_logger

logger = get_logger(__name__)


def find_abnormal_kline(dry_run=True, price_change_threshold=0.5, volume_change_threshold=100):
    """查找异常K线数据
    
    Args:
        dry_run: 是否只预览不删除
        price_change_threshold: 价格变化阈值（默认50%）
        volume_change_threshold: 成交量变化阈值（默认100倍）
    
    Returns:
        异常数据列表
    """
    client = get_clickhouse()
    
    print("=" * 60)
    print("检测K线异常数据")
    print("=" * 60)
    print(f"价格变化阈值: {price_change_threshold * 100}%")
    print(f"成交量变化阈值: {volume_change_threshold}倍")
    print()
    
    abnormal_records = []
    
    # 1. 检测价格为0或负数的数据
    print("=== 检测价格异常（<=0）===")
    result = client.execute("""
        SELECT code, date, period, open, high, low, close, volume
        FROM kline FINAL
        WHERE close <= 0 OR open <= 0 OR high <= 0 OR low <= 0
        ORDER BY code, date
        LIMIT 100
    """)
    if result:
        print(f"发现 {len(result)} 条价格<=0的数据:")
        for row in result[:10]:
            print(f"  {row[0]} {row[1]} {row[2]}: O={row[3]} H={row[4]} L={row[5]} C={row[6]}")
            abnormal_records.append({
                'code': row[0], 'date': row[1], 'period': row[2],
                'reason': '价格<=0'
            })
    else:
        print("未发现价格<=0的数据")
    print()
    
    # 2. 检测单日价格突变（与前一天相比变化超过阈值）
    print(f"=== 检测价格突变（单日变化>{price_change_threshold*100}%）===")
    # 使用窗口函数计算价格变化
    result = client.execute(f"""
        WITH ranked AS (
            SELECT 
                code, date, period, close,
                lagInFrame(close) OVER (PARTITION BY code, period ORDER BY date) as prev_close
            FROM kline FINAL
            WHERE period = 'daily'
        )
        SELECT code, date, period, close, prev_close,
               abs(close - prev_close) / prev_close as change_ratio
        FROM ranked
        WHERE prev_close > 0 
          AND abs(close - prev_close) / prev_close > {price_change_threshold}
        ORDER BY change_ratio DESC
        LIMIT 100
    """)
    if result:
        print(f"发现 {len(result)} 条价格突变数据:")
        for row in result[:20]:
            change_pct = row[5] * 100
            print(f"  {row[0]} {row[1]}: {row[4]:.2f} -> {row[3]:.2f} (变化 {change_pct:.1f}%)")
            abnormal_records.append({
                'code': row[0], 'date': row[1], 'period': row[2],
                'reason': f'价格突变{change_pct:.1f}%'
            })
    else:
        print("未发现价格突变数据")
    print()
    
    # 3. 检测成交量异常（与前5日均量相比放大超过阈值）
    print(f"=== 检测成交量异常（>{volume_change_threshold}倍）===")
    result = client.execute(f"""
        WITH ranked AS (
            SELECT 
                code, date, period, volume, close,
                avg(volume) OVER (
                    PARTITION BY code, period 
                    ORDER BY date 
                    ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING
                ) as avg_volume_5
            FROM kline FINAL
            WHERE period = 'daily'
        )
        SELECT code, date, period, volume, avg_volume_5, close,
               volume / avg_volume_5 as volume_ratio
        FROM ranked
        WHERE avg_volume_5 > 1000  -- 排除成交量太小的
          AND volume / avg_volume_5 > {volume_change_threshold}
        ORDER BY volume_ratio DESC
        LIMIT 100
    """)
    if result:
        print(f"发现 {len(result)} 条成交量异常数据:")
        for row in result[:20]:
            ratio = row[6]
            print(f"  {row[0]} {row[1]}: 成交量={row[3]:.0f}, 5日均量={row[4]:.0f}, 放大{ratio:.1f}倍, 收盘价={row[5]:.2f}")
            abnormal_records.append({
                'code': row[0], 'date': row[1], 'period': row[2],
                'reason': f'成交量放大{ratio:.1f}倍'
            })
    else:
        print("未发现成交量异常数据")
    print()
    
    # 4. 检测A股价格明显不合理的数据（如股价>1000元，A股很少有这么高的）
    print("=== 检测A股价格异常（>1000元）===")
    result = client.execute("""
        SELECT code, date, period, close, volume
        FROM kline FINAL
        WHERE period = 'daily'
          AND length(code) = 6
          AND (code LIKE '0%' OR code LIKE '3%' OR code LIKE '6%')
          AND close > 1000
        ORDER BY close DESC
        LIMIT 100
    """)
    if result:
        print(f"发现 {len(result)} 条A股价格>1000元的数据:")
        for row in result[:20]:
            print(f"  {row[0]} {row[1]}: 收盘价={row[3]:.2f}")
            abnormal_records.append({
                'code': row[0], 'date': row[1], 'period': row[2],
                'reason': f'A股价格异常高={row[3]:.2f}'
            })
    else:
        print("未发现A股价格>1000元的数据")
    print()
    
    return abnormal_records


def clean_abnormal_kline(abnormal_records, dry_run=True):
    """清理异常K线数据
    
    Args:
        abnormal_records: 异常数据列表
        dry_run: 是否只预览不删除
    """
    if not abnormal_records:
        print("没有需要清理的异常数据")
        return
    
    # 去重
    unique_records = {}
    for r in abnormal_records:
        key = (r['code'], str(r['date']), r['period'])
        if key not in unique_records:
            unique_records[key] = r
    
    print("=" * 60)
    print(f"共发现 {len(unique_records)} 条异常数据需要清理")
    print("=" * 60)
    
    if dry_run:
        print("[预览模式] 不会实际删除数据，添加 --execute 参数执行删除")
        print()
        print("异常数据列表:")
        for key, r in list(unique_records.items())[:50]:
            print(f"  {r['code']} {r['date']} {r['period']}: {r['reason']}")
        if len(unique_records) > 50:
            print(f"  ... 还有 {len(unique_records) - 50} 条")
        return
    
    client = get_clickhouse()
    
    # 按code分组删除，避免一次删除太多
    codes_to_clean = {}
    for key, r in unique_records.items():
        code = r['code']
        if code not in codes_to_clean:
            codes_to_clean[code] = []
        codes_to_clean[code].append(r)
    
    deleted_count = 0
    for code, records in codes_to_clean.items():
        dates = [f"'{r['date']}'" for r in records]
        dates_str = ','.join(dates)
        
        # 删除K线数据
        try:
            client.execute(f"""
                ALTER TABLE kline DELETE 
                WHERE code = '{code}' AND date IN ({dates_str})
            """)
            print(f"已提交删除 {code} 的 {len(records)} 条K线数据")
            deleted_count += len(records)
        except Exception as e:
            print(f"删除 {code} K线数据失败: {e}")
        
        # 删除对应的指标数据
        try:
            client.execute(f"""
                ALTER TABLE indicators DELETE 
                WHERE code = '{code}' AND date IN ({dates_str})
            """)
            print(f"已提交删除 {code} 的指标数据")
        except Exception as e:
            print(f"删除 {code} 指标数据失败: {e}")
    
    print()
    print(f"已提交删除 {deleted_count} 条异常数据")
    print("ClickHouse 将在后台异步执行删除")
    print("运行 'python clean_abnormal_kline.py --check' 查看执行状态")


def check_mutations():
    """检查mutation执行状态"""
    client = get_clickhouse()
    result = client.execute("""
        SELECT table, mutation_id, parts_to_do, is_done, create_time
        FROM system.mutations
        WHERE is_done = 0
        ORDER BY create_time DESC
        LIMIT 20
    """)
    
    if result:
        print("=== 正在执行的 mutations ===")
        for row in result:
            print(f"表: {row[0]}, ID: {row[1]}, 剩余: {row[2]} 个分区")
    else:
        print("没有正在执行的 mutations")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='清理K线异常数据')
    parser.add_argument('--execute', action='store_true', help='执行删除（默认只预览）')
    parser.add_argument('--check', action='store_true', help='检查mutation执行状态')
    parser.add_argument('--price-threshold', type=float, default=0.5, help='价格变化阈值（默认0.5即50%%）')
    parser.add_argument('--volume-threshold', type=float, default=100, help='成交量变化阈值（默认100倍）')
    
    args = parser.parse_args()
    
    if args.check:
        check_mutations()
    else:
        dry_run = not args.execute
        abnormal = find_abnormal_kline(
            dry_run=dry_run,
            price_change_threshold=args.price_threshold,
            volume_change_threshold=args.volume_threshold
        )
        clean_abnormal_kline(abnormal, dry_run=dry_run)
        
        if not dry_run:
            print()
            check_mutations()
