#!/usr/bin/env python3
"""
诊断异常数据来源

分析异常数据的特征，找出可能的原因
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.db import get_clickhouse
from common.logger import get_logger

logger = get_logger(__name__)


def diagnose_abnormal_data():
    """诊断异常数据"""
    client = get_clickhouse()
    
    print("=" * 70)
    print("异常数据诊断")
    print("=" * 70)
    
    # 1. 查看价格异常高的数据详情
    print("\n=== 价格异常高的数据（A股>1000元）===")
    result = client.execute("""
        SELECT code, date, period, open, high, low, close, volume, amount
        FROM kline FINAL
        WHERE period = 'daily'
          AND length(code) = 6
          AND (code LIKE '0%' OR code LIKE '3%' OR code LIKE '6%')
          AND close > 1000
        ORDER BY date DESC, code
    """)
    if result:
        print(f"发现 {len(result)} 条:")
        for row in result[:30]:
            code, date, period, open_p, high, low, close, volume, amount = row
            print(f"  {code} {date}: O={open_p:.2f} H={high:.2f} L={low:.2f} C={close:.2f}")
        if len(result) > 30:
            print(f"  ... 还有 {len(result) - 30} 条")
    else:
        print("未发现")

    
    # 2. 分析异常数据的日期分布
    print("\n=== 异常数据的日期分布 ===")
    result = client.execute("""
        SELECT date, count() as cnt
        FROM kline FINAL
        WHERE period = 'daily'
          AND length(code) = 6
          AND (code LIKE '0%' OR code LIKE '3%' OR code LIKE '6%')
          AND close > 1000
        GROUP BY date
        ORDER BY date DESC
    """)
    if result:
        for row in result[:30]:
            print(f"  {row[0]}: {row[1]}条异常数据")
    
    # 3. 分析异常数据的股票代码分布
    print("\n=== 异常数据的股票代码分布 ===")
    result = client.execute("""
        SELECT code, count() as cnt, min(date) as first_date, max(date) as last_date,
               min(close) as min_close, max(close) as max_close
        FROM kline FINAL
        WHERE period = 'daily'
          AND length(code) = 6
          AND (code LIKE '0%' OR code LIKE '3%' OR code LIKE '6%')
          AND close > 1000
        GROUP BY code
        ORDER BY cnt DESC
    """)
    if result:
        for row in result[:30]:
            print(f"  {row[0]}: {row[1]}条, 日期 {row[2]}~{row[3]}, 价格 {row[4]:.2f}~{row[5]:.2f}")
    
    # 4. 检查价格突变的具体情况
    print("\n=== 价格突变详情（单日变化>50%）===")
    result = client.execute("""
        WITH ranked AS (
            SELECT 
                code, date, period, close, volume,
                lagInFrame(close) OVER (PARTITION BY code, period ORDER BY date) as prev_close,
                lagInFrame(date) OVER (PARTITION BY code, period ORDER BY date) as prev_date
            FROM kline FINAL
            WHERE period = 'daily'
        )
        SELECT code, prev_date, date, prev_close, close, 
               (close - prev_close) / prev_close * 100 as change_pct,
               volume
        FROM ranked
        WHERE prev_close > 0 
          AND abs(close - prev_close) / prev_close > 0.5
        ORDER BY abs(close - prev_close) / prev_close DESC
    """)
    if result:
        print(f"发现 {len(result)} 条价格突变:")
        for row in result[:30]:
            code, prev_date, date, prev_close, close, change_pct, volume = row
            print(f"  {code}: {prev_date} {prev_close:.2f} -> {date} {close:.2f} ({change_pct:+.1f}%)")
    else:
        print("未发现价格突变")
    
    # 5. 检查是否有港股数据混入A股
    print("\n=== 检查是否有港股数据混入A股 ===")
    result = client.execute("""
        SELECT code, date, close, volume
        FROM kline FINAL
        WHERE period = 'daily'
          AND length(code) = 6
          AND code LIKE '0%'
          AND close > 500
        ORDER BY close DESC
    """)
    if result:
        print(f"发现 {len(result)} 条可疑数据（0开头代码但价格>500）:")
        for row in result[:20]:
            print(f"  {row[0]} {row[1]}: 收盘价={row[2]:.2f}, 成交量={row[3]:.0f}")
    
    print("\n" + "=" * 70)
    print("诊断完成")
    print("=" * 70)
    
    # 给出可能的原因分析
    print("\n=== 可能的原因分析 ===")
    print("""
1. 数据源API返回错误数据
   - 某些API在特定时间可能返回错误的价格数据

2. 并发采集导致数据混乱
   - 多个采集任务同时运行，可能导致数据串行

3. 港股/指数数据混入A股
   - 某些代码可能同时存在于A股和港股

建议操作：
1. 运行清理脚本删除异常数据：
   docker-compose exec api python scripts/clean_abnormal_kline.py --execute --wait

2. 使用批量采集补充删除的数据

3. 数据校验已在 save_kline_data 中添加：
   - A股价格不超过2000元
   - 价格必须为正数
   - high >= low
""")


if __name__ == "__main__":
    diagnose_abnormal_data()
