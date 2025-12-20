#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""检查采集数据是否有增加"""

from common.db import get_clickhouse, get_stock_list_from_db
from common.redis import get_json
from market.service.ws import kline_collect_progress

def check_data_increase():
    """检查采集数据是否有增加"""
    print("=" * 60)
    print("K线数据采集状态检查")
    print("=" * 60)
    
    # 检查ClickHouse中的数据
    ch = get_clickhouse()
    
    # 基本统计
    result = ch.execute("""
        SELECT 
            COUNT(DISTINCT code) as stocks,
            COUNT(*) as records,
            MIN(date) as earliest,
            MAX(date) as latest
        FROM kline 
        WHERE period = %(period)s
    """, {'period': 'daily'})
    
    stats = result[0]
    print(f"\n【ClickHouse kline表统计】")
    print(f"  已采集股票数: {stats[0]} 只")
    print(f"  总记录数: {stats[1]:,} 条")
    print(f"  数据日期范围: {stats[2]} 至 {stats[3]}")
    print(f"  平均每只股票: {stats[1] // stats[0] if stats[0] > 0 else 0} 条记录")
    
    # 检查最新数据
    result2 = ch.execute("""
        SELECT 
            COUNT(DISTINCT code) as stocks_with_latest
        FROM kline 
        WHERE period = %(period)s 
        AND date >= %(today)s
    """, {'period': 'daily', 'today': '2025-12-19'})
    
    latest_stocks = result2[0][0] if result2 else 0
    print(f"\n【最新数据统计】")
    print(f"  有最新数据(>=2025-12-19)的股票: {latest_stocks} 只")
    
    # 检查Redis中的股票总数
    a_stocks = get_json("market:a:spot") or []
    print(f"\n【Redis行情数据】")
    print(f"  A股总数: {len(a_stocks)} 只")
    
    # 对比
    coverage = (stats[0] / len(a_stocks) * 100) if a_stocks else 0
    print(f"\n【采集覆盖率】")
    print(f"  已采集: {stats[0]} / {len(a_stocks)} = {coverage:.1f}%")
    
    # 检查是否有正在运行的任务
    running_tasks = [k for k, v in kline_collect_progress.items() if v.get('status') == 'running']
    completed_tasks = [k for k, v in kline_collect_progress.items() if v.get('status') == 'completed']
    
    print(f"\n【采集任务状态】")
    print(f"  正在运行的任务: {len(running_tasks)} 个")
    print(f"  已完成的任务: {len(completed_tasks)} 个")
    
    if running_tasks:
        print(f"\n  运行中的任务:")
        for task_id in running_tasks:
            task = kline_collect_progress[task_id]
            print(f"    - {task_id[:8]}... 进度: {task.get('progress', 0)}% ({task.get('current', 0)}/{task.get('total', 0)})")
            print(f"      成功: {task.get('success', 0)}, 失败: {task.get('failed', 0)}")
    
    if completed_tasks:
        print(f"\n  最近完成的任务:")
        for task_id in completed_tasks[-3:]:  # 显示最近3个
            task = kline_collect_progress[task_id]
            print(f"    - {task_id[:8]}... {task.get('message', '')}")
            print(f"      开始时间: {task.get('start_time', '')}")
            print(f"      结束时间: {task.get('end_time', '')}")
    
    # 检查最近更新的股票
    result3 = ch.execute("""
        SELECT code, COUNT(*) as cnt, MAX(date) as latest_date 
        FROM kline 
        WHERE period = %(period)s 
        GROUP BY code 
        ORDER BY latest_date DESC 
        LIMIT 10
    """, {'period': 'daily'})
    
    print(f"\n【最近更新的10只股票】")
    for r in result3:
        print(f"  {r[0]}: {r[1]}条记录, 最新日期: {r[2]}")
    
    print("\n" + "=" * 60)
    
    # 对比之前的数据（之前是501只股票，350,482条记录）
    previous_stocks = 501
    previous_records = 350482
    
    stocks_increase = stats[0] - previous_stocks
    records_increase = stats[1] - previous_records
    
    print(f"\n【数据变化对比】")
    print(f"  股票数量变化: {previous_stocks} -> {stats[0]} ({'+' if stocks_increase >= 0 else ''}{stocks_increase})")
    print(f"  记录数量变化: {previous_records:,} -> {stats[1]:,} ({'+' if records_increase >= 0 else ''}{records_increase:,})")
    
    if stocks_increase > 0 or records_increase > 0:
        print(f"\n✅ 数据有增加！")
        if stocks_increase > 0:
            print(f"   新增股票: {stocks_increase} 只")
        if records_increase > 0:
            print(f"   新增记录: {records_increase:,} 条")
    else:
        print(f"\n⚠️  数据没有增加")

if __name__ == "__main__":
    check_data_increase()

