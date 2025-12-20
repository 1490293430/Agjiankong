#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""诊断采集问题"""

from common.db import get_clickhouse, get_kline_latest_date
from common.redis import get_json
from market_collector.cn import fetch_a_stock_kline

def diagnose_collection():
    """诊断采集问题"""
    print("=" * 60)
    print("采集问题诊断")
    print("=" * 60)
    
    # 1. 检查Redis中的股票总数
    a_stocks = get_json("market:a:spot") or []
    print(f"\n【1. Redis股票列表】")
    print(f"  A股总数: {len(a_stocks)} 只")
    
    # 2. 检查数据库中已有数据的股票
    ch = get_clickhouse()
    result = ch.execute("""
        SELECT code, COUNT(*) as cnt, MAX(date) as latest_date
        FROM kline 
        WHERE period = %(period)s
        GROUP BY code
        ORDER BY cnt DESC
    """, {'period': 'daily'})
    
    db_stocks = {r[0]: {'cnt': r[1], 'latest': r[2]} for r in result}
    print(f"\n【2. 数据库中已有数据的股票】")
    print(f"  已有数据股票数: {len(db_stocks)} 只")
    
    # 3. 检查按成交额排序后的股票列表
    sorted_stocks = sorted(a_stocks, key=lambda x: x.get("amount", 0) or 0, reverse=True)
    print(f"\n【3. 按成交额排序后的股票】")
    print(f"  前10只股票代码: {[s.get('code') for s in sorted_stocks[:10]]}")
    print(f"  前500只股票代码: {[s.get('code') for s in sorted_stocks[:500]]}")
    
    # 4. 检查前500只股票中，哪些在数据库中已有数据
    top500_codes = [s.get('code') for s in sorted_stocks[:500]]
    top500_in_db = [code for code in top500_codes if code in db_stocks]
    top500_not_in_db = [code for code in top500_codes if code not in db_stocks]
    
    print(f"\n【4. 前500只股票的数据状态】")
    print(f"  数据库中已有: {len(top500_in_db)} 只")
    print(f"  数据库中无数据: {len(top500_not_in_db)} 只")
    
    # 5. 测试一只没有数据的股票
    if top500_not_in_db:
        test_code = top500_not_in_db[0]
        print(f"\n【5. 测试采集一只没有数据的股票】")
        print(f"  测试股票代码: {test_code}")
        print(f"  开始测试采集...")
        
        try:
            kline_data = fetch_a_stock_kline(test_code, "daily", "", None, None, False, False)
            if kline_data:
                print(f"  ✅ 采集成功: 获取到 {len(kline_data)} 条数据")
                # 检查是否保存到数据库
                latest_date = get_kline_latest_date(test_code, "daily")
                if latest_date:
                    print(f"  ✅ 数据已保存到数据库: 最新日期 {latest_date}")
                else:
                    print(f"  ❌ 数据未保存到数据库")
            else:
                print(f"  ❌ 采集失败: 未获取到数据")
        except Exception as e:
            print(f"  ❌ 采集异常: {e}")
    
    # 6. 测试一只已有数据的股票（增量采集）
    if top500_in_db:
        test_code = top500_in_db[0]
        print(f"\n【6. 测试增量采集一只已有数据的股票】")
        print(f"  测试股票代码: {test_code}")
        print(f"  数据库中已有: {db_stocks[test_code]['cnt']} 条记录，最新日期: {db_stocks[test_code]['latest']}")
        print(f"  开始测试增量采集...")
        
        try:
            kline_data = fetch_a_stock_kline(test_code, "daily", "", None, None, False, False)
            if kline_data:
                print(f"  ✅ 采集成功: 获取到 {len(kline_data)} 条数据")
                # 检查最新日期是否有更新
                latest_date = get_kline_latest_date(test_code, "daily")
                if latest_date:
                    print(f"  数据库最新日期: {latest_date}")
                    if latest_date > db_stocks[test_code]['latest']:
                        print(f"  ✅ 数据已更新: {db_stocks[test_code]['latest']} -> {latest_date}")
                    else:
                        print(f"  ⚠️  数据未更新（可能已是最新）")
            else:
                print(f"  ❌ 采集失败: 未获取到数据")
        except Exception as e:
            print(f"  ❌ 采集异常: {e}")
    
    # 7. 检查采集逻辑问题
    print(f"\n【7. 采集逻辑分析】")
    print(f"  问题1: 当数据已是最新时，fetch_a_stock_kline 会直接返回数据库数据")
    print(f"  问题2: 如果返回了数据（即使是从数据库读取的），success_count 仍然会增加")
    print(f"  问题3: 但实际上并没有保存新数据，只是返回了已有数据")
    print(f"  问题4: 这导致采集任务显示'成功'，但实际上没有新增股票数据")
    
    print("\n" + "=" * 60)

if __name__ == "__main__":
    diagnose_collection()

