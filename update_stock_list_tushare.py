#!/usr/bin/env python3
"""
使用Tushare数据源更新股票列表到数据库
在Docker容器内运行，使用系统已有的Tushare配置
"""
import sys
import os

# 检测运行环境
if os.path.exists('/app'):
    # Docker容器内
    sys.path.insert(0, '/app')
    os.chdir('/app')
else:
    # 宿主机
    sys.path.insert(0, '/opt/Agjiankong/backend')
    os.chdir('/opt/Agjiankong/backend')

from datetime import datetime
from market_collector.tushare_source import fetch_stock_list_tushare, get_tushare_api
from market_collector.eastmoney_source import _classify_a_stock, _classify_hk_stock

print("=" * 70)
print("开始更新股票列表到数据库（使用Tushare）")
print("=" * 70)

# ==================== 检查Tushare配置 ====================
print("\n[0/4] 检查Tushare配置...")
api = get_tushare_api()
if not api:
    print("  ✗ Tushare未配置或Token无效")
    print("  请在.env文件中配置TUSHARE_TOKEN")
    sys.exit(1)
print("  ✓ Tushare配置正常")

# ==================== 获取A股列表 ====================
print("\n[1/4] 使用Tushare获取A股列表...")

all_a_stocks = []

try:
    # 使用Tushare获取A股列表
    tushare_stocks = fetch_stock_list_tushare()
    
    if tushare_stocks:
        print(f"  获取到 {len(tushare_stocks)} 条数据")
        
        # 遍历数据并过滤
        count_before = len(tushare_stocks)
        count_after = 0
        
        for stock in tushare_stocks:
            code = str(stock.get('code', ''))
            name = str(stock.get('name', ''))
            
            if not code or not name:
                continue
            
            # 使用与K线采集相同的分类函数
            sec_type = _classify_a_stock(code, name)
            
            # 只保留股票类型
            if sec_type != "stock":
                continue
            
            all_a_stocks.append({
                "code": code,
                "name": name,
                "market": "A",
                "sec_type": "stock"
            })
            count_after += 1
        
        print(f"  ✓ A股: {count_before}条（过滤后: {count_after}只股票）")
    else:
        print("  ✗ Tushare返回数据为空")
    
except Exception as e:
    print(f"  ✗ A股获取失败: {e}")
    import traceback
    traceback.print_exc()

print(f"\n  总计: {len(all_a_stocks)} 只A股")

# ==================== 获取港股列表 ====================
print("\n[2/4] 使用Tushare获取港股列表...")

all_hk_stocks = []

try:
    # Tushare获取港股列表
    print("  正在获取港股数据...")
    
    # 获取港股列表
    df = api.hk_basic(list_status='L', fields='ts_code,name')
    
    if df is not None and len(df) > 0:
        print(f"  获取到 {len(df)} 条数据")
        
        count_before = len(df)
        count_after = 0
        
        for _, row in df.iterrows():
            ts_code = str(row.get('ts_code', ''))
            name = str(row.get('name', ''))
            
            # 提取代码（去掉.HK后缀）
            code = ts_code.split('.')[0] if '.' in ts_code else ts_code
            
            if not code or not name:
                continue
            
            # 使用与K线采集相同的分类函数
            sec_type = _classify_hk_stock(code, name)
            
            # 只保留股票类型
            if sec_type != "stock":
                continue
            
            all_hk_stocks.append({
                "code": code,
                "name": name,
                "market": "HK",
                "sec_type": "stock"
            })
            count_after += 1
        
        print(f"  ✓ 港股: {count_before}条（过滤后: {count_after}只股票）")
    else:
        print("  ✗ Tushare返回港股数据为空")
    
except Exception as e:
    print(f"  ✗ 港股获取失败: {e}")
    import traceback
    traceback.print_exc()

print(f"\n  总计: {len(all_hk_stocks)} 只港股")

# ==================== 保存到数据库 ====================
print("\n[3/4] 保存到ClickHouse数据库...")

try:
    from common.db import save_stock_info_batch
    
    if all_a_stocks:
        count = save_stock_info_batch(all_a_stocks, "A")
        print(f"  ✓ A股保存成功: {count}只")
    else:
        print("  - A股无数据，跳过保存")
    
    if all_hk_stocks:
        count = save_stock_info_batch(all_hk_stocks, "HK")
        print(f"  ✓ 港股保存成功: {count}只")
    else:
        print("  - 港股无数据，跳过保存")
        
except Exception as e:
    print(f"  ✗ 保存失败: {e}")
    import traceback
    traceback.print_exc()

# ==================== 验证 ====================
print("\n[4/4] 验证数据库...")

try:
    from common.db import get_clickhouse
    client = get_clickhouse()
    
    result_a = client.execute("SELECT COUNT(DISTINCT code) FROM stock_info WHERE market = 'A'")
    a_count = result_a[0][0] if result_a else 0
    
    result_hk = client.execute("SELECT COUNT(DISTINCT code) FROM stock_info WHERE market = 'HK'")
    hk_count = result_hk[0][0] if result_hk else 0
    
    print(f"  ✓ 数据库中A股: {a_count}只")
    print(f"  ✓ 数据库中港股: {hk_count}只")
    
except Exception as e:
    print(f"  ✗ 验证失败: {e}")

print("\n" + "=" * 70)
print("更新完成！")
print("=" * 70)
