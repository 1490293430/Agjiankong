#!/usr/bin/env python3
"""
更新股票列表到数据库
在Docker容器内运行，获取A股和港股的完整列表并保存到ClickHouse
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
    # 加载环境变量
    from dotenv import load_dotenv
    load_dotenv('/opt/Agjiankong/.env')

import requests
import json
from datetime import datetime

# 导入东方财富的分类函数（与K线采集使用相同的过滤逻辑）
from market_collector.eastmoney_source import _classify_a_stock, _classify_hk_stock

print("=" * 70)
print("开始更新股票列表到数据库")
print("=" * 70)

# ==================== 获取A股列表 ====================
print("\n[1/4] 从东方财富获取A股列表...")

markets = [
    ("沪市主板", "m:1+t:2"),
    ("科创板", "m:1+t:23"),
    ("深市主板", "m:0+t:6"),
    ("中小板", "m:0+t:13"),
    ("创业板", "m:0+t:80"),
]

all_a_stocks = []

for market_name, fs in markets:
    try:
        url = "http://push2.eastmoney.com/api/qt/clist/get"
        params = {
            "pn": 1,
            "pz": 5000,
            "po": 1,
            "np": 1,
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fltt": 2,
            "invt": 2,
            "fid": "f3",
            "fs": fs,
            "fields": "f12,f14,f2,f3,f20,f21"
        }
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": "http://quote.eastmoney.com/"
        }
        
        response = requests.get(url, params=params, headers=headers, timeout=30)
        data = response.json()
        
        if data.get("rc") == 0:
            items = data.get("data", {}).get("diff", [])
            for item in items:
                code = item.get("f12")
                name = item.get("f14")
                
                if not code or not name:
                    continue
                
                # 使用与K线采集相同的分类函数
                sec_type = _classify_a_stock(code, name)
                
                # 只保留股票类型，过滤掉ETF、基金、指数、债券等
                if sec_type != "stock":
                    continue
                
                all_a_stocks.append({
                    "code": code,
                    "name": name,
                    "market": "A",
                    "sec_type": "stock"
                })
            print(f"  ✓ {market_name}: {len(items)}只（过滤后: {len([s for s in all_a_stocks if s['code'] in [i.get('f12') for i in items]])}只）")
        else:
            print(f"  ✗ {market_name}: 失败")
    except Exception as e:
        print(f"  ✗ {market_name}: 错误 - {e}")

# 去重
seen = set()
unique_a_stocks = []
for stock in all_a_stocks:
    if stock["code"] not in seen:
        seen.add(stock["code"])
        unique_a_stocks.append(stock)

print(f"\n  总计: {len(unique_a_stocks)} 只A股（去重后）")

# ==================== 获取港股列表 ====================
print("\n[2/4] 从东方财富获取港股列表...")

all_hk_stocks = []

try:
    url = "http://push2.eastmoney.com/api/qt/clist/get"
    
    # 港股需要分页获取
    for page in range(1, 30):  # 最多30页
        params = {
            "pn": page,
            "pz": 1000,
            "po": 1,
            "np": 1,
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fltt": 2,
            "invt": 2,
            "fid": "f3",
            "fs": "m:128+t:3,m:128+t:4,m:128+t:1,m:128+t:2",
            "fields": "f12,f14,f2,f3,f20,f21"
        }
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": "http://quote.eastmoney.com/"
        }
        
        response = requests.get(url, params=params, headers=headers, timeout=30)
        data = response.json()
        
        if data.get("rc") == 0:
            items = data.get("data", {}).get("diff", [])
            if not items:
                break
            
            for item in items:
                code = item.get("f12")
                name = item.get("f14")
                
                if not code or not name:
                    continue
                
                # 使用与K线采集相同的分类函数
                sec_type = _classify_hk_stock(code, name)
                
                # 只保留股票类型，过滤掉ETF、基金、指数、债券等
                if sec_type != "stock":
                    continue
                
                all_hk_stocks.append({
                    "code": code,
                    "name": name,
                    "market": "HK",
                    "sec_type": "stock"
                })
            
            print(f"  ✓ 第{page}页: {len(items)}只")
            
            if len(items) < 100:  # 如果返回少于100条，说明已经是最后一页
                break
        else:
            print(f"  ✗ 第{page}页: 失败")
            break
            
except Exception as e:
    print(f"  ✗ 港股获取失败: {e}")

# 去重
seen_hk = set()
unique_hk_stocks = []
for stock in all_hk_stocks:
    if stock["code"] not in seen_hk:
        seen_hk.add(stock["code"])
        unique_hk_stocks.append(stock)

print(f"\n  总计: {len(unique_hk_stocks)} 只港股（去重后）")

# ==================== 保存到数据库 ====================
print("\n[3/4] 保存到ClickHouse数据库...")

try:
    from common.db import save_stock_info_batch
    
    if unique_a_stocks:
        save_stock_info_batch(unique_a_stocks, "A")
        print(f"  ✓ A股保存成功: {len(unique_a_stocks)}只")
    
    if unique_hk_stocks:
        save_stock_info_batch(unique_hk_stocks, "HK")
        print(f"  ✓ 港股保存成功: {len(unique_hk_stocks)}只")
        
except Exception as e:
    print(f"  ✗ 保存失败: {e}")
    import traceback
    traceback.print_exc()

# ==================== 验证 ====================
print("\n[4/4] 验证数据库...")

try:
    from common.db import get_clickhouse
    client = get_clickhouse()
    
    # 查询A股数量
    result_a = client.execute("SELECT COUNT(DISTINCT code) FROM stock_info WHERE market = 'A'")
    a_count = result_a[0][0] if result_a else 0
    
    # 查询港股数量
    result_hk = client.execute("SELECT COUNT(DISTINCT code) FROM stock_info WHERE market = 'HK'")
    hk_count = result_hk[0][0] if result_hk else 0
    
    print(f"  ✓ 数据库中A股: {a_count}只")
    print(f"  ✓ 数据库中港股: {hk_count}只")
    
except Exception as e:
    print(f"  ✗ 验证失败: {e}")

print("\n" + "=" * 70)
print("更新完成！")
print("=" * 70)
