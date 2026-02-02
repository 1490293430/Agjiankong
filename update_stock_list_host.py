#!/usr/bin/env python3
"""
更新股票列表到数据库（宿主机版本）
在宿主机上运行，直接连接ClickHouse，无需其他依赖
"""
import requests
import json
from datetime import datetime

# ClickHouse连接配置（直接硬编码，避免依赖dotenv）
CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = 9000
CLICKHOUSE_DB = "stock"
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASSWORD = "changeme"

print("=" * 70)
print("开始更新股票列表到数据库")
print("=" * 70)

# ==================== A股分类函数 ====================
def _classify_a_stock(code: str, name: str) -> str:
    """A股证券分类（与K线采集使用相同逻辑）"""
    code = str(code or '').strip()
    name = str(name or '')
    name_upper = name.upper()
    
    # 名称优先判断
    if "ETF" in name_upper:
        return "etf"
    if "LOF" in name_upper or "基金" in name:
        return "fund"
    if "债" in name or "转债" in name:
        return "bond"
    if "指数" in name:
        return "index"
    
    # 代码规则判断
    if code.startswith(("600", "601", "603", "605")):
        return "stock"
    if code.startswith(("688", "689")):
        return "stock"
    if code.startswith(("000", "001")):
        index_keywords = ["上证", "深证", "沪深", "中证", "综指", "成指"]
        for kw in index_keywords:
            if kw in name:
                return "index"
        return "stock"
    if code.startswith(("002", "003")):
        return "stock"
    if code.startswith(("300", "301")):
        return "stock"
    if code.startswith(("510", "511", "512", "513", "515", "516", "517", "518", "560", "561", "562", "563")):
        return "etf"
    if code.startswith("159"):
        return "etf"
    if code.startswith("399"):
        return "index"
    if code.startswith(("501", "502", "505", "506")):
        return "fund"
    if code.startswith("16"):
        return "fund"
    if code.startswith(("11", "12")) and len(code) == 6:
        return "bond"
    if code.startswith(("4", "8", "920")):
        return "neeq"
    
    return "other"


def _classify_hk_stock(code: str, name: str) -> str:
    """港股证券分类（与K线采集使用相同逻辑）"""
    code = str(code or '').strip()
    name = str(name or '')
    name_upper = name.upper()
    
    if "指数" in name:
        return "index"
    if "ETF" in name_upper:
        return "etf"
    if "基金" in name or "REIT" in name_upper or "房托" in name:
        return "fund"
    if "债" in name:
        return "bond"
    if "牛" in name or "熊" in name or "权证" in name or "窝轮" in name:
        return "warrant"
    
    return "stock"


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
            count_before = len(items)
            count_after = 0
            
            for item in items:
                code = item.get("f12")
                name = item.get("f14")
                
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
            
            print(f"  ✓ {market_name}: {count_before}只（过滤后: {count_after}只）")
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
    
    for page in range(1, 30):
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
            
            count_before = len(items)
            count_after = 0
            
            for item in items:
                code = item.get("f12")
                name = item.get("f14")
                
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
            
            print(f"  ✓ 第{page}页: {count_before}只（过滤后: {count_after}只）")
            
            if len(items) < 100:
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
    from clickhouse_driver import Client
    
    client = Client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        database=CLICKHOUSE_DB,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD
    )
    
    # 保存A股
    if unique_a_stocks:
        # 准备批量插入数据
        rows = []
        for stock in unique_a_stocks:
            rows.append((
                stock["code"],
                stock["name"],
                stock["market"],
                stock.get("sec_type", "stock"),
                datetime.now()
            ))
        
        # 批量插入
        client.execute(
            "INSERT INTO stock_info (code, name, market, sec_type, update_time) VALUES",
            rows
        )
        print(f"  ✓ A股保存成功: {len(unique_a_stocks)}只")
    
    # 保存港股
    if unique_hk_stocks:
        rows = []
        for stock in unique_hk_stocks:
            rows.append((
                stock["code"],
                stock["name"],
                stock["market"],
                stock.get("sec_type", "stock"),
                datetime.now()
            ))
        
        client.execute(
            "INSERT INTO stock_info (code, name, market, sec_type, update_time) VALUES",
            rows
        )
        print(f"  ✓ 港股保存成功: {len(unique_hk_stocks)}只")
        
except Exception as e:
    print(f"  ✗ 保存失败: {e}")
    import traceback
    traceback.print_exc()

# ==================== 验证 ====================
print("\n[4/4] 验证数据库...")

try:
    from clickhouse_driver import Client
    
    client = Client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        database=CLICKHOUSE_DB,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD
    )
    
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
